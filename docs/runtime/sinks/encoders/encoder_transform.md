# Template Encoder

The template encoder is a top-level sink encoder, alongside `json`, `csv`, and `protobuf`. It is
not a transform attribute of the JSON encoder.

This document describes the implemented template encoder introduced for issue
[#382](https://github.com/emqx/VeloFlux/issues/382). It replaces the previous
`json + transform` API.

## Design Decision

The template encoder renders one input row at a time and owns the framing between rendered rows.
Its overall contract is still:

```text
Collection -> bytes
```

Its internal rendering unit is:

```text
Tuple -> one rendered item
```

For example, with a row template equivalent to:

```text
{"a":{{ json(.b) }}{{ "}" }}
```

two input rows are rendered independently:

```text
{"b":1} -> {"a":1}
{"b":2} -> {"a":2}
```

The encoder then inserts the configured framing between those rendered items and emits one payload
for the current sink delivery.

The template never receives the whole collection and does not need collection loop syntax. This
keeps template semantics independent of sink batching and makes the encoder streamable.

## Configuration API

The configuration is:

```json
{
  "encoder": {
    "type": "template",
    "props": {
      "template": "{\"a\":{{ json(.b) }}{{ \"}\" }}",
      "begin": "[",
      "delimiter": ",",
      "end": "]"
    }
  }
}
```

The fields are:

| Field | Required | Meaning |
|---|---:|---|
| `props.template` | yes | The row-level template. One input row produces one rendered item. |
| `props.begin` | no | Bytes emitted at `begin_delivery`. Defaults to empty. |
| `props.delimiter` | no | Bytes emitted between two rendered row items. Defaults to empty. |
| `props.end` | no | Bytes emitted at `finish_delivery`. Defaults to empty. |

`begin`, `delimiter`, and `end` are static framing strings. They are not rendered as templates and
are not allowed to inspect row values.

For JSON output, the configuration can produce an array. Because the current Upon
expression delimiter ends with `}}`, the JSON closing brace is rendered by a
separate expression:

```json
{
  "type": "template",
  "props": {
    "template": "{\"a\":{{ json(.b) }}{{ \"}\" }}",
    "begin": "[",
    "delimiter": ",",
    "end": "]"
  }
}
```

For line-oriented text:

```json
{
  "type": "template",
  "props": {
    "template": "value={{ .b }}",
    "delimiter": "\n"
  }
}
```

The template encoder does not have a JSON-specific `format=array|ndjson` option. The template
encoder is format-neutral; the user expresses the required framing through `begin`, `delimiter`,
and `end`.

## Template Language Profile

The template encoder currently uses the Upon engine. The row context is the dense
final output row, and fields are referenced from the row root, for example `.b`.
The encoder also registers `json(value)`, which renders a value as a JSON literal.

The implemented Upon profile defines:

- row fields referenced from the root context, such as `.b`;
- `json(value)` for rendering a JSON literal;
- render errors returned as encoder errors.

The profile does not need collection loops because the template input is one row.
Invalid expressions and render failures are returned by the encoder as encoding
errors. The `json()` helper is useful when a value must be embedded in a JSON
template; it does not make the encoder parse or validate the final rendered item.

The template context is the dense final output row resolved through planner `OutputLayout`. The
context must not be built by guessing source names or tuple indexes. A field is referenced from the
row root, for example `.b`; there is no `.row` wrapper. The implementation uses the existing Upon
engine, and its `json(...)` helper renders a value as a JSON literal.

## Delivery and Framing Semantics

The standard encoder lifecycle is:

```text
begin_delivery()
  -> emit begin

append(collection)
  -> for each tuple:
       render one row item
       emit delimiter if this is not the first item
       emit rendered item

finish_delivery()
  -> emit end
```

The encoder keeps the first-item state across multiple `append()` calls. Therefore a batch can span
multiple input collections while still producing one payload:

```text
begin_delivery()       -> "["
append(collection_1)   -> {"a":1}
append(collection_2)   -> ,{"a":2}
finish_delivery()      -> "]"
```

The connector receives one completed delivery payload and can send it as one MQTT publish, HTTP
request, or equivalent sink operation.

For an empty delivery, the encoder emits `begin + end`. With `begin="["` and `end="]"`, the result
is `[]`. If all three framing fields are empty, an empty delivery emits zero bytes.

The template encoder does not parse rendered items as JSON. If a user configures JSON-looking
framing, producing valid JSON remains the responsibility of the template and its `json` helper.

## Batching and Physical Plans

Batching controls the number of rows in one sink delivery; it does not change the row-level template
contract.

Conceptual physical shape:

```text
PhysicalProject
  -> PhysicalBatch
  -> PhysicalSinkEncoder(encoder=template)
  -> PhysicalSinkConnector
```

The physical optimizer may fuse the batch and encoder into:

```text
PhysicalProject
  -> PhysicalIncSinkEncoder(encoder=template, batch_count=10)
  -> PhysicalSinkConnector
```

The fused encoder must preserve the same lifecycle and framing semantics. It may receive multiple
collections through `append()`, but it emits one completed payload when the delivery finishes.

Explain should report the top-level `encoder=template` kind.

## Output Layout and Row Diff

The planner attaches the final `OutputLayout` to the template encoder factory. The template encoder
resolves every input tuple through this layout before rendering:

```text
Tuple + OutputLayout -> template row context -> rendered item
```

This preserves final SQL-visible names, aliases, computed columns, fixed message slots, and fixed
affiliate slots.

On row-diff branches, the template receives the dense final output row. The output mask is not
automatically converted into template framing or field omission. If a future template profile needs
mask-aware rendering, it must expose that metadata explicitly without changing the row-level
template contract.

## Factory and Runtime Responsibilities

The encoder factory owns shared configuration:

- encoder identifier `template`;
- compiled or compilable template;
- static framing strings;
- property registry and template profile configuration;
- planner-provided `OutputLayout`.

The processor-local runtime owns delivery state:

- whether the current delivery has emitted its first row;
- the current property snapshot, fixed at delivery start;
- delivery framing state;
- abort/reset state.

The runtime implements the existing `SinkEncoder` lifecycle. It can return chunks from
`begin_delivery`, `append`, and `finish_delivery`; individual chunks are not independent connector
messages. The completed delivery remains the sink connector's message/request boundary.

## Why This Design

This design keeps the responsibilities separate:

- the template renders one row;
- the encoder inserts static framing and produces bytes;
- batching chooses how many rows belong to one delivery;
- the connector sends one completed delivery;
- compression and encryption operate after encoding on the delivery bytes.

It also avoids forcing JSON semantics onto a generic template encoder while preserving the streaming
behavior of the existing JSON encoder.

## Non-Goals

The template encoder does not provide:

- a whole-collection template context;
- collection loop syntax as part of the encoder contract;
- a `Collection -> Collection` plan stage;
- implicit JSON parsing or validation of rendered output;
- multiple connector messages for one completed encoder delivery.
