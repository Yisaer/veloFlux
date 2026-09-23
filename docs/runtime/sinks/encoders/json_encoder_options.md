# JSON Encoder Options

This document explains the current option model for the built-in JSON sink encoder.

It covers two layers:

- user-facing encoder configuration
- runtime-only encoder options used internally while encoding rows

The user-facing configuration determines which runtime options are active for a given sink branch.

## Scope

This document is about the built-in JSON encoder only:

- `encoder.type = json`

It does **not** define option models for:

- `encoder.type = none`
- custom encoders
- generic sink output options such as `output.mode` or `output.omit_if_empty`

## Why Two Layers Exist

The JSON encoder has to combine several behaviors:

- array or NDJSON delivery framing
- plain full-row JSON object encoding
- row-diff mask-aware output
- object-field `null` omission

The pipeline API should expose only the options users need to control directly.
The encoder implementation still needs a small internal option set so that each encoding path stays
explicit and testable.

## User-Facing Options

### `encoder.props.format`

Meaning:

- select the framing for each encoded JSON delivery

Supported values:

- `array` (default)
- `ndjson`

Example:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "format": "ndjson"
    }
  }
}
```

Array format emits one compact JSON array per delivery:

```text
[{"a":1},{"a":2}]
```

NDJSON format emits one compact JSON text per row. Every row, including the last row, ends with LF:

```text
{"a":1}\n
{"a":2}\n
```

NDJSON uses UTF-8, does not emit a BOM, and does not emit outer brackets or row-separator commas.
LF and CR inside string values are JSON-escaped and therefore do not create record boundaries.

For a delivery whose lifecycle is started and finished with zero rows, array format emits `[]` and
NDJSON emits zero bytes. If no delivery is started, neither format creates an output by itself.

The format is a delivery-level encoder option. It does not change sink batching or make the file
sink append to a long-lived file.

Example output payload:

```json
[{"value":10,"label":"ok"},{"value":20,"label":"fail"}]
```

Related document:

- [Template Encoder](encoder_transform.md)

### `encoder.props.omit_null_columns`

Meaning:

- control whether the native JSON object encoding path omits object fields whose runtime value is
  `null`

Default:

- `true`

Example:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "omit_null_columns": true
    }
  }
}
```

Function enabled:

- object-field `null` omission for native JSON object encoding

Effect:

- on normal full-output rows, `null` object fields are omitted by default
- nested object fields inside `struct` values are also affected
- array `null` items are preserved
- this does not change template semantics
- this does not suppress row-diff fields selected by `output_mask`

Example input row:

```json
{"a":1,"b":null}
```

Output with `omit_null_columns=true`:

```json
{"a":1}
```

Output with `omit_null_columns=false`:

```json
{"a":1,"b":null}
```

Nested object example with `omit_null_columns=true`:

```json
{"a":{"b":1}}
```

Related document:

- [JSON Null Field Omission](json_null_column_omit.md)

## Runtime-Only Options

The current implementation uses an internal row-encoding option bundle equivalent to:

```rust
struct JsonRowEncodeOptions<'a> {
    output_layout: Option<&'a OutputLayout>,
    null_policy: NullColumnPolicy,
}
```

These options are not exposed directly in the pipeline API.
They are derived from the planner output layout, sink output mode, and encoder
configuration.

### `output_layout`

Meaning:

- provides the final output column order, datatype, and fixed tuple value
  reference used for every rendering mode

Function enabled:

- row-diff aware encoding

Typical source:

- final sink-side output layout built by the planner

Effect:

- the encoder can align runtime tuples with the final logical output columns
- the encoder can tell which field name should be emitted for each final output column

Conceptual example:

```text
SELECT a AS x, b AS y FROM stream
```

Final output layout:

```text
[x, y]
```

The encoder uses this layout for final output names and indexed runtime value access.
When a tuple carries an output mask, the JSON encoder applies that mask directly
while walking the layout. It does not use a separate output-mask mode.

Example:

```json
row values: {"a":null,"b":3}
mask:       [false, true]
```

Output:

```json
{"b":3}
```

### `null_policy`

Meaning:

- control whether the encoder's native object writer keeps or omits `null` object fields

Current modes:

- `KeepNulls`
- `OmitNullObjectFields`

#### `KeepNulls`

Meaning:

- emit `"field": null` for object fields whose value is `null`

Used for:

- native paths when `encoder.props.omit_null_columns = false`

Example:

```json
{"a":1,"b":null}
```

Output:

```json
{"a":1,"b":null}
```

#### `OmitNullObjectFields`

Meaning:

- omit object fields whose value is `null`

Used for:

- native full-output JSON encoding when `encoder.props.omit_null_columns = true`

Example:

```json
{"a":1,"b":null}
```

Output:

```json
{"a":1}
```

Nested object example:

```json
{"a":{"b":1,"c":null}}
```

Output:

```json
{"a":{"b":1}}
```

Array example:

```json
{"a":[1,null,2]}
```

Output:

```json
{"a":[1,null,2]}
```

Important limit:

- this policy does not override row-diff sparse semantics
- a changed-to-`null` field selected by the output mask is still emitted as `"field": null`

## Feature Mapping Summary

| User or runtime option | Meaning | Enables / affects |
|---|---|---|
| `encoder.props.format` | select `array` or `ndjson` delivery framing | JSON delivery boundaries and row separators |
| `encoder.props.omit_null_columns` | omit `null` object fields on native JSON object encoding | null-field omission |
| `output_layout` | align final columns with fixed runtime value references | native and row-diff-aware JSON encoding |
| tuple `output_mask` | select fields while walking the final output layout | row-diff sparse JSON encoding |
| `null_policy=KeepNulls` | keep object-field `null` values | legacy / explicit keep-null behavior |
| `null_policy=OmitNullObjectFields` | omit `null` object fields | default full-output null omission |

## Practical End-to-End Examples

### Example 0: NDJSON Delivery

Configuration:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "format": "ndjson"
    }
  }
}
```

Two output rows produce:

```text
{"a":1}\n
{"a":2}\n
```

### Example 1: Plain Full Output

Configuration:

```json
{
  "encoder": {
    "type": "json"
  }
}
```

Derived runtime behavior:

- `null_policy = OmitNullObjectFields`

Input row:

```json
{"a":1,"b":null}
```

Output row item:

```json
{"a":1}
```

### Example 2: Plain Full Output With Nulls Kept

Configuration:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "omit_null_columns": false
    }
  }
}
```

Derived runtime behavior:

- `null_policy = KeepNulls`

Input row:

```json
{"a":1,"b":null}
```

Output row item:

```json
{"a":1,"b":null}
```

### Example 3: Row-Diff JSON Output

Configuration:

```json
{
  "encoder": {
    "type": "json"
  },
  "output": {
    "mode": "delta"
  }
}
```

Derived runtime behavior:

- `output_layout` is required
- changed-to-`null` fields are still emitted

Runtime row state:

```json
row values: {"a":null,"b":10}
mask:       [true, false]
```

Output row item:

```json
{"a":null}
```
