# Sink And Output Model

## Purpose

This directory documents sink-side behavior in veloFlux:

- which sink connectors are built into the runtime
- how sink encoders are attached
- which sink-level common properties are applied before delivery
- how planner/physical plan stages are ordered near the sink boundary
- where future sink-side output features should live

The goal is to keep connector semantics, encoder semantics, and sink-side plan behavior clearly
separated.

## Layer Model

At a high level, sink-side delivery in veloFlux is split into three layers:

1. **Sink connector**
   - Delivers the final payload to an external system or in-process destination.
   - Examples: `mqtt`, `memory`, `kuksa`, `nop`.

2. **Sink encoder**
   - Converts a `Collection` into bytes when the connector expects bytes payloads.
   - Examples: `json`, `none`.

3. **Common sink properties**
   - Planner-managed sink-side behavior shared across connectors.
   - Today this mainly means sink batching.

These layers are modeled independently on purpose:

- one connector may work with multiple encoders
- one encoder may be reusable across multiple connectors
- common sink properties should not be reimplemented inside each connector

## Current Built-In Runtime Sink Connectors

The runtime currently registers these built-in sink connector kinds:

| Connector | Runtime built-in | Accepts bytes | Accepts collection | Notes |
|----------|-------------------|---------------|--------------------|-------|
| `mqtt` | yes | yes | no | Intended for encoded payloads such as JSON bytes. |
| `nop` | yes | yes | no | Useful for tests, benchmarks, and plan validation. |
| `kuksa` | yes | no | yes | Consumes decoded rows directly; pipeline API forces `encoder=none`. |
| `memory` | yes | yes | yes | Topic kind depends on whether an encoder is present. |

Additional notes:

- A `mock` sink connector implementation exists in the codebase, but it is a test utility and is
  not registered as a normal built-in runtime connector.
- A custom connector may support bytes, collections, or both, depending on its own implementation.

## Current Built-In Sink Encoders

The runtime currently registers the following built-in encoder kinds:

| Encoder | Runtime built-in | Output type | Streaming support | By-index projection support | Notes |
|--------|-------------------|-------------|-------------------|-----------------------------|-------|
| `json` | yes | bytes | yes | yes | Encodes a `Collection` as a JSON array payload. |
| `none` | planner pseudo-mode | collection passthrough | n/a | n/a | No encoder node is built; the connector receives decoded collections directly. |

Current transform support:

- `encoder.transform=template` is only supported for `encoder.type=json`.
- The transform is item-level (`row -> transformed JSON item`), not a standalone collection
  transform stage.
- When `encoder.type=none`, any configured transform is ignored by design.

See also:

- [Encoder Transform](/Users/yisa/Downloads/Github/veloFlux/docs/sinks/encoder_transform.md)
- [Row Diff Output](/Users/yisa/Downloads/Github/veloFlux/docs/sinks/row_diff_output.md)

## Current Common Sink Properties

Today the planner exposes one common sink property group:

- `batch_count`
- `batch_duration`

These are modeled as sink-level flush controls, not connector-specific controls.

Behavior:

- when batching is enabled, the planner inserts a `Batch` stage before delivery
- batching is applicable to both encoded sink paths and direct collection sink paths
- if the downstream encoder supports streaming, physical optimization may rewrite
  `Batch -> Encoder` into a single `StreamingEncoder`

## Current Connector-Specific Rules

### MQTT

- Intended for bytes payloads.
- In practice this means an encoder should be present.
- If a pipeline uses `encoder.type=none` with `mqtt`, the connector will receive collection
  payloads and reject them at runtime because MQTT sink does not implement collection delivery.

### Nop

- Intended for bytes payloads.
- Useful as a sink-side terminator for tests and benchmarks.
- Like MQTT, it does not implement collection delivery.

### Kuksa

- Requires decoded row access.
- The pipeline API forces `encoder=none`.
- Physical planning builds no encoder node for Kuksa sinks.

See also:

- [Kuksa Sink](/Users/yisa/Downloads/Github/veloFlux/docs/sinks/kuksa.md)

### Memory

- Supports both bytes topics and collection topics.
- When an encoder is present, the sink publishes bytes.
- When `encoder.type=none`, the sink publishes collections.
- For collection topics, the planner inserts a dedicated materialization stage to normalize row
  layout before the sink.

See also:

- [Memory Sink](/Users/yisa/Downloads/Github/veloFlux/docs/sinks/memory.md)

## Current Plan Order

### Main query pipeline

Before any sink-specific suffix is attached, the current query pipeline shape is:

```text
DataSource
  -> StatefulFunction? 
  -> Window?
  -> Aggregation?
  -> Filter?
  -> Order?
  -> Project
```

The exact middle stages depend on the query, but the important point for sink work is:

- the relational/query pipeline currently ends at the final `Project`
- sink-specific behavior is attached after that point, per sink branch

### Per-sink branch suffix

After the final `Project`, each sink branch is built independently.

#### Encoded bytes path

Without batching:

```text
Project -> Encoder -> DataSink
```

With batching:

```text
Project -> Batch -> Encoder -> DataSink
```

After `StreamingEncoderRewrite`:

```text
Project -> StreamingEncoder -> DataSink
```

#### Direct collection path

For direct collection sinks (`encoder.type=none`), no encoder node is built.

Generic shape:

```text
Project -> Batch? -> DataSink
```

Kuksa path:

```text
Project -> Batch? -> DataSink
```

Memory collection path:

```text
Project -> Batch? -> MemoryCollectionMaterialize -> DataSink
```

### Multi-sink pipelines

In multi-sink pipelines:

- all sink branches share the same upstream relational/query pipeline
- sink-specific suffixes are attached per branch after the shared `Project`
- physical rewrites must respect shared DAG constraints

This is especially important for encoder-oriented optimizations. For example:

- `ByIndexProjectionIntoEncoderRewrite` is all-or-nothing for a shared `Project`
- if any downstream encoder cannot honor delayed materialization, the rewrite is not applied

## Current Sink-Side Related Capabilities

These capabilities already exist near the sink boundary:

1. **Sink batching**
   - Planner-managed flush grouping using `Batch`.

2. **JSON encoder transform**
   - `encoder.type=json`
   - `encoder.transform=template`
   - implemented inside the JSON encoder, not as a standalone plan stage.

3. **Streaming encoder rewrite**
   - Rewrites `Batch -> Encoder` into `StreamingEncoder` when the encoder supports streaming.

4. **By-index projection into encoder rewrite**
   - Delays materialization of eligible by-index projected columns into the encoder.

5. **Memory collection materialization**
   - Normalizes collection rows into a stable layout before publishing to memory collection topics.

## Design Guideline For New Sink-Side Features

When adding new sink-side features, prefer to classify them explicitly as one of:

- connector capability
- encoder capability
- common sink property
- sink output mode
- physical optimization

Avoid hiding one category inside another unless the semantics are genuinely encoder-local or
connector-local.

In particular:

- row-level stateful output shaping should not be modeled as SQL scalar/stateful functions
- encoder-local byte formatting should stay inside encoders
- connector transport rules should stay inside connectors

## Related Documents

- [Memory Sink](/Users/yisa/Downloads/Github/veloFlux/docs/sinks/memory.md)
- [Kuksa Sink](/Users/yisa/Downloads/Github/veloFlux/docs/sinks/kuksa.md)
- [Row Diff Output](/Users/yisa/Downloads/Github/veloFlux/docs/sinks/row_diff_output.md)
- [Encoder Transform](/Users/yisa/Downloads/Github/veloFlux/docs/sinks/encoder_transform.md)
- [StreamingEncoderRewrite](/Users/yisa/Downloads/Github/veloFlux/docs/planner_optimize/physical/streaming_encoder_rewrite.md)
- [ByIndexProjectionIntoEncoderRewrite](/Users/yisa/Downloads/Github/veloFlux/docs/planner_optimize/physical/by_index_projection_into_encoder_rewrite.md)
