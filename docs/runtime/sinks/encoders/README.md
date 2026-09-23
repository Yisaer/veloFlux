# Sink Encoders

This directory contains sink-encoder behavior and encoder-local capabilities.

## Encoder Capabilities

Each encoder registered in `EncoderRegistry` implements the standard
`SinkEncoder` delivery API:

- `begin_delivery`
- `append`
- `finish_delivery`

This API is streaming by contract. An encoder that cannot emit incrementally may
buffer records in `append` and emit the final payload from `finish_delivery`;
that does not require a separate encoder category.

The planner binds an `OutputLayout` to each encoder factory. The layout defines
the ordered output columns and their fixed tuple value references. This contract
is independent of encoder kind and template configuration.

Current encoder-local documents:

- [CSV Sink Encoder](csv.md)
- [Template Encoder](encoder_transform.md)
- [JSON Encoder Options](json_encoder_options.md)
- [JSON Null Field Omission](json_null_column_omit.md)

The built-in JSON encoder supports `encoder.props.format=array|ndjson`. Framing is scoped to one
encoded delivery and does not change connector delivery boundaries.

The template encoder is documented in [Template Encoder](encoder_transform.md). It is a
top-level encoder kind and renders one item per input row.

The target built-in encoder set is:

| Encoder | Output type | Contract |
|---|---|---|
| `json` | bytes | Native JSON encoding with array or NDJSON framing. |
| `template` | bytes | One rendered template item per input row, with encoder-owned framing. |
| `csv` | bytes | CSV encoding from the planner-defined output layout. |
| `none` | collection | Planner pseudo-mode with no encoder node. |
