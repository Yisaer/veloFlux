# Sink Connectors

This directory contains connector-specific sink behavior.

Each encoded connector document describes the error classes it reports. The common
[sink retry policy](../retry.md) decides which classes are eligible for retry; collection-native
sinks currently use a direct send path.

- `mqtt.md`: MQTT sink delivery behavior, shared-client binding, and output-feature interaction.
- `http.md`: HTTP raw and multipart delivery, error classification, Content-Type handling, and
  delivery lifecycle.
- `memory.md`: In-process memory sink connector behavior.
- `file.md`: Local file sink byte delivery, filename generation, temporary files, and retention.
- `nng_pubsub.md`: NNG pub/sub sink framing and lifecycle behavior.
- `kuksa.md`: Kuksa sink design and typed update semantics.
- `kura.md`: Kura sink design, gRPC SetCurrent execution model, and mapping file contract.
- `video.md`: Video sink tuple contract, planner validation, and GStreamer-backed file output.

## Common Sink Properties

Property paths in connector tables are relative to one entry in the pipeline's `sinks` array.
All connector documents use the following columns: property, JSON type, whether it is required,
default, and description. `-` means no default value. Nested fields are required only under the
conditions stated in the table. Connector-specific encoder defaults and restrictions take precedence
over the common defaults below.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `id` | string | No | `<pipeline_id>_sink_<index>` | Sink identifier; index starts at zero. |
| `type` | string | Yes | - | Connector kind, as listed in each connector document. |
| `props` | object | Depends on connector | `{}` | Connector-specific fields described in each connector document. |
| `encoder` | object | No | `{"type":"json","props":{}}` | Encoder configuration; collection-native connectors may force `none`. |
| `encoder.type` | string | No | `json` | Registered encoder kind, or `none` where supported. |
| `encoder.props` | object | No | `{}` | Encoder-specific options; see [sink encoders](../encoders/). |
| `common_sink_props` | object | No | `{}` | Shared batching controls. |
| `common_sink_props.batch_count` | integer | No | - | Flush after this row count. |
| `common_sink_props.batch_duration` | integer | No | - | Flush on a fixed processing-time grid, in milliseconds. |
| `output` | object | No | Full output | Sink output policy; see [row diff](../output/row_diff_output.md), [empty suppression](../output/omit_if_empty.md), and [column filtering](../output/column_filter.md). |
| `output.mode` | string | Yes, if `output` is set | - | `full` or `delta`; connector and encoder restrictions apply. |
| `output.delta` | object | No | - | Delta settings; allowed only with `output.mode=delta`. |
| `output.delta.columns` | array of strings | No | - | Columns tracked for row differences; omission selects all output columns. |
| `output.omit_if_empty` | boolean | No | `false` | Suppress empty output. |
| `output.include_columns` | array of strings | No | - | Non-empty output column allowlist; mutually exclusive with `exclude_columns`. |
| `output.exclude_columns` | array of strings | No | - | Non-empty output column denylist; mutually exclusive with `include_columns`. |
| `delivery` | object | No | - | Transforms applied to encoded delivery bytes; requires an encoder. |
| `delivery.compression` | object | No | - | Optional [compression](../delivery/compress.md). |
| `delivery.compression.codec` | string | If compression is set | - | `gzip` or `zstd`. |
| `delivery.compression.level` | integer | No | Codec default | Compression level; gzip accepts `0` through `9`. |
| `delivery.encryption` | object | No | - | Optional [encryption](../delivery/encrypt.md). |
| `delivery.encryption.algorithm` | string | If encryption is set | - | Must be `aes-gcm`. |
| `delivery.encryption.key` | string (`SecretRef`) | If encryption is set | - | Secret reference to a base64 AES key; decoded size selects AES-128/192/256. |
| `retry` | object | No | `{}` | Common [retry policy](../retry.md); applies to encoded deliveries only. |
| `retry.retry_up_to` | string | No | `uncertain` | Eligible error classes: `transient`, `uncertain`, or `all`. |
| `retry.max_attempts` | integer | No | - | Maximum attempts including the first; must be greater than zero. Omission disables retry. |
| `retry.initial_backoff_ms` | integer | No | `1000` | Initial retry delay in milliseconds; must be greater than zero. |
| `retry.max_backoff_ms` | integer | No | `30000` | Maximum retry delay in milliseconds; must be at least `initial_backoff_ms`. |
| `retry.jitter` | boolean | No | `true` | Accepted, but not currently applied; backoff is deterministic. |

Batching semantics are described in [common sink properties](../README.md#current-common-sink-properties).
