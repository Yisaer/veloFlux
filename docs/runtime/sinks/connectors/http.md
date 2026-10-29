# HTTP Sink Connector Design

## Background

The HTTP sink delivers final sink-branch encoded payload bytes to a remote HTTP endpoint. Each
delivery unit (one `start_delivery` → `write_chunk*` → `finish_delivery` cycle) is sent as a single
HTTP request with the complete body. The connector accumulates chunks into an in-memory buffer
and flushes them on `finish_delivery`.

Like all byte-delivery connectors, encoder/output behavior upstream of the connector materially
affects what is sent. The HTTP sink requires an encoder (e.g. `json`, `csv`, `protobuf`) and rejects
`encoder.type=none`.

HTTP redirects are disabled. A 3xx response is an `Uncertain` delivery failure;
eligible retries use the original configured URL. Configure the final endpoint
directly so redirects cannot forward custom secret headers or payloads to another
destination. See [deployment security](../../deployment_security.md) for the
trusted control-plane and runtime property boundaries.

## Goals

- Define the runtime contract of the HTTP sink connector.
- Document HTTP-specific configuration parameters and the common sink retry behavior.
- Explain how encoder and sink-output features interact with HTTP delivery.
- Specify raw and multipart request body behavior.

## Non-Goals

- Generic encoder design beyond the HTTP sink boundary.
- TLS certificate management (default system trust store is used).
- API reference documentation for pipeline create/update.

## Configuration Model

See [common sink properties](README.md#common-sink-properties) for sink-level fields shared by connectors.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `type` | string | Yes | - | Must be `http`. |
| `props.url` | string | Yes | - | Target HTTP URL. |
| `props.method` | string | No | `POST` | HTTP method: `GET`, `POST`, `PUT`, `PATCH`, or `DELETE`. |
| `props.timeout_secs` | integer | No | `30` | Per-request timeout in seconds; non-negative. |
| `props.headers` | object (string values) | No | `{}` | Extra HTTP headers. Values support property templates; sensitive headers must use `auth` or `secret_headers`. |
| `props.auth` | object | No | - | Structured authentication; see the authentication fields below. |
| `props.secret_headers` | object (string values) | No | `{}` | Header names mapped to `SecretRef` strings; no property templates. |
| `props.content_type` | string | No | Inferred from encoder | Explicit request `Content-Type` in raw mode; rejected in multipart mode. |
| `props.max_body_size` | integer | No | `67108864` (64 MiB) | Maximum encoded delivery payload bytes; excludes multipart envelope overhead. |
| `props.body` | object | No | `{"type":"raw"}` | Request body configuration; see the body fields below. |
| `encoder.type` | string | No | `json` | Encoder kind; `none` is rejected. |

Retry is configured only through the common sink-level `retry` object, outside `props`.
Unknown HTTP properties are rejected.

### Authentication fields

`SecretRef` is a string such as `store:NAME`, or an inline literal subject to the configured secret
policy. Secret-bearing fields do not support property templates.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `props.auth.type` | string | Yes, if `auth` is set | - | `bearer` or `basic`. |
| `props.auth.token` | string (`SecretRef`) | For `bearer` | - | Bearer token secret reference. |
| `props.auth.username` | string | For `basic` | - | Basic authentication username. |
| `props.auth.password` | string (`SecretRef`) | For `basic` | - | Basic authentication password secret reference. |

Ordinary header values may use process property templates:

```json
{
  "headers": {
    "X-Vehicle-ID": "{{ prop(\"vin\") }}"
  }
}
```

`Authorization`, `Proxy-Authorization`, and `Cookie` remain prohibited in
ordinary `headers`; use `auth` or `secret_headers`. Those secret-bearing paths
are not template-enabled. Template-looking text in them is treated as literal
`SecretRef` input and follows the existing secret policy.

### Multipart body

Multipart mode uploads the final delivery as one file part and adds zero or more static UTF-8 text
parts:

```json
{
  "type": "http",
  "props": {
    "url": "https://example.com/api/offline/upload",
    "body": {
      "type": "multipart",
      "file_field_name": "d",
      "file_name": "payload.bin",
      "fields": {
        "tp": "1",
        "vin": "{{ prop(\"vin\") }}"
      }
    }
  },
  "encoder": {
    "type": "json"
  }
}
```

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `props.body.type` | string | Yes, if `body` is set | `raw` when `body` is omitted | `raw` or `multipart`. Remaining fields apply only to `multipart`. |
| `props.body.file_field_name` | string | For `multipart` | - | Form field name for the file part. |
| `props.body.file_name` | string | No | `payload.bin` | Filename in the file part's `Content-Disposition`. |
| `props.body.fields` | object (string values) | No | `{}` | UTF-8 text field name/value pairs; values support property templates. |

Field names and the filename are trimmed. Empty names and names containing CR, LF, or NUL are
rejected. A text field cannot use the file field name. Text values are preserved exactly after
property-template rendering. Property values are not expanded recursively.

The file part media type is always `application/octet-stream`. Multipart mode rejects both
`props.content_type` and a `Content-Type` entry in `props.headers`; reqwest generates the request
boundary and the matching `multipart/form-data; boundary=...` header.

Only header values and multipart text-field values support connector property
templates. URL, method, content type, header names, file names, and field names
remain static. See
[connector property templates](../../../syntax/connectors/property_templates.md).

With the local MQTT property provider enabled, all template-enabled values are
resolved from one snapshot after the complete encoded delivery is available and
before the first HTTP attempt. Retries preserve that snapshot. Without the
provider, values remain fixed at pipeline apply time.

## Delivery Lifecycle

```
start_delivery()
  └─ buffer.clear()                 // reset delivery buffer

write_chunk(bytes)                  // called 0..N times
  └─ buffer.extend(bytes)           // accumulate payload bytes
  └─ Err if buffer > max_body_size  // size guard

finish_delivery()
  └─ raw: send buffer as the complete HTTP body
  └─ multipart: send buffer as one file part with static text parts
     └─ on 2xx: return DeliveryResult
     └─ on 408/429/502/503/504: return SinkConnectorError::Transient
     └─ on 500/404/other non-success status: return SinkConnectorError::Uncertain
     └─ on connect/timeout error: return SinkConnectorError::Transient
     └─ on other reqwest send error: return SinkConnectorError::Uncertain

abort_delivery()
  └─ buffer.clear()                 // discard partial delivery
```

## Retry Strategy

Retry is implemented by `SinkProcessor`, not by the HTTP connector. The HTTP connector performs one
request per `finish_delivery()` call and classifies failures so the processor can make the retry
decision.

HTTP error classification and retry eligibility:

| Class | Examples | `transient` | `uncertain` (default) | `all` |
|---|---|---|---|---|
| `Transient` | connect failure, timeout, 408, 429, 502, 503, 504 | retry | retry | retry |
| `Uncertain` | other reqwest send failures; 500, 404, other remote non-success responses | stop | retry | retry |
| `Permanent` | invalid per-delivery template or oversized payload | stop | stop | retry |

Invalid static URLs fail during connector startup, before delivery retry begins.

Retry uses deterministic exponential backoff:

1. First attempt fails → wait `initial_backoff_ms`
2. Backoff doubles each attempt, capped at `max_backoff_ms`
3. Loop until success, an ineligible error, or `max_attempts` exhausted

When retry is not configured (`retry.max_attempts` unset), each delivery is attempted exactly once.

## Hot Path Considerations

- `start_delivery` and `write_chunk` are constant-time operations that append to a `Vec<u8>`. No
  I/O is performed.
- `finish_delivery` is the only connector method that performs network I/O. It is called from a
  tokio async context, so other pipeline branches are not blocked.
- The `SinkProcessor` retains the completed encoded payload while retries are pending and replays
  the full delivery on each attempt.
- `max_body_size` is exposed to `SinkProcessor`, so oversized encoded deliveries are rejected while
  being accumulated and before a connector attempt is made.
- In multipart mode, reqwest streams the multipart envelope around the retained delivery bytes. The
  connector does not build a second complete multipart buffer.

## Encoder and Output Feature Interaction

- **Encoder**: The HTTP sink requires an encoder (`json`, `csv`, `protobuf`). `encoder.type=none` is
  rejected because the sink needs serialized bytes to send as an HTTP body.
- **Batching** (`batch_count`, `batch_duration`): Batched rows are encoded into a single delivery
  unit and sent as one HTTP request body.
- **Delta output**: Row-diff output works with compatible encoders. CSV rejects
  `output.mode=delta` because CSV deliveries require stable dense rows.
- **Template encoder**: The `template` encoder renders one item per row and its framed output
  forms the HTTP body.
- **Compression / Encryption**: Delivery compression (`gzip`, `zstd`) and encryption (`aes-gcm`) are
  supported as pre-processing steps before the HTTP request is sent. In multipart mode, only the
  file part contains the transformed bytes; the multipart envelope and static fields remain
  untransformed.

## `Content-Type` Inference

When `content_type` is not explicitly configured:

| Encoder type | Inferred `Content-Type` |
|---|---|
| `json` with `props.format=array` or no format | `application/json` |
| `json` with `props.format=ndjson` | `application/x-ndjson` |
| `template` | no inferred value unless an explicit content type is configured |
| `csv` | `text/csv; charset=utf-8` |
| `protobuf` | `application/octet-stream` |
| Other / custom | (no Content-Type header set) |

The inference happens during physical plan building, before the connector is instantiated.
Explicit `content_type` always takes precedence in raw mode. Multipart mode disables inference and
does not allow an explicit request `Content-Type`.

`application/x-ndjson` is the media type recommended by the NDJSON specification and is a common
convention, but it is not an IANA-registered media type. Services that require another value can
set `content_type` explicitly.

Compression of the file payload does not automatically add a request-level `Content-Encoding`,
because that header would describe the entire multipart representation. Endpoint-specific headers
can still be configured explicitly.

## Size and Metrics

`max_body_size` limits the encoded delivery payload. In multipart mode this is the file part size;
multipart boundaries, part headers, static fields, and trailing framing are not included.

`DeliveryResult.bytes_written` uses the same payload-byte definition and does not include multipart
envelope overhead.

## Error Handling

- URL validation occurs in `ready()`. Invalid URLs are rejected before the pipeline is started.
- Proven local validation failures are propagated as `SinkConnectorError::Permanent`.
- Connect/timeout/408/429/502/503/504 failures are propagated as `Transient`.
- Other reqwest send errors are propagated as `Uncertain` because the remote outcome may be unknown.
- Other remote non-success responses, including 500 and 404, are propagated as `Uncertain`.
- The processor applies `retry_up_to` and `max_attempts` independently. Ineligible errors or
  exhausted retries are logged; ordinary deliveries are dropped and processing continues.

## Testing

- **Unit tests** (`connector/sink/http.rs`): config defaults, builder methods, content-type
  inference, status classification, and structurally parsed multipart requests.
- **E2E tests** (`tests/e2e/http_sink_e2e.rs`): full pipeline with mock stream source → JSON
  decoder → raw and multipart HTTP sinks → embedded axum test server. Verifies raw compatibility
  and that the complete JSON encoder output is delivered in the configured file part.
