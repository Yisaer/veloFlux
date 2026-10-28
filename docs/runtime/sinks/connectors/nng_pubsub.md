# NNG Pub/Sub Sink

The `nng_pubsub` sink publishes encoded payload bytes through an NNG `pub0` socket. The remote
peer is expected to use an NNG `sub0` socket and subscribe to the configured topic prefix.

## Configuration

See [common sink properties](README.md#common-sink-properties) for sink-level fields shared by connectors.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `type` | string | Yes | - | Must be `nng_pubsub`. |
| `props.url` | string | Yes | - | NNG endpoint to dial: `tcp://`, `ipc://`, or `inproc://`. |
| `props.topic` | string | Yes | - | Non-empty topic prefix prepended to every published message. |
| `props.topic_delimiter` | string | No | `":"` | Separator between topic bytes and payload bytes. |
| `props.topicDelimiter` | string | No | - | Alias for `topic_delimiter`; the snake_case field takes precedence when both are set. |
| `encoder.type` | string | No | `json` | Encoder producing the published bytes; collection delivery is unsupported. |

The sink does not inspect or reshape tuple data. It receives the final bytes from the configured
encoder and publishes this frame:

```text
[topic bytes][topic_delimiter bytes][payload bytes]
```

For example, `topic = "topic/can"` and `topic_delimiter = ":"` publishes:

```text
topic/can:{"v":1}
```

Dynamic sink topics are not part of the first implementation because the current VeloFlux sink
connector boundary receives encoded bytes, not tuple metadata.

## Lifecycle

The sink follows normal VeloFlux connector behavior. `ready` performs a bounded connection
attempt, and `send` can lazily reconnect when needed. If no peer is available, the sink returns
a connector error rather than waiting indefinitely.

NNG pub/sub is best-effort and has no acknowledgements. A successful publish means the local NNG
operation accepted the message; it does not prove that any subscriber received it.

## Error Classification

For encoded deliveries, invalid sink configuration or URL validation returns `Permanent`. Socket
open, message construction, dial, disconnected, and publish failures use `Other` or `Unavailable`,
which the common [sink retry policy](../retry.md) treats as `Uncertain`. Errors from `ready` fail
startup before delivery retry begins.

## Embedded `inproc` deployments

When veloFlux is embedded as a shared library, the sink may publish through an
`inproc://` URL to an NNG `sub0` socket owned by the host process. The URL
scheme is the only transport selector. The sink configuration does not add a
separate `transport` or `inproc` field, and the sink protocol remains
internally fixed to `pub`.

The host must listen and subscribe before starting the pipeline. The host
should consume output only after the pipeline reports readiness. The host and
veloFlux must use the same NNG library runtime; two separately linked static
NNG instances in one process cannot provide a reliable `inproc` connection.

See [NNG In-Process Integration](../../../integrations/ffi/nng_inproc.md) for
the complete embedded design.
