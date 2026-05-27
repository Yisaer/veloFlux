# NNG Pub/Sub Sink

The `nng_pubsub` sink publishes encoded payload bytes through an NNG `pub0` socket. The remote
peer is expected to use an NNG `sub0` socket and subscribe to the configured topic prefix.

## Configuration

Runtime sink properties:

| Field | Required | Default | Description |
|---|---:|---|---|
| `url` | yes | none | NNG endpoint to dial. Supported schemes: `tcp://`, `ipc://`, `inproc://`. |
| `topic` | yes | none | Topic prefix prepended to every published message. Empty topics are rejected. |
| `topic_delimiter` | no | `":"` | Application-level separator inserted between topic bytes and payload bytes. |

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
