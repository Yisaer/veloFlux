# Native sink runtime state

Collection-native sinks keep their physical sink node, runtime task, connector,
and input/output channels. This includes `nop` with `encoder:none`, memory
collection sinks, and other connectors that implement `send_collection`.
Fusion remains limited to the transform stages upstream of the sink.

`SinkProcessor` uses one execution loop for both native collections and encoded
deliveries. Connector readiness, control forwarding, terminal handling, closing,
metrics, and result forwarding stay in that loop.

Encoded-only state lives in an optional boxed `EncodedDeliveryState`: payload
accumulation, captured/replayed deliveries, the pending retry, and a parked
control terminal. Encoded inputs allocate this state once at startup. Native
inputs with no restored deliveries keep it absent. A nonempty restored delivery
cache retains the existing replay path.

A connector attempt is prepared only when a pending encoded delivery is ready.
Its clock is read when the attempt branch is polled, and the retry sleep is built
only when the retry branch is polled. Native collection processing does not
prepare dummy payloads or property contexts, or construct inactive retry timers.
Native handling still measures the real `send_collection` call and keeps the
existing counters and error boundaries.

Native collection delivery remains single-attempt, even if encoded retry options
are configured. A failed collection records an error and a dropped message;
subsequent collections continue. Successful collections are forwarded only when
result forwarding is enabled. Connector logging is preserved.

Checkpoint behavior is unchanged. A sink capture control signal switches the
common delivery mode to capturing. Native collections received during capture
remain unsupported and return the existing error. An empty native final
checkpoint records the same empty delivery-cache state and uses the same sink
checkpoint identity/version. Control and data terminals keep their respective
channels, and the sink retains its downstream output-domain contract.

