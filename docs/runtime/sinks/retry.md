# Sink Retry

Sink retry is implemented at the `SinkProcessor` layer, between encoded delivery processors and the
concrete `SinkConnector`. Connectors perform one attempt per call sequence and classify failures for
logging and metrics.

## Configuration

Retry is a common sink property:

```yaml
sinks:
  - type: http
    props:
      url: "https://api.example.com/data"
    retry:
      max_attempts: 3
      initial_backoff_ms: 1000
      max_backoff_ms: 30000
      jitter: true
```

Fields:

- `max_attempts`: maximum delivery attempts including the first. When omitted, retry is disabled and
  each delivery is attempted once.
- `initial_backoff_ms`: initial retry delay. Defaults to `1000`.
- `max_backoff_ms`: maximum retry delay. Defaults to `30000`.
- `jitter`: accepted for configuration compatibility, but current backoff is deterministic.

## Delivery State

The processor accumulates one encoded delivery into a full payload before attempting connector I/O.
On connector failure, it keeps that payload as the in-flight delivery, stops reading new data input,
and waits for the retry timer until `max_attempts` is exhausted. Control input remains active. If a
terminal control signal arrives while a delivery is pending, the processor parks the terminal signal
and finishes the pending delivery path before closing the connector.

Each attempt replays the full connector protocol:

```text
start_delivery() -> write_chunk(full_payload) -> finish_delivery()
```

Retries replay the same logical message. `messages_in` increments once when `SinkProcessor`
assembles the complete message, and `messages_out` increments once only after confirmed success.
Retry attempts do not increment either counter and do not have a separate attempt metric. Any
delivery that still returns an error after the final attempt increments `messages_dropped`.
`bytes_delivered` only includes successful messages. Error details remain available through
`error_count`, `last_error`, and logs.

## Error Handling

Connector error classification is used for logging and metrics only. The retry decision does not
depend on the error class: every connector delivery error is retried until `max_attempts` is
exhausted. After the final failed attempt, the processor logs the failure, drops the delivery, and
continues with the next upstream item.

Checkpoint snapshots do not preserve attempt counts, retry deadlines, or backoff state. Every
restored delivery starts a new retry lifecycle using the sink's current retry configuration.
Restored checkpoint deliveries are stricter: exhausting this new lifecycle fails replay and stops
the sink processor. New upstream data is not consumed before restored FIFO replay completes.

## Checkpoint Capture

During checkpoint-enabled graceful shutdown, `SinkProcessor` receives a `SinkCapture` barrier on
the control channel. It stops starting connector attempts, retains its pending delivery in the
in-memory capture cache, and forwards the barrier only after entering capture mode. A connector
attempt that is interrupted by capture has an uncertain outcome and its payload remains cached for
the sink checkpoint path.
