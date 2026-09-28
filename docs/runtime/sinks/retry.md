# Sink Retry

Sink retry is implemented at the `SinkProcessor` layer, between encoded delivery processors and the
concrete `SinkConnector`. Connectors perform one attempt per call sequence and classify failures;
the processor applies the configured policy to each classification.

This retry path handles encoded deliveries. Collection-native sinks send collections directly and
currently make one connector call per collection without using this retry configuration.

## Configuration

Retry is a common sink property:

```yaml
sinks:
  - type: http
    props:
      url: "https://api.example.com/data"
    retry:
      retry_up_to: uncertain
      max_attempts: 3
      initial_backoff_ms: 1000
      max_backoff_ms: 30000
      jitter: true
```

Fields:

- `retry_up_to`: highest eligible error class: `transient`, `uncertain`, or `all`. Defaults to
  `uncertain`. `all` also retries `Permanent` delivery errors. This controls eligibility only and
  does not change the attempt count.
- `max_attempts`: maximum delivery attempts including the first. When omitted, retry is disabled and
  each delivery is attempted once. Must be greater than zero when set.
- `initial_backoff_ms`: initial retry delay. Defaults to `1000`.
- `max_backoff_ms`: maximum retry delay. Defaults to `30000`.
- `jitter`: accepted for configuration compatibility, but current backoff is deterministic.

## Delivery State

The processor accumulates one encoded delivery into a full payload before attempting connector I/O.
On an eligible connector failure, it keeps that payload as the in-flight delivery, stops reading
new data input, and waits for the retry timer until `max_attempts` is exhausted. Control input
remains active. If a terminal control signal arrives while a delivery is pending, the processor
parks the terminal signal and finishes the pending delivery path before closing the connector.

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

Connectors classify errors conservatively. `Transient` indicates strong evidence that waiting may
help. `Uncertain` means the connector cannot determine whether the same delivery will work later;
HTTP 500 and 404 both belong here. `Permanent` requires strong evidence that the same delivery
cannot succeed under the current configuration, such as a local validation failure. Legacy
`Other` and `Unavailable` errors are treated as uncertain until their connectors provide a more
specific class.

This fallback also applies to encoded File, Memory, and NNG pub/sub deliveries. File sink path and
filename pattern validation failures, and NNG pub/sub configuration and URL validation failures,
are classified as `Permanent`. File system I/O failures and NNG connection or publish failures
remain uncertain because the connector cannot prove whether a later attempt will succeed. MQTT
and HTTP classify errors directly where the response provides enough evidence. MQTT publish
packets that exceed the connection or protocol size limit are `Permanent`: the resolved topic and
payload are known locally and replaying the same delivery cannot produce a smaller packet.

`retry_up_to: transient` retries only `Transient`; the default `uncertain` also retries
`Uncertain`, `Other`, and `Unavailable`. `retry_up_to: all` also retries `Permanent` delivery
errors, including errors that cannot recover while the payload and configuration remain the same.
All eligible errors share one `max_attempts` value and the same backoff state. A changed error
class does not reset the attempt count. After an ineligible error or the final attempt, the
processor logs the failure, drops the delivery, and continues with the next upstream item. Failure
logs include `retry_up_to` and a stop reason (`ineligible under retry policy` or `attempts
exhausted`).

Checkpoint snapshots do not preserve attempt counts, retry deadlines, or backoff state. Every
restored delivery starts a new retry lifecycle using the sink's current retry configuration.
Restored checkpoint deliveries are stricter: an ineligible error or exhausting this new lifecycle
fails replay and stops the sink processor. New upstream data is not consumed before restored FIFO
replay completes.

## Behavior Change

Previously every connector delivery error consumed the configured retry attempts. Under the new
default policy, a `Permanent` error ends the delivery immediately; `retry_up_to: all` preserves
the former eligibility for every delivery error. The default `uncertain` policy retains retry
eligibility for ambiguous HTTP responses such as 500 and 404 when `max_attempts` is configured.

## Checkpoint Capture

During checkpoint-enabled graceful shutdown, `SinkProcessor` receives a `SinkCapture` barrier on
the control channel. It stops starting connector attempts, retains its pending delivery in the
in-memory capture cache, and forwards the barrier only after entering capture mode. A connector
attempt that is interrupted by capture has an uncertain outcome and its payload remains cached for
the sink checkpoint path.
