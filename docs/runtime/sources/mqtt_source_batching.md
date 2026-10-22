# Opportunistic MQTT Source Batching

## Status And Migration Scope

**Implemented in `mqtt_source_round`.** This document describes the MQTT source batching subset
migrated from `w2_w4_action`. The implementation groups eligible buffered payloads and forwards
them through the byte-batch path described below. See [mqtt.md](./mqtt.md) for the surrounding
MQTT connection and runtime behavior.

The migration includes opportunistic payload grouping, the source-to-decoder batch path, sampler
handling, lifecycle forwarding, and adaptations that preserve existing message counters. Batch
observability now includes the source egress batch counter described below. W2/W4 benchmark tools,
publishers, reports, and CI workflow changes are outside this migration.

## Background And Motivation

Before batching, the standalone MQTT source forwarded each received PUBLISH payload as an
individual connector event. The datasource forwarded that payload as `StreamData::Bytes`, and
the decoder turned it into a collection. Each message therefore entered the connector channel
and downstream processing path separately, even when the MQTT client had already buffered
several consecutive PUBLISH events.

Grouping those already available payloads can amortize channel sends, downstream receives, and
per-input stage work. Payload decoding and per-row SQL computation still happen for the same
data; batching does not eliminate those costs. The design retains the shared `Bytes` payloads
instead of concatenating or copying their contents.

This is an opportunistic optimization. It introduces no collection timer or deliberate wait
for future messages. It does not guarantee a minimum batch size or a particular CPU reduction.

## Batch Formation

For a standalone MQTT connection:

1. Poll the MQTT event loop normally and receive the first PUBLISH.
2. Inspect the front of rumqttc's already populated `state.events` queue.
3. Drain consecutive PUBLISH events in arrival order while the batch contains fewer than
   **1024 MQTT payloads, including the first payload**.
4. Stop when the queue is empty, the payload count reaches the limit, or the next event is not
   an eligible PUBLISH. Leave that next event queued for normal event-loop handling.
5. Forward the first payload and the drained payloads as one source event, awaiting downstream
   capacity before proceeding.

The drain operation must not poll the socket, advance another event-loop poll, wait for more
data, skip protocol events, or reorder payloads. The MQTT library remains responsible for
protocol processing and acknowledgements. For MQTT 5, an invalid UTF-8 topic must remain on the
normal poll/error path rather than being bypassed by the drain operation.

There is no additional aggregate payload-byte limit. Existing per-packet MQTT limits still
apply. The 1024 limit counts raw MQTT payloads, not bytes or successfully decoded rows. A JSON
array inside one payload still consumes one payload slot, regardless of its eventual row count.

Batching is enabled by default. Set `VELOFLUX_MQTT_SOURCE_BATCHING=off` as an explicit
standalone-source bypass that forwards one payload at a time. This switch does not change
shared-client-backed sources.

## Source And Decoder Boundary

The grouped path is:

```text
MQTT event-loop queue
  -> ConnectorEvent::PayloadBatch(Vec<Bytes>)
  -> datasource
  -> StreamData::BytesBatch(Vec<Bytes>)
  -> optional sampler
  -> decoder
  -> Collection
```

A singleton may retain the existing `Payload` -> `Bytes` path. The MQTT source remains
byte-oriented: it does not parse JSON, count decoded rows, extract event time, or evaluate SQL.
`BytesBatch` is a data-lane input and must retain the same channel category during forwarding.

The decoder's batch-input support must preserve the existing payload decode rules:

- Decode payloads independently, in their original order, using the configured decoder and
  projection. Refresh shared projection state at the batch-input boundary.
- Append successfully decoded rows in payload order and emit one collection for the grouped
  input. Preserve row order within each payload as well.
- Record each payload decode failure through the decoder's existing error handling and continue
  decoding later payloads. Invalid input is not an MQTT connection error.
- Emit no collection when every payload fails. An empty payload vector also produces no output.
- Keep event-time extraction at the decoder boundary and retain the existing row-level
  timestamp validation and error behavior.

The batching limit must not move into the decoder or become a decoded-row admission policy.
No payload is split to enforce the source batch limit.

## Sampler And Lifecycle Compatibility

The existing sampler is placed before decode and therefore needs explicit byte-batch handling:

- `Latest` retains the last payload of a nonempty batch. An empty batch does not replace an
  already retained sample.
- `Packer` passes each payload to the merger in order. It must not concatenate raw payloads or
  pass a batch container as if it were one original message. Preserve successful merge effects
  and report errors through the sampler's existing error path.

Batching changes input grouping and may affect when timer-driven consumers observe available
input. Validation must check the intended sampling policy and ordering; it must not assume
identical scheduling between individual and grouped deliveries.

Every datasource forwarding path must handle `PayloadBatch`, including normal reception,
input draining, and existing checkpoint-fence coordination. A batch must remain in order with
the surrounding fence, error, and terminal events. A continuing checkpoint must not introduce
an extra flush or bypass the source fence. This does not add checkpoint support to a connector
that currently rejects checkpoint requests.

Sending remains backpressured. Await the connector channel's capacity for the emitted event;
do not replace buffered messages, add a lossy queue, or discard a batch because the consumer is
slow. Keep the existing shutdown, reconnection, resubscription, and non-terminal error handling.
The drain must leave protocol events for those existing handlers.

## Connection Ownership And Shared Streams

The batching owner is a standalone MQTT connection, identified by the absence of
`connector_key`. Both MQTT 3.1.1 and MQTT 5 standalone sources are in scope.

`shared=true` and `connector_key` describe different sharing mechanisms. A shared stream can own
a standalone MQTT connector, so its shared ingest runtime may receive the new batch events.
That path must preserve sampler placement, decode behavior, and backpressured fan-out.

A source using `connector_key` continues to consume individual events from the shared MQTT
client. Extending batching into the shared client's subscription and fan-out machinery is
outside this migration.

## Message And Batch Counters

Preserve the meaning of existing counters across the grouped path:

- Source ingress counts N MQTT messages when an input batch contains N payloads.
- Source egress counts N messages after the batch is successfully sent.
- Datasource and passthrough message counters count payloads rather than channel events.
- Existing byte counters sum the original payload lengths. Collection counters continue to
  count decoded rows, which can differ from the number of MQTT messages.

`veloflux_mqtt_source_batches_out_total{flow_instance, connector}` increments once after each
successful payload-event send to the connector's downstream channel. Both `Payload` and
`PayloadBatch` count as one batch. This includes singleton sends when batching is disabled and
individual payloads forwarded from shared MQTT clients. Failed sends, error events, and terminal
events do not increment it. Success describes the channel boundary, not decoder or sink completion.

The counter is bound to zero at subscription creation and accumulates across reconnects and new
subscriptions with the same labels until process restart. Dividing the rate of
`veloflux_mqtt_source_messages_out_total` by the rate of this counter gives the average number of
MQTT payloads per emitted batch. With no emitted batches, the ratio is undefined.

Definitions, label conventions, and an example query are documented in
[Prometheus Metrics Design](../metrics/prometheus.md). Batch-size histograms and maximum-size
gauges remain deferred.

## Validation Plan

Deterministic unit coverage lives in the `batching_tests` modules in `mqtt_protocol.rs`,
`decoder_processor.rs`, `sampler_processor.rs`, and `data_metrics.rs`. It covers queue order,
protocol boundaries, payload limits, large payloads, invalid MQTT 5 topics, decoder recovery,
multi-row payloads, sampling, and existing message/byte counters without depending on network
arrival timing. Complete MQTT-to-SQL-to-sink validation is registered through
`tests/e2e_tests.rs` in `tests/e2e/mqtt_source_batching.rs`. It creates the stream and pipeline
through the management API, establishes readiness with a retained MQTT marker, and sends a
burst of array payloads with an invalid payload in the middle. It verifies computed output
values, completeness, order, decoder error recovery, and trailing deliveries after graceful
stop without requiring a particular observed batch size.

Use existing connector, decoder, sampler, shared-stream, checkpoint, pipeline, and planner
tests where applicable. The following are verification dimensions; they do not claim that
every case already has test coverage. Confirm any new unit-test cases before implementation.

- Preserve payload order and stop before non-PUBLISH events; a queued protocol event remains
  available to the next normal poll.
- Include the first payload in the 1024 limit and leave remaining events queued when the
  limit is reached. A combined payload size above the old 256 KiB limit must not alone stop
  grouping, provided the individual MQTT packets are valid.
- Exercise both MQTT versions, singleton inputs, grouped inputs, and the explicit bypass.
- Compare decoded values and row order for object and array payloads, including invalid
  payloads between valid ones, all-invalid batches, empty input, and projection refreshes.
- Verify event-time extraction, sampler Latest/Packer behavior, and shared-ingest delivery.
- Preserve existing message/byte counts when a batch contains multiple payloads, and count
  source egress only after successful sending.
- Exercise slow consumers, data/control lanes, fence ordering, terminal events, and recovery
  paths without adding batch-specific lifecycle behavior.
- Check the affected public enum call sites and integration-test compilation after adding
  the new event variants; exhaustive matches must be updated even outside MQTT code.

## Performance Interpretation

The 1024 limit is an upper bound, not a target that the source fills. Available batches depend
on rumqttc's buffering and read implementation, the producer's write sizes and schedule, TCP
settings on each connection, broker forwarding, consumer backpressure, and task scheduling.
An empty event queue must still cause immediate forwarding of the available payloads.

The queue-drain policy follows the reference in rekuiper's `drain_buffered_publishes`, but the
counting boundary is deliberately raw MQTT payloads here rather than decoded records there.
Equivalent opportunistic policies do not imply identical observed batch distributions. Compare
actual input timing and publisher write batches before attributing a difference to the source
policy. `TCP_NODELAY` belongs to an individual TCP socket; publisher-to-broker settings and
broker-to-engine settings are separate variables.

No benchmark result or performance threshold is part of this design. The migration must first
preserve processing and delivery semantics; end-to-end performance requires separate evidence.
