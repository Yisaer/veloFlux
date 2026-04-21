# Shared Stream Metrics

## Background

A shared stream owns a manager-managed ingest runtime that is independent from any one downstream
pipeline.

That runtime is responsible for:

- starting lazily when the first consumer subscribes
- stopping when the last consumer leaves
- accepting decoded output from the internal shared ingest pipeline
- publishing each shared-stream message into the in-process fan-out hub

Because this runtime is a first-class resource, it needs its own Prometheus metrics instead of
forcing operators to reconstruct all shared-stream health from downstream pipeline metrics alone.

## Design Boundary

The shared stream metrics subsystem must describe the shared runtime resource itself.

It must not duplicate the processor subsystem.

This means the observability boundary is:

- lifecycle of the shared runtime
- number of active subscribers
- message publish activity at the shared fan-out boundary
- runtime errors owned by shared stream management

The following remain processor concerns and must continue to use the `processor` subsystem:

- row-level `records_in_total`
- row-level `records_out_total`
- processor `errors_total`
- processor `handle_duration_seconds`
- processor `send_backpressure_waits_total`

## Naming And Labels

Subsystem:

- `shared_stream`

Labels:

- `flow_instance`
- `key`

The `key` label is the shared stream name.

When stable error classification is needed, add:

- `kind`

No `stream` alias label should be introduced because `key` is already the canonical label for
manager-owned shared runtime resources.

## Metric Set

### `veloflux_shared_stream_running`

Type:

- gauge

Labels:

- `flow_instance`
- `key`

Semantics:

- `1` when the shared stream runtime is currently running
- `0` otherwise

This metric is intended to make lazy start and zero-subscriber reclaim behavior visible at a
glance.

### `veloflux_shared_stream_active_subscribers`

Type:

- gauge

Labels:

- `flow_instance`
- `key`

Semantics:

- current number of registered shared-stream consumers

This metric reflects fan-out attachment state, not downstream queue depth.

### `veloflux_shared_stream_starts_total`

Type:

- counter

Labels:

- `flow_instance`
- `key`

Semantics:

- increments once for each successful runtime start transition

This metric should increase on the first lazy start and on later restarts after the runtime was
reclaimed.

### `veloflux_shared_stream_errors_total`

Type:

- counter

Labels:

- `flow_instance`
- `key`
- `kind`

Semantics:

- counts shared stream runtime errors owned by the shared runtime resource

Recommended low-cardinality `kind` values:

- `start`
- `forward`
- `stop`
- `consumer`

The `kind` label must remain stable and low-cardinality.

### `veloflux_shared_stream_messages_in_total`

Type:

- counter

Labels:

- `flow_instance`
- `key`

Semantics:

- counts messages accepted by the shared stream runtime from the internal shared ingest pipeline

This is a shared-runtime boundary metric. It does not count rows.

One emitted shared-stream item counts as one message, even if that item contains multiple rows.

### `veloflux_shared_stream_messages_out_total`

Type:

- counter

Labels:

- `flow_instance`
- `key`

Semantics:

- counts successful publish events from the shared stream runtime into the fan-out hub

This counter increments once per publish event, regardless of how many subscribers are currently
attached.

This metric intentionally does not count per-subscriber deliveries.

## Why `messages_*` Instead Of `records_*`

The `processor` subsystem already uses `records_*` for row-level throughput.

Using `records_in_total` / `records_out_total` in `shared_stream` for publish-event counts would
create a unit mismatch that is easy to misread during incident analysis.

The shared stream subsystem therefore uses `messages_*_total` to make the unit explicit:

- processor metrics count rows
- shared stream metrics count publish events at the runtime boundary

## Diagnostic Value

These metrics are intended to answer high-level questions quickly:

- Is the shared runtime up right now
- How many consumers are attached
- Did the shared runtime restart repeatedly
- Is the shared runtime receiving messages from its internal ingest pipeline
- Is the shared runtime successfully publishing those messages into fan-out

Typical interpretations:

- `running = 0` and `active_subscribers = 0`
  - the shared runtime is currently reclaimed or not yet started
- `messages_in_total` increases while `messages_out_total` does not
  - the shared runtime accepted messages from ingest but did not publish them successfully into the
    fan-out boundary
- `messages_out_total` increases while one downstream pipeline does not observe input growth
  - the shared runtime already published the message, so the remaining problem is more likely in a
    downstream consumer path

## Non-Goals

This metrics set intentionally does not expose:

- per-subscriber delivery counters
- per-subscriber queue depth
- per-column decode state metrics
- row-level aggregation for shared-stream fan-out

If those become necessary later, they should be introduced as separate metrics with clear units and
clear subsystem ownership instead of overloading the shared stream resource counters.
