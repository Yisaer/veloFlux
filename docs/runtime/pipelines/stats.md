# Pipeline Stats Design

## Background

Pipeline stats are the manager-visible snapshot of per-processor runtime counters for one user
pipeline.

The purpose is operational visibility into the currently running processor graph, not a general
metrics export replacement and not a shared-stream observability API.

## Goals

- Define the scope and meaning of pipeline processor stats.
- Document the stable metric fields and custom-metric extension model.
- Clarify timeout and error behavior for stats collection.
- Explain the relationship between pipeline stats and shared-stream stats.

## Non-Goals

- Prometheus exposition details.
- Endpoint-by-endpoint API tutorial.
- Aggregated observability across multiple pipelines or flow instances.

## Scope Of Pipeline Stats

Pipeline stats are scoped to one pipeline id in one flow instance runtime.

They describe the processors that belong to the user pipeline runtime only. They do not include:

- shared-stream ingest processors
- synthetic aggregation across multiple pipelines
- control-plane state that exists only in storage

The pipeline must currently be running for stats collection to succeed.

## Processor Stats Model

Each response entry is keyed by `processor_id` and contains a snapshot object.

The stable base fields are:

- `records_in`
- `records_out`
- `error_count`
- `last_error`

These fields are part of the processor runtime contract and should remain the primary assertions in
tests and diagnostics.

## Metric Categories

Pipeline stats support two metric categories:

- stable base counters/errors listed above
- processor-specific custom metrics flattened into the same JSON object

Custom metrics are registered with:

- a stable metric id
- a flattened response field name
- a metric kind (`gauge` or `counter`)

The flattened field name must not collide with reserved base names. This keeps the JSON payload
simple while preserving extension room for processors such as empty suppression or windowing.

## Snapshot Semantics

Stats are a point-in-time read of the current in-memory processor counters.

Important implications:

- stats are not persisted across restarts
- stats reset when a runtime pipeline is rebuilt
- the response reflects the currently installed runtime graph, not historical executions

For in-process and worker backends, manager returns the same logical snapshot shape even though the
collection path differs.

## Timeout And Snapshot Semantics

The current collection query accepts `timeout_ms` and defaults to 5000 milliseconds.

Current behavior:

- in-process collection forwards the timeout to flow runtime collection
- worker collection forwards the timeout to the worker HTTP API
- timeout is surfaced as HTTP `504 Gateway Timeout`

Today, the in-process flow runtime only collects stats from already-running pipelines, so timeouts
mainly protect the manager/worker request path and future-proof the API contract rather than
wrapping a long historical aggregation job.

## Relationship To Shared Stream Stats

Pipeline stats and shared-stream stats intentionally model different ownership boundaries.

Pipeline stats cover:

- processors instantiated for one user pipeline

Shared-stream stats cover:

- processors owned by a shared ingest runtime

Manager therefore keeps them separate. Shared-stream processor visibility belongs to
`docs/api/streams/shared_stream_stats.md` and related runtime shared-stream designs, not to the
pipeline stats response.

## Processor Filtering

Manager filters out some internal processors before returning pipeline stats:

- `control_source`
- `PhysicalResultCollect_*`

The goal is to keep the payload focused on user-meaningful runtime work instead of framework
bookends. Tests should therefore assert stats on semantic processors such as decoder, project,
watermark, sampler, sink, and similar stages.

## Error Reporting

Current error surface:

- `404` when the pipeline id is unknown
- `400` when the pipeline exists but stats cannot be collected, for example because the pipeline is
  not running
- `504` on timeout

Processor-local failures do not turn the whole stats request into an error automatically. They are
reported through each entry's `error_count` and `last_error`.

## Testing Guidance

- Verify basic counter progression for a simple running pipeline.
- Verify `error_count` and `last_error` update when a processor records runtime errors.
- Verify custom flattened metrics appear alongside the base fields without reserved-name
  collisions.
- Verify filtered processors (`control_source`, `PhysicalResultCollect_*`) are absent from the
  returned list.
- Verify collecting stats for a stopped pipeline returns an error rather than stale counters.
- Verify timeout handling maps to `504` consistently for both in-process and worker-backed
  pipelines.
- Verify shared-stream ingest metrics are not mixed into normal pipeline stats.

## Future Work

- If pipeline stats later need historical or persisted semantics, that should be introduced as a
  separate observability layer rather than changing the meaning of the current snapshot response.
