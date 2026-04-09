# Test Plan 02 Covered Cases

## Scope

- This document tracks only the cases that were newly covered for the scope discussed in `tmp/test_plan_02.md`.
- The counted coverage comes from:
  - `src/flow/tests/planner/**`
  - `src/flow/tests/pipeline/**`
  - `tests/e2e/**`
- This is a covered-cases inventory for the newly added tests, not a full gap analysis.

## Planner Physical

### Eventtime hidden-column preservation

- Eventtime hidden-column preservation under simple projection is covered by `explain_eventtime_simple_projection_keeps_decoder_eventtime_column`.
- This case validates that:
  - `PhysicalDecoder` still carries `eventtime.column=event_ts`
  - the decoder schema still retains the eventtime column
  - top-level projection of `a` does not prune away the decoder-side eventtime requirement

## Runtime Pipelines

### Eventtime runtime semantics

- Out-of-order eventtime input handling is covered by `eventtime_tumbling_window_orders_out_of_order_input_before_flush`.
- This case validates that a tumbling eventtime window still produces the correct aggregate when tuples arrive out of order within the allowed late-tolerance range.

- Late-event dropping after watermark advancement is covered by `eventtime_tumbling_window_drops_tuple_older_than_current_watermark`.
- This case validates that:
  - a later event can advance the watermark and flush the older window
  - a tuple older than the current watermark is dropped
  - the dropped late tuple does not alter the next flushed window result

### Shared stream dynamic decode lifecycle

- Startup readiness for a new consumer is covered by `shared_stream_new_consumer_waits_for_required_columns_before_first_tuple`.
- This case validates that a newly started consumer does not observe initial `NULL` output before its required decoding columns are applied to the shared stream.

- Required-column shrink after consumer stop is covered by `shared_stream_required_columns_shrink_after_consumer_stop`.
- This case validates that:
  - shared-stream decoding columns expand to the union required by active consumers
  - the decoding column set shrinks after one consumer stops
  - the remaining consumer continues to produce correct output after the shrink

- Wildcard-driven full decode is covered by `shared_stream_wildcard_consumer_forces_full_decode_until_it_stops`.
- This case validates that:
  - a `SELECT *` consumer forces the shared stream into full-schema decoding while it is active
  - decoding columns fall back to the narrower required set after the wildcard consumer stops

## REST / E2E

### Shared stream stats endpoint semantics

- Rejection of non-shared streams is covered by `shared_stream_stats_rejects_non_shared_stream_via_rest`.
- Unknown-stream handling is covered by `shared_stream_stats_returns_404_for_unknown_stream`.
- Stopped-runtime status reporting is covered by `shared_stream_stats_reports_stopped_status_after_last_consumer_stops`.
- Together, these cases extend shared-stream stats coverage beyond the running happy path and validate endpoint-level error and status semantics.

### Pipeline stats endpoint semantics

- Stopped-pipeline rejection is covered by `pipeline_stats_returns_400_for_stopped_pipeline_via_rest`.
- Internal/runtime-boundary filtering is covered by `pipeline_stats_excludes_internal_and_shared_ingest_processors_via_rest`.
- These cases validate that the REST layer preserves the documented pipeline-stats contract by:
  - returning `400` for non-running pipelines
  - excluding internal processors such as `control_source`
  - excluding result-collect processors
  - excluding shared-ingest runtime processors from pipeline-local stats output
