# Test Plan 06 Interaction Pairings

## Scope

- This document continues the planner/runtime pairing extraction after `test_plan_01.md` through
  `test_plan_04.md`.
- The goal is to separate three outcomes for interaction-oriented coverage work:
  - interactions that already exist in tests and only need registry entries
  - interactions whose testcase already exists but whose `covers` annotation is incomplete
  - interactions that still need a new testcase

## Registry Additions Backed by Existing Tests

- `runtime.eventtime.on_change_tumbling`
  - Backed by `eventtime_tumbling_window_with_on_change_gate_suppresses_unchanged_rows`.
  - This testcase already validates source on-change gating together with eventtime execution,
    watermark advancement, and tumbling-window output.

- `runtime.shared_source.sampler_dynamic_decode_lifecycle`
  - Backed by `shared_stream_sampler_dynamic_decode_lifecycle_keeps_outputs_in_sync`.
  - This testcase already validates sampler behavior together with shared-source dynamic decode
    updates and lifecycle changes.

- `runtime.memory.bytes_input_batching`
  - Backed by `memory_source_bytes_topic_batches_json_output`.
  - This testcase already validates bytes-topic memory input together with sink batching.

- `runtime.memory.collection_layout_normalize_materialize_alias`
  - Backed by `memory_collection_source_layout_normalize_then_collection_sink_materialize_aliases`.
  - This testcase already validates collection-source layout normalization together with collection
    sink materialization and alias-visible output.

## Existing Tests That Needed `covers` Fixes

- `runtime.stateful.memory_output_batching`
  - Backed by `stateful_projection_with_batched_streaming_encoder`.
  - The testcase already exercised batching via `batch_count=2` and asserted two staged output
    payloads, but the previous `covers` list did not include `sink.output.batching`.

- `runtime.memory.collection_row_diff_omit_if_empty`
  - Backed by `memory_collection_sink_delta_omit_if_empty_suppresses_unchanged_collection`.
  - The testcase already exercised collection sink materialization, row-diff delta output, and
    omit-if-empty suppression, but the previous `covers` list omitted
    `sink.memory_collection.materialize` and `sink.output.row_diff`.

## New Interaction Testcases Still Needed

### 1. Row-diff batching partial late materialization

- Interaction ID: `runtime.row_diff_batching_partial_late_materialization`
- Required features:
  - `sink.output.row_diff`
  - `sink.output.batching`
  - `planner.physical.by_index_projection_into_row_diff_rewrite`
  - `planner.physical.partial_by_index_row_diff_and_encoder_rewrite`
- Why existing coverage is insufficient:
  - `row_diff_with_batch_keeps_row_diff_and_rewrites_to_streaming_encoder` validates row-diff plus
    batching, but not partial late materialization.
  - `splits_partial_late_materialization_between_row_diff_and_encoder` validates partial late
    materialization, but not batching.
- Recommended testcase shape:
  - Use a query such as `SELECT a, b, flag AS c FROM stream`.
  - Configure `delta_with_columns(["b"])` and sink batching.
  - Feed rows where `b` stays unchanged for one row while `flag` changes every row.
- Expected assertions:
  - The first row emits `a`, `b`, and `c`.
  - The middle unchanged-`b` row is still emitted in the same batch but omits `b` while keeping
    encoder-owned `c`.
  - The later changed-`b` row emits `b` again.
  - Batched output keeps row order and does not collapse encoder-owned pass-through columns.

### 2. Eventtime hidden on-change tumbling pairing

- Interaction ID: `runtime.eventtime.hidden_on_change_tumbling_pairing`
- Required features:
  - `planner.eventtime.hidden_column_preservation`
  - `source.on_change.gating`
  - `pipeline.runtime.eventtime`
  - `stream.watermark.propagation`
  - `stream.window.tumbling`
- Why existing coverage is insufficient:
  - `eventtime_tumbling_window_with_on_change_gate_suppresses_unchanged_rows` proves the runtime
    composition, but it does not explicitly establish the hidden-column preservation claim strongly
    enough for the planner/runtime pairing target.
- Recommended testcase shape:
  - Use a stream schema with visible columns `speed`, `rpm` and hidden eventtime column
    `event_ts`.
  - Query only `speed`, configure `on_change_with_columns(["rpm"])`, and enable eventtime.
  - Publish rows where `rpm` changes less frequently than `speed`, and advance watermark with a
    later event.
- Expected assertions:
  - Rows whose `rpm` does not change are suppressed before aggregation.
  - Window output reflects only the rows admitted by the source gate.
  - The eventtime window still flushes correctly even though `event_ts` is not projected.
  - The testcase should also assert that late data older than the current watermark is ignored.

### 3. Shared-tail barrier alignment

- Interaction ID: `runtime.shared_tail.barrier_alignment`
- Required features:
  - `planner.physical.shared_tail_barrier_insertion`
  - `processor.barrier.alignment`
- Why existing coverage is insufficient:
  - The planner explain suite validates barrier insertion.
  - Processor unit tests validate barrier alignment semantics.
  - There is still no end-to-end testcase proving that a multi-sink pipeline with a shared tail
    keeps downstream consumers synchronized because of the inserted barrier.
- Recommended testcase shape:
  - Build a two-sink pipeline whose shared tail forwards to result collection on both branches.
  - Introduce staggered downstream readiness or controlled branch delay so one branch would race
    ahead without barrier coordination.
- Expected assertions:
  - The shared tail does not forward the barrier downstream until both branches reach the same
    barrier.
  - Downstream observation order remains synchronized across both consumers.
  - No branch leaks post-barrier data before the matching barrier from the other branch arrives.
