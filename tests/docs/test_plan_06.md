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

### 1. Eventtime hidden on-change tumbling pairing

- Interaction ID: `runtime.eventtime.hidden_on_change_tumbling_pairing`
- Required features:
  - `planner.eventtime.hidden_column_preservation`
  - `source.on_change.gating`
  - `pipeline.runtime.eventtime`
  - `stream.watermark.propagation`
  - `stream.window.tumbling`
- Why existing coverage is insufficient:
  - Planner-side hidden-column preservation is already covered by
    `explain_pipeline_with_eventtime_enabled_keeps_hidden_eventtime_column_alive`.
  - Runtime-side source-on-change plus eventtime late-drop behavior is now covered by
    `eventtime_tumbling_window_with_on_change_gate_drops_late_rows_after_watermark`.
  - The interaction still does not have a single testcase that proves both planner preservation and
    runtime behavior in the same coverage record.
- Recommended testcase shape:
  - Keep planner and runtime assertions split across the existing planner explain suite and runtime
    pipeline suite unless a dedicated same-record interaction testcase becomes necessary.
- Expected assertions:
  - Planner-side assertions should keep hidden `event_ts` alive in schema and decoder metadata.
  - Runtime-side assertions should continue to prove source gating, watermark advancement, and
    late-row dropping.

### 2. Shared-tail barrier alignment

- Interaction ID: `runtime.shared_tail.barrier_alignment`
- Required features:
  - `planner.physical.shared_tail_barrier_insertion`
  - `processor.barrier.alignment`
- Why existing coverage is insufficient:
  - Planner-side barrier insertion is already covered by the explain suite.
  - Runtime-side graceful-close alignment is now covered by
    `shared_tail_barrier_graceful_close_flushes_batched_sibling_before_shutdown`.
  - The interaction still remains split across planner and runtime records rather than one combined
    same-record testcase.
- Recommended testcase shape:
  - Keep planner insertion and runtime barrier alignment in their current specialized suites unless
    a single same-record interaction testcase becomes worth the extra harness complexity.
- Expected assertions:
  - Planner-side assertions should continue to prove `PhysicalBarrier` insertion.
  - Runtime-side assertions should continue to prove that graceful shutdown does not terminate the
    shared tail before the batched sibling flushes.
