# StreamingAggregationRewrite

## Overview

`StreamingAggregationRewrite` is a physical-plan rewrite that fuses:

```
Window -> Aggregation
```

into a single streaming operator:

```
StreamingAggregation
```

The goal is to avoid intermediate buffering/materialization between windowing and aggregation
when aggregate updates can be applied incrementally per tuple.

Implementation: `src/flow/src/planner/optimizer.rs` (`StreamingAggregationRewrite`).

## Inputs (What It Recognizes)

This rule matches `PhysicalPlan::Aggregation(agg)` where the aggregation’s (first) child is one of:

- `PhysicalPlan::TumblingWindow`
- `PhysicalPlan::CountWindow`
- `PhysicalPlan::SlidingWindow`
- `PhysicalPlan::StateWindow`
- `PhysicalPlan::EosWindow`

## Preconditions (When It Is Safe)

The rewrite is applied only when **all aggregate calls are incremental**:

- `PhysicalAggregation::all_calls_incremental(&agg.aggregate_calls, &AggregateFunctionRegistry)`

If any call is non-incremental, the plan is left unchanged.
For EOS windows, the semantic fallback remains `EosWindowStage` followed by
`AggregationStage`. These stages may still share an execution region: this aggregation
rewrite and task/channel placement are separate decisions.

## Outputs (What It Produces)

When the match and preconditions hold, the rule replaces the `Window -> Aggregation` chain with
`PhysicalPlan::StreamingAggregation(PhysicalStreamingAggregation)`:

- `StreamingWindowSpec` is derived from the window node:
  - `Tumbling { time_unit, length }`
  - `Count { count }`
  - `Sliding { time_unit, lookback, lookahead }`
  - `State { open_expr, emit_expr, partition_by_exprs, ... }`
  - `Eos`
- The fused node inherits aggregation fields:
  - `aggregate_calls`, `aggregate_mappings`
  - `group_by_exprs`, `group_by_scalars`
- The fused node’s child becomes the window’s original upstream child (i.e. the window node is
  removed from the topology).
- The fused node keeps the aggregation node’s plan index (`agg.base.index()`), which stabilizes
  processor IDs and explain output across the rewrite.

## Runtime Mapping

At runtime, `PhysicalStreamingAggregation` is executed by the region executor and
dispatched by `StreamingWindowSpec`:

- `Count` → `StreamingCountStage`
- `Tumbling` → `StreamingTumblingStage`
- `Sliding` → `StreamingSlidingStage`
- `State` → `StreamingStateStage`
- `Eos` → `StreamingEosStage`

See `src/flow/src/processor/streaming_aggregation_processor.rs` and the concrete processor
implementations under `src/flow/src/processor/`.

For `Eos`, the streaming processor updates aggregate state for each incoming collection and
finalizes exactly once when it receives a graceful terminal signal on the data channel. Quick
terminal signals and terminal signals received on the control channel are forwarded without
finalizing aggregate state.

### Group Key Equality

Ordinary and streaming aggregation both group by the evaluated `Vec<Value>`,
using `Value` equality and hashing rather than Debug-formatted strings. For
each floating-point type, positive and negative zero belong to the same group,
as do all NaN representations. NULL keys also belong to the same group.
Values of different numeric types remain distinct keys; grouping does not
implicitly coerce them to a common type.

The rewrite therefore preserves group identity. Structured keys remove Debug
formatting and its String allocation, but evaluating group keys still builds
a value vector.

## Bound Aggregate Execution

Count, tumbling, state and EOS streaming aggregation share one group map and
one row transaction: prepare every call, then commit all updates only if every
prepare succeeds. Failed rows do not create empty groups or replace the last
successful row. Finalization, output construction and clearing use the same
code for all registered aggregate implementations.

When a stage is constructed, each call binds its registered function and its
argument access. Direct indexed columns are borrowed, while other expressions
use the existing expression evaluator. Registry entries own the internal execution
metadata: builtin COUNT, SUM, AVG, MIN and MAX use concrete accumulator factories;
other entries keep the existing boxed accumulator interface. The public aggregate
trait does not expose this builtin execution mechanism. Public `register_function`
registrations always use the boxed interface, including builtin-name overrides.
Function names and inline-placement flags do not select concrete execution.
The bound factory also determines whether wildcard arguments can use a non-null
sentinel, so workers do not classify aggregate implementations themselves.

COUNT, SUM, AVG, MIN and MAX provide concrete accumulators using the same typed
prepare/commit methods as their boxed adapters. Calls using the boxed interface
can coexist with concrete calls and expression arguments in the same row
transaction. There is no whole-query fast/generic mode. Only a concrete COUNT
can avoid materializing its wildcard argument; other implementations still
receive the original wildcard value.

Window and partition workers share immutable bindings, not accumulator state.
The bound implementations remain fixed for the lifetime of the stage. Row-update
and expression-argument buffers retain capacity but release values on success,
error and clear. Group-key vectors, new groups, boxed implementation updates and
expression results can still allocate; this is not a zero-allocation guarantee.

The ignored `aggregation_row_cost_comparison` test measures local release-mode
row-update and finalization costs. It is not an end-to-end throughput or
allocation benchmark and has no timing assertion in CI.

```sh
cargo test --release -p flow --lib aggregation_row_cost_comparison -- --ignored --nocapture
```

### Local Row-Cost Comparison

Measured on 2026-10-20 with rustc 1.96.0, release profile, pinned to CPU 0.
The original PR at `617dc297` and the unified executor ran the same test,
alternating old/new twice. Each run reports the median of seven samples; each
sample processes eight windows of 16,384 prebuilt rows and finalizes their
groups. The following values average the two per-run medians, in ns/row:

| Groups | Aggregate calls | Original PR | Unified | Change |
| --- | --- | --- | --- | --- |
| Global | 1 | 64.5 | 48.4 | -25% |
| Global | 5 | 103.2 | 94.0 | -9% |
| 16 | 1 | 83.2 | 79.0 | -5% |
| 16 | 5 | 122.8 | 123.4 | +0.5% |
| 4096 | 1 | 182.5 | 187.3 | +3% |
| 4096 | 5 | 362.3 | 365.7 | +1% |

One call means SUM; five calls mean COUNT, SUM, AVG, MIN and MAX over a direct
column. Grouped cases are close, including small regressions in these local
measurements. This does not establish a universal no-regression claim, cover
expression/custom-call performance, or replace end-to-end W2/W4 measurements.

## Worker Allocation Optimization

The implementation is scoped to the shared `AggregationWorker` in
`streaming_aggregation_processor.rs`, used by:

- `streaming_tumbling_aggregation_processor.rs`
- `streaming_count_aggregation_processor.rs`
- `streaming_state_aggregation_processor.rs`
- `streaming_eos_aggregation_processor.rs`

The four stages inherit the optimization through the worker without
changing their window, partition, trigger, or terminal semantics. Their source
files do not require changes. Sliding aggregation,
ordinary aggregation, Project, partition-key evaluation, and batch-function
evaluation are outside this work. The existing `RowScratch` argument and update
buffers remain in use.

### Step 1: Reuse Group Keys

Each worker keeps a reusable `Vec<Value>` query buffer. It evaluates group-by
expressions into it and queries `HashMap<Vec<Value>, GroupState>` using a borrowed
`&[Value]`. This preserves the existing `Value::Eq` and `Value::Hash` rules, including
NULL, per-type NaN equivalence, signed-zero equivalence, and separation between
numeric types. The representation supports multiple columns; a single-key
specialization is deferred until profiling justifies it.

On an existing-group hit, the worker prepares and commits all aggregate updates
before changing the last successful row or its key. Only after success, it exchanges the
query buffer with the state's optional latest-key vector. Equal keys can have
different representations, such as positive and negative zero or different NaN
bit patterns, so retaining the latest successful key remains necessary.

For a new group, the worker prepares and commits its updates before inserting it,
then moves the query key into the map without cloning it into the state. The state starts
without a latest-key vector. A subsequent successful hit supplies that vector.
Finalization selects the state's latest key when present, otherwise the map key.
This selection preserves the existing output construction for simple and
computed group-by expressions.

Key evaluation or aggregate preparation failures leave existing groups,
their latest keys, and their last successful rows unchanged. Failed new rows
do not insert groups. Temporary key values are released after success, error, and
worker clearing while reusable capacity is retained where ownership permits.
Each window or partition owns its mutable buffers independently.

Moving a buffer into a new group, or into a state on its first successful hit,
can require a replacement allocation on the next row. The intended benefits are
fewer allocations on repeated hits and no unconditional second key vector for
new groups, not allocation-free processing. Global aggregation already uses an
empty key vector and does not incur a heap allocation for that vector.

### Step 2: Share Aggregation Output Names

Fixed aggregate output names and computed group-by output names are prepared as
`Arc<str>` during worker binding and shared across derived workers. This
output metadata lives in worker bindings so the shared `GroupByMeta` helper does
not force changes to the separate sliding-aggregation implementation.

The crate-internal `AffiliateRow::insert_shared` entry point accepts a borrowed
`Arc<str>` and clones it only when adding a new slot. Finalization accesses it
through the existing `Tuple::affiliate_mut` entry point. The public `Arc<String>`
insertion API remains compatible; both paths use one insertion implementation
and avoid allocating names on overwrite. Finalization reuses bound names instead
of constructing per-group strings or an intermediate entry vector.
`AffiliateRow::new` duplicate-name behavior is unchanged.

Aggregate columns are still inserted before computed group-by columns, in
their existing order. Overwriting a name retains its original slot. The
planner's fixed `OutputLayout` indexes remain unchanged. Each output row retains
ownership of its affiliate index, keys, and values; these containers are not
cross-row scratch buffers. Project also uses the shared-name entry point with
its existing `PhysicalProjectField::field_name`; see
[Plan-Fixed Output Layout](../../performance/plan_fixed_output_slots.md).

### Validation Targets and Pending Measurement

Confirm concrete cases and expected results with the maintainer before adding
unit tests. The intended semantic coverage is:

- Repeated and new keys, including multi-column keys and empty global keys,
  preserve aggregate results and the last successful row.
- An existing-group update failure preserves all accumulators, the latest key,
  and the last successful row; a failed new-group update creates no group.
- NULL keys group together. Different NaN representations group within each
  floating-point type. Signed zeros group together. Different numeric types
  remain distinct. Computed group-by output retains the latest successful key's
  representation, including after a later failed row.
- Inserting affiliate names in the order `a`, `b`, `a` retains slots `a`, `b`
  and replaces only the value in slot `a`. Aggregation output order and fixed
  output-layout resolution remain unchanged.
- Tumbling, count, state, and EOS execution retain their existing lifecycle
  behavior through the shared worker. Run affected existing stage tests and the
  planner suite under `src/flow/tests/planner/`.

Group-key handling and shared-name output are separate implementation changes
and should be measured independently when evaluating their contributions.
Compare a baseline, the key-only change, and the additional shared-name change
using the same PR Action W2/W4 workloads. Inspect CPU, profiles, processed rows,
and source batch mean together; use CPU per processed row when comparing runs
with different processing volume. Record allocation measurements if available.
Do not assume a CPU reduction from the implementation alone, and do not run
`cargo bench` locally for this work. No W2/W4 performance measurements are
recorded for this optimization yet.

## Explain / Tests

The rewrite is covered by table-driven planner explain tests in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

Example cases include:

- `stateful_before_window_and_aggregation`
- `optimize_rewrites_streaming_agg_for_state_window`
- `eos global aggregate from history table`
- `explain_ndv_countwindow_non_incremental` (negative case)

## Limitations

- Only rewrites when the aggregation is **directly** on top of a supported window node.
- Only checks the aggregation’s first child (aggregations are expected to be unary in the
  current physical plan).
