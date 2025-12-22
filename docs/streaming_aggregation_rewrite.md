# `streaming_aggregation_rewrite`

This document describes the intent and mechanics of the physical optimization rule
`streaming_aggregation_rewrite` and how it relates to the streaming aggregation processors.

## Motivation

In the physical plan, a typical windowed aggregation can appear as:

- `Window -> Aggregation`

Where:

- `Window` establishes window boundaries (tumbling/count/sliding).
- `Aggregation` computes aggregates (and optional `GROUP BY`) over rows within each window.

Naively executing these as separate nodes often introduces avoidable intermediate buffering and
batch materialization. When aggregates support incremental updates, we can fuse windowing and
aggregation into one streaming operator.

## Rule Overview

`streaming_aggregation_rewrite` is a physical-plan rewrite that:

1. Finds `PhysicalPlan::Aggregation(agg)` nodes.
2. If the aggregation’s upstream node is a supported window type:
   - `PhysicalPlan::TumblingWindow`
   - `PhysicalPlan::CountWindow`
   - `PhysicalPlan::SlidingWindow`
3. And if **all aggregate calls are incremental** (per `AggregateFunctionRegistry`),
4. Then rewrites:

```
Window -> Aggregation
```

into:

```
StreamingAggregation
```

while removing the window node from the plan topology. The new fused node’s child becomes the
window’s original upstream child.

### Incremental Requirement

The rewrite is only valid when every aggregate function used by the query supports incremental
updates (e.g. `count`, `sum`, etc. depending on the registry). If any call is non-incremental,
the rule leaves the plan unchanged.

## How the Fused Node Is Represented

The fused physical node is `PhysicalPlan::StreamingAggregation(PhysicalStreamingAggregation)`.

It carries:

- A `StreamingWindowSpec` describing the window behavior (tumbling/count/sliding).
- The original aggregation’s:
  - `aggregate_calls`
  - `aggregate_mappings`
  - `group_by_exprs`
  - `group_by_scalars`

## Processor Design Mapping

At runtime, `PhysicalStreamingAggregation` is executed by `StreamingAggregationProcessor`,
which dispatches based on `StreamingWindowSpec`:

- `StreamingWindowSpec::Count` → `StreamingCountAggregationProcessor`
  - Data-driven windowing: count rows and flush when the target count is reached.
  - Each incoming tuple incrementally updates accumulators; when the count threshold is met, the
    processor finalizes and emits the aggregated output, then resets window state.

- `StreamingWindowSpec::Tumbling` → `StreamingTumblingAggregationProcessor`
  - Time-driven tumbling windows (processing-time in current implementation).
  - Maintains per-window aggregation state; flushes windows on watermark progression or graceful
    termination.

- `StreamingWindowSpec::Sliding` → `StreamingSlidingAggregationProcessor`
  - Per-tuple triggered sliding windows with optional delay (lookahead).
  - Maintains a deque of active windows; each tuple updates all windows whose range includes it.
  - Emits windows either immediately (delay == 0) or when deadline watermarks arrive (delay > 0).

### Shared Aggregation Logic

For count and tumbling windows, a shared incremental aggregation worker is used:

- It maintains `groups: HashMap<key_repr, GroupState>` where `key_repr` is derived from evaluated
  `GROUP BY` expressions for each tuple.
- For each tuple, it:
  - Evaluates `GROUP BY` scalars to compute the group key.
  - Updates accumulators for each aggregate call with evaluated argument values.
- When a window ends, it finalizes each group into an output tuple and emits a `RecordBatch`.

## Benefits

- Removes intermediate window node buffering and reduces overhead.
- Enables true streaming behavior where updates are applied incrementally per tuple.
- Makes window boundary handling and aggregation state updates local to a single operator,
  improving throughput and reducing allocations when applicable.

## Non-Goals / Current Limitations

- The rewrite currently targets only tumbling/count/sliding windows.
- State-driven windowing (`statewindow`) is not included in this rule yet.
- The rewrite depends on incremental aggregate support in the registry; non-incremental calls
  keep the original `Window -> Aggregation` topology.

