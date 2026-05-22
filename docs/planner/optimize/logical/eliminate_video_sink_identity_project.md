# EliminateVideoSinkIdentityProject

## Overview

`EliminateVideoSinkIdentityProject` removes a logical `Project` that only preserves the complete
video tuple immediately before a single video sink.

The purpose is to avoid per-frame tuple materialization on the video passthrough hot path. Video
frames can be large and frequent, and even shallow per-row `Tuple` / `RecordBatch` rebuilding adds
unnecessary CPU and allocation overhead when the projection does not change the sink-facing data.

Implementation: `src/flow/src/planner/logical_optimizer.rs`
(`EliminateVideoSinkIdentityProject`).

## Rule Placement

The rule runs after:

- `CommonSubexpressionElimination`
- `TopLevelColumnPruning`
- `StructFieldPruning`
- `ListElementPruning`

It is intentionally late in the logical optimizer sequence, so pruning and decode-projection rules
can still inspect the SQL projection before the no-op video project is removed.

## Inputs (What It Recognizes)

The rule matches:

```text
Tail
└─DataSink(connector=video, encoder=none)
  └─Project(identity video tuple)
    └─single video-source branch
```

The data sink branch must satisfy:

- there is a single `video` sink consumer for the project branch
- `forward_to_result` is disabled for that sink
- the project child contains exactly one `DataSource`
- that data source has `stream_type=video`

The project must be one of:

```sql
SELECT * FROM camera
```

or a complete identity projection of the video tuple:

```sql
SELECT payload, width, height, format, timestamp FROM camera
```

Qualified column references are accepted when they still preserve the output column names:

```sql
SELECT camera.payload, camera.width, camera.height, camera.format, camera.timestamp FROM camera
```

## Outputs (What It Produces)

The rule rewrites:

```text
DataSink(video)
└─Project([payload, width, height, format, timestamp])
  └─DataSource(video)
```

into:

```text
DataSink(video)
└─DataSource(video)
```

This means the physical plan also avoids building a `PhysicalProject` for the video passthrough
branch.

## Safety Conditions

The rule does not run when the projection changes the sink-facing tuple shape. Examples that are
not eliminated:

```sql
SELECT payload FROM camera
```

```sql
SELECT payload AS frame, width, height, format, timestamp FROM camera
```

```sql
SELECT payload, width + 1 AS width, height, format, timestamp FROM camera
```

These projections either remove required video metadata, rename columns, or compute new values.
They must remain visible to planner validation and runtime execution.

The rule also does not rewrite branches with multiple sources or non-video sources.

## Interaction With Video Sink Validation

This optimization does not relax the video sink contract. The physical planner still validates that
the final sink input contains:

- `payload: bytes`
- `width: integer`
- `height: integer`
- `format: string`
- `timestamp: timestamp`

If a query such as `SELECT payload FROM camera` targets a video sink, validation still fails because
the sink-facing tuple is incomplete.

## Explain Example

For:

```sql
SELECT * FROM camera
```

with a video sink, optimized explain omits both logical and physical project nodes:

```text
Logical:
Tail
└─DataSink(connector=video)
  └─DataSource(source=camera)

Physical:
PhysicalResultCollect
└─PhysicalDataSink(connector=video)
  └─PhysicalDataSource(source=camera)
```

For the same source sent to a regular JSON sink, the ordinary projection and encoder-side
optimization rules still apply. This rule is specific to direct video tuple sinks.
