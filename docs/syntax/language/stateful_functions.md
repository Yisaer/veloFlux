# Stateful Functions

## Background

veloFlux supports three different function categories in stream SQL:

- Pure scalar expressions evaluated per row.
- Aggregate functions evaluated over windows or batches.
- Stateful functions evaluated per row, but with internal state carried across rows.

A stateful function consumes the current row together with its own historical state and returns a
scalar value for the current row.

Example:

```sql
SELECT lag(a) FROM stream
```

`lag(a)` returns the previous value of `a` in the stream.

Stateful functions are similar to aggregates in that they require a registry and a dedicated
execution stage, but they differ in two important ways:

- They are evaluated for every input row.
- Their execution order must match expression dependency order.

## Supported Syntax

The current supported shape is:

```sql
stateful_fn(arg1, arg2, ...)
    [FILTER (WHERE boolean_expr)]
    [OVER (PARTITION BY expr1, expr2, ...)]
```

Examples:

```sql
SELECT lag(a) FROM stream
```

```sql
SELECT lag(a) FILTER (WHERE flag = 1) FROM stream
```

```sql
SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) FROM stream
```

Built-in stateful functions currently include:

- `lag`
- `latest`
- `changed_col`
- `had_changed`

## Built-in Functions

### `lag`

Signature:

```sql
lag(x [, offset [, ignore_null]])
```

Semantics:

- `offset` defaults to `1` and must be a positive integer.
- `ignore_null` defaults to `true`.
- `offset` and `ignore_null` are static literal configuration parameters.
- When `ignore_null = true`, a `NULL` current value does not advance the lag buffer.
- When `should_apply = false`, `lag` returns the current visible lag value and does not advance the
  lag buffer.

Examples:

```sql
SELECT lag(a) AS prev_a FROM stream
```

```sql
SELECT lag(a, 2, true) AS prev_a2 FROM stream
```

### `latest`

Signature:

```sql
latest(x)
```

Semantics:

- Tracks the latest accepted non-`NULL` value.
- `NULL` input does not overwrite the tracked state.
- If no tracked value exists yet, returns `NULL`.
- When `should_apply = false`, `latest` returns the tracked value if present; otherwise it returns
  `NULL`.

Example:

```sql
SELECT latest(status) AS latest_status FROM stream
```

### `changed_col`

Signature:

```sql
changed_col(ignore_null, x)
```

Semantics:

- Emits `x` only when it differs from the previous accepted value.
- `ignore_null` is a static boolean literal configuration parameter.
- The first accepted row is treated as changed and returns the current value.
- Returns `NULL` when the value does not change.
- When `ignore_null = true`, `NULL` does not update the tracked state.
- When `should_apply = false`, returns `NULL` and does not update the tracked state.

Example:

```sql
SELECT changed_col(true, status) AS status_change FROM stream
```

### `had_changed`

Signature:

```sql
had_changed(ignore_null, x1, x2, ...)
```

Semantics:

- Tracks one previous value per argument position after `ignore_null`.
- `ignore_null` is a static boolean literal configuration parameter.
- The first accepted row is treated as changed and returns `true`.
- Returns `true` if any tracked argument changes on the current accepted row.
- Returns `false` if no tracked argument changes.
- When `ignore_null = true`, `NULL` arguments are skipped and do not update tracked state.
- When `should_apply = false`, returns `false` and does not update tracked state.

Example:

```sql
SELECT had_changed(true, status, code) AS changed FROM stream
```

## Restrictions

For stateful functions, the current implementation supports only:

- Positional expression arguments.
- `FILTER (WHERE <expr>)`.
- `OVER (PARTITION BY <expr> [, <expr> ...])`.

The following are rejected:

- `DISTINCT`.
- Function-level `ORDER BY`.
- `OVER ()` without `PARTITION BY`.
- `OVER (ORDER BY ...)`.
- Window frames inside `OVER`.
- Named windows.
- Aggregate functions, window functions, or unresolved nested stateful functions inside a single
  stateful call's argument/filter/partition context.

The last restriction is important: nested stateful dependencies are supported by first rewriting the
inner stateful call to a placeholder, then letting the outer stateful call reference that
placeholder.

Example:

```sql
SELECT lag(a), lag(b) FILTER (WHERE lag(a)) FROM stream
```

is normalized by the parser into two ordered stateful calls:

- `lag(a) -> col_1`
- `lag(b) FILTER (WHERE col_1) -> col_2`

## Goals

- Parse stateful functions into `SelectStmt.stateful_mappings` using parser placeholders such as
  `col_1`.
- Support stateful `FILTER (WHERE ...)` and `OVER (PARTITION BY ...)`.
- Add dedicated logical and physical plan nodes for stateful evaluation.
- Execute stateful functions before windowing, aggregation, and final projection.
- Preserve deterministic evaluation order for dependent stateful calls.

## Non-goals

- Stateful functions in `HAVING`.
- Persisting stateful state across pipeline restarts.
- Distributed or exactly-once state management for stateful functions.
- Additional `OVER` features beyond `PARTITION BY`.

## Key Design Decisions

### 1) Shared placeholder allocation

Aggregate rewrite and stateful rewrite both use parser placeholders such as `col_1`, `col_2`, and
share a single `ColPlaceholderAllocator`.

This guarantees:

- No placeholder collision between rewrite passes.
- Stable placeholder reuse when identical stateful calls are deduplicated.

### 2) Ordered extraction, not planner-side sorting

The parser extracts stateful calls in post-order and stores them in execution order.

`SelectStmt.stateful_mappings` is an ordered `Vec<StatefulMappingEntry>`, not a `HashMap`.

Each entry contains:

- `output_column`
- `spec: StatefulCallSpec`

`StatefulCallSpec` captures:

- `func_name`
- `args`
- `when`
- `partition_by`
- `original_expr`

Logical and physical planning preserve this parser order directly. No additional topological sort is
performed in the planner.

### 3) Deduplication is based on normalized stateful spec

Repeated identical stateful calls share the same placeholder and are computed once.

Example:

```sql
SELECT lag(a), lag(a) FROM stream
```

is rewritten to:

```sql
SELECT col_1, col_1 FROM stream
```

with one mapping:

- `lag(a) -> col_1`

Deduplication uses a normalized key built from:

- function name
- rewritten argument list
- rewritten filter expression
- rewritten partition key list

This means the following are different calls:

- `lag(a)`
- `lag(a) FILTER (WHERE col_1)`

### 4) Plan placement

Stateful functions run before windowing, aggregation, filtering, and projection.

The plan topology is:

1. `DataSource`
2. `StatefulFunction`
3. `Window` (if any)
4. `Aggregation` (if any)
5. `Filter` (`HAVING` / `WHERE`, if any)
6. `Order` (if any)
7. `Project`
8. `Sink/Tail`

This guarantees that:

- later expressions can reference stateful placeholders
- windows and aggregations can consume derived stateful columns
- stateful dependencies remain deterministic

## Parser

### Rewrite scope

The parser rewrites stateful functions in:

- `SELECT` fields
- `WHERE`
- `ORDER BY`

The parser applies rewrites in this order:

1. stateful rewrite
2. aggregate rewrite

This allows expressions such as a stateful call inside an aggregate argument to rewrite the inner
stateful call first.

### Rewrite behavior

For each detected stateful function:

1. Recursively rewrite child expressions first.
2. Parse the current function into a `StatefulCallSpec`.
3. Build a normalized dedup key from the rewritten spec.
4. Reuse an existing placeholder if the same spec already exists.
5. Otherwise allocate a new placeholder and append a new ordered mapping entry.

Because the traversal is post-order, a dependent stateful call naturally sees earlier placeholders.

Example:

```sql
SELECT lag(a), lag(b) FILTER (WHERE lag(a)) FROM stream
```

becomes:

```sql
SELECT col_1, col_2 FROM stream
```

with ordered mappings:

- `lag(a) -> col_1`
- `lag(b) FILTER (WHERE col_1) -> col_2`

### `FILTER` and `OVER` parsing

For a stateful function call:

- `FILTER (WHERE expr)` is stored as `spec.when`.
- `OVER (PARTITION BY expr1, expr2, ...)` is stored as `spec.partition_by`.

Only `PARTITION BY` is supported inside `OVER`. The parser rejects `ORDER BY`, window frames, and
named windows for stateful functions.

## Registries and Runtime APIs

### Parser-facing trait

The parser only needs to know whether a function name is stateful:

- `parser::stateful_registry::StatefulRegistry`

### Flow runtime registry

The flow runtime owns a full registry used to instantiate implementations:

- `StatefulFunctionRegistry`
- `StatefulFunction`
- `StatefulFunctionInstance`
- `StatefulEvalInput`

`FlowInstance` and `PipelineRegistries` carry the shared stateful registry.

## Planner

### Logical plan

The logical planner inserts:

- `LogicalPlan::StatefulFunction(StatefulFunctionPlan)`

`StatefulFunctionPlan` stores ordered `calls`, where each call contains:

- `output_column`
- `spec`

The planner does not reorder these calls. It preserves parser output order so later calls can depend
on placeholders produced by earlier calls.

Column pruning must collect dependencies from:

- stateful arguments
- `FILTER (WHERE ...)`
- `OVER (PARTITION BY ...)`

Example:

```sql
SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) AS v1 FROM stream
```

requires the datasource schema:

- `[a, flag, k1, k2]`

### Physical plan

The physical planner inserts:

- `PhysicalPlan::StatefulFunction(PhysicalStatefulFunction)`

Each compiled call contains:

- `output_column`
- `func_name`
- `arg_scalars`
- `when_scalar`
- `partition_by_scalars`

### Explain output

Logical and physical explain output render the normalized stateful spec instead of the raw original
expression.

Example:

```sql
SELECT lag(a), lag(b) FILTER (WHERE lag(a)) FROM stream
```

is shown as:

```text
calls=[lag(a) -> col_1; lag(b) FILTER (WHERE col_1) -> col_2]
```

This makes stateful dependencies explicit in `EXPLAIN`.

## Processor

### Execution model

`StatefulFunctionProcessor` evaluates ordered stateful calls row by row and writes results into
affiliate columns using the parser placeholders.

For each stateful call, the processor maintains:

- one stateful function instance per partition

If `OVER (PARTITION BY ...)` is absent, all rows share a single implicit partition.

If `OVER (PARTITION BY ...)` is present, the processor evaluates the partition expressions on every
row and isolates state by partition key.

### `FILTER (WHERE ...)` semantics

For each row and for each stateful call:

1. Evaluate the partition key.
2. Evaluate `when_scalar` if present.
3. Evaluate the function with a `should_apply` flag.
4. If the condition is `true`, the function may advance its internal state.
5. If the condition is `false`, the function decides what to return and whether any internal state
   should change.
6. If the condition is `NULL`, treat it as `false`.

This is intentionally not modeled as one shared skipped-row behavior across all stateful
functions. For example, `lag`, `latest`, `changed_col`, and `had_changed` can all return different
values when `should_apply` is false.

Current built-in skipped-row behavior:

- `lag`: return the current visible lag value, do not advance the lag buffer
- `latest`: return the tracked value if present; otherwise return `NULL`
- `changed_col`: return `NULL`, do not update tracked state
- `had_changed`: return `false`, do not update tracked state

Example:

```sql
SELECT a + lag(b) FILTER (WHERE flag = 1) AS v FROM stream
```

When `flag = 0`:

- `lag(b)` does not consume the current row
- the lag buffer is not advanced
- the expression uses the current visible lag value for that partition

If no previous lag value exists for that partition, the result is `NULL`.

## Validation Examples

### Basic stateful call

```sql
SELECT lag(a) FROM stream
```

### Dependency through filter placeholder

```sql
SELECT lag(a), lag(b) FILTER (WHERE lag(a)) FROM stream
```

Normalized ordered calls:

- `lag(a) -> col_1`
- `lag(b) FILTER (WHERE col_1) -> col_2`

### Filter plus partitioning

```sql
SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) AS v1 FROM stream
```

Expected planner behavior:

- stateful call is compiled into one `StatefulFunction` node
- source column pruning keeps only `a`, `flag`, `k1`, and `k2`
- state is isolated per `(k1, k2)` partition

### Built-in behavior examples

```sql
SELECT latest(a) OVER (PARTITION BY k1) AS latest_a FROM stream
```

Expected runtime behavior:

- each `k1` partition keeps its own latest accepted non-`NULL` value
- `NULL` rows reuse the partition's tracked value
- a partition with no tracked value returns `NULL`

```sql
SELECT changed_col(true, a) FILTER (WHERE flag = 1) AS delta FROM stream
```

Expected runtime behavior:

- filtered-out rows produce `NULL`
- repeated accepted values produce `NULL`
- accepted changed values emit the current value

```sql
SELECT had_changed(true, a, b) FILTER (WHERE flag = 1) AS changed FROM stream
```

Expected runtime behavior:

- filtered-out rows return `false`
- accepted rows compare against the most recent accepted state
- a skipped row does not advance the tracked state for later comparisons
