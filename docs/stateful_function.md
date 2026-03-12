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
- the last output value per partition

If `OVER (PARTITION BY ...)` is absent, all rows share a single implicit partition.

If `OVER (PARTITION BY ...)` is present, the processor evaluates the partition expressions on every
row and isolates state by partition key.

### `FILTER (WHERE ...)` semantics

For each row and for each stateful call:

1. Evaluate the partition key.
2. Evaluate `when_scalar` if present.
3. If the condition is `true`, evaluate the function and advance the partition state.
4. If the condition is `false`, do not advance state and return the partition's previous output.
5. If the condition is `NULL`, treat it as `false`.

This is intentionally not the same as returning `NULL` for filtered-out rows.

Example:

```sql
SELECT a + lag(b) FILTER (WHERE flag = 1) AS v FROM stream
```

When `flag = 0`:

- `lag(b)` does not consume the current row
- the state is not advanced
- the expression uses the previously held `lag(b)` output for that partition

If no previous output exists for that partition, the result of the stateful call is `NULL`.

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
