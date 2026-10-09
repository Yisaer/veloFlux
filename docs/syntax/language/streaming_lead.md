# Streaming LEAD

This document defines the first streaming `lead` design. It supports the complete streaming LEAD
argument semantics while limiting the first implementation to a single input source.

## Syntax

```sql
lead(expr [, offset [, ignore_null]])
  [FILTER (WHERE candidate_condition)]
  [OVER ([PARTITION BY key1, key2, ...] [UNTIL until_condition])]
```

Examples:

```sql
SELECT a, lead(a) AS next_a
FROM stream
```

```sql
SELECT a, lead(a, 2, true) AS next_a
FROM stream
```

```sql
SELECT a,
       lead(a) OVER (
         UNTIL ts - origin.ts > 5
       ) AS next_a
FROM telemetry
```

`offset` is a positive integer literal and defaults to `1`. `ignore_null` is a boolean literal and
defaults to `true`. A custom default value is not supported. The first form waits for the
`offset`-th valid candidate row. The second form applies the same candidate counting rules, while
allowing `UNTIL` to stop a pending request early.

## Origin and probe row scopes

`UNTIL` is evaluated for a pending origin row and the current probe row. The input source has no
SQL alias; its name is the source name in the `FROM` clause.

The preferred short forms are:

```sql
ts
origin.ts
```

Inside `UNTIL`:

- an unqualified column such as `ts` refers to the current probe row;
- `origin.ts` refers to `ts` from the pending origin row and is lowered to an `OriginRow` expression
  with a bound column slot.

The explicit forms are also supported:

```sql
probe.telemetry.ts
origin.telemetry.ts
```

They mean the same as `ts` and `origin.ts` when the source is `telemetry`. The first path segment
is the row scope, the second is the source name, and the third is the column name.

These row-scope rules apply only inside `UNTIL`. Ordinary SQL expressions do not reinterpret
`origin.ts` or `probe.telemetry.ts` as row-scope references. If a source itself is named `origin`
or `probe`, the explicit three-part form remains unambiguous, for example
`origin.origin.ts` and `probe.origin.ts`.

The planner binds probe and origin references to column slots. Runtime evaluation must use these
bound slots rather than resolving column names on the hot path.

## Origin datum binding

An origin reference is represented by an origin-row expression with a stable column slot and a
processor-local datum:

```rust
struct OriginRow {
    column_slot: ColumnSlot,
    datum: Option<Datum>,
}
```

The parser and planner create the immutable slot binding. The mutable `datum` belongs to the
processor-local evaluator state, not to the shared `PhysicalPlan` or a shared expression tree.
This prevents concurrent processors or independent pending origins from overwriting each other's
origin values.

Before evaluating `UNTIL` for one pending origin, the processor:

1. reads the required column from the pending origin tuple using its bound slot;
2. writes the datum into the processor-local `OriginRow` binding;
3. evaluates the scalar expression against the current probe tuple and that origin binding;
4. overwrites the binding before evaluating the next pending origin.

The binding must be refreshed for every evaluation. An evaluator must not observe an origin datum
left over from a previous pending row. `None` means that no origin datum is bound, which is an
internal invalid state for a valid origin reference; `Some(Datum::Null)` means that the origin row
exists and its SQL value is `NULL`.

The same scalar evaluator is used for probe and origin expressions. Probe column references read
the current input tuple, while origin-row expressions read the processor-local `OriginRow` datum.
This preserves normal expression behavior, including NULL propagation, type errors, short-circuit
logic, and function evaluation order. The evaluator does not retain the row binding after the
current call.

## Evaluation semantics

`lead` creates an origin request for each input row. A later row in the same partition is a probe.
The probe is evaluated against older pending origins before it becomes a new origin.

Without `UNTIL`:

- the `offset`-th later candidate completes the oldest pending request in the partition;
- without `FILTER`, every later row is a candidate;
- with `FILTER`, a rejected probe does not complete the request;
- a candidate whose expression is `NULL` does not consume `offset` when `ignore_null` is `true`;
- a candidate whose expression is `NULL` consumes `offset` and completes the request with `NULL` when
  `ignore_null` is `false`;
- the current row still becomes a new origin after it has probed older requests.

With `UNTIL`:

- `UNTIL` is evaluated independently for every pending origin in the probe's partition;
- `UNTIL` is checked before `FILTER`;
- when `UNTIL` is `false`, the probe may complete the request if it passes `FILTER`;
- when `UNTIL` is `false`, a non-`NULL` candidate increments the request's match count;
- a `NULL` candidate increments the match count only when `ignore_null` is `false`;
- when `UNTIL` is `true`, the request stops and its result is `NULL`, even if `FILTER` would pass;
- the request resolves to the candidate value when its match count reaches `offset`;
- a probe that stops an older request still becomes a new origin after older requests are handled.

Boolean `NULL` is treated as `false`. A non-boolean `UNTIL` or `FILTER` result is an error.

For example:

```sql
SELECT a,
       lead(a) OVER (
         UNTIL ts - origin.ts > 5
       ) AS next_a
FROM telemetry;
```

For input rows `(a, ts) = (10, 0), (20, 2), (30, 4), (40, 10), (50, 12)`, the processor emits:

```text
input (10, 0)  -> no output
input (20, 2)  -> {a: 10, next_a: 20}
input (30, 4)  -> {a: 20, next_a: 30}
input (40, 10) -> {a: 30, next_a: NULL}
input (50, 12) -> {a: 40, next_a: 50}
normal end    -> {a: 50, next_a: NULL}
```

When a row is released, its `lead` value belongs to that origin row. A probe does not receive the
older origin's value as its own result.

## Ordering and normal end

The processor preserves input order across partitions. A completed row is released only when it
is the completed prefix of the deferred buffer. Consequently, an unresolved early origin may
temporarily hold completed rows from another partition.

On normal stream end, every unresolved request is completed with `NULL`, and buffered rows are
released before the end signal is forwarded. A quick stop or an error does not flush unresolved
requests.

## Stateful instance lifecycle

`lead` remains a stateful function and runs in the shared `StatefulFunctionProcessor`. It does not
use a separate LEAD processor.

The existing immediate stateful lifecycle is extended with deferred output:

```text
prepare current row
  -> validate all calls
  -> commit all function updates
  -> enqueue the current origin row
  -> resolve older origins
  -> release the completed input-order prefix
```

The function instance owns matching progress and pending origin identifiers for its partition. The
processor owns complete buffered tuples, collection metadata, output ordering, and normal-end
flushing.

Conceptually, the protocol contains:

```rust
enum StatefulEvalOutput {
    Immediate(Value),
    Deferred(Vec<ResolvedStatefulRow>),
}

struct ResolvedStatefulRow {
    origin: u64,
    value: Value,
}
```

The LEAD instance keeps static call configuration and per-origin progress:

```rust
struct LeadConfig {
    offset: usize,
    ignore_null: bool,
    has_until: bool,
}

struct LeadRequest {
    origin: u64,
    matched_count: usize,
}
```

`offset` and `ignore_null` are fixed for a stateful call after literal validation. They do not
change as later input rows arrive. `UNTIL`, `FILTER`, `ignore_null`, and `offset` are applied in
that order for each pending origin.

`prepare_eval` must not mutate function state. Its deferred result identifies older origins that
can be resolved; `commit_eval` applies the function's state update only after all calls for the
current row have prepared and validated successfully.

The instance also provides normal-end finalization:

```rust
fn finish(&mut self) -> Vec<ResolvedStatefulRow>;
```

For `lead`, `finish` resolves every remaining origin to `NULL`. The processor then attaches the
resolved values to buffered tuples and releases them in input order.

Immediate functions and deferred `lead` calls can appear in the same statement. Immediate results
are prepared and committed under the same row-level transaction; a failure in any call discards
all prepared updates for that input row and does not resolve or enqueue it.

## Restrictions

The first implementation supports:

- one input stream without SQL aliases;
- `lead(expr [, offset [, ignore_null]])` with optional `FILTER`;
- optional `PARTITION BY`;
- optional `UNTIL` on `lead` only;
- `SELECT` and query-level `WHERE` contexts supported by stateful functions.

It does not support:

- a custom default argument;
- `ORDER BY` or window frames in `OVER`;
- joins, grouping, aggregation, named windows, or query-level `ORDER BY` in the deferred LEAD path;
- using a deferred LEAD result as the input to another stateful function;
- checkpoint persistence or a LEAD-specific buffer cap/TTL.
