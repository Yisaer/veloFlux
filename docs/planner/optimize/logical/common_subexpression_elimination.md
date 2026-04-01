# CommonSubexpressionElimination

## Overview

`CommonSubexpressionElimination` (CSE) reduces repeated scalar computation by:
- detecting repeated subexpressions in a local region of the logical plan
- materializing them once into the tuple affiliate via `LogicalPlan::Compute`
- rewriting downstream expressions to read the materialized value (by name)

This is primarily a CPU optimization: the goal is to avoid evaluating the same expression multiple
times per tuple.

Implementation: `src/flow/src/planner/logical_optimizer.rs` (`CommonSubexpressionElimination`).

## Background / Motivation

SQL projections and filters often repeat the same scalar expression, especially after alias
expansion. Example:

```sql
SELECT a + 1 AS b, b + 1 AS c
FROM stream
WHERE b > 1 AND c > 1
```

After alias expansion, `b` becomes `(a + 1)` in later expressions, which can cause `(a + 1)` to
appear multiple times. Without CSE, the executor evaluates `(a + 1)` repeatedly.

`LogicalCompute` provides a dedicated mechanism to materialize intermediate values into the tuple
affiliate without changing upstream message forwarding semantics.

## Inputs (What It Recognizes)

This rule currently targets a local pattern around `Project`:
- expressions in `LogicalPlan::Project.fields[*].expr`
- optionally, the adjacent `LogicalPlan::Filter.predicate` if the `Project` child is a single
  `Filter` node

It considers a subexpression "the same" using a structural normalization key:
- parentheses (`Expr::Nested`) do not create a distinct expression key
- identifier, literal, and operator structure are included in the key

## Outputs (What It Produces)

When a repeated, eligible subexpression is found, CSE produces:

- a new `LogicalPlan::Compute` node with one or more temp fields:
  - `field_name = __vf_cse_N`
  - `expr = <original subexpression>`
- rewritten `Project` expressions (and `Filter` predicate, if included) where each occurrence of the
  chosen subexpression is replaced by `Identifier(__vf_cse_N)`

Physical mapping:
- `LogicalPlan::Compute` is built as `PhysicalPlan::Compute` (not `PhysicalProject`) to avoid
  breaking physical-only optimizations that assume `PhysicalProject` semantics.

## Temp Naming / Reserved Names

CSE temps use the internal derived naming convention:
- `__vf_cse_\\d+`

The `__vf_` prefix is reserved for internal use. Users cannot define aliases that collide with
`__vf_*` (including `__vf_cse_*`) or parser placeholders like `col_\\d+`.

Internal column helpers live in:
- `src/flow/src/expr/internal_columns.rs`

## Execution Semantics (Key Guarantee)

`Compute` materializes values into the tuple affiliate. Downstream expressions read these values
by name, which is crucial because temps do not belong to any source schema.

This is enabled by the SQL-to-scalar conversion rule:
- for internal derived identifiers (`col_\\d+`, `__vf_cse_\\d+`), compile them as
  `ColumnRef::ByName` so they are read from the affiliate instead of being resolved as a source
  column.

See:
- `src/flow/src/expr/sql_conversion.rs` (`convert_identifier_to_column`)

## Placement Relative To Filter

If the local shape is `Filter -> Project`, the optimizer chooses placement to preserve correctness
and avoid unnecessary work:

- If the rewritten `Filter.predicate` references any temp (`__vf_cse_*`), place `Compute` *below*
  the `Filter` so the filter can read the computed temps:
  - `Compute -> Filter -> Project` (in terms of dataflow direction)
- Otherwise, keep the filter selective by placing `Compute` *above* the `Filter`:
  - `Filter -> Compute -> Project`

## Safety / Degrade Behavior

To avoid semantic changes, this rule only rewrites "eligible" roots. Current conservative choices:
- does not CSE plain identifiers (`a`) or literals (`1`)
- does not CSE wildcard (`*`)
- does not CSE function calls (`func(...)`) to avoid issues with non-deterministic functions

If no repeated eligible subexpression exists, the plan is returned unchanged.

## Interaction With Other Logical Rules

`CommonSubexpressionElimination` runs before pruning rules:
- `TopLevelColumnPruning`
- `StructFieldPruning`
- `ListElementPruning`

Reason: inserting `Compute` can introduce new expressions that must be accounted for when
determining which source columns are required for decoding and execution.

## Limitations / Future Work

- Scope is currently local to `Project` (+ optional adjacent `Filter`), not across arbitrary plan
  boundaries.
- Function CSE is disabled; a future improvement could allow a whitelist of deterministic
  functions.
- The heuristic is cost-based but simple; it can be extended with better cost models and additional
  constraints (e.g., avoiding very cheap expressions).

