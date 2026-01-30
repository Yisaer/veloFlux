# Alias Computing (SELECT Alias Expansion)

## Overview

In veloFlux, **alias computing** means **expanding SELECT projection aliases into expressions** during
logical planning.

This is a planner-time rewrite on the `sqlparser::ast::Expr` stored in `parser::SelectStmt`:

- It enables a limited form of referencing SELECT aliases from other expressions.
- It is **not** a runtime “materialize alias into a column once” mechanism.

Implementation entry point:

- `src/flow/src/planner/logical/mod.rs` (`resolve_select_aliases`)

## Background / Motivation

`sqlparser` can parse projection aliases (e.g. `a + 1 AS b`). However, at execution time veloFlux
evaluates scalar expressions against input sources / tuple affiliate columns. A bare identifier
like `b` has no meaning unless it can be resolved to:

- an input column, or
- an internal derived column name, or
- an already-materialized value in the tuple affiliate.

For projection aliases, veloFlux currently chooses a conservative, explicit approach:

- **rewrite** alias references to their defining expressions in a small supported scope
- **fail fast** for other clauses to avoid late “column not found” errors

One important consequence of expanding aliases is repeated computation (the same expression can be
duplicated across SELECT/WHERE). This is one of the motivations for logical optimizations such as
common subexpression elimination (CSE):

- `docs/planner_optimize/logical/common_subexpression_elimination.md`

## User-Facing Semantics

### Supported

Aliases are supported in exactly these places:

1. **SELECT projection list**
   - You may reference aliases defined *earlier* in the same SELECT list (left-to-right).
2. **WHERE**
   - WHERE may reference any SELECT alias (using the final environment produced by the full SELECT
     list rewrite).
3. **ORDER BY**
   - ORDER BY may reference any SELECT alias (using the final environment produced by the full
     SELECT list rewrite).

Conceptually, alias references behave like macro expansion:

- Every `Identifier(alias)` is replaced by the aliased expression.
- Replacements are wrapped in parentheses (`Expr::Nested`) to preserve operator precedence.

Example:

```sql
SELECT a + 1 AS b, b + 1 AS c
FROM stream
WHERE b > 1 AND c > 1
```

After alias expansion:

- `b` becomes `(a + 1)`
- `c` becomes `((a + 1) + 1)`
- `WHERE` becomes `((a + 1) > 1) AND (((a + 1) + 1) > 1)`

### Explicitly Unsupported Clauses (Fail Fast)

If any of these clauses reference a SELECT alias, planning fails with a clear error:

- `HAVING`
- `GROUP BY`
- `WINDOW` definitions (e.g. expressions inside state window spec)

Rationale:

- We want deterministic, early errors instead of a later “column not found” during scalar lowering
  / physical compilation.

## Important Restrictions

### No Forward References in SELECT

Aliases are resolved left-to-right. Referencing an alias before it is defined is rejected:

```sql
SELECT b + 1 AS c, a + 1 AS b
FROM stream
```

This fails with:

- `forward reference to SELECT alias 'b' is not allowed`

### Qualified Names Are Not Aliases

Only unqualified identifiers are eligible for alias replacement:

- `b` may be an alias reference.
- `t.b` is treated as a qualified column reference and is **not** replaced.

### Alias Names Must Be Valid

SELECT alias names are validated before alias expansion:

- Must not collide with any input column name from sources.
- Must not be duplicated within the SELECT list.
- Must not use reserved internal names (e.g. `__vf_*`).

See:

- `src/flow/src/planner/logical/mod.rs` (`validate_select_alias_names`)
- `src/flow/src/expr/internal_columns.rs` (`is_reserved_user_name`)

## Implementation Design

### Where It Runs

Alias validation and expansion occur early in logical planning, before building the logical plan
nodes:

- `create_logical_plan` calls `validate_select_alias_names` and then `resolve_select_aliases`.

See:

- `src/flow/src/planner/logical/mod.rs` (`create_logical_plan`)

### Data Structures

`resolve_select_aliases` builds:

- `all_aliases: HashSet<String>`: all alias names appearing in the SELECT projection list
- `env: HashMap<String, Expr>`: a mapping from alias name to its rewritten defining expression

### Algorithm (Two-Phase Rewrite)

1. **Rewrite SELECT fields left-to-right (fallible)**
   - For each projection expression:
     - Replace references to aliases already in `env`.
     - If an identifier matches `all_aliases` but is not in `env`, report a forward reference.
   - If the field has an alias, record `env[alias] = rewritten_expr`.

2. **Rewrite WHERE and ORDER BY using the final `env` (infallible)**
   - Walk expressions and replace any identifier found in `env`.

Implementation helpers:

- `rewrite_expr_with_aliases_select_item` (fallible: enforces no forward references)
- `rewrite_expr_with_aliases` (infallible: plain substitution)

Both walkers are structural and recurse into common expression forms (binary/unary ops, nested
expressions, casts, CASE, functions, JSON access, list/map access, etc.).

### Why Fail Fast for Other Clauses

The current planner/lowering pipeline does not treat projection aliases as resolvable columns in
clauses like `GROUP BY` / `HAVING` / window specs. Allowing them without explicit rewriting would
produce confusing errors downstream.

Fail-fast keeps the behavior explicit and makes it easier to extend clause-by-clause later.

## Tests

Pipeline test coverage exists for alias usage in SELECT + WHERE:

- `src/flow/tests/pipeline/pipeline_tests.rs` (`alias_can_be_used_in_select_and_where`)

## Limitations / Future Work

- Add clause-by-clause support (if desired) with explicit semantics.
- Consider a “materialize alias once per tuple” mechanism (e.g. via a `Compute` node) to avoid
  repeated evaluation after expansion.
- Define semantics for non-deterministic functions when an alias is referenced multiple times (macro
  expansion can evaluate the expression multiple times).
