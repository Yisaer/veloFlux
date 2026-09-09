# Named Expressions

## Overview

`WITH NAMED EXPRESSIONS` declares named SQL expressions that can be referenced by the rest of the
query. Named expressions are parser-time bindings: veloFlux expands them into the expression AST
before the existing stateful and aggregate parser rewrites. The feature does not imply runtime
materialization, an additional tuple column, or single evaluation.

## Syntax

```sql
WITH NAMED EXPRESSIONS
    <name> AS (<expression>) [, ...]
SELECT ...
```

Example:

```sql
WITH NAMED EXPRESSIONS
    b AS (a + 1),
    c AS (b + 1),
    d AS (c + 1)
SELECT d AS result
FROM stream
```

The query is self-contained: `d` is defined by the query itself, while `result` is only the output
column name.

## Semantics

- Named expressions are visible in expressions in the query scope.
- `SELECT ... AS ...` only assigns an output name; SELECT aliases are not visible to other
  expressions.
- Named expressions may reference input columns and other named expressions.
- Named expression dependencies are resolved in declaration order.
- A named expression may reference only input columns and named expressions declared earlier.
- Forward references are rejected by the parser.
- Cyclic dependencies are rejected as forward references.
- Named expressions are not added to the output unless explicitly selected.
- Qualified identifiers such as `stream.d` are not named expression references.
- A named expression must not collide with another named expression, a SELECT output alias, or an
  input column in any referenced source.
- Input-column collision validation is performed by the planner after source schemas are resolved;
  the parser validates collisions that are visible from the SQL text itself.

## Parser Rewrite Model

Named expression expansion is performed entirely in the parser. The parser consumes the declaration
and returns the same expression shape used by queries without named expressions. `SelectStmt` retains
declaration names only as validation metadata for the planner; no logical plan node is required for
the declaration itself.

The parser applies the following stages:

1. Extract the `WITH NAMED EXPRESSIONS` prefix.
2. Parse each declaration expression into a `sqlparser::ast::Expr`.
3. Resolve declarations from left to right using the expressions declared so far.
4. Reject collisions with SELECT output aliases and rewrite all query expression locations using
   the completed environment.
5. Run the existing null-predicate, stateful-function, and aggregate rewrites.
6. Return the normalized `SelectStmt` to the planner.

The named expression rewrite must happen before stateful and aggregate extraction. This ensures that
calls introduced by expansion, such as `lag(a)` or `sum(a)`, are visible to the existing parser
rewriters and keep their normal placeholder, deduplication, and execution-order semantics.

The rewrite applies to all expression locations represented by `SelectStmt`, including projection,
WHERE, GROUP BY, HAVING, ORDER BY, and window expressions. Existing validation still determines
whether the expanded expression is legal in each execution stage.

For each declaration, the parser maintains an environment containing only earlier declarations:

```text
WITH NAMED EXPRESSIONS
    b AS (a + 1),
    c AS (b + 1)
```

is resolved as:

```text
b := a + 1
c := (a + 1) + 1
```

An identifier matching a later declaration is reported as a forward reference rather than being
left unresolved as an input column. This makes declaration order deterministic and avoids dependency
graph construction in the planner.

## Name Resolution

Named expression replacement applies only to unqualified `Expr::Identifier` nodes. It does not
replace compound identifiers. Therefore:

```sql
WITH NAMED EXPRESSIONS d AS (a + 1)
SELECT d, stream.d
FROM stream
```

resolves `d` to the named expression and preserves `stream.d` as a qualified input reference.

The parser cannot inspect the resolved source schema. After source schemas are resolved, the planner
rejects any named expression whose name matches an input column in a referenced source. Therefore,
qualified references do not provide an escape hatch for a named-expression/input-column collision.

## Evaluation Model

Named expressions use AST expansion rather than a `Compute` plan node:

```text
WITH NAMED EXPRESSIONS b AS (a + 1), c AS (b + 1)
SELECT c FROM stream
```

is lowered conceptually as:

```text
SELECT (a + 1) + 1 FROM stream
```

This may repeat ordinary scalar computation when a named expression is referenced more than once.
It also means that named expressions do not promise the value-sharing or evaluation-once semantics
of a materialized intermediate column.

## Stateful, Aggregate, and Window Functions

Named expression expansion is completed before aggregate and stateful function extraction. This
allows the existing rewrite and deduplication passes to see the complete expression tree and
preserve their execution order.

The existing parser and planner validation must validate the expanded expression against the
execution stage of every referenced function. Named expressions must not be used to bypass
restrictions on aggregate functions, stateful functions, or window definitions.

In particular, AST expansion must not create multiple independent stateful calls merely because a
named expression is referenced multiple times. Stateful calls must continue to be normalized and
deduplicated by the existing stateful rewrite.

## Errors

Parsing or subsequent existing expression validation fails for:

- a duplicate named expression;
- a named expression colliding with a SELECT output alias;
- a named expression colliding with an input column;
- a forward reference to a later named expression;
- an invalid named expression expression;
- a named expression used in an unsupported execution stage after expansion.

Errors should identify the named expression and its declaration position where applicable.

## Relationship to SELECT Aliases

SELECT aliases only assign output names and are not visible to other expressions. This keeps `AS`
unambiguous:

```sql
SELECT a + 1 AS b, b + 1 AS c FROM stream
```

Here `b` in the second expression is resolved as an input name and is not the first projection
alias. To reuse an expression, declare it explicitly:

```sql
WITH NAMED EXPRESSIONS b AS (a + 1)
SELECT b, b + 1 AS c
FROM stream
```
