# StateWindow in StreamDialect

This document describes the current `statewindow` design in synapseFlow, including the planned `OVER (PARTITION BY ...)` capability.

## Where It Appears

In StreamDialect, window functions appear in the `GROUP BY` clause:

- Window functions are only allowed in `GROUP BY`.
- At most one window function is allowed per statement.
- When present, the parser extracts the window into `SelectStmt.window`.
- All other `GROUP BY` expressions remain in `SelectStmt.group_by_exprs`.

See also: `docs/window.md`.

## Syntax

`statewindow` is a window function with two general SQL expressions (typically booleans):

```sql
statewindow(open_expr, emit_expr)
```

To maintain independent state per logical key, `statewindow` may include an `OVER` clause:

```sql
statewindow(open_expr, emit_expr) OVER (PARTITION BY key_expr1, key_expr2, ...)
```

### Restrictions

For `statewindow`, the `OVER` clause supports **only**:

- `PARTITION BY <expr> [, <expr> ...]`

`ORDER BY`, window frames, named windows, and other `OVER` features are not supported for `statewindow` in StreamDialect.

## Semantics

`statewindow` buffers input rows between an "open" condition and an "emit" condition.

For a given partition (see below), the state machine is:

- When inactive and `open_expr == true`, start buffering (do not emit even if `emit_expr == true`).
- When active, buffer every incoming row. If `emit_expr == true`, emit the buffered batch and close (become inactive).
- When inactive and `emit_expr == true`, ignore.

### Partitioned Semantics (`OVER (PARTITION BY ...)`)

When `OVER (PARTITION BY ...)` is present:

- A **partition key** is computed per incoming row by evaluating all `key_expr*`.
- The operator maintains an independent `statewindow` state machine **per partition key**.
- `open_expr` / `emit_expr` are evaluated against the current row and apply only to that row’s partition.
- Rows from different partition keys never share buffered state.

When `OVER` is absent:

- All rows belong to a single implicit partition (the entire stream shares one state machine).

### Control / End-of-Stream

On graceful termination, if a partition is active and has buffered rows, the current buffered batch may be flushed before shutdown (matching the current processor behavior).

## Examples

### Global (single partition)

```sql
SELECT *
FROM users
GROUP BY statewindow(a > 0, b = 1)
```

### Partitioned by keys

```sql
SELECT *
FROM users
GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY user_id)
```

```sql
SELECT *
FROM users
GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY region, device_type)
```

## Error Cases (Expected)

- Multiple window functions in `GROUP BY` (e.g. `statewindow(...)` + `tumblingwindow(...)`) are rejected.
- Unsupported `OVER` features for `statewindow` are rejected (e.g. `OVER (ORDER BY ...)`, frames).
- `OVER ()` with no `PARTITION BY` expressions is rejected.

