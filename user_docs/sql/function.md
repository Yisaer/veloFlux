# Functions

This document describes the built-in SQL-visible functions in veloFlux.

## Overview

### Scalar functions

- `concat(a: string, b: string) -> string`: concatenate two strings.

### Aggregate functions

- `avg(x: numeric) -> float64`: average of numeric values.
- `count(x: any) -> int64`: count rows or non-`NULL` values.
- `deduplicate(x: any) -> list<any>`: distinct non-`NULL` values in first-seen order.
- `max(x: any) -> any`: maximum non-`NULL` value.
- `median(x: numeric) -> float64`: exact median of numeric values.
- `min(x: any) -> any`: minimum non-`NULL` value.
- `sum(x: numeric) -> numeric`: sum of numeric values.
- `ndv(x: any) -> int64`: number of distinct values.
- `last_row(x: any) -> any`: last observed value (by processing order).
- `stddev(x: numeric) -> float64`: population standard deviation.
- `stddevs(x: numeric) -> float64`: sample standard deviation.
- `var(x: numeric) -> float64`: population variance.
- `vars(x: numeric) -> float64`: sample variance.

### Stateful functions

- `lag(x: any) -> any`: previous row’s value (by processing order).

## Function details

### `concat(a, b)`

- Kind: scalar
- Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`
- Semantics: returns the concatenation of `a` and `b`.
- Constraints:
  - Requires exactly 2 arguments.
  - Both arguments must be strings.
  - Current implementation does not accept `NULL` as an argument.

Examples:

```sql
SELECT concat('hello', 'world') AS s FROM s
SELECT concat(first_name, last_name) AS full_name FROM s
```

### `avg(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: average of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type is `float64`.

Examples:

```sql
SELECT avg(x) AS avg_x FROM s GROUP BY tumblingwindow('ss', 10)
SELECT avg(amount) FROM orders GROUP BY user_id
```

### `count(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics:
  - `count(*)` counts all rows in the group/window.
  - `count(x)` counts non-`NULL` values of `x`.
- Constraints:
  - Requires exactly 1 argument.
  - Return type is `int64`.
  - `DISTINCT` is not supported.

Examples:

```sql
SELECT count(*) AS rows FROM s GROUP BY tumblingwindow('ss', 10)
SELECT count(device_id) AS seen_devices FROM s GROUP BY site_id
```

### `deduplicate(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: collect distinct non-`NULL` values in first-seen order.
- Constraints:
  - Requires exactly 1 argument.
  - Ignores `NULL` inputs.
  - Returns `NULL` if all inputs are `NULL` or the group/window is empty.
  - Return type is `list<T>`, where `T` matches the input type.

Examples:

```sql
SELECT deduplicate(tag) AS tags FROM s GROUP BY tumblingwindow('ss', 10)
SELECT deduplicate(user_id) FROM s GROUP BY region
```

### `max(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: maximum non-`NULL` value in the group/window.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be comparable scalar data.
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type matches the input type.

Examples:

```sql
SELECT max(score) AS max_score FROM s GROUP BY tumblingwindow('ss', 10)
SELECT max(status) FROM s GROUP BY device_id
```

### `median(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: exact median of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type is `float64`.
  - When the number of non-`NULL` inputs is even, the result is the average of the two middle values.

Examples:

```sql
SELECT median(latency) AS p50 FROM s GROUP BY tumblingwindow('ss', 10)
SELECT median(amount) FROM orders GROUP BY user_id
```

### `min(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: minimum non-`NULL` value in the group/window.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be comparable scalar data.
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type matches the input type.

Examples:

```sql
SELECT min(score) AS min_score FROM s GROUP BY tumblingwindow('ss', 10)
SELECT min(status) FROM s GROUP BY device_id
```

### `sum(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: sum of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type matches the input numeric type.

Examples:

```sql
SELECT sum(x) AS total FROM s GROUP BY tumblingwindow('ss', 10)
SELECT sum(amount) FROM orders GROUP BY user_id
```

### `ndv(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Constraints:
  - Requires exactly 1 argument.
  - Ignores `NULL` inputs.
  - Return type is `int64`.

Examples:

```sql
SELECT ndv(user_id) AS unique_users FROM s GROUP BY tumblingwindow('ss', 10)
```

### `last_row(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: return the last observed value of `x` in the group/window.
- Constraints:
  - Requires exactly 1 argument.
  - Return type matches the argument type.
  - “Last” is defined by the pipeline processing order (no explicit `ORDER BY` support yet).

Examples:

```sql
SELECT last_row(status) AS last_status FROM s GROUP BY device_id
```

### `stddev(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: population standard deviation of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Returns `0.0` when exactly one non-`NULL` value is present.
  - Return type is `float64`.

Examples:

```sql
SELECT stddev(latency) AS latency_stddev FROM s GROUP BY tumblingwindow('ss', 10)
```

### `stddevs(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: sample standard deviation of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if fewer than 2 non-`NULL` values are present.
  - Return type is `float64`.

Examples:

```sql
SELECT stddevs(latency) AS latency_stddev_sample FROM s GROUP BY tumblingwindow('ss', 10)
```

### `var(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: population variance of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Returns `0.0` when exactly one non-`NULL` value is present.
  - Return type is `float64`.

Examples:

```sql
SELECT var(latency) AS latency_var FROM s GROUP BY tumblingwindow('ss', 10)
```

### `vars(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: sample variance of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if fewer than 2 non-`NULL` values are present.
  - Return type is `float64`.

Examples:

```sql
SELECT vars(latency) AS latency_var_sample FROM s GROUP BY tumblingwindow('ss', 10)
```

### `lag(x)`

- Kind: stateful
- Allowed clauses: `SELECT`, `WHERE`
- Semantics: return the previous row’s value of `x`.
- Constraints:
  - Requires exactly 1 argument.
  - First row returns `NULL`; subsequent rows return the previous row’s value.
  - Row order is the pipeline processing order (no explicit `ORDER BY` support yet).

Examples:

```sql
SELECT lag(x) AS prev_x, x FROM s
SELECT * FROM s WHERE lag(speed) > 0
```
