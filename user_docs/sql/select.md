# SELECT syntax

This document describes the supported `SELECT` statement syntax in veloFlux.

veloFlux currently accepts exactly one SQL statement, and it must be a `SELECT`.

## Supported grammar (overview)

```text
SELECT <projection>
FROM <source_name> [, <source_name> ...]
[WHERE <expr>]
[GROUP BY <expr> [, <expr> ...]]
[HAVING <expr>]
```

Notes:

- Window declarations are written inside `GROUP BY` (see `user_docs/sql/window.md`).
- `HAVING` filters aggregate results after grouping/windowing.
- `<source_name>` can be a stream or a registered table.

## Projection

`<projection>` is one or more projection items separated by commas:

- `SELECT <expr> [, <expr> ...]`
- `SELECT <expr> AS <alias>`
- `SELECT *`
- `SELECT <source>.*`

Examples:

```sql
SELECT a, b + 1 FROM s
SELECT a + b AS total FROM s
SELECT * FROM s
SELECT t.* FROM t
```

## FROM

`FROM <source_name>`

The source name must match a stream or table exposed by the runtime catalog.

Example:

```sql
SELECT * FROM source_stream
SELECT * FROM history_table
```

### Multiple streams

Comma-separated inputs use VeloFlux per-event evaluation:

```sql
SELECT faults.ts AS fault_ts, messages.ts AS message_ts, payload
FROM faults, messages
```

Each event supplies its own source row; all other sources supply a NULL placeholder
row. No historical or latest values are joined to the current event. Collections
use the same behavior per row. This NULL-padding rule differs from standard SQL
Cartesian-product semantics for comma FROM.

Fields retain their source identity and type. Same-name columns require source
qualification and may have different types. Unique column names may be unqualified.
Unknown or ambiguous references fail planning; valid fields of an absent source
are NULL. Use aliases when necessary to produce distinct sink output column names.

Each source keeps its own sampling, decoding, and projection. Shared inputs are
supported. Multi-source queries currently support processing time only. All processing-time queries, including single-source queries,
assign time when each collection is consumed at the query input boundary, replacing
source tuple timestamps; payload timestamp fields are unchanged. All rows in a
collection share this processing time.
See [Multi-source FROM](../../docs/syntax/multi_source_from.md) for execution and
conditional-window examples.

### Table Sources

Table sources are finite scan sources. A table scan reads the available table data once and then ends
the pipeline's data path.

Currently supported table query forms:

```sql
SELECT * FROM history_table
SELECT speed + 1 AS next_speed FROM history_table WHERE ts > 1
```

Current limitations for table sources:

- table aliases are not supported
- explicit INNER/LEFT stream-table lookup joins use the existing provider lookup contract; see
  [Lookup joins](../../docs/runtime/sources/table_lookup_join.md)
- aggregate functions over table scans require `GROUP BY eoswindow()`

## WHERE

`WHERE <expr>`

Filters rows after windowing and aggregation in the current VeloFlux execution order.
For conditional windows, trigger events therefore remain available before the final
output filter. A window-owned `FILTER (WHERE ...)` instead filters input collection.

Example:

```sql
SELECT * FROM s WHERE a > 10 AND b != 0
```

## GROUP BY

`GROUP BY <expr> [, <expr> ...]`

Defines grouping keys for aggregation/windowing. A window declaration, when used, is written as a
special item inside `GROUP BY` (see `user_docs/sql/window.md`).

Examples:

```sql
SELECT a FROM s GROUP BY a
SELECT * FROM s GROUP BY tumblingwindow('ss', 10)
SELECT * FROM s GROUP BY tumblingwindow('ss', 10), device_id
```

## HAVING

`HAVING <expr>`

Filters aggregate result rows after `GROUP BY` windowing and aggregation. `HAVING` requires a
windowed aggregation query.

Examples:

```sql
SELECT sum(a) AS s
FROM s
GROUP BY countwindow(4)
HAVING sum(a) > 10

SELECT sum(a) AS s
FROM s
GROUP BY countwindow(4)
HAVING last_agg_hit_count() < 3
```

Constraints:

- `HAVING` may reference aggregate expressions such as `sum(a)`.
- `HAVING` may reference `last_agg_hit_count()`.
- Raw non-aggregate input columns are not allowed in `HAVING`.
- Projection aliases are not currently supported in `HAVING`.

## Validation workflow

- Fetch schema: `GET /streams/describe/:name`
- For table sources, ensure the table has been registered in the runtime catalog before pipeline
  creation.
- Create pipeline with SQL: `POST /pipelines`
- Validate lowering/execution plan: `GET /pipelines/:id/explain`
