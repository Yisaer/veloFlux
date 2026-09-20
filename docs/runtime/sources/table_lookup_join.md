# Stream-to-Table Lookup Join

This document defines the first stream-to-table lookup join implementation. It describes the
implementation scope for the current change and the boundaries that keep later table providers
compatible with the same planner and processor design.

## Current Scope

The first implementation supports one live stream joined with one or more registered memory tables:

```sql
SELECT live_stream.device_id, device_table.category
FROM live_stream
INNER JOIN device_table
  ON live_stream.device_id = device_table.device_id
```

The current scope is intentionally limited to:

- `INNER JOIN` only; plain `JOIN` is normalized to `INNER JOIN`.
- One unbounded stream on the left side.
- One or more registered tables on the right side.
- A `memory_mock` table provider.
- Stream and table relation aliases are not supported.
- At least one equality predicate connecting a stream expression with a table column.
- Zero, one, or multiple lookup matches per stream row.

The following capabilities are deferred:

- `LEFT`, `RIGHT`, `FULL`, and `CROSS JOIN`.
- Stream-to-stream and table-to-table joins.
- History-table lookup.
- Stream and table relation aliases.
- Provider-native SQL or raw SQL expressions in lookup requests.
- Unbounded table materialization in the join processor.

## Relational Semantics

The live stream is the probe side. The table does not produce an independent data stream. For each
input stream row, the join evaluates the lookup expressions and asks the table provider for bounded
matching rows.

For an `INNER JOIN`:

```text
zero matches  -> no output row
one match     -> one enriched output row
N matches     -> N enriched output rows
```

Each output row contains the live-stream fields followed by the table fields according to the
planner-owned output layout. The output timestamp remains the timestamp of the live row; lookup
completion time is not used as the event timestamp.

The lookup join is placed before the window and aggregation operators:

```text
DataSource
  -> LookupJoin
  -> Window
  -> Aggregation or StreamingAggregation
  -> Project
  -> Sink
```

This makes the enriched row the input to window partitioning, window filters, aggregation, and
projection. It also allows the existing window-plus-aggregation rewrite to keep the lookup join as
its upstream child:

```text
LookupJoin -> StreamingAggregation
```

The lookup join must preserve the event timestamp, output schema, and control/data channel
semantics required by downstream window processing.

## SQL and Parser Contract

The parser contract must preserve the structure that is needed by planning:

- left source;
- right table source;
- normalized join type;
- the complete `ON` expression.

Relation aliases are not part of the current lookup-join contract. Qualified columns must use the
registered stream and table names directly.

The planner must not reconstruct JOIN semantics from an unordered list of source names.

For the first implementation, an `ON` expression is valid only when it contains at least one
stream/table equality predicate. The equality predicate may be written in either direction:

```sql
l.device_id = t.device_id
t.device_id = l.device_id
```

The planner binds the table-side column and retains the stream-side expression for per-row
evaluation. Unsupported join shapes fail during planning, before pipeline startup.

## Logical and Physical Plan

The logical plan contains a unary lookup-join node. The table is not represented as a second plan
child because it does not emit rows independently:

```text
LogicalLookupJoin
  |
  +-- LogicalDataSource
```

The lookup-join node contains:

- normalized join type (`INNER`);
- table name and schema;
- equality-key bindings;
- required table projection derived from all downstream expression dependencies;
- planner-owned output layout.

The physical plan keeps the same unary topology and resolves the registered provider through the
catalog. The generic physical node depends on `TableProvider`, not on `MemoryMockTableProvider`.
This keeps the join processor reusable when another provider implements the existing contract.

## Provider Request Contract

The existing structured `LookupRequest` contract is sufficient for the first implementation. The
lookup join constructs one request for each live row:

```text
LookupRequest
  projection: required table columns only
  equality_keys: table column -> value evaluated from the live row
  filters: empty in the first equality-only implementation
  time_range: absent in the first implementation
  batch_size: maximum number of table rows emitted in one lookup batch
  cancellation: processor-scoped cancellation token
```

The provider receives values and structured predicates, not SQL AST nodes or provider-native query
strings. A provider must use the lookup capability advertised for the requested equality columns;
it must not silently fall back to a full table scan for a lookup join. The
`MemoryMockTableProvider` maintains per-column indexes, intersects the candidate row sets for
multi-column equality lookups, and returns matching rows in deterministic insertion order.

The planner validates that every table column used by the join equality predicate is advertised
as an indexed lookup key. `TableLookupCapability::lookup_key_sets` describes the indexed key sets;
the current memory provider advertises one indexed key set for each table column, which allows it
to serve composite equality predicates by intersecting those indexes.

`TableProvider::lookup` emits matching rows through a bounded `LookupOutput` channel and returns
only lookup metadata after all batches have been emitted. The provider does not return the complete
lookup result as one `Vec<Tuple>`.

`batch_size` is a batching parameter, not a total-result limit. A single stream row may match more
than one batch:

```text
one stream row
  -> lookup batch 1 (up to 1024 table rows)
  -> lookup batch 2 (up to 1024 table rows)
  -> lookup batch N
```

The initial implementation uses `1024` as the lookup batch size. It does not reject the 1025th
matching row with a resource-limit error.

## Processor Behavior

`LookupJoinProcessor` has one data input and one data output. The table provider is an external
lookup dependency rather than a second data channel.

For each input tuple it:

1. Evaluates the stream-side expressions from the planned key bindings.
2. Builds a `LookupRequest`.
3. Calls `TableProvider::lookup`.
4. Consumes lookup batches as they are emitted by the provider.
5. Combines every returned table row with the live row.
6. Emits output collections of at most 1024 joined rows downstream.

The processor processes one probe row at a time. It consumes all lookup batches for that probe row
before requesting the next probe row. A single probe row may therefore produce multiple output
collections, and one input collection may produce multiple output collections. The processor
splits joined output into collections of at most 1024 rows. This is output batching only; it does
not cap the total number of matches for a probe row.

The provider output channel is bounded, so downstream backpressure propagates into the table
lookup. The processor must not accumulate all matches for one probe row or all output collections
for one input collection. Bounded concurrent lookups are a later optimization and must not be
introduced as unbounded task spawning.

Control messages remain on the control channel and data messages remain on the data channel. When
the processor stops, its cancellation token cancels an in-flight provider request.

## Output Layout

The planner owns the final output layout:

```text
live-stream slots + lookup-table slots
```

The processor uses this layout when combining rows. It must not infer positions from source names,
field names, or the order returned by the provider. Required table projection is also derived from
the planner output and expression dependencies.

## Validation and Errors

Planning rejects:

- a missing stream or table definition;
- a right-hand relation that is not a table;
- a table without lookup capability;
- more than one stream or lookup table in the current scope;
- unsupported join types;
- an `ON` expression without a stream/table equality key;
- unresolved or ambiguous qualified columns.

Provider errors and cancellation are observable runtime errors. A failed lookup does not silently
become an empty inner-join result. Total-result resource limits such as `max_bytes` and lookup
timeouts are deferred; `batch_size` only controls streaming granularity.

## Implementation and Test Coverage

The implementation is expected to be layered as follows:

1. Extend the parser `SelectStmt` contract with structured JOIN information.
2. Add logical and physical lookup-join nodes and equality-key validation.
3. Add `LookupJoinProcessor` using the generic `TableProvider` interface.
4. Connect the node before window planning and verify the streaming-aggregation rewrite.
5. Add parser cases, planner SQL-to-EXPLAIN cases, and memory-provider pipeline cases.

The provider-side memory lookup behavior is implemented in:

- `src/flow/src/table/provider.rs`
- `src/flow/src/table/memory.rs`

The lookup-join planner and processor coverage belongs beside the existing planner and table-scan
coverage:

- `src/flow/tests/planner/`
- `src/flow/tests/pipeline/`

History provider lookup remains deferred. Its existing bounded scan behavior is documented in
`table_scan.md` and is not part of this lookup-join implementation.
