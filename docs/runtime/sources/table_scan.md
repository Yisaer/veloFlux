# Table Providers and Table Scans

This document describes the runtime design for finite table data and table providers.

## Model

`TableDefinition` describes table metadata:

- SQL-visible table name
- logical row schema
- provider type and persisted properties
- decoder configuration for providers that consume encoded payloads

`TableProvider` is the single runtime contract for table behavior. A provider exposes the table
schema and capabilities, and may implement bounded `scan` and parameterized `lookup` operations.
Provider instances are runtime resources and are not serialized into storage exports. The current
stream-to-table lookup join scope is documented in `table_lookup_join.md`.

The current provider implementations are:

- `HistoryTableProvider`: reads NanoMQ history Parquet files.
- `MemoryMockTableProvider`: supplies deterministic rows declared by a `memory_mock` table type
  and implements the current lookup contract.

Both implementations use the same provider contract. The table type selects the concrete provider
when the manager creates the `TableDefinition`; there is one provider registration per table.
History lookup is deferred; its current runtime support is bounded table scan only.

## Provider Contract

The provider contract uses structured requests rather than SQL strings or provider-native queries.

Scan requests contain a batch size. Providers emit each `RecordBatch` through the scan output as
soon as it is available; the scan response contains only completion metadata and never accumulates
the complete table. The default batch size is `100` and provider-specific configuration may
override it. Lookup requests contain:

- projected columns
- equality keys and values
- supported filters
- an inclusive time range
- per-request limits
- a cancellation token

Provider capabilities describe supported scan and lookup operations, equality key columns,
projection, predicates, and time-range filtering. Planner and runtime validation should use these
capabilities instead of checking concrete provider types.

## History Provider

`HistoryTableProvider` reads files using the existing NanoMQ naming convention:

```text
nanomq_{topic}-{start_ts}~{end_ts}_{seq}_{hash}.parquet
```

It discovers matching files, sorts them by sequence, reads the Parquet `data` column, and decodes
each payload with the table decoder. The configured time column must be `Int64` or `UInt64`, and
the payload column must be `Binary`.

History table definitions are created through the existing table lifecycle and automatically get a
runtime `HistoryTableProvider` binding. The manager persists the table definition and provider
configuration.

History scans preserve the existing error policy: discovery and request validation errors fail the
scan, while an unreadable file, an invalid Parquet batch, or a malformed payload is logged and
skipped so later files and payloads can still be emitted.

## Memory Mock Provider

`MemoryMockTableProvider` is the provider implementation for tables declared with
`TableType::MemoryMock`. The manager accepts JSON objects in `props.rows` and converts them directly
to `Tuple` rows and `RecordBatch` collections using the declared table schema. It stores those rows
in memory and supports:

- equality-key lookup
- filter and time-range evaluation
- projection
- deterministic insertion-order results
- row replacement and append operations
- request recording
- injected delay and provider errors
- cancellation checks
- bounded scan batches

The declaration is persisted and restored through the normal table storage, export, import, and
startup lifecycle. It is a table type at the same level as `History`, not a separate provider
registry or API resource.

## Table Scan Runtime

The current SQL table-scan path is:

```text
TableDefinition(type)
  -> HistoryTableProvider::scan or MemoryMockTableProvider::scan
  -> TableScanProcessor
  -> downstream operators
```

Table scans are finite. They read the currently visible table data once and terminate the data
channel with `StreamData::stream_graceful_end(0)`. The control channel remains reserved for control
signals. Each provider batch is forwarded before the next batch is read, so large tables do not
need to be materialized in the provider response.

Supported table-scan SQL currently includes scalar projection, `WHERE` filtering, and aggregate
queries with `GROUP BY eoswindow()`.

## Manager and Runtime Lifecycle

The manager persists table definitions through `POST /tables`, `GET /tables`, and
`DELETE /tables/:name`. Runtime provider instances are created when a table is registered in a
`FlowInstance` and are removed with the table. Export/import contains table metadata and provider
configuration, not runtime provider objects. For `memory_mock`, the initial rows are part of the
persisted provider configuration.

## Test Coverage

Table-scan planner and pipeline coverage lives in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`
- `src/flow/tests/pipeline/table_scan_tests.rs`

Provider binding coverage is in `src/flow/src/catalog/mod.rs`, and MemoryMock provider behavior is
covered in `src/flow/src/table/memory.rs`.

Revisioned DELETE accepts `?revision=N`; see [resource deletion](../../api/storage_delete.md)
for the shared version timeline, cleanup, and tombstone behavior.
