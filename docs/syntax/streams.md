# Streams and Schema

This document describes stream metadata and schema introspection from a **SQL and schema** perspective.

For agent-specific guidance (workflow, validation loop, do/don’t), see `docs/agents_readme.md`.

## Manager API

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

### List Streams

`GET /streams`

Returns a list of known streams with their schemas.

Example:

```bash
curl -s http://127.0.0.1:8080/streams | jq .
```

### Describe Stream

`GET /streams/describe/{stream_name}`

Returns one stream’s schema and metadata. Agents should prefer this endpoint when they already know (or have resolved) the stream name.

Example:

```bash
curl -s http://127.0.0.1:8080/streams/describe/user | jq .
```

If the stream does not exist, the API should return `404` with a descriptive message.

## Response Shape (Recommended Contract)

Agents should treat field names as stable API contract. Optional fields may be absent.

### Stream

- `name: string` (the identifier used in SQL)
- `shared: boolean` (whether the stream is shared)
- `schema: { columns: Column[] }`
- Optional: `eventtime`, `decoder`, `stream_type`, `props`, `shared_stream` (implementation-defined)

### Column

- `name: string`
- `data_type: string`
- Optional:
  - `fields: Column[]` (only when `data_type == "struct"`)
  - `element: Column` (only when `data_type == "list"`)

## Type Strings

The schema uses a compact set of type strings. Agents must not assume other names.

Common scalars:

- `null`, `bool`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `string`

Nested types:

- `struct` (with `fields`)
- `list` (with `element`)

## Nested Types

Streams may contain nested types (struct/list). The introspection schema represents nested types structurally:

- Struct columns provide `fields[]`.
- List columns provide an `element` column (its `name` may be `"element"` in responses).

Important: nested types in schema do not automatically imply that SQL supports nested field access syntax. Agents should rely on `validate_sql` and `explain_sql` to confirm supported expressions.

## Column Order and Stability

Agents should preserve the column order as returned by the schema when:

- Displaying schemas to users
- Reasoning about index-based semantics in execution/explain output (if applicable)

Do not reorder columns arbitrarily.
