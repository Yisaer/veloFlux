# WASM UDF REST API (Manager)

This document describes the Manager REST API for registering and managing
WASM user-defined functions. The UDF runtime supports batch evaluation only.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/udfs/upload` | Upload and register a WASM UDF |
| `GET` | `/udfs` | List persisted UDF records |
| `GET` | `/udfs/:name` | Get one persisted UDF record |
| `DELETE` | `/udfs/:name` | Delete one persisted UDF record |

These routes are available when the Manager is built with the `wasm_udf`
feature.

## Upload a UDF

`POST /udfs/upload` accepts `multipart/form-data` with these fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | text | yes | Canonical function name; normalized to lowercase and validated as a resource id |
| `revision` | text | yes | Positive JSON-safe resource revision |
| `wasm_file` | binary | yes | Compiled `.wasm` module |

The module is validated before it is persisted. Its metadata name must match
`name`, and its metadata must declare `abi_version: 1` and
`execution_mode: "batch"`. The module must export `memory`,
`udf_metadata_ptr() -> i32`, `udf_metadata_len() -> i32`, and
`udf_batch(i32, i32) -> i32`. Legacy `udf_row` modules are rejected.

Successful response (`200 OK`):

```json
{
  "name": "my_udf",
  "revision": 1,
  "wasm_sha256": "a1b2c3...",
  "status": "registered"
}
```

The status may indicate that the record was persisted but hot registration
failed. A restart reloads persisted UDFs.

## Persisted UDF record

`GET /udfs` returns an array. `GET /udfs/:name` returns one object:

```json
{
  "name": "my_udf",
  "revision": 1,
  "wasm_sha256": "a1b2c3...",
  "size_bytes": 4096
}
```

The WASM metadata JSON and binary are not included in these responses.

## Batch ABI

The host invokes `udf_batch(input_ptr, input_len)` once for each input
collection. The input bytes are a JSON object with column-oriented arguments:

```json
{
  "row_count": 2,
  "args": [[1, 2], [10, 20]]
}
```

The returned buffer contains a length-prefixed JSON payload with one result per
input row:

```json
{
  "values": [11, 22]
}
```

For row-level failures, the module may return a `results` array. The host keeps
successful rows and drops rows with errors after recording the error:

```json
{
  "results": [
    {"value": 11},
    {"error": {"code": "invalid_argument", "message": "row-specific error"}},
    {"value": 33}
  ]
}
```

Malformed envelopes, mismatched column lengths, missing arguments, and argument
type errors return an error envelope instead of a default value:

```json
{
  "error": {
    "code": "invalid_argument",
    "message": "argument `a` at row 1 has an incompatible type"
  }
}
```

The SDK can generate the required exports and batch wrapper. See the
`veloflux-udf-sdk` crate for the `define_udf!` macro and WASM build command.

## Revisioned Deletion

DELETE also accepts optional `?revision=N`. It uses the shared revision timeline,
records a tombstone even for an absent resource, and returns the
[Apply response](../../docs/api/storage_apply.md). A greater revision is required
for recreation; equal deletion replay retries cleanup. Omitting the parameter
keeps ordinary DELETE behavior and does not create or clear a tombstone.
See [resource deletion](../../docs/api/storage_delete.md) for conflicts and cleanup.
