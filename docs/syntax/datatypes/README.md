# Data Types

This directory documents SQL-visible data type semantics for stream schemas and expression values.

- `schema.md`: Schema declaration syntax and the current built-in type surface.
- `json_decode.md`: JSON decoder behavior for schema-typed input columns.
- `timestamp.md`: `timestamp` type semantics and the initial RFC3339 decode contract.

## Scope

Data type documentation is split from function and window syntax because schema types define the
contract between decoded source rows, expression evaluation, and encoded sink output.

The current schema type surface includes:

- `null`
- `bool` / `boolean`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `string`
- `list`
- `struct`
- `timestamp`

The initial `timestamp` implementation keeps schema syntax minimal and reserves column-level
options for later extension.
