# JSON Decode Semantics

The built-in JSON decoder converts JSON object payloads, or arrays of JSON objects, into decoded
rows using the stream schema.

## Root Shape

Accepted payload roots:

- A JSON object, decoded as one row.
- A JSON array containing only JSON objects, decoded as multiple rows.

Rejected payload roots:

- Scalars such as strings, numbers, booleans, and `null`.
- Arrays that contain non-object elements.

## Column Mapping

For each schema column:

- A matching JSON object key is decoded into that column's value.
- A missing key decodes to `NULL`.
- Extra JSON keys not present in the schema are ignored.

This keeps the decoded row layout stable and schema-driven.

## Current Scalar Decode Behavior

The current JSON decoder has a mixed strict and native conversion model:

| Schema type | Accepted JSON shape |
| --- | --- |
| `null` | Always decodes to `NULL`. |
| `bool` / `boolean` | JSON boolean. Non-boolean values decode to `NULL`. |
| `int64` | JSON number representable as signed 64-bit integer. Other values decode to `NULL`. |
| `uint64` | JSON number representable as unsigned 64-bit integer. Other values decode to `NULL`. |
| `int8`, `int16`, `int32` | Uses native JSON number conversion today; range enforcement is not applied at decode time. |
| `uint8`, `uint16`, `uint32` | Uses native JSON number conversion today; range enforcement is not applied at decode time. |
| `float32`, `float64` | Uses native JSON number conversion today. |
| `string` | Uses native JSON string conversion today. |
| `bytes` | Base64-encoded JSON string. Invalid base64 and non-string values decode to `NULL`. |

The native conversion path maps JSON values to the closest internal value shape:

- JSON `null` -> `NULL`
- JSON boolean -> `bool`
- JSON signed integer number -> `int64`
- JSON unsigned integer number -> `uint64`
- JSON floating point number -> `float64`
- JSON string -> `string`
- JSON array -> `list`
- JSON object -> `struct`

## Struct Decode

For a `struct` column:

- The JSON value must be an object.
- Declared fields are decoded by field name.
- Missing declared fields decode to `NULL`.
- Extra JSON object fields are ignored.

If the JSON value is not an object, the whole struct value decodes to `NULL`.

## List Decode

For a `list` column:

- The JSON value must be an array.
- Each array element is decoded using the declared element type.
- The list length is preserved.

If the JSON value is not an array, the whole list value decodes to `NULL`.

Decode projection may skip selected list indexes. Skipped indexes remain present as `NULL` so list
positions stay stable.

## Timestamp Decode

`timestamp` accepts RFC3339 strings only. Invalid or unsupported inputs decode to `NULL`.

See `timestamp.md` for the timestamp-specific contract.
