# Schema Data Types

Stream schemas declare the logical type of each decoded column. The schema type is the contract used
by decoders, expression evaluation, planner schema binding, and sink encoders.

## Column Syntax

A scalar column declares a name and a data type:

```json
{
  "name": "speed",
  "data_type": "float64"
}
```

The initial timestamp design uses the same shape:

```json
{
  "name": "event_time",
  "data_type": "timestamp"
}
```

Column-level `props` are intentionally not part of the initial timestamp implementation. Future
versions may add `props.decode` for per-column decode formats without changing the `data_type`
contract.

## Built-In Scalar Types

| Type | Meaning |
| --- | --- |
| `null` | Missing or unknown value. |
| `bool`, `boolean` | Boolean value. |
| `int8` | Signed 8-bit integer. |
| `int16` | Signed 16-bit integer. |
| `int32` | Signed 32-bit integer. |
| `int64` | Signed 64-bit integer. |
| `uint8` | Unsigned 8-bit integer. |
| `uint16` | Unsigned 16-bit integer. |
| `uint32` | Unsigned 32-bit integer. |
| `uint64` | Unsigned 64-bit integer. |
| `float32` | 32-bit floating point number. |
| `float64` | 64-bit floating point number. |
| `string` | UTF-8 string. |
| `bytes` | Opaque binary payload. |
| `timestamp` | Absolute timestamp value normalized to UTC. |

## List Syntax

Lists declare one element type:

```json
{
  "name": "samples",
  "data_type": "list",
  "element": {
    "name": "element",
    "data_type": "float64"
  }
}
```

The `element.name` value is descriptive. The element's `data_type` defines the list item type.

## Struct Syntax

Structs declare a fixed set of fields:

```json
{
  "name": "vehicle",
  "data_type": "struct",
  "fields": [
    {
      "name": "id",
      "data_type": "string"
    },
    {
      "name": "speed",
      "data_type": "float64"
    }
  ]
}
```

Nested `list` and `struct` declarations use the same `element` and `fields` shape recursively.

## Type Names

Schema parsing is case-insensitive for `data_type` names. Schema introspection should render type
names in lowercase canonical form, for example `float64`, `string`, or `struct`.
