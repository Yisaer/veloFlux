# GBF Schema

GBF (General Binary Format) schema defines binary packet structures for decoding.

## Overview

GBF schemas describe:
- **Packet structure**: Header fields, payload fields
- **Custom types**: Reusable frame definitions
- **Embedded formats**: Payload decoding (e.g., CAN signals)

---

## Schema Structure

```json
{
  "types": {
    "frame_type": { ... }
  },
  "packet": {
    "fields": [ ... ]
  }
}
```

| Property | Description |
|----------|-------------|
| `types` | Named type definitions (reusable structures) |
| `packet` | Root packet definition |

---

## Supported Types

| Type | Size | Description |
|------|------|-------------|
| `u8` | 1 byte | Unsigned 8-bit |
| `u16be` | 2 bytes | Unsigned 16-bit big-endian |
| `u16le` | 2 bytes | Unsigned 16-bit little-endian |
| `u32be` | 4 bytes | Unsigned 32-bit big-endian |
| `u32le` | 4 bytes | Unsigned 32-bit little-endian |
| `u64be` | 8 bytes | Unsigned 64-bit big-endian |
| `u64le` | 8 bytes | Unsigned 64-bit little-endian |
| `bytes` | Variable | Raw byte payload |
| `sequence` | Variable | Array of typed items |

---

## Field Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | String | Field name |
| `type` | String | Field type |
| `const` | u64 | Magic byte constraint |
| `length_ref` | String | Reference to length field |
| `length_unit` | String | `"bytes"` or `"count"` |
| `item` | Object | Type for sequence items |
| `format` | Object | Marks embedded payload |
| `read_mask` | u64 | Bit mask after reading |
| `read_shift` | u32 | Bit shift after masking |

---

## Example: CAN Packet

```json
{
  "types": {
    "can_frame": {
      "fields": [
        { "name": "magic", "type": "u8", "const": 85 },
        { "name": "can_id", "type": "u16be" },
        { "name": "data_len", "type": "u8" },
        {
          "name": "payload",
          "type": "bytes",
          "length_ref": "data_len",
          "format": { "id_ref": "can_id" }
        }
      ]
    }
  },
  "packet": {
    "fields": [
      { "name": "ts", "type": "u64be" },
      { "name": "total_len", "type": "u16be" },
      {
        "name": "frames",
        "type": "sequence",
        "item": { "type": "can_frame" },
        "length_ref": "total_len",
        "length_unit": "bytes"
      }
    ]
  }
}
```

---

## Bit-Field Extraction

Use `read_mask` and `read_shift` for packed fields:

```json
{
  "name": "data_len",
  "type": "u8",
  "read_mask": 127,
  "read_shift": 0
}
```

Extracts: `value & 0x7F`

---

## Loading

```rust
use veloflux_ex::schema::gbf::GbfSchema;

let schema = GbfSchema::load("/path/to/packet.json")?;
```

---

## See Also

- [GBF Decoder](../decoder/gbf.md) - Full decoder documentation
- [DBC Schema](dbc.md) - CAN signal definitions
