# GBF Schema

GBF (General Binary Format) schema defines binary packet structures for decoding.

## Overview

> **Breaking change (VF-83.1):** The root packet key has been renamed from
> `"packet"` to `"structure"`. The named-type library (`"types"` key) has been
> removed. All struct definitions must be inlined inside the `"structure"` field
> of the sequence that uses them (replaces `"item"`). See the Migration Guide below.

GBF schemas describe:
- **Packet structure**: Header fields, payload fields
- **Embedded formats**: Payload decoding (e.g., CAN signals)

---

## Schema Structure

```json
{
  "structure": {
    "type": "struct",
    "fields": [ ... ]
  }
}
```

| Property | Description |
|----------|-------------|
| `structure` | Root packet definition |

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
| `length_unit` | String | `"bytes"` (only supported value) |
| `structure` | Object | Definition for sequence items (replaces `"item"`) |
| `format` | Object | Marks embedded payload |
| `format.id_ref` | String | **Required** when `format` is present — name of the sibling integer field that carries the CAN ID |
| `read_mask` | u64 | Bit mask after reading |
| `read_shift` | u32 | Bit shift after masking |

---

## Example: CAN Packet

```json
{
  "structure": {
    "type": "struct",
    "fields": [
      { "name": "ts", "type": "u64be" },
      { "name": "total_len", "type": "u16be" },
      {
        "name": "frames",
        "type": "sequence",
        "length_ref": "total_len",
        "length_unit": "bytes",
        "structure": {
          "type": "struct",
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
use veloflux_sdv::schema::gbf::GbfSchema;

let schema = GbfSchema::load("/path/to/packet.json")?;
```

---

## See Also

- [GBF Decoder](../decoder/gbf.md) - Full decoder documentation
- [DBC Schema](dbc.md) - CAN signal definitions

---

## Migration Guide

When migrating from earlier schema versions:

**Before:**
```json
{
  "types": {
    "can_frame": {
      "fields": [
        { "name": "magic",    "type": "u8",    "const": 85 },
        { "name": "can_id",   "type": "u16be" },
        { "name": "data_len", "type": "u8",    "read_mask": 127 },
        { "name": "payload",  "type": "bytes", "length_ref": "data_len",
          "format": { "id_ref": "can_id" } }
      ]
    }
  },
  "packet": {
    "fields": [
      { "name": "ts",        "type": "u64be" },
      { "name": "total_len", "type": "u16be" },
      { "name": "frames",    "type": "sequence",
        "item": { "type": "can_frame" },
        "length_ref": "total_len", "length_unit": "bytes" }
    ]
  }
}
```

**After:**
```json
{
  "structure": {
    "type": "struct",
    "fields": [
      { "name": "ts",        "type": "u64be" },
      { "name": "total_len", "type": "u16be" },
      { "name": "frames",    "type": "sequence",
        "length_ref": "total_len", "length_unit": "bytes",
        "structure": {
          "type": "struct",
          "fields": [
            { "name": "magic",    "type": "u8",    "const": 85 },
            { "name": "can_id",   "type": "u16be" },
            { "name": "data_len", "type": "u8",    "read_mask": 127 },
            { "name": "payload",  "type": "bytes", "length_ref": "data_len",
              "format": { "id_ref": "can_id" } }
          ]
        }
      }
    ]
  }
}
```
