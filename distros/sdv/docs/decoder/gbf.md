# GBF Decoder

The GBF (General Binary Format) decoder converts binary packets into structured data. GBF is a **container format** that defines packet structure, while the **embedded payload format** (e.g., DBC for CAN signals) is specified separately.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Decoder Configuration                       │
├─────────────────────────────────────────────────────────────────┤
│ schema_path         │ Path to GBF schema JSON (structure)       │
│ format_type         │ Embedded payload format type (e.g., "dbc")│
│ format_schema_path  │ Path to format schema (e.g., DBC JSON)    │
└─────────────────────────────────────────────────────────────────┘
```

**Key Design Principle**: The GBF schema defines *structure only*. Embedded payload format information is external, allowing:
- Reuse of format schemas (e.g., one DBC shared across multiple GBF schemas)
- Clean separation of container vs. content definitions

---

## GBF Schema

The GBF schema is a JSON file defining packet structure using primitive types and nested structures.

### Supported Types

| Type | Size | Description |
|------|------|-------------|
| `u8` | 1 byte | Unsigned 8-bit integer |
| `u16be` | 2 bytes | Unsigned 16-bit big-endian |
| `u16le` | 2 bytes | Unsigned 16-bit little-endian |
| `u32be` | 4 bytes | Unsigned 32-bit big-endian |
| `u32le` | 4 bytes | Unsigned 32-bit little-endian |
| `u64be` | 8 bytes | Unsigned 64-bit big-endian |
| `u64le` | 8 bytes | Unsigned 64-bit little-endian |
| `bytes` | Variable | Raw byte payload |
| `sequence` | Variable | Array of typed items |

### Field Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | String | Field name for reference |
| `type` | String | One of the supported types |
| `const` | u64 | Constant value constraint (e.g., magic bytes) |
| `length_ref` | String | Reference to field containing length |
| `length_unit` | String | `"bytes"` or `"count"` for sequences |
| `item` | Object | Type reference for sequence items |
| `format` | Object | Marks field as embedded payload (see below) |
| `read_mask` | u64 | Bit mask applied after reading |
| `read_shift` | u32 | Bit shift applied after masking |

### Embedded Payload

Fields with a `format` object are treated as **embedded payloads**. The format type is determined by the decoder configuration, not the schema:

```json
{
  "name": "payload",
  "type": "bytes",
  "length_ref": "data_len",
  "format": {
    "id_ref": "can_id"
  }
}
```

- `id_ref`: Reference to the field containing the message ID (used for signal lookup)

---

## Example Schema

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

## Decoder Configuration

```json
{
  "type": "gbf",
  "props": {
    "schema_path": "/path/to/packet.json",
    "format_type": "can",
    "format_schema_path": "/path/to/signals.dbc.json"
  }
}
```

| Property | Required | Description |
|----------|----------|-------------|
| `schema_path` | Yes | Path to GBF schema JSON |
| `format_type` | Yes | Embedded format type (e.g., `"can"`) |
| `format_schema_path` | Yes | Path to format-specific schema (e.g., DBC JSON) |

---

## Edge Cases

### Frame Alignment Recovery

If a frame's magic byte doesn't match, the decoder scans forward byte-by-byte until it finds the next valid magic byte (`0x55`), enabling recovery from corrupted or padded data.

### Bit-Field Extraction

Use `read_mask` and `read_shift` for packed fields:

```json
{
  "name": "data_len",
  "type": "u8",
  "read_mask": 240,
  "read_shift": 4
}
```

This extracts the high nibble: `(value & 0xF0) >> 4`

### Padding

Zero bytes (`0x00`) between frames are automatically skipped.

---

## Signal Value Types

| Condition | Output Type |
|-----------|-------------|
| `factor = 1.0` AND `offset = 0.0` | `Int64` |
| Factor and offset are integers | `Int64` |
| Fractional factor or offset | `Float64` |

## Bit Ordering

- **Little-endian (Intel)**: `is_big_endian = false` – LSB first
- **Big-endian (Motorola)**: `is_big_endian = true` – MSB first

---

## Error Handling

The GBF decoder handles various error conditions gracefully:

### Errors (decode returns error)

| Condition | Behavior |
|-----------|----------|
| Empty payload | Returns error: "no packets found" |
| Payload smaller than header | Returns error: "no packets found" |
| No valid frames in any packet | Returns error: "no valid frames decoded" |

### Graceful Recovery

| Condition | Behavior |
|-----------|----------|
| Invalid magic byte | Skips to next byte, continues scanning |
| Truncated frame (payload shorter than `data_len`) | Stops processing current packet, uses available frames |
| Frame without matching DBC message | Signal values set to `Null`, other signals unaffected |
| Packet with zero frames (heartbeat) | Creates dummy frame with timestamp, returns row with all nulls |

### Example: Truncated Packet

If `total_len` claims 100 bytes but only 14 bytes are present:
- Decoder reads available data
- Partial frames are discarded
- If no complete frames exist, returns error

### Example: Invalid Magic Recovery

```
[0xAA] [0x55] [can_id] [len] [payload]
 ↑      ↑
 skip   found valid magic, continue decoding
```

The decoder scans forward to find the next `0x55` magic byte, enabling recovery from:
- Corrupted data
- Padding bytes
- Misaligned frames
