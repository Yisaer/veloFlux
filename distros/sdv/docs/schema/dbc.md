# DBC Schema

CAN database schema for defining signal structures. Supports both legacy JSON format and standard Vector DBC files.

## Overview

The DBC schema defines:
- **Buses**: CAN network channels (chassis, powertrain, etc.)
- **Messages**: CAN frames identified by ID
- **Signals**: Data fields within messages with encoding parameters

## Supported Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| JSON | `.json` | Custom JSON format with buses, messages, and signals |
| DBC | `.dbc` | Standard Vector DBC format |
| Directory | folder | Multiple DBC files with naming convention |

---

## Loading Schema

The schema loader auto-detects the format based on the path:

```rust
use veloflux_ex::schema::dbc::load_can_schema;

// Single DBC file (defaults to Bus ID 0, Name "Bus0")
let schema = load_can_schema("/path/to/signals.dbc")?;

// Single JSON file
let schema = load_can_schema("/path/to/schema.json")?;

// Directory of DBC files
let schema = load_can_schema("/path/to/dbc_dir/")?;
```

---

## Directory Naming Convention

When loading a directory, all `.dbc` files **must** follow the pattern:

```
{id}_{name}.dbc
```

| Filename | Bus ID | Bus Name |
|----------|--------|----------|
| `1_chassis.dbc` | 1 | chassis |
| `2_powertrain.dbc` | 2 | powertrain |
| `3_body_control.dbc` | 3 | body_control |

**Validation Rules:**
- All files must match the naming pattern → Error on invalid format
- Bus IDs must be unique → Error on duplicate ID

---

## JSON Format

```json
{
  "buses": [
    {
      "name": "Chassis",
      "id": 1,
      "messages": [
        {
          "name": "WheelSpeed",
          "id": 256,
          "frameId": "0x100",
          "length": 8,
          "signals": [
            {
              "name": "FrontLeft",
              "start": 0,
              "length": 16,
              "scale": 0.01,
              "offset": 0,
              "isBigEndian": false,
              "isSigned": false
            }
          ]
        }
      ]
    }
  ]
}
```

### Signal Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | String | Signal name (used in column: `{bus}__{frameId}__{name}`) |
| `start` | u32 | Start bit position |
| `length` | u32 | Bit length |
| `scale` | f64 | Factor: `physical = raw * scale + offset` |
| `offset` | f64 | Offset: `physical = raw * scale + offset` |
| `isBigEndian` | bool | `true` = Motorola, `false` = Intel byte order |
| `isSigned` | bool | `true` for signed values |
| `isMultiplexer` | bool | `true` if this is the MUX selector |
| `isMultiplexed` | bool | `true` if this signal is multiplexed |
| `multiplexerValue` | i64 | MUX value that activates this signal |

### Output Column Naming

Signals are named using the pattern:

```
{BusName}__{FrameIdHex}__{SignalName}
```

Examples:
- `Chassis__0x100__FrontLeft`
- `Powertrain__0x200__EngineRPM`

### Data Types

| Condition | Output Type |
|-----------|-------------|
| Integer scale and offset | `Int64` |
| Fractional scale or offset | `Float64` |

### Multiplexed Signals

Multiplexed signals share the same bit positions but are activated by different MUX selector values:

```json
{
  "signals": [
    {
      "name": "MuxSelector",
      "start": 0,
      "length": 8,
      "isMultiplexer": true
    },
    {
      "name": "Speed",
      "start": 8,
      "length": 16,
      "isMultiplexed": true,
      "multiplexerValue": 0
    },
    {
      "name": "Temperature",
      "start": 8,
      "length": 16,
      "isMultiplexed": true,
      "multiplexerValue": 1
    }
  ]
}
```

When `MuxSelector = 0`, only `Speed` is decoded. When `MuxSelector = 1`, only `Temperature` is decoded.

---

## Decoder Configuration

Use DBC schema with the GBF decoder:

```json
{
  "type": "gbf",
  "props": {
    "schema_path": "/path/to/packet.json",
    "format_type": "can",
    "format_schema_path": "/path/to/signals.dbc"
  }
}
```

The `format_schema_path` accepts `.dbc`, `.json`, or a directory path.

