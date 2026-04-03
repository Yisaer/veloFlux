# Schema

Schema definitions for CAN signal decoding and binary packet parsing.

## Module Structure

```
schema/
├── dbc.md    - CAN DBC schema (JSON + .dbc file support)
└── gbf.md    - GBF packet structure schema
```

## Available Schema Types

| Schema | Type Name | Description |
|--------|-----------|-------------|
| [DBC Schema](dbc.md) | `dbc` | CAN signal definitions from JSON or DBC files |
| [GBF Schema](gbf.md) | `gbf` | Binary packet structure definition |
