# Encoders

Custom encoders for transforming data into various output formats.

## Module Structure

```
encoder/
├── columnar_csv_json.md   - Columnar JSON with CSV-style comma-separated values
└── columnar_json.md       - Columnar JSON with proper JSON arrays
```

## Available Encoders

| Encoder | Type Name | Description |
|---------|-----------|-------------|
| [ColumnarCsvJsonEncoder](columnar_csv_json.md) | `columnar_csv_json` | Time-batched CSV-in-JSON |
| [ColumnarJsonEncoder](columnar_json.md) | `columnar_json` | Standard columnar JSON arrays |
