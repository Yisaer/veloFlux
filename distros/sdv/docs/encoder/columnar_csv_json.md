# ColumnarCsvJsonEncoder

A streaming columnar JSON encoder with time-based batching.

## Overview

Accumulates record values into column buffers and emits columnar JSON when the batch duration elapses. Values are comma-separated strings.

## Output Format

```json
{"email":"a@b.com,c@d.com","id":"1,2","name":"foo,bar"}
```

## Configuration

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | String | Encoder identifier for logging/metrics |
| `batch_duration` | Duration | Time to accumulate records before flushing |

## Example

```rust
use encoder::ColumnarCsvJsonEncoder;
use std::time::Duration;

let encoder = ColumnarCsvJsonEncoder::new("my_encoder", Duration::from_secs(1));
```
