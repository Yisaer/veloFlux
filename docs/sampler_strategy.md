# Pipeline Sampler Strategy

This document describes the design and implementation of the **Sampler** processor in veloFlux, enabling efficient downsampling of high-frequency data streams.

## Background

In many IoT and automotive use cases, data sources produce events at very high frequencies (e.g., 200Hz-1kHz CAN bus signals). Processing every single event in the pipeline—especially decoding raw bytes into structured tuples—can be prohibitively expensive and unnecessary for downstream consumers that only need updates at a lower rate (e.g., 1Hz or 10Hz).

## Goals

- **Reduce CPU Load**: Downsample data *before* expensive operations like decoding.
- **Configurable Strategy**: Support different downsampling behaviors (e.g., "latest value").
- **Seamless Integration**: Configure sampling at the stream definition level.

## Non-goals

- Complex content-based filtering (handled by `Filter` processor).
- Decoded message rate limiter (use window operators instead).

## Architecture: Bytes-First Processing

To achieve maximum performance, the Sampler is designed as a **bytes-first** processor.

- **Placement**: The `PhysicalSampler` is inserted into the pipeline immediately after the Source and **before** the Decoder (`PhysicalDecoder`).
- **Input**: Operates on `StreamData::Bytes(Vec<u8>)` (raw payloads).
- **Benefit**: Discarded messages are never decoded, saving significant CPU cycles.

The physical plan structure is:
`Source -> Sampler -> Decoder -> [Processors]`

## Configuration

### StreamDefinition

`StreamDefinition` gains an optional `sampler` configuration:

- `sampler.interval`: The sampling window duration (e.g., "100ms", "1s").
- `sampler.strategy`: The sampling strategy to apply.

Supported strategies:
1.  **`latest`**: Keep only the most recent message received within the interval.

Example (JSON):
```json
{
  "name": "can_stream",
  "sampler": {
    "interval": "1s",
    "strategy": "latest"
  }
}
```

## Strategies

### Latest Strategy

The `latest` strategy is a lossy downsampling method ideal for telemetry where intermediate values are less critical than the most current state.

**Algorithm:**
1.  Define a repeating time window of `interval` duration.
2.  Within the window, accept incoming `StreamData::Bytes`.
3.  Overwrite a buffer with the newest incoming payload.
4.  At the end of the window:
    - If a payload exists in buffer, emit it downstream.
    - Clear buffer.
    - Wait for next window.

**Result**: For a 200Hz input and 1s interval, the pipeline processes 1 message per second (the 200th, 400th, etc.), discarding 199 messages *without decoding them*.

### Packer Strategy

The `packer` strategy accumulates multiple raw payloads and merges them using a registered **Merger** (e.g., `CanMerger` for CAN data). On each interval tick, the merged result is emitted as a single payload.

**Use Case**: High-density CAN bus data where multiple signals (distinct CAN IDs) arrive rapidly. The Packer merges them into one consolidated frame, reducing downstream processing while preserving all distinct signals.

**Configuration Example**:
```json
{
  "sampler": {
    "interval": "1s",
    "strategy": {
      "type": "packer",
      "merger": {
        "type": "can_merger",
        "props": { "schema": "/path/to/schema.dbc" }
      }
    }
  }
}
```

**Algorithm:**
1.  Accept incoming `StreamData::Bytes`.
2.  Pass bytes to the registered `Merger::merge()` method.
3.  At the end of the interval, call `Merger::trigger()` to emit the merged payload.
4.  The merged bytes are passed to the downstream Decoder.

**Requirements:**
- The `merger.type` must be registered in the `MergerRegistry` provided by the binary.
- *Note*: `can_merger` in the example above is hypothetical. Users must ensure the specified merger type is available in their VeloFlux distribution.
- The stream decoder must be compatible with the merged output format.

## Implementation checklist

- [x] Extend `StreamDefinition` with `sampler.interval` and `sampler.strategy`.
- [x] Implement `PhysicalSampler` node in physical planner.
- [x] Ensure `PhysicalSampler` wraps `PhysicalSource` and is wrapped by `PhysicalDecoder` (bytes-first).
- [x] Implement `SamplerProcessor` with `latest` strategy logic.
- [x] Implement `SamplerProcessor` with `packer` strategy logic.
- [x] Verify shutdown handling (emit buffered value).
- [x] Verify with integration tests (stats: 5-in/1-out for latest, merger tests for packer).

