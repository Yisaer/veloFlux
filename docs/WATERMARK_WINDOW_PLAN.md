# Watermark-Driven Windows Plan

This document captures the requirements, design, and implementation steps for unifying time-based window processing around watermark-driven time progression (supporting both wall-clock and event-time scenarios).

## Requirements
- Add a `timestamp` column to tuples; decoders populate it with the current time by default.
- In physical planning, automatically insert a `PhysicalWatermark` before time-aware window operators (e.g., tumbling window) and streaming aggregations that depend on window time.
- Implement a Watermark processor that advances time either by wall-clock ticker or by consuming upstream watermarks (event time).
- Implement a TumblingWindow processor that closes and emits windows based on received watermarks instead of directly reading system time.
- Refactor `StreamingTumblingAggregationProcessor` to advance time and emit aggregations based on received watermarks (removing its internal ticker dependency).

## Design Overview
- **Data plane**: Extend tuple schema with `timestamp`, keeping schema validation and (de)serialization aligned. Decoders set wall-clock timestamps; event-time sources can override as needed.
- **Planning**: When constructing physical plans for time-based windows/streaming aggregations, insert a `PhysicalWatermark` node ahead of the window operator. Configuration covers wall-clock (ticker period derived from window config) and event-time (pass-through/align upstream watermarks).
- **Processors**:
  - **WatermarkProcessor**: In wall-clock mode, emits watermarks on a ticker; in event-time mode, forwards/aligns incoming watermarks. Ensures monotonicity and cooperates with backpressure.
  - **TumblingWindowProcessor**: Maintains window buffers; advances/flushes windows when watermark passes window end boundaries. No direct dependency on system time.
  - **StreamingTumblingAggregationProcessor**: Uses incoming watermarks to close aggregation windows and emit results; removes internal ticker and aligns with TumblingWindow semantics.

## Implementation Steps
1. **Tuple timestamp**: Extend tuple struct/schema with `timestamp`; update decoder to populate it with current time by default and adjust serialization/schema validation.
2. **Planner insertion**: Update `PhysicalPlanner` to inject `PhysicalWatermark` before time-based `PhysicalWindow` and streaming aggregation nodes; derive ticker interval from window config for wall-clock mode, allow pass-through for event time.
3. **Watermark processor**: Implement `WatermarkProcessor` with wall-clock ticker emission and event-time forwarding; ensure ordered, monotonic watermarks and clean shutdown behavior.
4. **TumblingWindow processor**: Implement processor that buffers by window boundaries and flushes on watermark advancement; handle late/closing semantics consistently; remove system-time usage.
5. **Streaming aggregation**: Refactor `StreamingTumblingAggregationProcessor` to drive window lifecycle via incoming watermarks (no internal ticker); align with TumblingWindow processor behavior.
6. **Testing**: Add/adjust tests covering parse → plan (with injected watermark) → watermark processor → window/aggregation behavior for both wall-clock (ticker simulated) and event-time (injected watermarks) paths; verify window closure and output timing.

