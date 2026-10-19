# Watermarks and Time Progression

veloFlux uses **watermarks** to drive time progression for time-aware operators. Window
processors do not read wall-clock directly; they flush based on incoming watermark messages.

This document focuses on how processing-time and event-time watermark streams are produced and
how they interact with window operators.

See also: `docs/syntax/windows/tumbling_window.md` and `docs/syntax/windows/sliding_window.md`.

## Concepts

- **Tuple timestamp**: Each tuple carries a `timestamp` (a `SystemTime`) which is used as the
  time coordinate for windowing.
- **Watermark**: A message that signals "time has advanced up to T". Window operators may flush
  state when they observe watermarks that pass window boundaries.

## Watermark Producers

veloFlux configures the synchronous `WatermarkStage` for the selected time mode. The region
driver owns timer waits and invokes the stage on input, periodic ticks, or deadlines:

### Processing-time watermark stage

The region schedules the clocks; the stage generates watermarks according to the time-based
window configuration:

- Tumbling processing-time mode emits periodic watermarks at a fixed interval.
- Sliding processing-time mode emits periodic watermarks (tick interval is `1s`) to advance
  time and enable downstream window GC. With `slidingwindow(..., lookahead)`, the stage also
  exposes per-tuple deadlines at `tuple.timestamp + lookahead`. The region schedules these
  callbacks, and the stage produces the corresponding deadline watermarks.

### Event-time watermark stage

`WatermarkStage` configured for event time produces the event-time watermark stream,
independently of processing-time watermark generation.

## Window Stages Consume Watermarks

`TumblingWindowStage` and `SlidingWindowStage` are watermark-driven:

- They flush and emit windows based on watermark messages observed upstream.
- Which semantics they implement (processing time vs event time) is determined by which watermark
  stage sits immediately upstream.
- The same operator logic is used for both modes; the contract is "flush based on received
  watermarks".

## Planning (High-level)

Physical planning inserts a watermark stage before time-aware window stages as needed:

- For processing-time execution, it inserts a process-time watermark stage configured for the
  window type.
- For event-time execution, it inserts an event-time watermark stage.

For `slidingwindow(..., lookahead)`, delayed emission requires the upstream watermark stream to
advance to each trigger's deadline (`t + lookahead`). In processing time, the sliding
configuration of `WatermarkStage` provides these deadline watermarks. In event time, this depends on upstream event-time watermark
behavior and must be treated as part of the operator contract.
