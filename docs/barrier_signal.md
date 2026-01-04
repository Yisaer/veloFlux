# Barrier Control Signals

## Background

In the `flow` module, processors communicate through two strictly isolated channels:

- **Data channel**: carries `StreamData` (including `StreamData::Control(ControlSignal)`).
- **Control channel**: carries `ControlSignal` directly.

Both channels may carry control signals, and both channels can have **multiple upstreams**
connected to a single processor.

In our design, **every processor is barrier-aware**: a processor may need to delay handling
some control signals until **all upstreams** have reached the same synchronization point,
regardless of whether the processor is “join-like” in a relational sense.

Example: a pipeline has multiple sinks; the result collector sits downstream of them and
should not handle a “sync” signal until every upstream sink has produced it.

## Design Goals

- Provide a **generic**, reusable mechanism so processors can “wait for all upstreams” before
  acting on some control signals.
- Keep **channel isolation**: the barrier state for the data channel and the control channel must
  be maintained independently.
- Avoid requiring **upstream identity**: alignment only depends on “expected upstream count” and
  arrival counts, not which upstream sent the signal.
- Support multiple occurrences over time using a monotonically increasing `barrier_id`.

## Signal Model

`ControlSignal` is split into two categories:

- `ControlSignal::Instant(InstantControlSignal)`
  - Processed immediately (no alignment).
- `ControlSignal::Barrier(BarrierControlSignal)`
  - Must be aligned across upstreams by some processors before it is processed.

Barrier signals consist of:

- `BarrierControlSignalKind`: the “kind” without id (e.g. `StreamGracefulEnd`, `SyncTest`).
- `BarrierControlSignal`: the “kind + barrier_id” (e.g. `StreamGracefulEnd { barrier_id }`).

### Channel Carrier Rule

A barrier signal is delivered via **exactly one channel**, determined by the upstream API that
emits it (control vs data). There is no “broadcast the same signal to both channels” behavior.

## `barrier_id` Allocation

`barrier_id` is allocated at the pipeline head (`ControlSourceProcessor`) using a monotonically
increasing counter shared by both channels. The head constructs a `BarrierControlSignal` by
calling `BarrierControlSignalKind::with_id(barrier_id)` and then sends it via the chosen channel.

This ensures:

- Barriers can appear multiple times over the lifetime of a pipeline.
- A processor can detect and reject overlapping barriers deterministically.

## Barrier Alignment Mechanism

Barrier alignment is implemented in `src/flow/src/processor/barrier.rs` as `BarrierAligner`.

Each processor is expected to integrate barrier alignment and maintain **two independent
instances**:

- One for barrier signals carried by the **data channel** (`StreamData::Control(...)`).
- One for barrier signals carried by the **control channel** (`ControlSignal`).

Alignment key:

- `BarrierKey = (barrier_id, kind)`

Internal state:

- `arrived_count`: number of upstream arrivals for the current key.

Rules:

- **Expected upstreams** are configured per channel via `expected_upstreams = inputs.len()`.
- If `expected_upstreams == 1`, the barrier is immediately complete.
- **No overlap**: while a barrier is pending, receiving a different `(barrier_id, kind)` is an error.
- **No duplicates**: receiving more than `expected_upstreams` for the same key is an error.

Processing flow:

- On `ControlSignal::Instant`, return it immediately.
- On `ControlSignal::Barrier`, call the aligner:
  - `Pending` → swallow (do not process/forward yet).
  - `Complete(barrier)` → return the barrier for downstream processing.

This logic is shared as `align_control_signal(...)` to avoid duplicating “instant vs barrier”
handling across processors.

## Processor Integration

Barrier alignment is a cross-cutting concern: processors should apply the same alignment rule
whenever they receive a `ControlSignal`, no matter whether the signal arrives via the control
channel or is carried inside `StreamData::Control` on the data channel.

At the time of writing, barrier alignment is wired into `ResultCollectProcessor` and
`FilterProcessor` as the initial adopters, and the same mechanism is intended to be rolled out
to all processors.

When a processor receives a control signal (either via the control channel or wrapped in
`StreamData::Control` on the data channel), it uses:

- `align_control_signal(&mut aligner, signal)`

If `align_control_signal` returns `None`, the processor waits for the remaining upstream arrivals.
If it returns `Some(signal)`, the processor processes the signal as usual.

Default “processing” behavior for control signals is:

- **Forward downstream** via the same channel type (data/control isolation still applies).

Some barrier kinds may trigger additional actions before forwarding.
For example, `StreamGracefulEnd` is a terminal barrier that lets processors perform graceful
shutdown work and then forward the signal.

## Terminal Semantics

Processors exit only when they observe a terminal control signal, as determined by:

- `ControlSignal::is_terminal()`

Terminal barrier signals are still subject to alignment: a processor only exits after the barrier
reaches `Complete` (i.e. after all upstreams delivered the same barrier).

## Testing Support: `SyncTest`

`BarrierControlSignal::SyncTest { barrier_id }` is a no-op barrier used by unit tests to validate
alignment behavior without attaching any additional semantics to the barrier itself.

The `ResultCollectProcessor` tests validate:

- A barrier on the **data channel** is forwarded only after all upstreams deliver it.
- A barrier on the **control channel** is forwarded only after all upstreams deliver it.
