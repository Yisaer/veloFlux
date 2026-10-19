# StreamingEncoderRewrite

## Summary

Fuses a `PhysicalBatch → PhysicalSinkEncoder` chain into a single
`PhysicalIncSinkEncoder`.

## Motivation

The physical plan builder always inserts a `PhysicalBatch` node when batching is
enabled (`batch_count` or `batch_duration`). This creates a two-node chain:

```
PhysicalBatch → PhysicalSinkEncoder(Immediate)
```

For registered sink encoders, the `BatchProcessor` and `SinkEncoderProcessor`
can be merged into one fused processor, eliminating an extra data-pass and an
async task. The `SinkEncoder` delivery API is streaming by contract: an encoder
that cannot emit incrementally may buffer records in `append` and emit from
`finish_delivery`.

## Conditions

The rule fuses the chain when **all** of the following hold:

1. The current node is `PhysicalSinkEncoder`.
2. Its unique child is `PhysicalBatch`.

`encoder=none` branches do not contain `PhysicalSinkEncoder`, so they keep a
standalone `PhysicalBatch`.

## Rewrite

### Before

```
PhysicalSinkEncoder(encoder=json, common=default)
  └─ PhysicalBatch(batch_count=10, batch_duration=...)
      └─ PhysicalProject / ...
```

### After

```
PhysicalIncSinkEncoder(encoder=json, batch_count=10, batch_duration=...)
  └─ PhysicalProject / ...
```

The fused node carries:

- `sink_id` and `encoder` from `PhysicalSinkEncoder`
- `common` (batch params) from `PhysicalBatch`
- `children` from `PhysicalBatch` (skipping the batch node)

## Runtime Mapping

SinkEncoder and IncSinkEncoder both lower to the shared synchronous EncoderStage.
Builtin encoder registrations explicitly opt in to region execution; custom registrations
default to an isolated task. The former retains Immediate policy, and the latter carries
CountOnly / DurationOnly / Combined policy from the original batch. Eligible encoders,
computation, and byte transforms share a region without internal channels. PhysicalBatch
for native output uses the shared count/timed batch engines independently of encoding.

The standalone SinkEncoderProcessor uses EncoderDriver for transport and timer waits;
its framing and batching algorithms are the same EncoderStage methods used by regions.
The fixed PhysicalFusedTransform rewrite has been removed from SQL optimization.

## Output Layout

Both immediate and incremental sink encoders receive the same planner-derived
`OutputLayout`. Fusing the batch node does not change fixed value references or
the logical output order.
