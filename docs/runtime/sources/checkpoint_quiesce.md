# Source Checkpoint Quiesce

## Purpose

Checkpoint-enabled graceful shutdown stops source ingestion before sink capture and final
checkpoint propagation. `SourceQuiesce` reuses the source checkpoint-fence protocol; it does not
introduce a separate source shutdown mechanism.

## Runtime State

`DataSourceProcessor` uses three externally meaningful states:

```text
Running -> Quiesced -> Stopped
```

There is no separate `Quiescing` state. Handling `SourceQuiesce` is one awaited control operation,
and overlapping in-flight control signals are outside the protocol.

## SourceQuiesce Handling

`SourceQuiesce` is a non-terminal barrier sent through the control channel. A datasource handles it
as follows:

1. Request a quiescing checkpoint fence from its source connector.
2. Continue forwarding payloads that precede the connector fence.
3. Receive the matching `CheckpointFence` and the connector-owned checkpoint state.
4. Stage that state in `DataSourceProcessor` and enter `Quiesced`.
5. Forward `SourceQuiesce` only after the state is staged and the connector has stopped producing.

Control-channel fan-in alignment and the pipeline-tail acknowledgement then confirm that every
source branch is quiesced. The acknowledgement does not mean that the staged source data has
crossed the complete data pipeline; the final data-channel checkpoint barrier provides that
ordering guarantee.

## Connector Fence Mode

The source checkpoint request distinguishes a continuing fence from a quiescing fence:

```text
Continue: emit the fence and resume production
Quiesce:  emit the fence and stop production
```

For `Quiesce`, stopping production and emitting the fence belong to the same connector operation.
Closing a connector only after `DataSourceProcessor` observes the fence is insufficient because it
could allow post-fence payloads to be produced in between.

## Final Checkpoint Handling

When the final checkpoint barrier later reaches a quiesced datasource, the processor records the
staged source state under the final checkpoint ID, forwards the final barrier, closes the connector,
and stops. The `SourceQuiesce` barrier ID identifies the fence operation only; source, operator, and
sink snapshots all belong to the final checkpoint generation.

A direct final-checkpoint request remains valid as a fallback. If no source state has been staged,
the datasource establishes a quiescing fence, records its state, and completes the final checkpoint
using the existing combined path.

Repeated `SourceQuiesce` in `Quiesced`, or a final checkpoint that cannot obtain valid staged source
state, fails the graceful shutdown instead of committing an incomplete checkpoint.

The test-only scripted mock source uses the same fence contract. Its checkpoint state is the index
of the next configured payload, so restarting a pipeline does not re-emit payloads already placed
before the committed source fence.
