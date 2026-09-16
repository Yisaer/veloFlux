# Sink Delivery Checkpoint Cache

## Status

This document defines the sink delivery checkpoint-cache design.

## Purpose

When checkpointing is enabled, a successful graceful shutdown must preserve sink deliveries that
have not been confirmed as sent. The cache belongs to `SinkProcessor`; sink connectors do not
participate in checkpoint serialization or restore.

The cache sits at the final encoded payload boundary:

```text
SinkEncoder -> Compress -> Encrypt -> SinkProcessor -> SinkConnector
                                           |
                                           +-> delivery cache in capture mode
```

## Normal Operation

Normal delivery does not pass through the checkpoint cache. `SinkProcessor` sends each completed
delivery directly to the connector using the existing delivery and retry behavior.

## Graceful Shutdown

Source ingestion is quiesced before sink capture begins. Sink delivery capture then uses two
barriers:

1. The runtime sends `SourceQuiesce` through the control channel and waits for its pipeline-tail
   acknowledgement.
2. The runtime sends a non-terminal `SinkCapture` barrier through the control channel.
3. Each `SinkProcessor` stops starting connector deliveries, retains pending or uncertain delivery
   state, enters capture mode, and only then forwards the barrier.
4. Existing control-channel fan-in alignment and the pipeline-tail acknowledgement confirm that all
   sink branches have entered capture mode.
5. The runtime sends the final checkpoint barrier through the data channel.
6. Upstream processors, encoders, compression, and encryption flush before forwarding the final
   barrier. Completed deliveries arriving at a sink during this phase are appended to its in-memory
   cache and are not sent to the connector.
7. When the final barrier reaches a sink, the sink records its ordered cached deliveries in the
   checkpoint snapshot and forwards the barrier.
8. After the final barrier reaches the pipeline tail, the runtime atomically commits the complete
   checkpoint manifest.

A capture, snapshot, barrier, or checkpoint commit failure must prevent graceful shutdown from
reporting success.

## Cache Boundaries

The cache stores complete payload bytes after encoding and delivery transforms. It preserves
delivery boundaries and FIFO order.

The current implementation does not limit the cache by delivery count or total bytes. Capture is
used only during checkpoint-enabled graceful shutdown, after source ingestion has been quiesced,
so the first implementation accepts the remaining finite pipeline output in memory and persists it
at the final checkpoint. Capacity limits, configuration, and the behavior when a limit is reached
are deferred until the shutdown workload and memory requirements are better understood.

An in-flight delivery with no confirmed remote result is retained as uncertain and may be sent
again after restart. This provides at-least-once delivery and may produce duplicates.

Delivery startup is part of the in-flight lifecycle. If capture interrupts `start_delivery`, the
sink treats that delivery as uncertain and invokes connector abort handling. Connector startup must
also be cancellation-safe because blocking work may outlive the cancelled async future. The file
sink therefore owns temporary-file cleanup through its delivery state, including when a detached
blocking startup finishes after capture.

## Snapshot Format

Each sink records an `OperatorSnapshot` with the stable checkpoint key `sink:<sink id>`, operator
kind `sink`, and sink delivery-cache state version `1`. The state contains the FIFO `deliveries`
array. Each entry contains the final payload bytes and uncertain flag. Retry progress is explicitly
not checkpointed: attempt counts, retry deadlines, and backoff state are runtime-only information.
Property snapshots are not checkpointed either. A restored delivery captures the current process
properties immediately before its first connector attempt and reuses that snapshot for retries.
The encoded payload itself is preserved unchanged and may already contain rendered property values.
An empty cache is recorded as an empty array so the sink still participates in the final checkpoint
generation.

The sink snapshot is embedded in the checkpoint manifest together with the other operator
snapshots. The manager serializes the complete manifest as one opaque checkpoint record in the
shared redb backend's independent `checkpoints` table. The sink cache does not currently use a
separate database, table, or blob store.

## Restore

On startup, `SinkProcessor` restores cached deliveries from the latest completed checkpoint and
replays them in FIFO order before consuming new deliveries for the same sink. After replay is
complete, the sink returns to its normal direct-delivery path. Every restored delivery starts a new
retry lifecycle using the sink's current retry configuration. If that lifecycle exhausts its
attempts without a confirmed delivery, replay fails and the processor stops instead of discarding
the restored payload and accepting newer data.

The first implementation targets encoded-byte sinks. Collection-native sink caching is not in the
current scope and has no durable snapshot design yet.
