# Operator Fusion Plan (Historical)

Status: superseded by [general execution regions](execution_regions.md).

This document records the former fixed sink-template design and measurements from the
experimental `feat/fuse-stateless-sink` branch. Stateful/window exclusions below are not
restrictions of the region architecture.

Production SQL planning no longer emits PhysicalFusedTransform; its physical variant and
runtime entry are removed. The former executor and standalone engine channel wrappers have also been removed.
EncoderStage is synchronous, and the region driver owns transport waits.
See the region document for current adapters, placement, diagnostics, and remaining work.

## Background

emqx/VeloFlux#107 asks for Spark whole-stage style operator fusion adapted to
true streaming: fuse stateless operators into one physical unit, keep clear
boundaries for stateful operators, stay record-by-record (no micro-batching).

The rekuiper comparison (1 core, same MQTT data) showed VeloFlux spending
~2.5µs per rule evaluation vs ~1.2µs for rekuiper on narrow fan-out workloads.
Stage-level attribution (`handle_duration` histograms, N=200 shared-stream
fan-out) put ~60% of that cost *between* tasks (channels, wakeups, scheduler)
and only ~40% inside stage handlers, with SQL evaluation near zero. That makes
task-boundary elimination, not expression optimization, the highest-ROI lever,
and it is exactly what #107 prescribes.

## Phase 0: Task-Level Fusion of Sink-Terminating Stateless Chains (Measured)

Implemented on the experimental branch: optimizer rule `fuse_stateless_sink`
plus `FusedStatelessSinkProcessor`. (Superseded: the connector now stays
outside the fused span; see Phase 3. Rule renamed to
`fuse_stateless_transform`, node to `PhysicalFusedTransform`.)

### What It Fuses

`Filter? -> Project -> [Immediate SinkEncoder] -> SinkConnector(nop)` becomes
one `PhysicalFusedStatelessSink` node and one task. Row transforms reuse the
original row-stage functions. This historical prototype also copied the
sink delivery machine; Phase 3 removed that duplication and retains the
standalone connector task.

### Gates (Anything Else Keeps the Unfused Plan)

- Sink connector is `nop`.
- No `Batch`/`IncSinkEncoder` (batch timer flush stays on its own task).
- No `RowDiff`/`EmptySuppress`/`ColumnFilter`/`Compress`/`Encrypt` in the span.
- No pipeline-state counters on filter/project (`processor_state`,
  non-empty `pipeline_state_usage`).
- Single-child spine everywhere (a multi-child filter bails instead of
  dropping a leg). Shared stages are fusion boundaries: a consumed node
  with multiple consumers keeps the branch unfused.

### Measured Effect (s1, 500 msg/s, single core, same mqttgen data)

| Rules | Config | Before | Fused | rekuiper |
|-------|--------|--------|-------|----------|
| 200 | json, no batch | 26.1% | 16.5% | 13.5% |
| 200 | encoder none | 19.5% | 12.9% | 13.5% |
| 500 | encoder none | 52.3% | 33.9% | 31.2% |

Heap also drops (fewer tasks/queues: 60 MiB -> 32 MiB anon at N=500).
Single-rule MQTT throughput (w1 up to 200k msg/s, w2 windows) is unaffected:
batched/windowed plans do not match the rule.

### What It Does Not Do

- Intermediate `Collection`s between filter/project/encode are still
  materialized; only channel hops and wakeups are gone. The cache-locality
  half of #107 (no intermediate data structures) is Phase 2 work.

## Design Decisions

1. **Observability follows actual operators.** Fused pipelines expose one
   stats entry per fused task (`PhysicalFusedStatelessSink`) instead of one
   per legacy stage. No shadow/legacy processor IDs are synthesized. Pipeline
   level totals that summed per-stage counters change meaning accordingly
   (e.g. `mpsc_links` 5 -> 3 on the canonical chain); tests assert the new
   shape, not the old numbers.
2. **No semantic trades for CPU.** Batching stays time/count based, stats stay
   per-record, hub delivery stays per-message. Fusion removes task boundaries
   only (this rules out hub micro-batching and stats sampling as alternatives).
3. **Reuse stage semantics across execution topologies.** Row transforms,
   batching, encoding, and delivery-boundary compression/encryption use shared
   implementations. Fusion replaces internal channel connections with direct
   calls; the connector retains delivery, retry, and checkpoint ownership.

## Phase 1: Productionize Sink Fusion

- Gate unit tests: state-counter bail, batch bail, non-nop bail, multi-child
  bail (bail paths are currently covered only indirectly by unchanged
  snapshots).
- Coverage registry: add `sink.fused_stateless_sink` (or equivalent) feature
  with `covers:` annotations, following the nop+none precedent.
- Close the `max_delivery_bytes` gap: **done**. HTTP, MQTT, and custom
  connector branches remain unfused, preserving incremental post-transform
  size checks in the sink task before the batch completes.
- OTA/update test: replace a running fused pipeline via update and verify no
  output duplication/loss across the switch.
- Docs: runtime sink table + pipeline API + this directory.

## Phase 2: Row-Level Fusion (Removed)

Eliminating intermediate `Collection`s via a per-row loop (filter/project
evaluated row by row, writing straight into delivery bytes or a single
output collection) was implemented and then **removed**: it duplicated
every stage's semantics in a second form (row views, hand-assembled
payloads, separate suppress accounting) for an unmeasured gain on narrow
rows (+0.4pp at N=200). The standing rule, kept by Phase 3, is: no new
data models inside the fused task. Collections stay collections upstream
of encode (shared `apply_*` functions only); bytes stay bytes downstream;
the fused operator handles exactly one representation change — the
shared encoder stage itself.

Removed with the row loop: the `as_any_mut` downcast dispatch and trait
requirement, the `append_tuple` row primitives, and the per-format
byte-equality tests.

## Phase 3: Delivery-Chain Fusion (Implemented, Experimental)

Extends the fused span from the nop-only chain to eligible connector delivery
chains. Qualification follows the reuse rule: stage semantics must have one shared
implementation for both standalone and fused execution. Batching and encoding
share their state machine; compression/encryption share the complete delivery
handlers. External I/O and unsupported topology remain fusion boundaries.

Connectors stay outside the fused span: external I/O is a fusion boundary,
like lookup joins. The optimizer rule (`fuse_stateless_transform`) peels
the pure transform chain and leaves the sink connector node untouched, so
retry, capture, and checkpointing stay in the single proven sink task and
are never transcribed a second time.

### Fused Shape

`input -> [Order?] -> [Filter?] -> Project -> [EmptySuppress?] -> [Batch?] ->
[Encode?] -> [Compress?] -> [Encrypt?]` becomes one `PhysicalFusedTransform`
node and one task, sitting directly below the standalone sink connector.

- HTTP, MQTT, and custom connector branches keep the standalone transform
  chain. Their delivery-size limits must reject incremental bytes before
  count/duration batching finishes; custom limits are unknown to the planner.
- The connector (any kind: nop, memory, file, MQTT, ...) is never fused.
  Connector-specific physical EXPLAIN details (e.g. HTTP body summary)
  stay on the sink node.
- `filter_before_order` records the peel order; execution replays it
  (`Filter -> Order -> Project` is canonical).
- Order fuses as a whole-collection sort inside the task (batch delay
  semantics unchanged).
- Shared stages remain standalone, including when a sibling branch cannot
  fuse. Consumer counts use plan indices across the DAG, so filter/project
  state advances once per input rather than once per sink.
- Pipeline-state counters (`last_hit_count`, `last_hit_time_unix_ms`,
  `last_agg_hit_count`) ride along untouched: the fused task threads the same
  state handles into the same `apply_filter`/`apply_projection` functions, so
  collection-path equivalence is exact.
- `EmptySuppress` (`omit_if_empty`) replays `collection_is_effectively_empty`
  inside the task and registers the same `collections_forwarded` /
  `collections_suppressed` counters. Filter-matched-nothing emits nothing
  with no counter movement, exactly like `FilterProcessor`.
- `EncoderStage` owns the encoder, delivery state, and batch policy. Both
  execution modes use its input slicing, empty-input rules, framing, finish,
  and abort paths. Every output is awaited before the next piece is encoded.
- `BatchPolicy` shares configuration validation and fixed-grid scheduling.
  `NativeBatchStage` shares the standalone non-partitioned count/tail drains;
  native batching retains row buffering, while encoded batching retains
  encoder state rather than rows. Both shared input entries check the saved
  deadline again after row preparation and flush a due batch before appending
  new input, including exact-boundary and delayed timer polling cases. A failed
  encoder due flush is reported at the timer boundary; recoverable failures do
  not discard the newly prepared collection.
- Runtime-bound custom functions are bound inside every fused filter,
  project, and order expression before constructing the task, using the same
  bindings as standalone nodes.
- `delivery_transform` contains the complete compress/encrypt delivery
  handlers used by standalone channel adapters and the inline chain. START,
  CHUNK, END, ABORT, empty drains, and final authentication framing therefore
  have one implementation.
- Encoder failures retain the standalone input boundary: abort the active
  delivery and stop the current input. Recoverable transform failures stay
  inside their stage and do not stop the upstream encoder; later count
  deliveries can still succeed. `ChannelClosed` propagates from byte stages,
  matching standalone transform tasks. Encoder-only execution retains the
  original encoder's error handling.
- `max_delivery_bytes` and result forwarding stay in the sink task. Limited
  connector branches retain incremental enforcement on post-transform bytes;
  they never accumulate an entire batch inside the fused task.

### EXPLAIN

The fused node shows `fused=` stage strings in peel order (`filter/order/project/encoder/compress/encrypt`, no
connector suffix) plus `codec=`/`level=` and
`algorithm=`/`key_bits=`/`key_id=` (never key material), mirroring the
standalone nodes. The sink node above keeps its connector details.

### Implementation Notes

- The fused task owns `EncoderStage` and `ByteTransformChain`. Inline
  connections implement the private `DeliveryOutput` interface with static
  dispatch, without internal tasks, queues, or boxed futures.
- The fused task owns no delivery machinery: no pending/queue/cache, no
  parked terminals, no retry backoff, no checkpoint snapshots. Encoded
  terminal control flushes before forwarding. Encoder failures are recorded;
  byte-stage transport failures terminate the task. Native control retains
  `BatchProcessor`'s existing order. Data terminals flush before forwarding in both modes.
  Checkpoint barriers pass through untouched; the sink task owns capture
  and snapshots.
- Immediate empty collections retain encoder framing (JSON `[]`, empty
  NDJSON deliveries) unless filtered or suppressed. Batched empty inputs
  do not start a delivery.
- Stats reflect the fused task boundaries: collection input, collection
  output for native mode, and message/byte output for encoded mode.
  Internal stages do not contribute collection output counters.
- Output layout derivation replays the projection for native fusion and
  uses the captured encoder layout for encoded fusion, preserving aliases,
  expression slots, and sink-level column ordering/filtering.
- Encoded output retains the standalone START/CHUNK/END/ABORT protocol.
  The fused task has no complete-payload assembly buffer; the unchanged sink
  accumulator owns that buffer. Native output remains collections.
- Fused message/byte counters describe the final task output. Internal encoder
  abort accounting is driven by encoder lifecycle notifications, including
  failures before START is emitted. A delivery contributes at most one
  `messages_aborted`, even when several stages fail; each error still contributes
  to `error_count`. Accounting resets when the next encoded delivery begins.

## Phase 4: Wider Patterns (Compute/File Deferred)

- `Compute`/map stages: deferred. Map semantics (per-row affiliate writes
  with row retention) do not change hop count materially; revisit only with
  measurements showing otherwise.
- Batch-aware fusion: **done** through shared `EncoderStage`, `BatchPolicy`,
  and `NativeBatchStage` implementations. Partitioned batching remains outside
  sink fusion. Retry/capture/checkpoint state stays in the connector task.
- The historical CPU measurements above predate the shared-stage refactor.
  Its performance has not yet been measured; final-channel chunk count and
  removal of the extra complete-payload buffer should be evaluated separately.
- Other sinks: file and memory delivery chains can fuse. HTTP, MQTT, and
  custom branches retain streaming transforms for delivery-size enforcement;
  window/aggregation/join internals stay unfused permanently.

## Non-Goals

- Micro-batching or per-record stats dilution as a CPU lever.
- Runtime changes from #358 scope (Compio, `io_uring` as a premise,
  cross-core pipeline sharding).
- Fusing stateful operators (windows, aggregations, joins, state windows).
  Row/collection-scoped pipeline-state counters are not in this category:
  they ride along by reference (Phase 3), never rewritten.

## Equivalence Checklist (Applies to Every Phase)

1. Row-for-row identical output on shared corpora.
2. Empty-filter suppression preserved.
3. Order preserved.
4. Backpressure preserved (no new buffering).
5. Terminal/close semantics identical.
6. Control/watermark/error passthrough identical.
7. Barrier forwarding + checkpoint snapshot/restore parity, including
   restore-compatibility of `sink:{id}` snapshots.
8. `forward_to_result` behavior identical in both modes.

## Verification Methodology

- s1 ladder (N=50/100/200/500/750, 500 msg/s, `taskset` single core, shared
  MQTT stream, `nop` sinks) against rekuiper on identical data; report CPU,
  anon heap plateau, and drain lag with the sustainability criteria from the
  rekuiper multiple_rules harness.
- s2 (file sink + batching, exact-loss proof) as the no-regression gate for
  unfused paths.
- Local verification runs affected library, pipeline, and planner cases; full
  suites run in remote CI. Paired standalone/fused tests compare framing,
  delivery contents, partial-delivery aborts, and recovery after stage failures.
