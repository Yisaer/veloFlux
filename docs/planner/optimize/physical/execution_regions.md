# Fixed Execution Regions

Status: implemented for the current physical stages under the fixed strategy. Existing
snapshot coverage and execution boundaries are described below.

## Planning and execution

`planner::execution::ExecutionPlan` derives execution placement from the optimized
physical DAG. It does not replace semantic nodes with a new fused physical template.
Regions contain stage indices in input-to-output order and identify their output stage.
The processor builder creates one task for each region and omits internal channel links.

The fixed strategy keeps sources/scans, sink connectors (including nop/memory), lookup
drivers, opaque scalar/batch/aggregate functions, shared-stream ownership, and result coordination
separate. No SQL hint, user region configuration, workload profile, or adaptive placement
is required. The initial implementation conservatively isolates all table scans/lookups;
provider-specific in-memory fusion has not been implemented.

Supported synchronous adapters are Decoder, Filter, Project, Compute, Order, EmptySuppress,
CollectionLayoutNormalize, MemoryCollectionMaterialize, RowDiff, SourceChangeGate,
BatchFunctionCompute, StatefulFunction, TumblingWindow, CountWindow,
StateWindow, SlidingWindow, EosWindow, Aggregation, ProcessTimeWatermark,
EventtimeWatermark, Barrier, Batch, Sampler, SinkEncoder, IncSinkEncoder,
SinkCompress, SinkEncrypt, and StreamingAggregation with any supported window spec. Consecutive supported stages fuse in arbitrary valid order,
including repeated stages, fragments without Project, and fragments away from a sink. Supported engine singletons use the same region executor as multi-stage regions.
All current engine stages have adapters. A future stage must provide an adapter before region construction can accept it; there is no standalone engine fallback.

Fan-out output edges and fan-in input edges remain external. Shared stages are counted
by physical plan index and execute once. Repeated graph traversal does not create a
second region for a shared stage. Collection input/output metrics measure semantic stage
boundaries; eliminated internal links do not contribute to pipeline link counts.

`RegionProcessor` drives synchronous engines and awaits only its external input/output
and control channels and timed-stage clocks. Filter, Project, Order, and Compute reuse their synchronous handler
implementations. No stage suspend/resume framework, internal mailbox, or per-stage future
is introduced. Data-lane checkpoint barriers pass through the data output. Window stages
consume or forward watermarks with the same behavior as standalone execution. Out-of-band control remains on the control output.

## Direct single-output execution

The runtime captures whether an engine emits at most one immediate result when binding
its stage. Consecutive such engines (including Filter, Compute, Project and Order) use a
direct synchronous `Option<StreamData>` handoff. They do not construct generic output
descriptions, push an output cursor, call `next_output`, or perform a later empty poll
between stages. The ingress event may still have one immediate description for the driver.
This is output arity, not an inline-safety declaration: opaque callbacks retain their
isolated blocking-worker path and the planner's region strategy is unchanged.

Only incremental or multiple-output descriptions remain on the depth-first cursor stack.
Each extracted result traverses the complete synchronous suffix and awaits its external
send before its producer advances. Empty output stops that branch; recoverable and fatal
errors retain their existing stage policies. Encoder send recovery requires the retained
cursor to belong to the actual final encoder, rather than an upstream encoder whose bytes
passed through a different final stage. Both direct stage calls and cursor extraction
consume the region's 64-step work budget; there are no per-stage futures or row chunks.

## Incremental window output

Standalone and region execution share `TumblingWindowStage` and `StreamingTumblingStage`.
Their synchronous event methods buffer rows or update accumulators; their synchronous
output methods extract at most one completed window per call, preserving partition
insertion order, millisecond boundaries, and window metadata. The region routes that
window through subsequent stages and awaits external send before extracting another.
It retains output descriptions along the stage path, bounded by region depth, rather
than collecting completed windows in an intermediate result vector.

For tumbling windows, data-lane graceful end drains all buffered windows before forwarding
the terminal event. Data-lane quick end and control-lane terminal events discard pending
window state.
Control-lane terminal events retain their own lane. Incremental aggregation row errors
remain recoverable; finalization errors remain fatal. In region execution, the producing
stage records each extracted collection before transport waits. Stage handling duration
excludes external sends; backpressure is recorded by the shared transport helper.

CountWindow and count-only Batch share `CountWindowStage`. It moves owned rows
into per-partition deques, preserving partition insertion order across the whole input,
and extracts one completed batch per send. The unpartitioned path avoids partition hashing;
deque prefix drains avoid repeatedly shifting the remaining rows. As in the existing
Batch, these batches do not add window metadata. Data-lane terminal events flush
partial batches before forwarding the event. Control-lane terminal events are forwarded
on the control lane first, then buffered data is flushed on the data lane. Input EOF also
flushes partial batches. Timed/combined batching shares the synchronous TimedBatchStage
with region execution.

Streaming count aggregation executes through `StreamingCountStage`.
It retains the current input and a row offset, updating groups only until the next complete
window. External send/backpressure precedes evaluation of the remaining input rows. Output
metadata retains the first/last timestamps of each completed count window. Partial windows
are discarded on every terminal/EOF path, including graceful termination, preserving the
existing distinction from CountWindow. It no longer constructs a vector of every completed
window before sending the first output. Partition workers and their metadata are allocated
only when a partition is first encountered.

Plain Aggregation reuses `AggregationStage::process_batch_with_grouping` for grouping,
empty input, row-error recovery, metadata propagation, and finalization. Configuration and
finalization errors remain fatal in both execution placements. Region error handling
separates these fatal errors from recoverable collection-transform errors.

EOS window and EOS streaming aggregation use synchronous stage engines. Only data-lane graceful termination emits the final window before the terminal
signal; quick/control termination and input EOF discard buffered state. Stateful functions
share `StatefulStage`, including partition-local instances, transactional prepare/commit,
pending origin ordering, `lead` resolutions, and data-terminal `finish` output. Control
termination and EOF retain the standalone discard policy. Terminal output uses a bounded
two-event description; normal inputs keep the compact single-output path.

State window and state streaming aggregation share synchronous row-at-a-time engines.
They retain open/emit behavior (an opening row cannot close its newly opened window),
row-error recovery, window metadata, and data-graceful flush versus quick/control/EOF
discard. Each completed output is sent before evaluating later input rows or draining
another pending partition. Partition keys remain structured values; no Debug string key
is constructed in the hot path. Pending graceful windows drain in partition insertion
order, while ordinary outputs retain row-completion order.

Layout normalization, collection materialization, row diff, source change gating, and
safe batch-function compute reuse their existing synchronous handlers and state types.
Output layout, masks, metadata, empty collection behavior, and row-error recovery stay in
those handlers. Runtime dependencies are resolved only for stages that use them; a pure
Filter/Compute region does not require an aggregate registry.

## Function execution classification

`CustomFunc::supports_inline_execution()`, `AggregateFunction::supports_inline_execution()`,
and `StatefulFunction::supports_inline_execution()` default to false. Engine-controlled
built-ins explicitly opt in; `delay` retains isolation because it calls blocking thread
sleep. The planner inspects typed scalar expression trees, including CASE branches, without invoking
functions, hoisting calls, or changing short-circuit evaluation. Opaque functions isolate
their entire owning expression stage. Batch-function definitions use the same metadata.

Aggregate call compilation captures the registered implementation's inline property in
`AggregateCall::supports_inline_execution`. Ordinary and streaming aggregation inspect
that property as well as their typed arguments and grouping expressions. Replacing a
builtin name with an opaque implementation therefore retains isolation; function names
are not an allowlist. Streaming rewrites preserve the compiled call metadata. Manual
Rust construction of `AggregateCall` must now supply this field from its function's
contract; SQL and connector configuration are unchanged.

Stateful call compilation similarly captures `StatefulCall::supports_inline_execution`
from its registered function implementation. Engine built-ins opt in, and custom stateful
implementations default to isolation. Deferred evaluation is a synchronous state contract,
not a reason to introduce a stage Future or retain a transport boundary.

This classification is an internal execution contract, not a security boundary or a
measurement of arbitrary custom code. Opaque stages use isolated singleton regions.
Their synchronous processing, incremental output extraction, timer callbacks, and lifecycle
flushes run through `TaskSpawner::spawn_blocking`; channel waits remain in the asynchronous
region shell. Engine-controlled regions retain direct synchronous calls without worker hops.

A process-wide semaphore limits admitted opaque jobs to `min(available_parallelism, 4)`.
A transport acquires its permit before submitting a job, admits no subsequent input while
that job or its output is pending, and waits for each external send before extracting the
next output. The job owns stage state and has no channel sender. Cancelling a transport
cancels pending permit acquisition and discards late job results. A running native callback
cannot be forcibly interrupted: it retains the permit until it returns, including across
rule cancellation/rebuild. Stuck callbacks can exhaust this bounded pool; isolation does
not provide a timeout or native-code sandbox. Callback duration excludes permit, worker
queue, and transport waits. Control/timer callbacks within the same opaque stage remain
serialized behind its current callback and output, preserving existing event ordering.

The fixed sink-template rewrite is removed. An opaque expression remains visible as its
own physical stage and execution boundary; no template can hide it during placement.

## Diagnostics

`PipelineExplain::to_json()` returns `options`, `logical`, `physical`, and `execution`.
The execution field contains strategy version, stage classifications, region membership,
and retained links (or an execution-planning error). Text EXPLAIN includes the same
execution view. Both JSON and text reuse `execution_to_json()`, which remains available
for callers needing only placement information. The logical/physical field contents are unchanged.

Boundary reasons distinguish fixed policy (`policy_external_io`, `policy_sink_connector`,
`policy_opaque_udf`), topology/ownership, and implementation (`stage_adapter_missing`).
A missing adapter is explicit migration debt, not a permanent semantic boundary.

## Sliding windows and watermark clocks

Plain and aggregating sliding windows use synchronous stage engines. Logical bounds remain inclusive `[t - lookback, t + lookahead]`; triggers,
partition order, retained history, and millisecond metadata do not depend on region
placement. Each extraction produces one window; downstream backpressure stops further
row evaluation and window extraction. Graceful data end drains pending windows; control,
quick end, and EOF discard them.

Watermark stages expose synchronous event handlers, a periodic interval, and the next
data-driven deadline. The outer driver creates and polls Tokio clocks; stages contain no
asynchronous timer waits. A region polls clocks in rotating order, preserving each clock's
periodic-before-deadline priority. Processing-time sliding deadlines retain monotonic
watermarks. The legacy PhysicalWatermark variant lowers to the same processing-time
engine; it does not retain a separate execution implementation. Event-time sorting, late-event rejection, and collection-before-watermark
ordering reuse the existing state machine. Event-time control graceful end flushes data
through the remaining stages before forwarding the signal on the control lane.

During a blocked output drain, the driver preserves the standalone send-before-next-event
order. A timer becoming due does not permit concurrent state mutation or a later event to
bypass earlier data. Supervisor cancellation can abort the pending transport await. Polling
all stage callbacks while output is blocked is not a requirement of fusion.

## Decoder and stage diagnostics

Standalone and region execution share `DecoderStage`, including decode projection,
shared applied projection state, recoverable decode errors, event-time parsing, and
collection-native passthrough. Decoder registrations are isolated by default; builtins
explicitly register an inline contract. Replacing a builtin name with an ordinary custom
registration removes that contract. Event-time parsers likewise default to isolation and
builtin parsers opt in; the decoder stage inherits the parser's contract.

Each semantic stage owns its `ProcessorStats`, declared input/output domains, handling
duration, errors, and engine-specific counters/gauges. These handles are exported under
physical stage IDs, including stages inside a multi-stage region. A stage records its
output when it produces/extracts a result; downstream backpressure cannot advance the
remaining cursor, but the extracted result is already reflected in its output counter.

Regions own scheduling and transport progression only. They do not create, aggregate,
or update metrics. The opaque worker transport calls the same instrumented stage methods
without timing its worker acquisition, queue wait, output wait, or downstream execution.
The final sink accounts successful external delivery. `LinkOutput` owns instrumentation
for retained output channels, and the builder exposes their transport stats separately.
Pipeline connection gauges are exposed under `pipeline_transport`.

Stage timing includes synchronous process, extraction, timer, control flush, lifecycle,
and Barrier alignment callbacks, including callbacks returning no result or an error.
Clock scheduling, executor bookkeeping, channel waits, and cooperative yields are outside
stage handling time. Ordinary immediate outputs have no extra extraction timing sample.
There are no per-invocation stage tracing spans.

Stage handling-time histograms are opt-in through
`metrics.stage_handling_time_enabled` (default `false`). Counters, errors, and gauges
remain enabled. Stages capture the process-wide setting at construction; disabled
callbacks perform no metric clock reads or histogram observations. Histogram handles
are bound only when a duration is actually recorded.

## Fan-in alignment

Barrier is the first stage of a region and may fuse with its exclusive downstream
computation. Incoming edges remain external and ordered by input port. The synchronous
`BarrierStage` owns alignment state; the region pauses a checkpointed
input until every input reaches that checkpoint. It drains the aligned checkpoint through
the suffix before resuming inputs, so post-checkpoint rows cannot overtake the barrier.
Data and control alignment retain separate state and their original channel lanes.
Ordinary regions retain the direct fan-in stream path; only Barrier regions allocate the
indexed, pausable input stream. Fan-out still creates independent external connections.

## Timed batches and sampling

Timed/combined Batch uses the synchronous `TimedBatchStage`. The
stage owns buffered rows and an absolute monotonic deadline; the driver owns the sleep.
The grid origin is captured when the task starts. Input preparation is followed by a
deadline check before appending new rows, so a boundary row starts the next grid cell.
A single input is checked once: output backpressure does not introduce new duration
splits halfway through its count drains. Extract one batch and await output before
appending the remainder of the input. Data terminal flushes before forwarding; control
terminal forwards on the control lane before data flush; EOF flushes the remainder.

Sampler uses the same synchronous `SamplerStage` and `StrategyState` for Latest and
Packer. Latest keeps the last data item and passes non-data events immediately. Packer
retains its merger, optional fused decode, schema artifact, and shared projection state.
Sampling clocks retain Skip behavior; watermark clocks retain Delay behavior. All timer
waits remain in the outer driver. Data terminal flushes before forwarding; control
terminal forwards before data flush; EOF also flushes. Merger registrations default to
isolation, with explicit inline registration for engine-controlled implementations.

Shared ingest plans carry the direct decoder instance's inline contract and the merger
registration contract. Direct decoder instances default to isolation; builtins opt in.
Ingest regions preserve shared stream ownership boundaries and use the live applied
projection state when a consumer attaches or releases columns.

## Delivery transforms

SinkCompress and SinkEncrypt use synchronous delivery steps. Consecutive transforms, including repeated codecs, share a region;
each step returns at most one frame before its recoverable error. The driver delivers
that frame before reporting the error and processing another input. No completed delivery
or vector of output frames is assembled between transforms. Sink connectors remain
independent and retain incremental delivery-size checks.

A terminal control signal received during an active delivery is parked at the region
boundary. Continue consuming data until END or ABORT completes all active transforms,
then forward the signal on the control lane. Data terminal or input EOF instead aborts
an unfinished delivery and forwards ABORT only when a START was emitted. EOF also
forwards a parked terminal after abort processing. Preserve natural empty compression
drains, encryption headers/final frames, and recovery after malformed delivery input.

Each encoder and byte transform measures its own generated bytes and delivery outcomes.
Abort counts are local to the stage's delivery lifecycle; regions do not aggregate them.
Writer ownership moves into the task once; the ownership mutex adds no per-frame locks.

SinkEncoder and IncSinkEncoder use the synchronous EncoderStage. The region owns all
transport waits; no encoder stage method awaits. One count segment is encoded and sent before remaining input advances.
Lifecycle notifications precede its frame, and a recoverable error follows that frame's
successful send. Count/duration policies retain the same fixed grid and check a due
deadline after input preparation. A recoverable due-flush failure does not discard the
new input. Immediate empty input emits a delivery; batched empty input does not start one.

Encoder registrations default to isolation; builtins explicitly opt in. Planning captures
the registration contract, including replacements of builtin names, for both encoder
forms. An isolated encoder leaves its computation prefix and byte-transform suffix free
to fuse independently. Final output layouts remain the captured planner layouts.

Control terminal traverses watermark/encoder pre-flush hooks in stage order and drains
their data through the suffix before forwarding control. Data terminal and EOF also
flush the encoder. Encoder errors abort current delivery and skip remaining rows of the
failed input; following inputs can recover. Begin failure aborts partially initialized
encoder resources even before START. Abort accounting remains once per delivery at the
owning stage; errors retain their physical stage identities. Encoder-only output failures
retain standalone recovery, including timer/control flush handling; regions containing
byte transforms stop on ChannelClosed. Stage output metrics count production, while
transport counts send failures and sinks count successful external delivery.

## Execution and recovery boundaries

- All Engine and OpaqueFunction stages built from SQL use region transport, including
  singleton regions. Retired standalone processor structs, channel loops, and builder
  variants are removed. Decoder, sampler, and encoder resources are constructed directly
  as synchronous stages, without allocating temporary channel wrappers.
- The fixed sink-template rule, physical variant, production entry, and historical test
  executor are removed. Semantic physical stages remain visible in EXPLAIN; task placement
  is reported separately as regions. Tests compare singleton-region chains with fused
  regions, exercise synchronous engines directly, and retain SQL input/output assertions.
  HTTP/MQTT/custom sink connectors retain independent tasks and incremental delivery checks.
- The only current snapshot owners are datasource and sink processors, both isolated by
  the fixed strategy. Their semantic keys are assigned before runtime grouping and routed
  through the existing validate/staged-restore/rollback path. Region numbers are never
  persisted as participant identities. Barrier alignment is inline and preserves input
  ports; Continue does not force window/batch flush. No new window recovery schema is
  introduced. A participant registry inside regions is unnecessary for the currently
  implemented snapshot owners; adding new recoverable engines requires their own schema.
- Each physical index must identify one immutable Arc definition. All physical optimization
  passes memoize by input Arc and preserve shared nodes, including fan-in barriers. An
  independently constructed definition reusing an index is rejected even if its visible
  configuration looks equivalent; callers must reuse the Arc. This conservative ownership
  rule covers differing expressions, captured implementations, layouts, state, and secrets
  without Debug fingerprints or exposing configuration in errors. Validation precedes
  semantic rewrites so eliminated nodes cannot hide duplicate definitions. Streaming
  aggregation/encoder rewrites retain shared window/batch owners rather than copying them.
- All adapter migrations must preserve their existing error, terminal, timer, and delivery
  behavior before they become eligible for inlining.

Unexpected external control closure and data lag follow the entry stage's policy.
Data EOF propagates through stages in execution order: drain each predecessor before
applying the next stage's input-closure policy. Filter, layout normalization,
materialization, and Barrier retain their strict failure policy when closure reaches
them. A strict suffix cannot make a tolerant entry reject an independent control-lane
closure, or skip a predecessor's buffered data. Explicit terminal signals use their
existing stage policies. These distinctions do not introduce restart behavior: the
pipeline supervisor stops remaining tasks and marks a managed pipeline failed on an
unexpected processor error; it does not automatically restart it.

## Validation

Planner SQL cases assert structured execution EXPLAIN for ordinary chains, ordering,
HTTP sink isolation, and opaque function placement. Graph-level cases cover fragments
across timed batch/sampler stages and shared fan-out/fan-in identities. Runtime
cases compare singleton-region chains against fused execution, suppression, data/control
lane routing, and bounded output backpressure. Tumbling cases cover partitioned
millisecond windows, metadata, watermark boundaries, graceful/quick/control termination,
and retention of undrained windows while the output channel is full. Existing planner
and pipeline cases remain the semantic regression checks. Count cases cover partitions
spanning multiple inputs, completion order and metadata, all exit paths, empty/grouped
batch aggregation, fatal aggregation errors, and prevention of trailing-row evaluation
while count aggregation output is blocked.

Opaque aggregate SQL cases cover ordinary and incremental implementations overriding a
builtin name, while asserting that downstream Filter/Project stages continue to fuse.

EOS SQL placement cases use table scans, the supported SQL source for `eoswindow()`.
Runtime buffered-window cases compare singleton and multi-stage region execution across EOS/state
windows, aggregation, partitions, metadata, data graceful/quick termination, control
termination, and EOF. Existing stateful and lead SQL cases exercise pending-row behavior.

Encoder runtime cases compare Immediate/count/duration/combined policies, optional
compression, empty input, data/control graceful and quick termination, and EOF against
singleton-region execution. Fault injection wraps the real JSON encoder to verify begin cleanup,
ABORT-after-START ordering, once-per-delivery accounting, skipped remaining rows on a
failed input, and recovery on following inputs. Closed-output cases distinguish encoder-only
recovery from fatal byte-transform transport failure across feed, timer, control, data end,
and EOF. Count backpressure prevents encoding further segments. SQL placement cases cover
ordinary and incremental builtin encoder replacements defaulting to isolation. Stats cases
verify stage domains, stage-owned errors, exported stage stats, and link
reduction across runtime rebuilds.

Opaque runtime cases verify a blocking scalar callback on a current-thread runtime leaves
asynchronous timers free to progress, normal output survives the worker transfer, aborted
transports publish no late output, and cancellation retains the worker permit until the
callback exits. Encoder equivalence cases also exercise the isolated worker driver across
Immediate/count/duration/combined batching and all terminal lanes. Existing SQL UDF and
opaque merger pipeline cases exercise actual builder selection.

Graph validation cases reject independent same-index definitions before and after optimization,
retain one shared fan-in definition/barrier, and prevent semantic streaming rewrites from
copying shared window/batch ownership.

## Cooperative work budgets

The region yields after 64 synchronous dispatch/extraction steps in a long chain or drain,
without changing Collection/delivery boundaries or publishing partial outputs. Handle duration
excludes the yield wait. This addresses the loss of scheduling opportunities when internal
channels are removed; it does not impose a new bound on an individual semantic handler.
Sorting, aggregation, codecs, and large collection handlers already execute synchronously
before region fusion as well. Their possible segmentation is a separate kernel optimization, not a
VF-107 completion requirement. The previously added row-segmentation path has been removed.

A current-thread runtime case observes progress midway through a 1,024-stage chain, verifies
that a control terminal does not bypass the current data, and checks cancellation can stop
the chain before publishing output. Existing backpressure and window-drain cases verify
that output remains incremental and no later ordered event overtakes a blocked output.
