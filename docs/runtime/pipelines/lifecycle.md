# Pipeline Lifecycle Semantics

## Background

Pipelines in veloFlux are both stored metadata and runtime objects.

The manager persists the pipeline spec and the user's desired run state in storage, and execution
happens inside a specific flow instance. Runtime and scheduler status are observed in memory and
are not persisted as desired state.

## Goals

- Define the stable pipeline resource model across storage and runtime.
- Clarify create, upsert, start, stop, and delete semantics.
- Explain how the two-value desired state interacts with runtime success or failure.

## Non-Goals

- SQL planning internals.
- Sink-specific connector behavior.
- Full REST endpoint reference.

## Pipeline Resource Model

A pipeline resource consists of:

- stored pipeline spec
- required persisted `revision`
- target `flow_instance_id`
- stored desired run state (`running` or `stopped`)
- runtime/scheduler status (`running`, `stopping`, `stopped`, `scheduled_running`,
  `scheduled_stopped`, `scheduled_end`, or `failed`)
- runtime installation inside the selected flow instance

`running` is only used by non-scheduled pipelines. Scheduled pipelines use
`scheduled_running` while a schedule window is active; they never expose the
ordinary `running` status.

The stored spec is the source of truth.

## Create Semantics

Create behavior:

1. canonicalize and validate the target `flow_instance_id`
2. validate pipeline request shape
3. acquire per-pipeline and shared-MQTT operation guards
4. persist the pipeline spec into storage
5. apply the pipeline to the runtime
6. runtime create failure triggers storage rollback

## Upsert Semantics

Upsert keeps the existing pipeline identity and flow-instance binding.

Current rules:

- `revision` is required and must be in `1..=9007199254740991`
- a greater revision performs replacement
- a lower revision returns `409 Conflict` with `older_revision`
- an equal revision and equal normalized definition is idempotent and does not
  mutate runtime
- an equal revision and different definition returns `409 Conflict` with
  `same_revision_different_spec`
- if a stored pipeline exists, its `flow_instance_id` is reused
- the new request must still pass normal create validation
- previous desired state is read from storage before replacement

Upsert behavior:

- explain the new definition before deleting the old runtime
- delete the old runtime and storage record
- persist the new spec
- create the new runtime
- if previous desired state was `running`, persist `running` again and best-effort restart

Upsert is therefore replace-in-place for metadata, but restart after replacement is best effort
rather than transactional.

## Start Semantics

Starting a pipeline first persists desired state as `running`, then asks the runtime or scheduler
to converge.

Start does not change pipeline revision.

Behavior:

- `start_pipeline` is idempotent when the runtime is already running
- if no runtime graph is currently materialized, flow lazily builds it from the stored definition
- start failure preserves the stored `running` desired state and records a runtime error;
  the runtime status becomes `failed`
- if a processor task exits unexpectedly after startup begins, the runtime status becomes `failed`
  and the desired state remains unchanged

## Stop Semantics

Stopping a pipeline first persists desired state as `stopped`, then asks the runtime to stop.

Stop does not change pipeline revision.

Supported stop modes at the manager surface are:

- `graceful` (default)
- `quick`

Behavior:

- stop is idempotent for already stopped pipelines
- flow marks status `stopped` and drops the runtime pipeline object
- a running pipeline is closed using the requested stop mode and timeout

The scheduler uses `graceful` when a scheduled run window ends. This internal behavior is
independent of the default mode for manual stop requests.

## Delete Semantics

Delete is storage-authoritative with best-effort runtime cleanup.

Current behavior:

- load the stored pipeline first
- derive the flow-instance binding from stored metadata when possible
- try to delete the runtime copy
- ignore runtime cleanup failures after logging
- delete the stored pipeline record

For running pipelines, delete performs a quick close with a 5-second timeout.

The important invariant is that delete removes manager-owned metadata even if runtime cleanup is
imperfect. This avoids undeletable control-plane objects caused by stale runtime state.

## Desired State Persistence

The manager stores only the user's desired run state separately from the runtime object:

- `running`: the user wants the pipeline to run. For scheduled pipelines, patrol is enabled.
- `stopped`: the user has manually stopped the pipeline. Patrol is disabled.

The API exposes a unified real-time `status`. It is derived from the desired state, schedule
window, and runtime state; `scheduled_running`, `scheduled_stopped`, `scheduled_end`, `failed`, and
`stopping` are never persisted as desired state.

Why this matters:

- startup hydration needs a durable desired-state input
- create and upsert can succeed for storage while runtime application is incomplete
- patrol must distinguish the user's `stopped` intent from the transient runtime object

Current persistence behavior:

- scheduled pipeline creation enables schedule control with desired state `running`
- start writes `running` before runtime convergence; a failed convergence leaves desired state
  `running` and exposes runtime status `failed`
- stop writes `stopped` before runtime convergence; a failed stop returns the command error
  while preserving the explicit stopped intent and observed runtime state
- processor-fatal runtime failures record an in-memory runtime error without
  rewriting desired state
- upsert preserves the previous desired state when possible
- manual stop writes `stopped` and disables patrol; manual start writes `running` and re-enables
  schedule control
- when a schedule window is active and desired state is `running`, status is `scheduled_running`
  and patrol starts the runtime
- when a schedule window is inactive but a future window exists, status is `scheduled_stopped`
- when all finite datetime ranges have ended, status is `scheduled_end`; this is a derived terminal
  status, not a persisted desired state
- when a schedule window ends, patrol gracefully stops the runtime by default. The runtime reports
  `stopping` during this transition; `stopping` is transient and is not persisted
- revision orders artifact definitions; operational start/stop state changes do
  not increment it

## Failed Runtime Semantics

`failed` is a runtime status, not a persisted desired state. It means startup or rebuild failed,
or a processor task returned an error, panicked, or exited unexpectedly while the pipeline was
expected to run. The current error belongs to the runtime and is never written to storage.

Failure behavior:

- the pipeline supervisor aborts remaining processor tasks
- flow marks the runtime pipeline as `failed`
- flow keeps status and the failed processor, timestamp, and reason together in the runtime snapshot
- startup hydration creates fresh runtime state and retries desired `running` pipelines; a new
  startup failure records its reason and timestamp in memory
- scheduled patrol skips a pipeline whose current in-memory runtime status is `failed`, while a
  restart allows a pipeline with desired state `running` to retry

Runtime failure handling does not replay data, rebuild state, or restart the pipeline automatically.

Manual lifecycle behavior:

- start is allowed for failed pipelines and acts as an explicit retry; it keeps desired state
  `running` and clears the current runtime error before retrying
- stop is allowed for failed pipelines; it writes desired state `stopped`, clears the current
  runtime error, and reports `stopped`
- delete is allowed for failed pipelines and removes the stored definition, desired state, and
  runtime error

## Failure And Rollback Semantics

Important failure behaviors:

- create failure rolls back storage
- upsert validates before removing the old runtime, reducing destructive failures
- restart after upsert is best effort; a failed restart preserves the two-value desired state and
  records a runtime error
- a processor-fatal runtime failure leaves desired state unchanged and requires later runtime
  recovery or an explicit lifecycle action
- delete does not roll back metadata when runtime cleanup fails

These are deliberate tradeoffs between control-plane durability and synchronous runtime parity.

## Convergence Matrix

The manager persists `desired_state` before attempting runtime convergence. A
`stopping` runtime is busy; a new lifecycle operation returns `409 Conflict`
and must be retried after the stop completes.

### Non-scheduled pipelines

| desired_state | Current status | Expected behavior |
| --- | --- | --- |
| `running` | `running` | Already converged; return success. |
| `running` | `stopped` | Start the runtime; expose `running` after success. |
| `running` | `stopping` | Return `409 Conflict` and wait for stopping to finish. |
| `running` | `failed` | Clear the runtime error and retry start; keep `failed` if the retry fails. |
| `stopped` | `stopped` | Already converged; return success. |
| `stopped` | `running` | Stop the runtime; expose `stopped` after success. |
| `stopped` | `stopping` | Return `409 Conflict` and wait for stopping to finish. |
| `stopped` | `failed` | Do not retry runtime start; converge to `stopped`. |

### Scheduled pipelines

| desired_state | Current status | Expected behavior |
| --- | --- | --- |
| `running` | `scheduled_running` | Already converged; skip the operation. |
| `running` | `scheduled_stopped` | Remain `scheduled_stopped`; patrol starts the runtime in a valid window. |
| `running` | `scheduled_end` | Remain `scheduled_end`; patrol does not schedule the current revision again. |
| `running` | `stopped` | Derive `scheduled_stopped` when a future window exists, or `scheduled_end` when all windows have ended. |
| `running` | `stopping` | Return `409 Conflict` and wait for stopping to finish. |
| `running` | `failed` | Clear the runtime error and derive `scheduled_stopped` or `scheduled_end`; patrol may retry in a later valid window. |
| `stopped` | `scheduled_running` | Stop the runtime; expose `stopped` and disable patrol. |
| `stopped` | `scheduled_stopped` | Convert to `stopped` and disable patrol. |
| `stopped` | `scheduled_end` | Convert to `stopped` and disable patrol. |
| `stopped` | `stopped` | Already converged; return success. |
| `stopped` | `stopping` | Return `409 Conflict` and wait for stopping to finish. |
| `stopped` | `failed` | Do not retry runtime start; converge to `stopped`. |

Scheduled pipelines expose only `scheduled_running`, `scheduled_stopped`,
`scheduled_end`, `stopped`, `stopping`, and `failed` as runtime statuses.

## Testing Guidance

- Verify create rollback for pipelines when runtime creation fails after storage persistence.
- Verify upsert preserves `flow_instance_id` and previous desired state.
- Verify running pipelines are best-effort restarted after upsert and keep their desired state when
  restart fails.
- Verify stop persists desired state before runtime stop.
- Verify delete removes storage even when runtime cleanup reports an error.

## Incremental Apply

`POST /storage/apply` stores the pipeline deployment declaration in its definition,
separately from operational desired state. Higher revision applies inline `run_state` (default
`Stopped`); same-revision identical declaration replay preserves manual start/stop.
Both records survive restart. Ordinary pipeline upsert keeps its existing
preserve-operational-intent behavior.

Apply validates submitted pipelines before committing the complete delta. Runtime
replacement happens after that commit; a build/start/rebind failure does not
roll back the revision. The manager reports the failure in
the batch result without persisting a runtime acknowledgment. Restart hydration
retries committed configuration according to operational desired state. Explicit
start rebuilds an absent or outdated runtime from stored metadata; an existing
failed runtime uses the ordinary lifecycle retry against the current catalog.
Same-revision Apply preserves manual lifecycle changes and does not retry runtime
installation; it reports current observable results, or `null` when application
cannot be confirmed. Scheduled patrol retains its ordinary handling of current
in-memory failed runtimes.

Apply defaults to `restart_pipelines=false`: upstream configuration changes keep
unsubmitted pipelines' processor graphs and runtime state. With
`restart_pipelines=true`, affected existing pipelines stop before the dependency
batch is installed, then rebuild and restore their operational desired state.
Submitted newer pipelines follow incoming `run_state` in either mode. Runtime
MQTT bindings are retained in pipeline snapshots so client replacement and cascade
selection remain safe when catalog definitions have changed without a restart.
Shared clients and shared streams still used by unselected pipelines keep their
old runtime and report that the new configuration has not applied.
See [the API contract](../../api/storage_apply.md) for recovery and scheduling.
