# Pipeline Lifecycle Semantics

## Background

Pipelines in veloFlux are both stored metadata and instance-scoped runtime objects.

The manager persists the pipeline spec and desired state in storage, but execution happens inside a
specific flow instance backend:

- in-process runtime
- worker-process runtime

Because those backends do not apply changes in the same way, lifecycle semantics are defined around
desired state first and runtime convergence second.

## Goals

- Define the stable pipeline resource model across storage and runtime.
- Clarify create, upsert, start, stop, and delete semantics.
- Explain how desired-state persistence interacts with runtime success or failure.
- Document where worker and in-process behavior intentionally diverge.

## Non-Goals

- SQL planning internals.
- Sink-specific connector behavior.
- Full REST endpoint reference.

## Pipeline Resource Model

A pipeline resource consists of:

- stored pipeline spec
- target `flow_instance_id`
- stored desired state (`running` or `stopped`)
- optional runtime installation inside the selected flow instance

The stored spec is the source of truth. Runtime copies may be missing transiently, especially for
worker-backed pipelines, but manager can rebuild them from storage and pipeline context.

## Create Semantics

Common create behavior:

1. canonicalize and validate the target `flow_instance_id`
2. validate pipeline request shape
3. acquire per-pipeline and shared-MQTT operation guards
4. persist the pipeline spec into storage
5. apply the pipeline to the selected runtime backend

Backend-specific behavior:

- in-process:
  - manager builds the pipeline definition immediately
  - runtime create failure triggers storage rollback
- worker:
  - manager builds worker apply context from storage and SQL references
  - worker receives the pipeline with desired state `stopped`
  - if worker apply fails, the stored pipeline remains persisted even though runtime installation
    failed

The worker case is an intentional degrade mode: metadata durability wins over synchronous runtime
convergence.

## Upsert Semantics

Upsert keeps the existing pipeline identity and flow-instance binding.

Current rules:

- if a stored pipeline exists, its `flow_instance_id` is reused
- the new request must still pass normal create validation
- previous desired state is read from storage before replacement

In-process upsert behavior:

- explain the new definition before deleting the old runtime
- delete old runtime and storage record
- persist the new spec
- create the new runtime
- if previous desired state was `running`, persist `running` again and best-effort restart

Worker upsert behavior:

- delete the old stored record when present
- persist the new spec
- reapply to worker with desired state derived from the old stored desired state
- if worker apply fails and the previous desired state was `running`, manager downgrades stored
  desired state to `stopped`

Upsert is therefore replace-in-place for metadata, but restart after replacement is best effort
rather than transactional.

## Start Semantics

Starting a pipeline first persists desired state as `running`, then asks the runtime to converge.

In-process behavior:

- `start_pipeline` is idempotent when the runtime is already running
- if no runtime graph is currently materialized, flow lazily builds it from the stored definition
- start failure rolls stored desired state back to `stopped`

Worker behavior:

- manager first persists desired state as `running`
- if worker reports the pipeline missing, manager rebuilds worker apply context and reapplies the
  pipeline with desired state `running`
- start or reapply failure rolls stored desired state back to `stopped`

This makes start the main repair path when worker runtime state drifts behind storage.

## Stop Semantics

Stopping a pipeline first persists desired state as `stopped`, then asks the runtime to stop.

Supported stop modes at the manager surface are:

- `quick` (default)
- `graceful`

In-process behavior:

- stop is idempotent for already stopped pipelines
- flow marks status `stopped` and drops the runtime pipeline object
- a running pipeline is closed using the requested stop mode and timeout

Worker behavior currently degrades to a fixed runtime contract:

- manager still accepts the stop request and persists desired state as `stopped`
- worker stop always uses `quick` mode with a fixed 5-second timeout

This means the public stop shape is richer than current worker execution semantics. Tests should
treat graceful stop as an in-process guarantee today, not as a worker-wide one.

## Delete Semantics

Delete is storage-authoritative with best-effort runtime cleanup.

Current behavior:

- load the stored pipeline first
- derive the flow-instance binding from stored metadata when possible
- try to delete the runtime copy
- ignore runtime cleanup failures after logging
- delete the stored pipeline record

For in-process runtimes, deleting a running pipeline performs a quick close with a 5-second timeout.

The important invariant is that delete removes manager-owned metadata even if runtime cleanup is
imperfect. This avoids undeletable control-plane objects caused by stale runtime state.

## Desired State Persistence

Desired state is a separate stored record, not just a mirror of runtime status.

Why this matters:

- startup hydration for workers needs a durable desired-state input
- create and upsert can succeed for storage while runtime application is incomplete
- list/status views may need to compare stored intent with current runtime state

Current persistence behavior:

- create does not automatically create a `running` desired state
- start writes `running` before runtime converge
- stop writes `stopped` before runtime converge
- upsert preserves the previous desired state when possible

## In-Process Vs Worker Behavior

The backend split is intentional, not incidental.

In-process pipelines:

- are validated and built locally during create/upsert
- can use the requested stop mode and timeout directly
- roll back storage on create-time runtime build failure

Worker pipelines:

- depend on manager-built context payloads derived from stored streams, shared MQTT configs, and
  memory topics
- may be absent from worker runtime even when stored metadata exists
- can be repaired by start or startup hydration
- currently stop with quick semantics only

## Failure And Rollback Semantics

Important failure behaviors:

- in-process create failure rolls back storage
- worker create failure leaves the stored pipeline in place
- in-process upsert validates before removing the old runtime, reducing destructive failures
- restart after upsert is best effort; a failed restart leaves desired state downgraded to
  `stopped`
- delete does not roll back metadata when runtime cleanup fails

These are deliberate tradeoffs between control-plane durability and synchronous runtime parity.

## Build Context Role

Worker-backed pipelines cannot be applied from the stored SQL alone.

Manager rebuilds a context payload that includes:

- referenced streams
- referenced shared MQTT client configs
- referenced memory topics

This context is derived from parsed SQL and sink definitions, then sent with worker apply requests.
As a result, worker lifecycle behavior depends on the current storage snapshot of adjacent
resources, not only on the pipeline row itself.

## Testing Guidance

- Verify create rollback for in-process pipelines when runtime creation fails after storage
  persistence.
- Verify worker create failure leaves the stored pipeline present and recoverable.
- Verify upsert preserves `flow_instance_id` and previous desired state.
- Verify running pipelines are best-effort restarted after in-process upsert and downgrade to
  stopped when restart fails.
- Verify worker start can recover a missing runtime pipeline by reapplying from storage context.
- Verify stop persists desired state before runtime stop.
- Verify worker stop uses quick semantics regardless of requested graceful mode.
- Verify delete removes storage even when runtime cleanup reports an error.
- Verify startup hydration reapplies worker-backed pipelines according to stored desired state.

## Future Work

- If worker stop gains graceful/timeout support later, the current documented degrade behavior
  should be narrowed rather than deleted silently.
