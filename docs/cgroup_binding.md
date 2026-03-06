# cgroup Binding in Multi-FlowInstance Mode

## Purpose

This document describes the cgroup binding model for both worker-process Flow instances and local
thread-level Flow instances.

## Binding Modes

veloFlux supports two different cgroup binding modes for extra Flow instances.

### Process-level binding

Used by worker-process instances.

Characteristics:

- binding target is a worker subprocess
- binding mechanism is `cgroup.procs`
- binding happens before worker runtime starts serving
- isolation boundary is the worker process

### Thread-level binding

Used by local thread-level instances.

Characteristics:

- binding target is the runtime worker thread of an in-process Tokio runtime
- binding mechanism is `cgroup.threads`
- binding happens during runtime worker thread startup
- isolation boundary is the runtime thread set rather than a whole process

## Current Policy

- `default` does not currently perform thread-level cgroup binding by default.
- Worker-process instances support process-level cgroup binding before worker runtime starts.
- Local thread-level instances are intended to support thread-level cgroup binding under a threaded
  domain rooted at the main process branch.
- Startup gate synchronization files are not used.

## Configuration Semantics

Worker-process binding is configured through a process-level cgroup path associated with the worker
instance.

Local thread-level binding is configured through a thread-level child cgroup path associated with an
in-process instance runtime.

These two path types are not semantically equivalent:

- worker-process path -> `cgroup.procs`
- local thread-level path -> `cgroup.threads`

## Worker Process Binding Flow

For each worker-process instance:

1. Manager prepares worker launch arguments.
2. If a process-level cgroup path is configured, Manager launches the worker through a shell wrapper
   command.
3. The wrapper writes its own PID to `/sys/fs/cgroup<path>/cgroup.procs`.
4. The wrapper `exec`s the veloFlux worker process.
5. Worker process starts after cgroup migration is complete.

This gives a practical "join cgroup before worker starts serving" behavior.

## Thread-Level Runtime Binding Flow

For each local thread-level instance:

1. An external launcher places the main veloFlux process into the expected threaded domain root via
   `cgroup.procs`.
2. The local `FlowInstance` creates a dedicated Tokio runtime.
3. During runtime worker thread startup, the runtime worker thread joins its target child cgroup via
   `cgroup.threads`.
4. CPU quota and weight are applied to the child threaded cgroup.

This model depends on cgroup v2 threaded subtree rules:

- the main process must already belong to the same threaded domain before runtime threads are moved
- child runtime cgroups must be valid threaded children under that domain

## Why Two Models Exist

Worker-process binding exists because it:

- avoids main-process self-migration during worker startup
- keeps the isolation boundary process-scoped
- preserves separate worker metrics/profile server semantics

Thread-level binding exists because it:

- allows multiple in-process Flow instances to coexist in one process
- allows per-runtime CPU isolation without spawning a subprocess for every instance
- matches the dedicated-Tokio-runtime model of local thread-level instances

## Coexistence Rules

Process-level and thread-level binding can coexist in one veloFlux deployment.

Recommended structure:

- one process-level cgroup branch for the main veloFlux process
- under that branch, one threaded domain root for local thread-level instances
- separate process-level cgroup branches for worker subprocess instances

Operationally:

- worker subprocesses should continue to join dedicated process-level cgroups via `cgroup.procs`
- local runtime worker threads should join dedicated child threaded cgroups via `cgroup.threads`
- the same cgroup path should not be treated as both a worker-process target and a thread-level
  runtime target

## Worker Lifecycle Safety

Worker-process instances:

- Manager listens for both `SIGINT` and `SIGTERM` and performs worker cleanup on shutdown.
- Worker subprocesses may be launched with Linux `PR_SET_PDEATHSIG=SIGTERM` in a pre-exec hook.
- If Manager exits unexpectedly, the kernel can signal workers to reduce orphan-process risk.

Local thread-level instances:

- Their runtime worker threads terminate with the main process or runtime shutdown.
- No subprocess orphan handling is needed because the instance is in-process.

## Operational Notes

- cgroup tree and CPU controller delegation must be prepared before launching veloFlux.
- process-level worker binding and thread-level runtime binding require different cgroup topology.
- thread-level runtime binding requires a valid threaded domain root for the main veloFlux process.
- Linux-only cgroup settings must not be configured on macOS or unsupported environments.
- Worker startup failure due to cgroup setup will surface as worker early-exit in manager logs.
- Local thread-level runtime binding failure should be treated as local instance startup failure,
  not as a silent best-effort downgrade.
