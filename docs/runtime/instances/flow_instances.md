# Flow Instances

## Background

In veloFlux, a `flow::FlowInstance` is a **process-local runtime container** that owns:

- a catalog (stream definitions)
- pipeline runtime (planning, execution, and lifecycle)
- instance-scoped runtime resources (shared stream runtime, memory pub/sub, shared clients, etc.)

By default, the Manager runs a single in-process instance named `default`.

veloFlux also supports **extra Flow instances** for isolation. Extra instances can be hosted in two
ways:

- **worker-process instance**: hosted in a local worker subprocess and accessed through an internal
  HTTP API
- **local thread-level instance**: hosted inside the main veloFlux process with its own dedicated
  Tokio runtime

These two instance types are intended to coexist in the same veloFlux deployment.

## Goals

- Provide isolation between groups of pipelines by binding each pipeline to a `flow_instance_id`.
- Support both worker-process isolation and in-process runtime isolation under one Manager API.
- Keep the public Manager REST API simple: pipelines bind to an instance by id; instances are not
  created via REST.
- Make multi-instance behavior observable and operationally explicit.

## Non-Goals

- A distributed / multi-host Flow cluster.
- Autoscaling or dynamic instance creation at runtime via REST.
- Cross-instance shared state or cross-instance execution.
- Treating all instance types as equivalent in fault isolation or observability semantics.

## Terminology

- `flow_instance_id`: an identifier that decides **where a pipeline runs**.
- `default`: the in-process Flow instance inside the Manager process.
- **worker-process instance**: a local subprocess that hosts one non-default `flow::FlowInstance`
  and exposes an internal HTTP API.
- **local thread-level instance**: an in-process non-default `flow::FlowInstance` with its own
  dedicated Tokio runtime; its runtime threads may be bound to a dedicated thread-level cgroup.
- **instance backend**: the runtime hosting mode of a Flow instance, such as worker-process or
  local thread-level.

## Architecture Overview

```text
┌───────────────────────────────────────────────────────────┐
│ Main server process                                       │
│                                                           │
│  ┌──────────────┐                                         │
│  │ Manager HTTP │  REST                                   │
│  │ API (:8080)  │◀──────────────────────────────────────┐ │
│  └──────┬───────┘                                       │ │
│         │                                               │ │
│         │ in-process calls                              │ │
│         ▼                                               │ │
│   FlowInstance(id=default)                              │ │
│   FlowInstance(id=fi_local_1, dedicated Tokio runtime)  │ │
│   FlowInstance(id=fi_local_2, dedicated Tokio runtime)  │ │
│                                                           │ │
│   spawns                                                  │ │
│    ▼                                                      │ │
│   Worker subprocesses (loopback)                          │ │
└────┬──────────────────────────────────────────────────────┘ │
     │ internal HTTP                                         │
     ▼                                                       │
┌───────────────────────────────┐                            │
│ Worker (id=fi_worker_1)       │                            │
│ - FlowInstance(id=fi_worker_1)│                            │
│ - /internal/v1/pipelines/...  │                            │
└───────────────────────────────┘                            │
                                                             │
```

Key points:

- The Manager process always hosts `default` in-process.
- A deployment may mix local thread-level extra instances and worker-process extra instances.
- The Manager persists metadata (streams, pipelines, connector configs) in storage and uses it to
  rehydrate whichever instance backend is configured for each `flow_instance_id`.

## Instance Backend Types

### Worker-process instance

Characteristics:

- hosted in a separate OS process
- communicates with Manager through loopback HTTP
- supports process-level cgroup binding before worker runtime starts
- supports independent metrics and profiling endpoints per worker process

Trade-offs:

- strongest isolation boundary
- higher startup and control-plane complexity
- separate process lifecycle management is required

### Local thread-level instance

Characteristics:

- hosted inside the main veloFlux process
- owns its own dedicated Tokio runtime
- can bind runtime worker threads to a dedicated thread-level cgroup
- shares process-level memory space and process-level observability surface with Manager

Trade-offs:

- lower overhead than spawning a worker process
- suitable for runtime-level CPU isolation
- does not provide separate process fault isolation
- does not imply a separate metrics/profile HTTP server per local instance

## Configuration Model

Extra instances are declared under `server.extra_flow_instances`.

Conceptually, each extra instance must declare:

- `id`
- an instance backend type
- backend-specific runtime options
- optional cgroup binding options appropriate for that backend

Worker-process instances and local thread-level instances do **not** share identical runtime or
cgroup semantics, so their configuration should be treated as backend-specific even if they are
listed under the same `extra_flow_instances` section.

Common constraints:

- `id` must be non-empty and must not be `default`
- all declared ids must be unique
- Linux-only cgroup settings must not be configured on unsupported platforms

Worker-process specific constraints:

- worker listener addresses must be loopback-only
- worker listener addresses must be unique
- worker cgroup path, if configured, is interpreted as a **process-level** cgroup path

Local thread-level specific constraints:

- the main veloFlux process must already be placed into the expected threaded domain root before
  local instance runtimes start
- local instance cgroup paths, if configured, are interpreted as **thread-level child cgroups**
  for runtime worker threads

## Runtime Startup and Lifecycle

### Manager startup

- Main process initializes config, logging, storage, and the in-process `default` FlowInstance.
- Depending on configuration, Manager may also create local thread-level extra instances in-process.
- Manager may additionally spawn worker-process extra instances.

### Worker-process instance startup

For each configured worker-process instance:

1. Manager validates worker addresses and worker config.
2. If a process-level cgroup path is configured, the worker subprocess joins that cgroup before
   worker exec.
3. Worker process initializes logging, metrics, profiling, and Flow runtime.
4. Manager probes the worker endpoint and marks the worker as ready after successful connect.

### Local thread-level instance startup

For each configured local thread-level instance:

1. The main veloFlux process must already be inside the expected threaded domain root.
2. Manager constructs an in-process `FlowInstance` with a dedicated Tokio runtime.
3. During runtime worker thread startup, each runtime thread joins the configured child threaded
   cgroup, if thread-level binding is enabled for that instance.
4. Manager keeps routing pipeline operations to the in-process instance without worker HTTP hops.

### Shutdown and orphan prevention

Worker-process instances:

- Manager tracks spawned subprocesses and performs kill/wait on shutdown.
- On Linux, worker subprocesses may use `PR_SET_PDEATHSIG=SIGTERM` so the worker is signaled if the
  parent manager process dies unexpectedly.

Local thread-level instances:

- Their lifecycle is bound to the main process lifecycle.
- Runtime worker threads exit as their Tokio runtime is shut down.
- No subprocess orphan scenario exists because these instances are in-process.

## Resource Model: Global Metadata vs Instance Runtime

The Manager API persists **global metadata** in storage, but runtime installation is instance-aware.

### Streams

- `POST /streams` persists the stream definition in storage.
- The Manager installs the stream into in-process instances according to the current implementation
  policy.
- Worker-process instances may still rely on lazy installation through per-pipeline build context
  delivery when a pipeline is applied.

The important invariant is that streams remain global metadata, while pipeline specs decide *where*
execution happens.

### Memory topics and shared connector configs

The same principle applies:

- definitions are persisted globally
- runtime installation remains backend-aware
- worker-process instances may install resources lazily during pipeline apply
- local thread-level instances may install resources directly in-process

### Pipelines

A pipeline is persisted in storage as a spec that includes `flow_instance_id` (optional in REST;
defaults to `default`).

At runtime:

- if the target instance backend is in-process, Manager routes the operation directly to the local
  `FlowInstance`
- if the target instance backend is worker-process, Manager routes the operation through the worker
  internal API

This backend difference is an implementation detail of the Manager. The pipeline contract still
remains: `flow_instance_id` decides where the pipeline runs.

## Isolation and Trade-offs

What you gain with extra instances in general:

- workload partitioning by `flow_instance_id`
- dedicated Tokio runtime per non-default instance backend that chooses in-process hosting
- the option to use process-level isolation when stronger boundaries are required

What worker-process instances provide:

- process-level address-space isolation
- process-level cgroup binding
- separate per-process metrics/profile servers

What local thread-level instances provide:

- in-process hosting with lower overhead
- dedicated Tokio runtime per instance
- runtime worker thread CPU isolation through thread-level cgroup binding

What neither model provides automatically:

- horizontal scaling of a single pipeline across hosts
- implicit coordination or shared state across instances
- identical observability and failure semantics across all instance backends

## Coexistence Model

Worker-process instances and local thread-level instances can coexist in one veloFlux deployment,
provided their hosting and cgroup semantics are kept explicit.

Recommended cgroup shape:

- one process-level root for the whole veloFlux deployment
- one branch for the main process and its thread-level local runtimes
- separate process-level branches for worker subprocess instances

In other words:

- worker-process instances should continue using process-level cgroup placement via `cgroup.procs`
- local thread-level instances should use thread-level placement via `cgroup.threads` under a
  threaded domain rooted at the main process branch

These two models should not be treated as interchangeable just because both are configured under
`extra_flow_instances`.

## Known Limitations

- Stream/resource installation behavior may differ across instance backends until runtime routing is
  fully unified.
- Worker-process instances and local thread-level instances do not share the same observability or
  fault-isolation guarantees.
- Existing management code may still assume that non-default instances are worker-backed until the
  routing layer is generalized.

## When to Use Each Extra Instance Type

Choose a worker-process instance when you need:

- stronger failure isolation
- separate per-instance metrics/profile HTTP servers
- process-level cgroup placement

Choose a local thread-level instance when you need:

- lower overhead than a worker subprocess
- dedicated Tokio runtime scheduling inside the main process
- thread-level CPU control for a specific in-process instance
- co-location with Manager while still separating runtime CPU budgets
