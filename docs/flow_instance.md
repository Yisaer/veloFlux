# Flow Instances

## Background

In veloFlux, a `flow::FlowInstance` is a **process-local runtime container** that owns:

- a catalog (stream definitions)
- pipeline runtime (planning, execution, and lifecycle)
- instance-scoped runtime resources (shared stream runtime, memory pub/sub, shared clients, etc.)

By default, the Manager runs a single in-process instance named `default`.

This project also supports **extra Flow instances** for isolation. Extra instances are hosted in
**local worker subprocesses** (spawned by the main server process) and communicate with the Manager
through an internal HTTP API.

## Goals

- Provide isolation between groups of pipelines by running them in separate worker subprocesses.
- Keep the public Manager REST API simple: pipelines bind to an instance by id; instances are not
  created via REST.
- Make multi-instance behavior observable via per-instance metrics/profiling endpoints.

## Non-Goals

- A distributed / multi-host Flow cluster (extra instances are loopback-only workers).
- Autoscaling or dynamic instance creation at runtime via REST.
- Cross-instance shared state or cross-instance execution.

## Terminology

- `flow_instance_id`: an identifier that decides **where a pipeline runs**.
- `default`: the in-process Flow instance inside the Manager process.
- *worker*: a local subprocess that hosts one non-default `flow::FlowInstance` and exposes an
  internal API for the Manager.

## Architecture Overview

```text
┌───────────────────────────────┐
│ Main server process           │
│                               │
│  ┌──────────────┐             │
│  │ Manager HTTP  │  REST       │
│  │ API (:8080)   │◀───────────┐│
│  └──────┬───────┘            ││
│         │                    ││
│         │ in-process calls   ││
│         ▼                    ││
│   FlowInstance(id=default)   ││
│                               │
│  spawns                        │
│   ▼                            │
│  Worker subprocesses (loopback)│
└───┬───────────────────────────┘
    │ internal HTTP
    ▼
┌───────────────────────────────┐
│ Worker (id=fi_1)              │
│  - FlowInstance(id=fi_1)      │
│  - /internal/v1/pipelines/... │
└───────────────────────────────┘
```

Key points:

- The Manager process only hosts `default` in-process.
- Each extra instance is implemented as a **separate OS process** (worker).
- The Manager persists metadata (streams, pipelines, connector configs) in storage and uses it to
  (re)hydrate both `default` and worker instances.

## Configuration

Extra instances are declared in YAML config under `server.extra_flow_instances`:

```yaml
server:
  manager_addr: "0.0.0.0:8080"
  extra_flow_instances:
    - id: "fi_1"
      worker_addr: "127.0.0.1:18081"
      metrics_addr: "127.0.0.1:19898"
      profile_addr: "127.0.0.1:16061"
      cgroup_path: "/veloflux-ci/perf/fi_1"
```

Constraints enforced by the runtime:

- `id` must be non-empty and must not be `default`.
- `worker_addr`, `metrics_addr`, and `profile_addr` must be **loopback** addresses.
- `worker_addr` must be unique across instances.
- `cgroup_path` is optional and Linux-only.

Operational notes:

- Workers are spawned by the main server process (same binary) with:
  `--worker --flow-instance-id <id> --config <path>`.
- Current implementation applies cgroup binding only to worker subprocesses.
- If `cgroup_path` is configured, worker startup uses a pre-exec wrapper:
  1) write worker pid to `/sys/fs/cgroup<path>/cgroup.procs`, then
  2) `exec` the worker process.
- Because of that, extra instances require a concrete config file path so workers can read the
  same configuration.

## Runtime Startup and Lifecycle

### Manager startup

- Main process initializes config/logging and creates the in-process `default` FlowInstance.
- Manager server starts and then spawns configured worker subprocesses.

### Worker startup

For each configured worker:

1. Manager validates worker addresses and spawns the subprocess.
2. If `cgroup_path` is configured, the subprocess joins that cgroup before worker exec.
3. Worker process initializes logging/metrics/profiling, then starts its internal HTTP listener.
4. Manager probes `worker_addr` and marks the worker as ready only after successful connect.

### Shutdown and orphan prevention

- Manager listens for both `SIGINT` and `SIGTERM`.
- On shutdown, Manager sends kill/wait to all tracked worker subprocesses.
- On Linux, worker subprocesses are launched with `PR_SET_PDEATHSIG=SIGTERM` so workers receive
  `SIGTERM` if the parent manager process dies unexpectedly.

## Resource Model: Global Metadata vs Instance Runtime

The Manager API persists **global metadata** in storage, but runtime installation is instance-aware.

### Streams

- `POST /streams` persists the stream definition in storage.
- The Manager installs the stream into the in-process `default` instance immediately.
- For non-default instances, the stream definition is installed **lazily** when a pipeline is
  applied to a worker: the Manager computes a per-pipeline build context and sends the referenced
  stream specs to the worker.

This is why `POST /streams` does not require `flow_instance_id`: streams are treated as global
metadata, and pipelines decide *where* they execute.

### Memory topics and shared connector configs

Similar pattern:

- `POST /memory/topics` persists the topic spec in storage and declares it in `default`.
- Worker instances declare memory topics (and create shared MQTT clients) on-demand when applying
  pipelines that reference them.

### Pipelines

A pipeline is persisted in storage as a spec that includes `flow_instance_id` (optional in REST;
defaults to `default`).

- If `flow_instance_id == default`: the Manager builds and installs the pipeline directly in the
  in-process instance.
- If `flow_instance_id != default`: the Manager sends an internal `apply` request to the worker,
  including the pipeline spec and the minimal build context (referenced streams + connector configs).

For debugging, `GET /pipelines/:id/buildContext` returns the exact payload that is used for worker
apply.

## Isolation and Trade-offs

What you gain with extra instances:

- Process-level isolation (separate address space).
- A dedicated Tokio runtime per non-default instance (pipelines in different instances do not share
  the same runtime).
- Per-instance metrics and profiling endpoints.

What you do **not** get:

- Horizontal scalability: multiple instances do not shard a single pipeline.
- Implicit coordination: if two pipelines in different instances read from the same external source
  (e.g. same MQTT topic), they will both consume/compute independently.

## Known Limitations

- **Stream deletion checks are local-only.** `DELETE /streams/:name` currently checks whether a
  stream is referenced by pipelines by inspecting the in-process `default` runtime only. Pipelines
  running in worker subprocesses are not queried, so it is possible to delete a stream that is
  still referenced by a remote pipeline. Treat stream deletion as an operator action and validate
  usage across instances before removing it.

## When to Use Extra Instances

Extra instances are a good fit when you need:

- isolation for noisy pipelines (CPU/memory heavy)
- separate profiling/metrics per group of pipelines
- safe experimentation without interfering with `default`
