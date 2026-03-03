# cgroup Binding in Multi-FlowInstance Mode

## Purpose

This document describes how veloFlux binds processes to Linux cgroup v2 paths when
`server.extra_flow_instances` is enabled.

Current design goals:

- keep cgroup ownership logic simple and local to each process
- avoid cross-process startup synchronization files
- make binding success/failure observable in process logs

## Configuration

All fields are under `server`:

- `default_cgroup_path`  
  Optional cgroup path for the main process (`flow_instance_id=default`).
- `extra_flow_instances[].cgroup_path`  
  Optional cgroup path for each worker subprocess.

If a cgroup path is not configured, that process skips cgroup binding.

## Binding Sequence

### 1) Main process (`default`)

The main process binds itself to `default_cgroup_path` before creating its Tokio runtime.

- success: logs a single `flow instance bound to cgroup` record
- failure: logs reason + cgroup snapshot, then exits startup

### 2) Worker subprocesses (`extra_flow_instances`)

Each worker process performs self-binding during worker bootstrap:

1. worker loads config and resolves its own `FlowInstanceSpec`
2. worker initializes logging
3. worker binds **its own PID** to `spec.cgroup_path` (if configured)
4. worker creates Tokio runtime and starts worker server

If worker binding fails, worker exits immediately with error, and Manager treats it as early worker
exit during readiness checks.

## Why No Startup Gate

Startup gate synchronization files are removed in this design.

Reason:

- worker self-binding removes Manager-side PID migration timing concerns
- startup logic is simpler (no ready/fail marker lifecycle)
- failure ownership is explicit (worker fails in worker log)

## cgroup Join Behavior

`join_pid` behavior:

- validates cgroup path format (`/...`, no parent traversal)
- if process is already in target cgroup (`/proc/<pid>/cgroup`), returns success (no-op)
- writes PID into `cgroup.procs`
- if `cgroup.procs` returns `EINVAL`, falls back to `cgroup.threads`

On failure, logs include cgroup snapshot fields for diagnosis:

- `cgroup.type`
- `cgroup.subtree_control`
- `cpu.max`
- `cpuset.*`

## Operational Notes

- cgroup tree and controller delegation must be prepared before starting veloFlux.
- worker cgroup path permissions must allow the worker process to write `cgroup.procs` (or
  `cgroup.threads` fallback).
- stream/pipeline workload isolation verification should use per-instance output QPS and CPU usage
  from Prometheus/Grafana.
