# cgroup Binding and Startup Gate Synchronization

## Purpose

This document explains how veloFlux binds FlowInstance processes to Linux cgroup v2 paths and how
the startup gate coordinates Manager/worker startup order.

The goals are:

- keep CPU isolation deterministic at process level
- avoid worker runtime initialization before cgroup binding is complete
- provide explicit failure signals and logs for operations/debugging

## Configuration

All fields are under `server`:

- `default_cgroup_path`  
  Optional cgroup v2 path for the in-process default FlowInstance (Manager process).
- `extra_flow_instances[].cgroup_path`  
  Optional cgroup v2 path per worker subprocess.
- `startup_gate_path`  
  Optional base directory for startup gate marker files.
- `startup_gate_timeout_ms`  
  Optional worker wait timeout for gate readiness.

## Effective Enablement Rules

- Manager default-instance binding runs only when `default_cgroup_path` is configured.
- Worker cgroup binding runs per instance only when that instance has `cgroup_path`.
- Startup gate is enabled only if:
  - at least one extra instance has a non-empty `cgroup_path`, and
  - `startup_gate_path` is configured.
- If no extra instance declares `cgroup_path`, startup gate is skipped.

## Startup Sequence

### 1) Manager (`default`) pre-runtime bind

Before creating the Tokio runtime, the main process tries to join `default_cgroup_path`.

- success: logs one `flow instance bound to cgroup` record
- failure: logs reason + cgroup snapshot and exits startup

This keeps the Manager binding attempt in a pre-runtime stage.

### 2) Worker spawn and gate-controlled startup

For each `extra_flow_instances` entry (serially):

1. Manager spawns worker process (`--worker ...`).
2. If configured, Manager binds worker PID to `cgroup_path`.
3. If startup gate is enabled:
   - on bind success: Manager writes `<instance>.ready`
   - on bind failure: Manager writes `<instance>.fail`, kills workers, returns error
4. Worker process waits for gate readiness before creating its runtime and listeners:
   - sees `.ready` -> continue startup
   - sees `.fail` -> exit with failure
   - timeout -> exit with failure

This guarantees worker runtime init happens only after Manager-side binding succeeds.

## Startup Gate Details

- Manager creates a per-run gate directory under `startup_gate_path` (for example:
  `boot-<pid>-<timestamp_ms>`).
- Marker writes are atomic (tmp file + rename).
- Marker naming:
  - ready: `<instance>.ready`
  - fail: `<instance>.fail`
- On gate session creation, stale entries under `startup_gate_path` are removed best-effort.
- On Manager shutdown (normal path), the per-run gate directory is removed best-effort.

## Failure Behavior

If any worker bind/gate step fails:

- current worker is terminated
- previously started workers are terminated
- startup returns error to Manager bootstrap path

This keeps startup behavior fail-fast and avoids partial multi-instance availability.

## cgroup Join Implementation Notes

- `join_pid` validates cgroup path format (`/...`, no parent traversal).
- If target cgroup already matches `/proc/<pid>/cgroup`, the join is treated as no-op success.
- Primary write target is `cgroup.procs`.
- If `cgroup.procs` write returns `EINVAL`, code attempts fallback via `cgroup.threads`.
- Failure logs include a cgroup snapshot (`cgroup.type`, `cgroup.subtree_control`, `cpu.max`,
  and `cpuset.*`) for root-cause diagnosis.

## Operational Guidance

- Prepare cgroup tree/controller delegation before starting veloFlux.
- Use startup logs to verify:
  - default Manager bind
  - per-worker bind result
  - gate enabled/disabled reason
- If startup fails, inspect:
  - cgroup path existence and type (domain/leaf)
  - controller delegation (`cpu` in relevant subtree controls)
  - gate directory permission and timeout settings
