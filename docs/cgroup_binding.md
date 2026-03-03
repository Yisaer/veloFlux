# cgroup Binding in Multi-FlowInstance Mode

## Purpose

This document describes the cgroup binding model for worker flow instances.

Current policy:

- Manager (`default`) does not perform in-process cgroup binding.
- Worker flow instances support cgroup binding before worker runtime starts.
- Startup gate synchronization files are not used.

## Configuration

Worker cgroup binding is configured by:

- `server.extra_flow_instances[].cgroup_path`

If `cgroup_path` is absent for an instance, that worker starts without cgroup migration.

## Worker Pre-Start Binding Flow

For each worker spec:

1. Manager prepares worker launch arguments.
2. If `cgroup_path` is set, Manager launches worker through a shell wrapper command:
   - wrapper writes its own PID to `/sys/fs/cgroup<path>/cgroup.procs`
   - wrapper then `exec`s the veloFlux worker process
3. Worker process starts after cgroup migration is complete.

This gives a practical "join cgroup before worker starts serving" behavior.

## Why This Model

- avoids manager self-migration failure modes
- avoids startup gate file lifecycle management
- keeps isolation focused on extra worker instances

## Worker Lifecycle Safety

- Manager listens for both `SIGINT` and `SIGTERM` and performs worker cleanup on shutdown.
- Worker subprocesses are launched with Linux `PR_SET_PDEATHSIG=SIGTERM` in pre-exec hook.
- If Manager exits unexpectedly, kernel sends `SIGTERM` to workers to reduce orphan-process risk.

## Operational Notes

- cgroup tree and CPU controller delegation must be prepared before launching veloFlux.
- `cgroup_path` is Linux-only; do not set it on macOS.
- Worker startup failure due to cgroup setup will surface as worker early-exit in manager logs.
