# cgroup Binding for Flow Instances

## Purpose

This document describes the cgroup binding model for local thread-level Flow instances.

## Thread-level Binding

Used by local thread-level instances.

Characteristics:

- binding target is the runtime worker thread of an in-process Tokio runtime
- binding mechanism is `cgroup.threads`
- binding happens during runtime worker thread startup
- isolation boundary is the runtime thread set rather than a whole process

## Current Policy

- `default` does not currently perform thread-level cgroup binding by default.
- Extra instances support thread-level cgroup binding under a threaded domain rooted at the main
  process branch.

## Configuration Semantics

Thread-level binding is configured through a thread-level child cgroup path associated with an
in-process instance runtime.

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

## Operational Notes

- cgroup tree and CPU controller delegation must be prepared before launching veloFlux.
- thread-level runtime binding requires a valid threaded domain root for the main veloFlux process.
- Linux-only cgroup settings must not be configured on macOS or unsupported environments.
- Thread-level runtime binding failure should be treated as local instance startup failure,
  not as a silent best-effort downgrade.
