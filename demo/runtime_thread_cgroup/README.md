# Runtime Thread Cgroup Demo

## Background

This demo exists to validate a specific architecture direction for veloFlux:

- one main process hosts multiple in-process Flow instances,
- each Flow instance owns an independent Tokio runtime,
- each runtime can bind its worker threads into a dedicated thread-level cgroup,
- CPU quota can then be applied per runtime rather than only per process.

The main question behind this demo is:

- can one process host multiple Tokio runtimes,
- bind each runtime to a different threaded child cgroup,
- and observe stable per-runtime CPU usage that matches the configured `cpu.max` budget?

This demo is designed to answer that question with a minimal synthetic workload before changing the
real veloFlux startup and runtime model.

## Design

### Cgroup model

The demo follows the same model that should be used later by veloFlux.

1. An external launcher places the whole demo process into one threaded domain root.
2. Each Tokio runtime binds its own worker thread into a dedicated threaded child cgroup.
3. CPU quota is configured on the child cgroups with `cpu.max`.

The cgroup topology is:

- `/sys/fs/cgroup/veloflux-runtime-demo`
- `/sys/fs/cgroup/veloflux-runtime-demo/rt_a`
- `/sys/fs/cgroup/veloflux-runtime-demo/rt_b`

Where:

- `veloflux-runtime-demo` is the threaded domain root,
- `rt_a` is a threaded child cgroup,
- `rt_b` is a threaded child cgroup.

### Why the process must enter the root cgroup first

Linux cgroup v2 threaded mode requires the process to already belong to the same threaded domain
before its threads can be redistributed into threaded child cgroups via `cgroup.threads`.

That is why the demo uses **approach A**:

- `run_demo.sh` moves the process into `veloflux-runtime-demo/cgroup.procs`,
- `src/main.rs` only validates that the process is already in the expected root cgroup,
- runtime worker threads are then moved to `rt_a/cgroup.threads` and `rt_b/cgroup.threads`.

This matches the intended production model for veloFlux:

- an external launcher prepares process placement,
- the program performs runtime thread placement.

## Implementation

### Layout

- `setup_cgroup.sh`: prepares and cleans up the demo cgroup tree.
- `run_demo.sh`: places the demo process into the threaded domain root and starts the binary.
- `src/main.rs`: the standalone Rust demo program.

### `setup_cgroup.sh`

The script is responsible for:

- creating the threaded domain root and child cgroups,
- enabling `cpu` in `cgroup.subtree_control`,
- switching child cgroups to `threaded`,
- assigning CPU quota via `cpu.max`,
- cleaning up the tree after the demo.

By default:

- `rt_a` gets `cpu.max = 20000 100000`
- `rt_b` gets `cpu.max = 80000 100000`

This means the intended ceilings are approximately:

- `rt_a`: 20% of one core
- `rt_b`: 80% of one core

Cleanup also attempts to move any remaining demo threads back to the threaded root, then moves any
remaining demo processes back to the parent cgroup, and finally removes the demo cgroup tree.

### `run_demo.sh`

The launcher script performs the process-level cgroup placement required by threaded mode.

Its flow is:

1. call `setup_cgroup.sh setup` unless `--skip-setup` is used,
2. move the launcher process into `veloflux-runtime-demo/cgroup.procs`,
3. start the demo binary from within that root cgroup.

### `src/main.rs`

The program creates two independent Tokio runtimes:

- `rt_a`
- `rt_b`

Each runtime:

- uses `tokio::runtime::Builder::new_multi_thread()`,
- uses `on_thread_start(...)` to bind the worker thread to its own child cgroup,
- runs the same synthetic CPU-heavy workload,
- reports runtime-specific metrics once per sample interval.

The workload is an integer-only compute loop with periodic `yield_now()` calls, chosen to keep the
signal CPU-bound, repeatable, and easy to compare across runtimes.

## Runtime CPU measurement

The demo reports several metrics, but they do not all serve the same purpose.

### Primary metric: `thread_cpu`

`thread_cpu` is the main proof for runtime CPU usage.

It is computed by:

- recording the native thread ids that belong to each runtime,
- reading `/proc/self/task/<tid>/stat`,
- summing `utime + stime` for the threads of that runtime,
- converting the delta into percentage of one CPU core.

This is the closest metric in the demo to:

- "how much CPU did this Tokio runtime actually consume?"

### Supporting metric: `cgroup`

`cgroup` is computed from the target cgroup's `cpu.stat usage_usec`.

This is a useful kernel-side cross-check, but it is not the primary proof for runtime CPU because a
cgroup could theoretically contain unrelated threads.

### Supporting metric: `ops/s`

`ops/s` shows how much useful work each runtime completes under the same wall-clock interval.

If quota is working, the runtime with the larger CPU budget should complete substantially more work.

### Auxiliary metric: `busy`

`busy` is derived from Tokio `RuntimeMetrics::worker_total_busy_duration()`.

In this demo it is treated as auxiliary only. The stable conclusions are based on:

- `thread_cpu`
- `cgroup`
- `ops/s`

## Requirements

- Linux with cgroup v2 mounted at `/sys/fs/cgroup`
- Permission to create cgroups and write `cgroup.procs` and `cgroup.threads`
- A Rust toolchain with Cargo

## Run

Prepare and build:

```bash
cd demo/runtime_thread_cgroup
cargo build --release
```

Run the demo with the launcher script:

```bash
sudo ./run_demo.sh --bin ./target/release/runtime_thread_cgroup_demo -- \
  --duration-secs 20 \
  --sample-ms 1000 \
  --worker-threads 1 \
  --tasks-per-runtime 8 \
  --chunk-iters 5000000
```

Check cgroup status while the demo is running:

```bash
sudo ./setup_cgroup.sh status
```

Clean up after the demo exits:

```bash
sudo ./setup_cgroup.sh cleanup
```

## Expected behavior

If binding and throttling work correctly:

- `rt_a` should stay close to 20% of one core,
- `rt_b` should stay close to 80% of one core,
- `rt_b` should achieve much higher `ops/s` than `rt_a`,
- `rt_a` should show stronger throttling than `rt_b`.

## Experimental result

The demo was executed on Linux with the following command:

```bash
sudo ./run_demo.sh --bin ./target/release/runtime_thread_cgroup_demo -- \
  --duration-secs 20 \
  --sample-ms 1000 \
  --worker-threads 1 \
  --tasks-per-runtime 8 \
  --chunk-iters 5000000
```

Representative output showed:

- `rt_a thread_cpu` stayed around `19% ~ 21%` of one core,
- `rt_b thread_cpu` stayed around `78% ~ 81%` of one core,
- `rt_a cgroup` stayed around `20%` of one core,
- `rt_b cgroup` stayed around `80%` of one core,
- `rt_b ops/s` was about 4x higher than `rt_a`,
- `rt_a throttled_usec` was substantially larger than `rt_b`.

Final summary from the experiment:

```text
rt_a: total_ops=780000000 thread_cpu=19.6%_of_one_core machine=9.82% cgroup_usage_ms=4016.5 throttled_count=200 throttled_ms=15658.6
rt_b: total_ops=3305000000 thread_cpu=79.3%_of_one_core machine=39.63% cgroup_usage_ms=16191.1 throttled_count=199 throttled_ms=3898.4
```

These numbers are consistent with the configured quotas:

- `rt_a`: `cpu.max = 20000 100000`
- `rt_b`: `cpu.max = 80000 100000`

## Conclusion

The demo validates the following claims:

- multiple Tokio runtimes can coexist inside one process,
- each runtime can bind its worker thread to a dedicated thread-level cgroup,
- CPU quota applied through `cpu.max` can be enforced per runtime,
- runtime CPU can be observed reliably by aggregating thread CPU time,
- this model is a viable basis for in-process multi-Flow-instance support in veloFlux.

For veloFlux itself, the corresponding production model should be:

- start the main process from an external launcher that places it into a threaded domain root,
- create one Tokio runtime per local Flow instance,
- bind runtime worker threads into per-instance child cgroups during runtime thread startup.
