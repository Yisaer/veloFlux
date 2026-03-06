# Perf PR Host Scripts

These scripts support two perf validation modes for multi-`flow_instance` CPU isolation:

- `worker_process`: extra flow instances run as local worker subprocesses and bind with `cgroup.procs`
- `thread_level`: extra flow instances run in-process with dedicated Tokio runtimes and bind with `cgroup.threads`

The shared perf workflow is intentionally limited to four operations:

1. setup cgroup
2. start veloflux
3. deploy stream/pipeline
4. run workload

Only `start_veloflux.sh` starts veloflux.
No script in this folder starts or manages emqx/prometheus/grafana.

## Reference config

Worker-process config:

```text
scripts/perf_pr_host/config.yaml
```

Thread-level config:

```text
scripts/perf_pr_host/config.thread_level.yaml
```

Update the cgroup paths in the selected config file to match your `--base-cg`.

## Mode A: worker-process extra flow instances

### 1) Setup cgroup

```bash
sudo ./scripts/perf_pr_host/setup_cgroup.sh \
  --mode worker_process \
  --base-cg /veloflux-ci/perf-pr-host-001/veloflux
```

Topology:

```text
/base
/base/manager
/base/fi_critical
/base/fi_best
```

Default cgroup policy:

- base `cpu.max = 100000 100000` (total 1 CPU)
- `manager` `cpu.weight = 300`
- `fi_critical` `cpu.max = 90000 100000`
- `fi_best` `cpu.max = 25000 100000`
- `fi_critical` `cpu.weight = 10000`
- `fi_best` `cpu.weight = 1`

The script prints:

```text
CG_MODE=worker_process
CG_BASE=...
CG_MAIN=...
CG_FI_CRITICAL=...
CG_FI_BEST=...
```

Use them like this:

- `CG_MAIN` -> `start_veloflux.sh --main-cgroup`
- `CG_FI_*` -> `server.flow_instances[].cgroup.process_path`

### 2) Start veloflux

```bash
./scripts/perf_pr_host/start_veloflux.sh \
  --main-cgroup /veloflux-ci/perf-pr-host-001/veloflux/manager \
  --veloflux-bin ./veloflux-linux-x86_64 \
  --config ./scripts/perf_pr_host/config.yaml \
  --data-dir ./data
```

In this mode:

- main veloflux process joins `manager`
- `fi_critical` and `fi_best` start as worker subprocesses
- subprocess binding uses `cgroup.procs`

## Mode B: thread-level in-process extra flow instances

### 1) Setup cgroup

```bash
sudo ./scripts/perf_pr_host/setup_cgroup.sh \
  --mode thread_level \
  --base-cg /veloflux-ci/perf-pr-host-001/veloflux
```

Topology:

```text
/base
/base/main
/base/main/fi_critical
/base/main/fi_best
```

Notes:

- `/base/main` is prepared as the threaded-domain root for the main veloflux process
- `fi_critical` and `fi_best` are threaded child cgroups for dedicated Tokio runtime workers
- quotas still use the same default values as worker-process mode

The script prints:

```text
CG_MODE=thread_level
CG_BASE=...
CG_MAIN=...
CG_FI_CRITICAL=...
CG_FI_BEST=...
```

Use them like this:

- `CG_MAIN` -> `start_veloflux.sh --main-cgroup`
- `CG_FI_*` -> `server.flow_instances[].cgroup.thread_path`

### 2) Start veloflux

```bash
./scripts/perf_pr_host/start_veloflux.sh \
  --main-cgroup /veloflux-ci/perf-pr-host-001/veloflux/main \
  --veloflux-bin ./veloflux-linux-x86_64 \
  --config ./scripts/perf_pr_host/config.thread_level.yaml \
  --data-dir ./data
```

In this mode:

- main veloflux process joins the threaded-domain root `/.../main`
- `fi_critical` and `fi_best` stay in the main process
- each extra instance owns a dedicated Tokio runtime
- each runtime worker thread binds itself through `cgroup.threads`

## Shared steps: deploy stream/pipeline

Use Go workload resource subcommands (idempotent and converge-oriented):

```bash
./scripts/perf_pr_host/go_workload/bin/go_workload provision \
  --base-url http://127.0.0.1:8080
```

`provision` behavior:

- reconcile `perf_pr_stream_critical` + `perf_pr_pipeline_critical`
- reconcile `perf_pr_stream_best` + `perf_pr_pipeline_best`
- delete/recreate existing target resources when needed
- continue on missing/existing resources and converge to final target state

Default flow instance binding:

- `perf_pr_pipeline_critical` -> `fi_critical`
- `perf_pr_pipeline_best` -> `fi_best`

Delete all resources (idempotent):

```bash
./scripts/perf_pr_host/go_workload/bin/go_workload delete \
  --base-url http://127.0.0.1:8080
```

Start both pipelines:

```bash
./scripts/perf_pr_host/go_workload/bin/go_workload start \
  --base-url http://127.0.0.1:8080
```

Pause both pipelines:

```bash
./scripts/perf_pr_host/go_workload/bin/go_workload pause \
  --base-url http://127.0.0.1:8080
```

Legacy shell helper is still available:

```bash
./scripts/perf_pr_host/deploy_stream_pipeline.sh <create|start|pause|delete>
```

## Shared steps: run workload

Build Go workload binary (first time or after code update):

```bash
GOPROXY=${GOPROXY:-https://proxy.golang.org,direct} \
  go -C ./scripts/perf_pr_host/go_workload build \
  -o ./scripts/perf_pr_host/go_workload/bin/go_workload .
```

The first build downloads Go modules from proxy/network.

Run with defaults:

```bash
./scripts/perf_pr_host/run_workload.sh
```

Default behavior:

- `qps=20`
- `columns=15000`
- `str-len=10`
- `duration=60s`

The script does not scrape or dump metrics. Use Prometheus/Grafana directly for observation.

Lower-pressure example:

```bash
./scripts/perf_pr_host/run_workload.sh --qps 10 --duration-secs 30
```

Per-pipeline QPS override example:

```bash
./scripts/perf_pr_host/run_workload.sh --qps 20 --qps-critical 30 --qps-best 10
```

Use `--workload-bin <path>` to run a prebuilt binary from another location.

## Cleanup

```bash
sudo ./scripts/perf_pr_host/cleanup_cgroup.sh \
  --base-cg /veloflux-ci/perf-pr-host-001/veloflux
```

The cleanup script is topology-aware enough to work for both perf modes.

## Package this directory

Create a `tar.gz` package from repo root:

```bash
tar -czf perf_pr_host_scripts.tar.gz -C scripts perf_pr_host
```

Create a `zip` package from repo root:

```bash
cd scripts && zip -r ../perf_pr_host_scripts.zip perf_pr_host
```
