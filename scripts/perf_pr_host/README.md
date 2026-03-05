# Perf PR Host Scripts

These scripts are intentionally limited to four operations:

1. setup cgroup
2. start veloflux (manager joins cgroup before exec)
3. deploy stream/pipeline
4. run workload

Only `start_veloflux.sh` starts veloflux.
No script in this folder starts or manages emqx/prometheus/grafana.

## Reference config

Reference config file:

```text
scripts/perf_pr_host/config.yaml
```

macOS local startup validation config (no cgroup binding):

```text
scripts/perf_pr_host/config.macos.yaml
```

Update these paths using `setup_cgroup.sh` output:

- `server.extra_flow_instances[0].cgroup_path`
- `server.extra_flow_instances[1].cgroup_path`

## 1) Setup cgroup

```bash
sudo ./scripts/perf_pr_host/setup_cgroup.sh --base-cg /veloflux-ci/perf-pr-host-001/veloflux
```

Default cgroup policy:

- base `cpu.max = 100000 100000` (total 1 CPU)
- `fi_critical` `cpu.max = 90000 100000`
- `fi_best` `cpu.max = 25000 100000`
- `fi_critical` `cpu.weight = 10000`
- `manager` `cpu.weight = 300`
- `fi_best` `cpu.weight = 1`

The script prints:

```text
CG_BASE=...
CG_MANAGER=...
CG_FI_CRITICAL=...
CG_FI_BEST=...
```

Use those values in:

- veloflux config (`extra_flow_instances[].cgroup_path`) for worker binding
- `start_veloflux.sh --manager-cgroup` for main veloflux process binding

## 2) Start veloflux (manager pre-join cgroup)

```bash
./scripts/perf_pr_host/start_veloflux.sh \
  --manager-cgroup /veloflux-ci/perf-pr-host-001/veloflux/manager \
  --veloflux-bin ./veloflux-linux-x86_64 \
  --config ./scripts/perf_pr_host/config.yaml \
  --data-dir ./data
```

This ensures the main veloflux process joins `manager` cgroup before runtime starts.

## 3) Deploy stream/pipeline

Use Go workload resource subcommands (idempotent and converge-oriented):

```bash
./scripts/perf_pr_host/go_workload/bin/go_workload provision \
  --base-url http://127.0.0.1:8080
```

`provision` behavior:

- Reconcile `perf_pr_stream_critical` + `perf_pr_pipeline_critical`
- Reconcile `perf_pr_stream_best` + `perf_pr_pipeline_best`
- Delete/recreate existing target resources when needed
- Continue on missing/existing resources and converge to final target state

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

## 4) Run workload

Build Go workload binary (first time or after code update):

```bash
GOPROXY=${GOPROXY:-https://proxy.golang.org,direct} \
  go -C ./scripts/perf_pr_host/go_workload build -o ./scripts/perf_pr_host/go_workload/bin/go_workload .
```

The first build downloads Go modules from proxy/network.

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

## Optional cleanup

```bash
sudo ./scripts/perf_pr_host/cleanup_cgroup.sh --base-cg /veloflux-ci/perf-pr-host-001/veloflux
```

## Package this directory

Create a `tar.gz` package from repo root:

```bash
tar -czf perf_pr_host_scripts.tar.gz -C scripts perf_pr_host
```

Create a `zip` package from repo root:

```bash
cd scripts && zip -r ../perf_pr_host_scripts.zip perf_pr_host
```
