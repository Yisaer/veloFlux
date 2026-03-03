# Perf PR Host Scripts

These scripts are intentionally limited to three operations:

1. setup cgroup
2. deploy stream/pipeline
3. run workload

No script in this folder starts or manages veloflux/emqx/prometheus/grafana.

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

- `server.default_cgroup_path`
- `server.extra_flow_instances[0].cgroup_path`
- `server.extra_flow_instances[1].cgroup_path`

## 1) Setup cgroup

```bash
sudo ./scripts/perf_pr_host/setup_cgroup.sh --base-cg /veloflux-ci/perf-pr-host-001/veloflux
```

Default cgroup policy:

- base `cpu.max = 100000 100000` (total 1 CPU)
- `fi_critical` `cpu.max = 95000 100000`
- `manager` `cpu.weight = 200`
- `fi_best` `cpu.weight = 100`

The script prints:

```text
CG_BASE=...
CG_MANAGER=...
CG_FI_CRITICAL=...
CG_FI_BEST=...
```

Use those values in your veloflux config (`default_cgroup_path` and `extra_flow_instances[].cgroup_path`).

## 2) Deploy stream/pipeline

```bash
./scripts/perf_pr_host/deploy_stream_pipeline.sh create
```

`create` behavior:

- Create/reconcile `perf_pr_stream_critical` + `perf_pr_pipeline_critical`
- Create/reconcile `perf_pr_stream_best` + `perf_pr_pipeline_best`
- If stream/pipeline already exists, it is skipped/reconciled (idempotent)
- Pipelines are not auto-started (`--no-start`)

Default flow instance binding:

- `perf_pr_pipeline_critical` -> `fi_critical`
- `perf_pr_pipeline_best` -> `fi_best`

Delete all resources (idempotent):

```bash
./scripts/perf_pr_host/deploy_stream_pipeline.sh delete
```

Start both pipelines:

```bash
./scripts/perf_pr_host/deploy_stream_pipeline.sh start
```

Pause both pipelines:

```bash
./scripts/perf_pr_host/deploy_stream_pipeline.sh pause
```

## 3) Run workload

```bash
./scripts/perf_pr_host/run_workload.sh
```

Default behavior runs both phases:

- phase A: `rate_critical=100`, `rate_best=100`, `duration=60s`
- phase B: `rate_critical=5`, `rate_best=100`, `duration=60s`
 
The script does not scrape or dump metrics. Use Prometheus/Grafana directly for observation.

Use `--phase a` or `--phase b` to run only one phase.

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
