# synapseFlow

an open-source streaming processor written in Rust and designed for resource-sensitive environments.

[![codecov](https://codecov.io/gh/Yisaer/synapseFlow/branch/main/graph/badge.svg)](https://app.codecov.io/gh/Yisaer/synapseFlow)

## Quick start
Prereq: Rust stable toolchain and `make`.

Build and test:
- `make build` / `make release` / `make release-thin`
- `make test`
- `make fmt` / `make clippy`

Run the server (defaults to metrics + profiling features):
```bash
cargo run --bin synapse-flow -- --data-dir ./tmp/data
```
Flags:
- `--enable-profiling` / `--disable-profiling` override `PROFILE_SERVER_ENABLE`.
- `--data-dir <path>` sets where metadata is stored (default `./tmp`).

Environment:
- `MANAGER_ADDR` (default `0.0.0.0:8080`) REST API bind address.
- `METRICS_ADDR` (default `0.0.0.0:9898`) Prometheus endpoint when `metrics` feature is on.
- `METRICS_POLL_INTERVAL_SECS` (default `5`) system metrics cadence.
- `PROFILE_SERVER_ENABLE` (`true`/`false`) enables profiling HTTP endpoints; `PROFILE_ADDR` defaults to `0.0.0.0:6060`.

## Managing streams and pipelines
The manager exposes HTTP endpoints once the server is running.

Create a stream:
```bash
curl -XPOST http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "name": "source_stream",
    "type": "mqtt",
    "schema": {
      "type": "json",
      "columns": [
        {"name": "user_id", "data_type": "int64"},
        {"name": "score", "data_type": "float64"}
      ]
    },
    "props": {"broker_url": "tcp://127.0.0.1:1883", "topic": "/yisa/data"},
    "decoder": {"type": "json", "props": {}}
  }'
```

Create a pipeline that reads from the stream and writes to MQTT:
```bash
curl -XPOST http://localhost:8080/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "id": "demo-pipeline",
    "sql": "SELECT user_id, score FROM source_stream WHERE score > 0",
    "sinks": [
      {
        "type": "mqtt",
        "props": {"topic": "/yisa/data2"},
        "encoder": {"type": "json", "props": {}}
      }
    ]
  }'
```

Start the pipeline:
```bash
curl -XPOST http://localhost:8080/pipelines/demo-pipeline/start
```
List and remove resources:
- `GET /streams` / `DELETE /streams/:name`
- `GET /pipelines` / `DELETE /pipelines/:id`

## Project layout
- `src/flow/` — planner + processors; connector/codec registries; pipeline execution.
- `src/parser/` — StreamDialect SQL → `SelectStmt`.
- `src/datatypes/` — schemas and runtime values used across the workspace.
- `src/manager/` — Axum REST API for streams/pipelines.
- `src/storage/` — persists streams/pipelines under a data directory.
- `src/telemetry/` — Prometheus metrics and jemalloc/pprof profiling (feature-gated).

## Development notes
- Default build enables `metrics` and `profiling`; use `--no-default-features` to disable.
- Narrow tests while iterating, e.g. `cargo test -p flow convert::`.
- Use `test_simple/` for isolated end-to-end experiments without touching main data.
- Extensibility for connectors/codecs lives in `docs/EXTENSIBILITY.md`; windowing plan in `docs/WATERMARK_WINDOW_PLAN.md`.
