# veloFlux

an open-source streaming processor written in Rust and designed for resource-sensitive environments.

[![codecov](https://codecov.io/gh/Yisaer/veloFlux/branch/main/graph/badge.svg)](https://app.codecov.io/gh/Yisaer/veloFlux)

## Quick start
Prereq: Rust stable toolchain, `make`, and `protoc` (protobuf compiler, e.g. `apt-get install protobuf-compiler`).

The `tonic-build` codegen requires `protoc` to compile gRPC service stubs from proto definitions bundled in the `flow` crate.

Build and test:
- `make build` / `make release` / `make release-thin`
- `make test`
- `make fmt` / `make clippy`

Run the server with built-in defaults (profiling support is compiled in, but profiling endpoints are disabled):
```bash
cargo run --bin veloflux -- --data-dir ./tmp/data
```

Run a release binary directly:
```bash
./veloflux
```

Flags:
- `--data-dir <path>` sets where metadata is stored (default `./tmp` in the current working directory).
- `--config <path>` loads a YAML config (defaults to optional `./config.yaml`).

For production deployments, pass an explicit `--data-dir` so metadata is not tied
to the process working directory.

The repository provides one configuration template, `etc/config.example.yaml`.
Load it explicitly to use its settings; profiling endpoints remain disabled:

```bash
./veloflux --config etc/config.example.yaml --data-dir ./data
```

You can also copy the template to a local `config.yaml`, which is loaded automatically
from the current working directory when `--config` is omitted. If that file does not
exist, the server uses built-in defaults and supported environment overrides.

Example configuration:
```yaml
profiling:
  enabled: false
  addr: "127.0.0.1:6060"
metrics:
  poll_interval_secs: 5
server:
  manager_addr: "127.0.0.1:8080"
```
Precedence: CLI flags > supported environment variables > configuration file > built-in defaults.

For diagnostics, explicitly set `profiling.enabled: true` or
`VELOFLUX_PROFILING__ENABLED=true` before starting the server.

Manager and profiling listeners bind to loopback by default. Manager is a trusted
control plane without built-in authentication; expose it only through an authenticated
gateway. Cross-container or remote gateways require an explicit bind address and
network isolation. See [deployment security](docs/runtime/deployment_security.md).

## Managing streams and pipelines
The manager exposes HTTP endpoints once the server is running.

Create a stream:
```bash
curl -XPOST http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "name": "source_stream",
    "revision": 1,
    "type": "mqtt",
    "schema": {
      "type": "json",
      "props": {
        "columns": [
          {"name": "user_id", "data_type": "int64"},
          {"name": "score", "data_type": "float64"}
        ]
      }
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
    "revision": 1,
    "sql": "SELECT user_id, score FROM source_stream WHERE score > 0",
    "sinks": [
      {
        "type": "mqtt",
        "props": {"broker_url": "tcp://127.0.0.1:1883", "topic": "/yisa/data2"},
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

REST API docs:
- [API overview and trust model](user_docs/api/README.md)
- Streams: `user_docs/api/stream.md`
- Tables: `user_docs/api/table.md` (runtime catalog; manager REST endpoints pending)
- Pipelines: `user_docs/api/pipeline.md`
- Export: `user_docs/api/export.md`

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
- Extensibility for connectors/codecs lives in `docs/runtime/extensibility/registry_extensibility.md`; windowing plan in `docs/syntax/windows/watermarks.md`.

## Linux release compatibility

Linux binary and FFI releases target glibc 2.28 or newer. Video builds additionally
require compatible GStreamer runtime libraries. Builds use a fixed compatibility
userland and pass ELF ABI checks and a native runtime smoke test before packaging.
See [Linux release compatibility](docs/development/linux-release-compatibility.md).
