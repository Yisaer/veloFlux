# Environment Configuration Overrides

## Background

veloFlux currently loads process startup configuration from `config.yaml`.

In real deployments, a small subset of configuration values changes across environments more
frequently than the rest of the config file. Typical examples are bind addresses, log verbosity,
and profiling or metrics toggles.

These values are good candidates for environment-variable overrides because they are deployment
specific, scalar, and often adjusted by container or orchestration systems.

At the same time, not every field in `config.yaml` should be exposed through OS environment
variables. Some sections describe runtime topology rather than simple deployment parameters, and
those sections are easier to review and maintain in versioned config files.

## Goals

- Allow a small, explicit whitelist of deployment-oriented config fields to be overridden by OS
  environment variables.
- Keep environment-variable naming stable and predictable.
- Apply the same override semantics in both manager and worker startup paths.
- Preserve `config.yaml` as the source of truth for structured topology configuration.

## Non-Goals

- Provide a generic environment-variable mirror for the entire config schema.
- Support overriding `server.flow_instances` from environment variables.
- Support partial overrides for arrays or nested structured objects.
- Introduce runtime hot reload for environment-variable changes.

## Supported Fields

Only the following environment variables are recognized:

| Config path | Environment variable |
| --- | --- |
| `logging.output` | `VELOFLUX_LOGGING__OUTPUT` |
| `logging.level` | `VELOFLUX_LOGGING__LEVEL` |
| `logging.include_source` | `VELOFLUX_LOGGING__INCLUDE_SOURCE` |
| `logging.file.dir` | `VELOFLUX_LOGGING__FILE__DIR` |
| `profiling.enabled` | `VELOFLUX_PROFILING__ENABLED` |
| `profiling.addr` | `VELOFLUX_PROFILING__ADDR` |
| `profiling.cpu_profile_freq_hz` | `VELOFLUX_PROFILING__CPU_PROFILE_FREQ_HZ` |
| `metrics.addr` | `VELOFLUX_METRICS__ADDR` |
| `metrics.poll_interval_secs` | `VELOFLUX_METRICS__POLL_INTERVAL_SECS` |
| `server.manager_addr` | `VELOFLUX_SERVER__MANAGER_ADDR` |

All other `config.yaml` fields remain file-only configuration.

In particular, `server.flow_instances` is intentionally excluded because it describes instance
topology and backend-specific runtime structure. That section is better maintained in `config.yaml`
where it can be reviewed as one coherent unit.

## Naming Rules

Environment-variable names follow these rules:

- use the `VELOFLUX_` prefix
- use `__` as the config-path separator
- keep the config field name and convert it to uppercase

Examples:

- `logging.level` -> `VELOFLUX_LOGGING__LEVEL`
- `metrics.poll_interval_secs` -> `VELOFLUX_METRICS__POLL_INTERVAL_SECS`

## Merge Priority

The effective startup configuration uses the following precedence:

`defaults < config.yaml < environment variables < CLI`

This means:

- built-in defaults fill missing fields
- `config.yaml` overrides defaults
- supported environment variables override the loaded file values
- CLI flags remain the final override layer for fields that already have CLI support

## Value Semantics

Supported value types are intentionally simple:

- string fields use the raw environment-variable string
- boolean and integer fields are parsed according to their target config type
- enum fields must use the same lowercase values accepted by `config.yaml`

Examples:

```bash
export VELOFLUX_LOGGING__LEVEL=debug
export VELOFLUX_LOGGING__OUTPUT=stdout
export VELOFLUX_PROFILING__ENABLED=false
export VELOFLUX_METRICS__ADDR=0.0.0.0:19998
export VELOFLUX_SERVER__MANAGER_ADDR=0.0.0.0:18080
```

## Worker-Process Behavior

Worker processes reload configuration during their own startup path.

To keep behavior consistent, environment-variable overrides are applied in the shared config loader
instead of being patched only in the manager bootstrap path. This ensures that:

- manager startup sees the same effective config as before spawning workers
- worker processes inherit the same supported overrides from the parent process environment

Because `server.flow_instances` remains file-only configuration, deployments that use
`worker_process` instances still require a config file.

## Implementation Outline

The implementation keeps the override surface explicit:

1. Load defaults and optionally parse `config.yaml`.
2. Apply only the documented environment-variable whitelist.
3. Convert the effective config into `ServerOptions`.

The whitelist is implemented as a centralized binding table in the config loader. Adding support for
a new environment variable requires an explicit binding entry that declares:

- the environment-variable name
- the target config path
- the apply logic for the target field

Unsupported `VELOFLUX_*` variables are ignored by the config loader and warned as unsupported.
