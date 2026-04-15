# Syslog Logging Design

This document defines the first syslog backend design for veloFlux logging.

## Background

Some deployments rely on the host syslog service for log collection, retention,
forwarding, and operational filtering. In those environments, writing only to
`stdout` or local rotating files adds extra log shipping work outside the
application.

veloFlux already emits most runtime logs through `tracing`, which makes syslog
support a backend integration problem rather than an application call-site
migration.

## Goals

- Add a syslog output backend without changing runtime logging call sites.
- Keep process-global logging initialization centralized.
- Preserve structured logging context in the rendered syslog message body.
- Keep syslog I/O off the main runtime execution path.
- Support manager, worker, and embedded startup paths consistently.

## Non-Goals

- This design does not enable remote UDP syslog transport in the current
  implementation.
- This design does not enable remote TCP syslog transport in the current
  implementation.
- This design does not support multi-backend fan-out in the first version.
- This design does not define host-process logging callbacks for the embedded
  runtime.

## Configuration

The syslog backend is enabled by selecting `logging.output=syslog`.

Example:

```yaml
logging:
  output: syslog
  level: info
  disable_timestamp: true
  include_source: false
  syslog:
    enable: true
    level: info
    tag: "veloflux"
    network: ""
    address: ""
```

This example is directly usable for local syslog. Leaving both
`logging.syslog.network` and `logging.syslog.address` empty tells veloFlux to
connect to the host local syslog service by platform convention, such as
`/var/run/syslog` on macOS or `/dev/log` on many Linux distributions.

Example local-syslog-focused snippet:

```yaml
logging:
  output: syslog
  level: info
  disable_timestamp: true
  syslog:
    enable: true
    tag: "veloflux"
    network: ""
    address: ""
```

### Fields

#### `logging.syslog.network`

Transport name aligned with eKuiper.

Current behavior:

- keep it empty to use local syslog
- non-empty values are reserved for a future remote syslog enhancement

#### `logging.syslog.address`

Remote endpoint address aligned with eKuiper.

Current behavior:

- keep it empty to use local syslog
- non-empty values are reserved for a future remote syslog enhancement

#### `logging.syslog.enable`

Whether the syslog sink is explicitly enabled.

When `logging.output=syslog` is selected, this field must be `true`.

#### `logging.syslog.level`

Sink-local level for syslog output.

If omitted, it inherits the global `logging.level`.

#### `logging.syslog.tag`

Base application tag used to derive the effective syslog ident.

Default:

- `veloflux`

## Effective Ident Rules

To keep logs attributable in multi-process deployments, the effective syslog
ident should be derived by startup path:

- manager: `<tag>-manager`
- worker: `<tag>-worker-<instance_id>`
- embedded: `<tag>-embedded`

This keeps operator-side filtering stable even when multiple veloFlux runtime
processes use the same host syslog service.

## Severity Mapping

The backend should map `tracing` levels to syslog severities in the obvious
way:

- `ERROR` -> error
- `WARN` -> warning
- `INFO` -> informational
- `DEBUG` -> debug
- `TRACE` -> debug

The first version does not need a separate trace-specific syslog priority.

## Message Body Shape

The syslog message body should remain human-readable and preserve useful event
context.

Recommended content:

- event target
- event message
- structured event fields
- source file and line when `logging.include_source=true`

Recommended exclusions:

- ANSI styling
- terminal-only formatting assumptions
- duplicated wall-clock timestamp text in the message body when
  `logging.disable_timestamp=true`

## Startup Behavior

When `logging.output=syslog` is selected:

1. load config;
2. validate that `logging.syslog.enable=true`;
3. resolve the effective runtime ident from `logging.syslog.tag`;
4. resolve the platform local syslog endpoint and connect to it;
5. install the process-global subscriber;
6. continue normal runtime startup.

If syslog initialization fails, startup should fail rather than silently
falling back to another backend.

## Runtime Failure Behavior

After startup succeeds, the runtime should prioritize forward progress over
guaranteed log delivery.

If syslog writes fail after startup:

- do not block processor or connector hot paths;
- use bounded buffering;
- attempt background recovery where practical;
- tolerate bounded record loss;
- emit rate-limited diagnostics for persistent backend failure.

## Worker-Process Considerations

Worker processes are independent OS processes and therefore own independent
syslog connections.

Because workers may emit to the same host syslog service concurrently, the
derived worker ident is required and should include the flow instance id.

## Deployment Notes

- Keep `logging.output=syslog` and `logging.syslog.enable=true` aligned.
- Keep `logging.syslog.network=""` and `logging.syslog.address=""` for the
  current local-syslog-only implementation.
- Prefer `include_source=false` in production unless file/line metadata is
  required for active debugging.
- Prefer `disable_timestamp=true` when syslog already supplies the timestamp
  envelope you need.
- Keep runtime code on `tracing` macros so the selected backend remains
  effective.
- Reserve direct `eprintln!` output for startup-time diagnostics that happen
  before logging is initialized.

## Environment Variables

The syslog backend adds these explicit override bindings:

- `VELOFLUX_LOGGING__DISABLE_TIMESTAMP`
- `VELOFLUX_LOGGING__SYSLOG__ENABLE`
- `VELOFLUX_LOGGING__SYSLOG__LEVEL`
- `VELOFLUX_LOGGING__SYSLOG__TAG`
- `VELOFLUX_LOGGING__SYSLOG__NETWORK`
- `VELOFLUX_LOGGING__SYSLOG__ADDRESS`

These overrides follow the same whitelist-based policy used by the rest of the
configuration loader.
