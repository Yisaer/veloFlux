# Pipeline API

## NNG Pub/Sub Sink

An NNG pub/sub sink uses the same request shape for TCP, IPC, and embedded
in-process transports. The URL scheme is the transport selector; do not add a
separate `transport` or `inproc` property.

```json
{
  "id": "can_pipeline",
  "revision": 1,
  "sql": "SELECT * FROM can_input",
  "sinks": [
    {
      "id": "output",
      "type": "nng_pubsub",
      "props": {
        "url": "inproc://veloflux/output",
        "topic": "result/can",
        "topic_delimiter": ":"
      }
    }
  ]
}
```

For an embedded deployment, the host process must listen on the corresponding
NNG endpoint before starting the pipeline. The host should publish or consume
business data only after the pipeline's NNG source and sink connectors are
ready. `running` indicates that pipeline tasks have started; it does not by
itself guarantee that an NNG peer is connected.

See [NNG In-Process Integration](../integrations/ffi/nng_inproc.md) for the
host startup order, URL-based transport selection, connection-layer design,
and shared `libnng` requirement.

## MQTT 5 Sink Properties

An MQTT sink may set `props.protocol_version` to `v5` and provide User Properties as an ordered
array. MQTT 3.1.1 remains the default when the version is omitted. Each value may use a process
property template.

```json
{
  "id": "mqtt_output",
  "type": "mqtt",
  "props": {
    "broker_url": "tcp://127.0.0.1:1883",
    "topic": "processed/telemetry",
    "protocol_version": "v5",
    "user_properties": [
      { "key": "source", "value": "{{ prop(\"site\") }}" },
      { "key": "tag", "value": "primary" },
      { "key": "tag", "value": "edge" }
    ]
  }
}
```

Each item must contain string `key` and `value` fields. Keys are literal. Values support the
connector template profile, where `prop()` reads process-wide properties; `.row` and incoming MQTT
5 User Properties are not available. Order and duplicate keys are preserved. With `connector_key`,
omit connector-local `protocol_version`; the referenced shared MQTT client owns the version. User
Properties require that effective shared version to be `v5`.

When the local MQTT property provider is enabled, template values are resolved
once per delivery and retries preserve that snapshot. Otherwise they are fixed
when the pipeline is applied.

## Schedule Options

Pipeline schedules are declared inside `options.schedule` when creating or upserting a pipeline.
The scheduler owns the lifecycle while desired state is `running`. Manual `stop` persists
`stopped` and disables schedule patrol; manual `start` persists `running` and leaves the next
runtime decision to patrol.

```json
{
  "options": {
    "schedule": {
      "cron": "*/10 * * * *",
      "duration_secs": 300,
      "datetime_ranges": [
        {
          "begin_timestamp_ms": 1767225600000,
          "end_timestamp_ms": 1767312000000
        }
      ]
    }
  }
}
```

| Field | Type | Description |
| --- | --- | --- |
| `cron` | string | Optional Linux-compatible 5-field cron expression or supported recurring nickname. Expressions are evaluated in UTC. See [Pipeline Schedule Cron Syntax](../syntax/pipeline_schedule_cron.md). |
| `duration_secs` | integer | Required when `cron` is present and forbidden without `cron`. Must be greater than `0`. |
| `datetime_ranges` | array | Optional UTC timestamp ranges in milliseconds. When cron is absent, at least one range is required and the ranges define the complete run windows. |

At least one scheduling mode must be present: `cron` with `duration_secs`, or one or more
`datetime_ranges`. Each datetime range uses `[begin_timestamp_ms, end_timestamp_ms)` semantics:

- `begin_timestamp_ms` must be non-negative.
- `end_timestamp_ms` must be non-negative.
- `begin_timestamp_ms` must be less than `end_timestamp_ms`.
- The begin timestamp is included in the run window and the end timestamp is excluded.
- At most 128 normalized ranges are accepted. Multiple ranges are combined with OR semantics.

Cron windows also use open boundaries. The effective run window depends on the configured modes:

```text
cron only:              (cron_fire, cron_fire + duration_secs)
datetime ranges only:   any [begin_timestamp_ms, end_timestamp_ms)
cron and ranges:        (cron_fire, cron_fire + duration_secs) intersect any datetime_range
```

Multiple cron windows are combined with OR semantics. When consecutive windows overlap, the
pipeline remains scheduled to run across the overlap instead of being stopped and restarted at the
next cron fire.

If the cron window crosses the end of the matched datetime range, the pipeline is stopped at the
range end. `GET /pipelines/:id` returns `schedule_status.auto_stop_at` as the effective current
window end, not just the raw cron duration end. Patrol-triggered stops use graceful shutdown by
default so already accepted data can be drained and finalized before the runtime exits.
When all configured datetime ranges have ended, the pipeline enters `scheduled_end`; patrol will
not schedule that pipeline again for the current revision.

On create, a scheduled pipeline enables schedule control with desired state `running`. Upsert and
process restart preserve the two-value desired state. The patrol scheduler derives the current
runtime status from the schedule window; no scheduler status is persisted to bypass evaluation.

Only the user's desired run state is stored separately from the runtime:

- `Running`: the user wants the pipeline to run; scheduled pipelines remain under patrol control.
- `Stopped`: the user has manually stopped the pipeline; patrol skips it.

REST responses expose real-time `status` values. For non-scheduled pipelines, the runtime values
are `running`, `stopping`, `stopped`, and `failed`. Scheduled pipelines use
`scheduled_running`, `scheduled_stopped`, `scheduled_end`, `stopped`, `stopping`, and `failed`;
they never expose the ordinary `running` status. None of these values is persisted as desired
state.

During a graceful patrol stop, the runtime may temporarily report `stopping`. This is an
in-memory transition state and is not persisted as a desired state. Patrol treats `stopping` as
an operation in progress and does not start the runtime again until the stop has completed. A
manual stop persists desired state `stopped` immediately to disable schedule control, while the runtime may
remain `stopping` until graceful shutdown finishes. A manual start received during this transition
is rejected as busy; retrying it after the stop completes only re-enables schedule control, and the
next patrol decides whether the runtime should start.

## Pipeline Status

Pipeline list and get responses expose one unified lifecycle status. The manager persists the
two-value control state before asking runtime to converge. If runtime cannot converge, the runtime
status becomes `failed` and the current error is recorded in memory; desired state remains
either `running` or `stopped`. A pipeline can report:

- `running`
- `stopping`
- `stopped`
- `scheduled_running`
- `scheduled_stopped`
- `scheduled_end`
- `failed`

`failed` means startup or rebuild failed, or a processor task returned an error, panicked, or
exited unexpectedly while the pipeline was expected to run. For processor failures, the pipeline
supervisor aborts the remaining tasks and records the root cause in the current runtime snapshot.

`running`, `stopping`, `stopped`, and `failed` describe runtime lifecycle states for ordinary
pipelines. `scheduled_running`, `scheduled_stopped`, and `scheduled_end` describe scheduler-derived
states for scheduled pipelines.
The status is computed from the persisted desired state, the schedule window, and the runtime.
Manual stop changes desired state to `stopped` and disables patrol. Manual start changes desired
state to `running`; patrol then derives `scheduled_running`, `scheduled_stopped`, or
`scheduled_end` from the current schedule.

Responses may include `last_runtime_error` when the current runtime has a current failure context.
It describes the current runtime failure attempt; it is not a historical error record. A new
startup attempt replaces the previous context, and a successful recovery clears it.

`last_runtime_error` has this shape:

```json
{
  "processor_id": "filter_1",
  "processor_kind": "filter",
  "reason": "processor returned error: boom",
  "failed_at_ms": 1797249600000
}
```

Startup hydration creates fresh runtime state from the pipeline definition and desired state.
Runtime errors are not persisted and do not survive restart. A new startup failure records
its reason and timestamp in the current runtime snapshot. Within the same process, the current runtime `failed` status prevents
scheduled patrol from retrying repeatedly. A manual start is an explicit retry and clears the
current failure context before runtime convergence; for scheduled pipelines, patrol performs the
later runtime start. Manual stop clears the failure context, writes desired state `stopped`, and
disables schedule patrol. Delete removes the stored pipeline and the failure context.

`GET /pipelines/:id/stats` is only available for a running pipeline. For a failed pipeline, the
endpoint returns `400 Bad Request` with the failed processor and reason instead of returning stale
processor counters.

### Runtime convergence matrix

The manager persists `desired_state` before attempting runtime convergence. If the current runtime
status is `stopping`, lifecycle operations return `409 Conflict` and the caller must wait for the
stop to finish.

For non-scheduled pipelines:

| desired_state | Current status | Expected behavior |
| --- | --- | --- |
| `running` | `running` | Already converged; return success. |
| `running` | `stopped` | Start the runtime; expose `running` after success. |
| `running` | `stopping` | Return `409 Conflict` and wait for stopping to finish. |
| `running` | `failed` | Clear the runtime error and retry start; keep `failed` if the retry fails. |
| `stopped` | `stopped` | Already converged; return success. |
| `stopped` | `running` | Stop the runtime; expose `stopped` after success. |
| `stopped` | `stopping` | Return `409 Conflict` and wait for stopping to finish. |
| `stopped` | `failed` | Do not retry runtime start; converge to `stopped`. |

For scheduled pipelines:

| desired_state | Current status | Expected behavior |
| --- | --- | --- |
| `running` | `scheduled_running` | Already converged; skip the operation. |
| `running` | `scheduled_stopped` | Remain `scheduled_stopped`; patrol starts the runtime in a valid window. |
| `running` | `scheduled_end` | Remain `scheduled_end`; patrol does not schedule the current revision again. |
| `running` | `stopped` | Derive `scheduled_stopped` when a future window exists, or `scheduled_end` when all windows have ended. |
| `running` | `stopping` | Return `409 Conflict` and wait for stopping to finish. |
| `running` | `failed` | Clear the runtime error and derive `scheduled_stopped` or `scheduled_end`; patrol may retry in a later valid window. |
| `stopped` | `scheduled_running` | Stop the runtime; expose `stopped` and disable patrol. |
| `stopped` | `scheduled_stopped` | Convert to `stopped` and disable patrol. |
| `stopped` | `scheduled_end` | Convert to `stopped` and disable patrol. |
| `stopped` | `stopped` | Already converged; return success. |
| `stopped` | `stopping` | Return `409 Conflict` and wait for stopping to finish. |
| `stopped` | `failed` | Do not retry runtime start; converge to `stopped`. |
