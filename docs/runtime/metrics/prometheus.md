# Prometheus Metrics Design

## Overview

veloFlux uses a single Prometheus definition layer for all exported metrics.

All Prometheus collectors must be declared in one dedicated crate and must not be created inside
business modules such as connectors, processors, runtime services, or telemetry collectors.

This design enforces:

- one source of truth for metric definitions
- stable naming and label conventions
- consistent subsystem boundaries
- predictable Prometheus query patterns

## Definition Ownership

Prometheus metric definitions are owned by the `veloflux_metrics` crate.

Other crates may:

- obtain an existing collector handle from `veloflux_metrics`
- update metric values
- attach label values to an existing collector

Other crates must not:

- call `prometheus::register(...)`
- create `Opts` or `HistogramOpts` directly
- hardcode full Prometheus metric names
- define ad hoc `Lazy<...>` collectors in local modules

## Naming Model

Every metric must use the Prometheus three-part naming model:

- `namespace`
- `subsystem`
- `name`

The namespace is fixed:

- `namespace = veloflux`

The subsystem identifies a stable observability domain:

- `mqtt_source`
- `mqtt_sink`
- `processor`
- `runtime`
- `flow_instance`

The name identifies the metric itself:

- `records_in_total`
- `records_out_total`
- `errors_total`
- `handle_duration_seconds`
- `memory_usage_bytes`

The final metric name is generated as:

```text
<namespace>_<subsystem>_<name>
```

Examples:

- `veloflux_mqtt_source_records_in_total`
- `veloflux_mqtt_sink_records_out_total`
- `veloflux_processor_errors_total`
- `veloflux_processor_handle_duration_seconds`
- `veloflux_runtime_memory_usage_bytes`
- `veloflux_flow_instance_cpu_usage_percent`

## Naming Rules

The following naming rules apply to every new metric:

- Use `snake_case`.
- Counter names must end with `_total`.
- Histogram names must carry the unit in the name, such as `_seconds` or `_bytes`.
- Gauge names should describe the observed quantity directly and should not append `_gauge`.
- The `name` part must not repeat the subsystem meaning.

Good examples:

- `veloflux_processor_records_in_total`
- `veloflux_runtime_heap_in_use_bytes`

Bad examples:

- `veloflux_processor_processor_records_in_total`
- `veloflux_runtime_memory_gauge`

## Label Conventions

Label keys must use `snake_case` and must stay consistent across subsystems.

Shared label keys:

- `flow_instance`
- `pipeline_id`
- `processor_id`
- `connector`
- `metric`

Alias labels must not be introduced when an existing canonical label already exists.

For example:

- use `flow_instance`
- do not use `flow_instance_id`

## Subsystem Boundaries

### `mqtt_source`

This subsystem contains source-side connector ingress and egress counters.

Examples:

- `veloflux_mqtt_source_records_in_total`
- `veloflux_mqtt_source_records_out_total`

Expected labels:

- `flow_instance`
- `connector`

### `mqtt_sink`

This subsystem contains sink-side connector ingress and publish-success counters.

Examples:

- `veloflux_mqtt_sink_records_in_total`
- `veloflux_mqtt_sink_records_out_total`

Expected labels:

- `flow_instance`
- `connector`

### `processor`

This subsystem contains metrics for processor execution and processor-level custom stats.

Standard processor metrics:

- `veloflux_processor_records_in_total`
- `veloflux_processor_records_out_total`
- `veloflux_processor_errors_total`
- `veloflux_processor_handle_duration_seconds`
- `veloflux_processor_send_backpressure_waits_total`

Expected labels:

- `flow_instance`
- `pipeline_id`
- `processor_id`

### Processor Custom Metrics

Processor-specific metrics are exposed through shared metric families instead of dynamically
registering a new Prometheus collector for every processor-local stat.

Shared families:

- `veloflux_processor_custom_gauge`
- `veloflux_processor_custom_counter_total`

Expected labels:

- `flow_instance`
- `pipeline_id`
- `processor_id`
- `metric`

Examples of `metric` label values:

- `rows_buffered`
- `collections_in`
- `collections_forwarded`
- `collections_suppressed`

This keeps the Prometheus definition layer stable while still allowing processors to expose
structured custom stats.

### `runtime`

This subsystem contains process-wide runtime metrics.

Examples:

- `veloflux_runtime_cpu_usage_percent`
- `veloflux_runtime_memory_usage_bytes`
- `veloflux_runtime_tokio_tasks_inflight`
- `veloflux_runtime_heap_in_use_bytes`
- `veloflux_runtime_heap_in_allocator_bytes`

These metrics describe the whole process and therefore must not be labeled as a specific
`flow_instance`.

### `flow_instance`

This subsystem contains metrics that are truly scoped to an individual in-process flow instance.

Example:

- `veloflux_flow_instance_cpu_usage_percent`

Expected labels:

- `flow_instance`

## Runtime and Flow Instance Separation

Process-level runtime metrics and flow-instance metrics must not be mixed.

The distinction is:

- `runtime`: the entire veloFlux process
- `flow_instance`: one flow instance inside the process

This separation is required so that Prometheus queries remain semantically correct.

For example:

- overall process memory belongs to `runtime`
- per-instance CPU sampling belongs to `flow_instance`

## Query Ergonomics

The naming and labeling rules are designed to keep Prometheus queries simple and uniform.

Examples:

```promql
sum by (connector) (rate(veloflux_mqtt_source_records_in_total[5m]))
```

```promql
sum by (flow_instance, pipeline_id, processor_id) (
  rate(veloflux_processor_records_out_total[5m])
)
```

```promql
veloflux_runtime_memory_usage_bytes
```

```promql
veloflux_flow_instance_cpu_usage_percent{flow_instance="default"}
```

## Design Summary

After the redesign, the metrics system follows four hard rules:

1. All Prometheus metric definitions live in `veloflux_metrics`.
2. Every metric uses explicit `namespace`, `subsystem`, and `name`.
3. Labels are canonicalized and reused consistently.
4. Runtime, connector, processor, and flow-instance metrics are separated by stable subsystem
   boundaries.
