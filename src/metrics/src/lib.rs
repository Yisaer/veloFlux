#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

mod common;
mod flow_instance;
mod mqtt_sink;
mod mqtt_source;
mod processor;
mod runtime;

use prometheus::{Gauge, GaugeVec, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec};

pub fn set_default_flow_instance_id(id: &str) {
    flow_instance::set_default_id(id);
}

pub fn default_flow_instance_id() -> &'static str {
    flow_instance::default_id()
}

pub fn mqtt_source_records_in_total() -> &'static IntCounterVec {
    mqtt_source::records_in_total()
}

pub fn mqtt_source_records_out_total() -> &'static IntCounterVec {
    mqtt_source::records_out_total()
}

pub fn mqtt_sink_records_in_total() -> &'static IntCounterVec {
    mqtt_sink::records_in_total()
}

pub fn mqtt_sink_records_out_total() -> &'static IntCounterVec {
    mqtt_sink::records_out_total()
}

pub fn processor_records_in_total() -> &'static IntCounterVec {
    processor::records_in_total()
}

pub fn processor_records_out_total() -> &'static IntCounterVec {
    processor::records_out_total()
}

pub fn processor_errors_total() -> &'static IntCounterVec {
    processor::errors_total()
}

pub fn processor_custom_gauge() -> &'static IntGaugeVec {
    processor::custom_gauge()
}

pub fn processor_custom_counter_total() -> &'static IntCounterVec {
    processor::custom_counter_total()
}

pub fn processor_handle_duration_seconds() -> &'static HistogramVec {
    processor::handle_duration_seconds()
}

pub fn processor_send_backpressure_waits_total() -> &'static IntCounterVec {
    processor::send_backpressure_waits_total()
}

pub fn flow_instance_cpu_usage_percent() -> &'static GaugeVec {
    flow_instance::cpu_usage_percent()
}

pub fn runtime_cpu_usage_percent() -> &'static Gauge {
    runtime::cpu_usage_percent()
}

pub fn runtime_memory_usage_bytes() -> &'static IntGauge {
    runtime::memory_usage_bytes()
}

pub fn runtime_tokio_tasks_inflight() -> &'static IntGauge {
    runtime::tokio_tasks_inflight()
}

pub fn runtime_heap_in_use_bytes() -> &'static IntGauge {
    runtime::heap_in_use_bytes()
}

pub fn runtime_heap_in_allocator_bytes() -> &'static IntGauge {
    runtime::heap_in_allocator_bytes()
}
