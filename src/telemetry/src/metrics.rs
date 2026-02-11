use once_cell::sync::Lazy;
use prometheus::{IntGauge, Opts};

use crate::flow_instance::flow_instance_id;

fn register_int_gauge_with_flow_instance(name: &str, help: &str) -> IntGauge {
    let opts = Opts::new(name, help).const_label("flow_instance", flow_instance_id().to_string());
    let gauge = IntGauge::with_opts(opts).expect("create int gauge");
    prometheus::register(Box::new(gauge.clone())).expect("register int gauge");
    gauge
}

pub static CPU_USAGE_GAUGE: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge_with_flow_instance("cpu_usage", "CPU usage in percentage"));

pub static MEMORY_USAGE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_flow_instance("memory_usage_bytes", "Resident memory usage in bytes")
});

pub static TOKIO_TASKS_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_flow_instance(
        "tokio_tasks_inflight",
        "Number of currently running Tokio spawn tasks",
    )
});

pub static HEAP_IN_USE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_flow_instance(
        "heap_in_use_bytes",
        "Bytes actively allocated by the global allocator",
    )
});

pub static HEAP_IN_ALLOCATOR_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_flow_instance(
        "heap_in_allocator_bytes",
        "Bytes reserved by the allocator from the operating system",
    )
});
