use once_cell::sync::Lazy;
use prometheus::{Gauge, IntGauge};

use crate::common::{opts, register_collector};

static RUNTIME_CPU_USAGE_PERCENT: Lazy<Gauge> = Lazy::new(|| {
    register_collector(
        Gauge::with_opts(opts(
            "runtime",
            "cpu_usage_percent",
            "CPU usage in percentage",
        ))
        .expect("create runtime cpu gauge"),
    )
});

static RUNTIME_MEMORY_USAGE_BYTES: Lazy<IntGauge> = Lazy::new(|| {
    register_collector(
        IntGauge::with_opts(opts(
            "runtime",
            "memory_usage_bytes",
            "Resident memory usage in bytes",
        ))
        .expect("create runtime memory gauge"),
    )
});

static RUNTIME_TOKIO_TASKS_INFLIGHT: Lazy<IntGauge> = Lazy::new(|| {
    register_collector(
        IntGauge::with_opts(opts(
            "runtime",
            "tokio_tasks_inflight",
            "Number of currently running Tokio spawn tasks",
        ))
        .expect("create runtime tokio tasks gauge"),
    )
});

static RUNTIME_HEAP_IN_USE_BYTES: Lazy<IntGauge> = Lazy::new(|| {
    register_collector(
        IntGauge::with_opts(opts(
            "runtime",
            "heap_in_use_bytes",
            "Bytes actively allocated by the global allocator",
        ))
        .expect("create runtime heap in use gauge"),
    )
});

static RUNTIME_HEAP_IN_ALLOCATOR_BYTES: Lazy<IntGauge> = Lazy::new(|| {
    register_collector(
        IntGauge::with_opts(opts(
            "runtime",
            "heap_in_allocator_bytes",
            "Bytes reserved by the allocator from the operating system",
        ))
        .expect("create runtime heap in allocator gauge"),
    )
});

pub fn cpu_usage_percent() -> &'static Gauge {
    &RUNTIME_CPU_USAGE_PERCENT
}

pub fn memory_usage_bytes() -> &'static IntGauge {
    &RUNTIME_MEMORY_USAGE_BYTES
}

pub fn tokio_tasks_inflight() -> &'static IntGauge {
    &RUNTIME_TOKIO_TASKS_INFLIGHT
}

pub fn heap_in_use_bytes() -> &'static IntGauge {
    &RUNTIME_HEAP_IN_USE_BYTES
}

pub fn heap_in_allocator_bytes() -> &'static IntGauge {
    &RUNTIME_HEAP_IN_ALLOCATOR_BYTES
}
