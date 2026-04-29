#![allow(clippy::expect_used)] // prometheus metric registration — Lazy::new closures run once at startup

use once_cell::sync::{Lazy, OnceCell};
use prometheus::GaugeVec;

use crate::common::{opts, register_collector, FLOW_INSTANCE_LABEL};

static DEFAULT_FLOW_INSTANCE_ID: OnceCell<String> = OnceCell::new();

static FLOW_INSTANCE_CPU_USAGE_PERCENT: Lazy<GaugeVec> = Lazy::new(|| {
    register_collector(
        GaugeVec::new(
            opts(
                "flow_instance",
                "cpu_usage_percent",
                "CPU usage of one flow instance in percent of one core",
            ),
            &[FLOW_INSTANCE_LABEL],
        )
        .expect("create flow instance cpu gauge vec"),
    )
});

pub fn set_default_id(id: &str) {
    let id = id.trim();
    if id.is_empty() {
        return;
    }
    let _ = DEFAULT_FLOW_INSTANCE_ID.set(id.to_string());
}

pub fn default_id() -> &'static str {
    DEFAULT_FLOW_INSTANCE_ID
        .get()
        .map(|id| id.as_str())
        .unwrap_or("default")
}

pub fn cpu_usage_percent() -> &'static GaugeVec {
    &FLOW_INSTANCE_CPU_USAGE_PERCENT
}
