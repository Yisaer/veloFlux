use once_cell::sync::{Lazy, OnceCell};
use prometheus::{GaugeVec, Opts};

static FLOW_INSTANCE_ID: OnceCell<String> = OnceCell::new();
static FLOW_INSTANCE_CPU_USAGE_PERCENT: Lazy<GaugeVec> = Lazy::new(|| {
    let opts = Opts::new(
        "flow_instance_cpu_usage_percent",
        "CPU usage of one flow instance in percent of one core",
    );
    let vec = GaugeVec::new(opts, &["flow_instance_id"]).expect("create flow instance cpu gauge");
    prometheus::register(Box::new(vec.clone())).expect("register flow instance cpu gauge");
    vec
});

pub fn set_flow_instance_id(id: &str) {
    let id = id.trim();
    if id.is_empty() {
        return;
    }
    let _ = FLOW_INSTANCE_ID.set(id.to_string());
}

pub(crate) fn flow_instance_id() -> &'static str {
    FLOW_INSTANCE_ID
        .get()
        .map(|s| s.as_str())
        .unwrap_or("default")
}

pub(crate) fn set_flow_instance_cpu_usage_percent(instance_id: &str, value: f64) {
    FLOW_INSTANCE_CPU_USAGE_PERCENT
        .with_label_values(&[instance_id])
        .set(value);
}
