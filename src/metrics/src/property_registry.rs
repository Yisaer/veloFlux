#![allow(clippy::expect_used)] // prometheus metric registration — Lazy::new closures run once at startup

use once_cell::sync::Lazy;
use prometheus::{IntCounter, IntCounterVec, IntGauge};

use crate::common::{opts, register_collector, KIND_LABEL};

static CONNECTED: Lazy<IntGauge> = Lazy::new(|| {
    register_collector(
        IntGauge::with_opts(opts(
            "property_registry_mqtt",
            "connected",
            "Whether the local MQTT property provider is connected",
        ))
        .expect("create property registry MQTT connected gauge"),
    )
});

static UPDATES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "property_registry_mqtt",
                "updates_total",
                "Total number of MQTT property update outcomes",
            ),
            &[KIND_LABEL],
        )
        .expect("create property registry MQTT updates counter vec"),
    )
});

static ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "property_registry_mqtt",
                "errors_total",
                "Total number of local MQTT property provider errors",
            ),
            &[KIND_LABEL],
        )
        .expect("create property registry MQTT errors counter vec"),
    )
});

static RECONNECTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_collector(
        IntCounter::with_opts(opts(
            "property_registry_mqtt",
            "reconnects_total",
            "Total number of successful local MQTT property provider reconnects",
        ))
        .expect("create property registry MQTT reconnects counter"),
    )
});

pub fn connected() -> &'static IntGauge {
    &CONNECTED
}

pub fn updates_total() -> &'static IntCounterVec {
    &UPDATES_TOTAL
}

pub fn errors_total() -> &'static IntCounterVec {
    &ERRORS_TOTAL
}

pub fn reconnects_total() -> &'static IntCounter {
    &RECONNECTS_TOTAL
}
