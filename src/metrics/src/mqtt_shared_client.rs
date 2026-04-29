#![allow(clippy::expect_used)] // prometheus metric registration — Lazy::new closures run once at startup

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, IntGaugeVec};

use crate::common::{opts, register_collector, FLOW_INSTANCE_LABEL, KEY_LABEL, KIND_LABEL};

static MQTT_SHARED_CLIENT_CONNECTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_collector(
        IntGaugeVec::new(
            opts(
                "mqtt_shared_client",
                "connected",
                "Whether the shared MQTT client is currently connected to the broker",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create mqtt shared client connected gauge vec"),
    )
});

static MQTT_SHARED_CLIENT_ACTIVE_REFS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_collector(
        IntGaugeVec::new(
            opts(
                "mqtt_shared_client",
                "active_refs",
                "Number of active runtime handles currently holding the shared MQTT client",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create mqtt shared client active refs gauge vec"),
    )
});

static MQTT_SHARED_CLIENT_RX_MESSAGES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "mqtt_shared_client",
                "rx_messages_total",
                "Total number of MQTT publish messages accepted from the broker by the shared client",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create mqtt shared client rx messages counter vec"),
    )
});

static MQTT_SHARED_CLIENT_TX_MESSAGES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "mqtt_shared_client",
                "tx_messages_total",
                "Total number of MQTT publish messages successfully sent to the broker through the shared client",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create mqtt shared client tx messages counter vec"),
    )
});

static MQTT_SHARED_CLIENT_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "mqtt_shared_client",
                "errors_total",
                "Total number of shared MQTT client runtime errors",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL, KIND_LABEL],
        )
        .expect("create mqtt shared client errors counter vec"),
    )
});

static MQTT_SHARED_CLIENT_RECONNECTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "mqtt_shared_client",
                "reconnects_total",
                "Total number of successful reconnect transitions of the shared MQTT client",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create mqtt shared client reconnects counter vec"),
    )
});

pub fn connected() -> &'static IntGaugeVec {
    &MQTT_SHARED_CLIENT_CONNECTED
}

pub fn active_refs() -> &'static IntGaugeVec {
    &MQTT_SHARED_CLIENT_ACTIVE_REFS
}

pub fn rx_messages_total() -> &'static IntCounterVec {
    &MQTT_SHARED_CLIENT_RX_MESSAGES_TOTAL
}

pub fn tx_messages_total() -> &'static IntCounterVec {
    &MQTT_SHARED_CLIENT_TX_MESSAGES_TOTAL
}

pub fn errors_total() -> &'static IntCounterVec {
    &MQTT_SHARED_CLIENT_ERRORS_TOTAL
}

pub fn reconnects_total() -> &'static IntCounterVec {
    &MQTT_SHARED_CLIENT_RECONNECTS_TOTAL
}
