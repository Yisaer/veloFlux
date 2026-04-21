use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, IntGaugeVec};

use crate::common::{opts, register_collector, FLOW_INSTANCE_LABEL, KEY_LABEL, KIND_LABEL};

static SHARED_STREAM_RUNNING: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_collector(
        IntGaugeVec::new(
            opts(
                "shared_stream",
                "running",
                "Whether the shared stream runtime is currently running",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create shared stream running gauge vec"),
    )
});

static SHARED_STREAM_ACTIVE_SUBSCRIBERS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_collector(
        IntGaugeVec::new(
            opts(
                "shared_stream",
                "active_subscribers",
                "Number of active subscribers currently attached to the shared stream runtime",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create shared stream active subscribers gauge vec"),
    )
});

static SHARED_STREAM_STARTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "shared_stream",
                "starts_total",
                "Total number of successful shared stream runtime starts",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create shared stream starts counter vec"),
    )
});

static SHARED_STREAM_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "shared_stream",
                "errors_total",
                "Total number of shared stream runtime errors",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL, KIND_LABEL],
        )
        .expect("create shared stream errors counter vec"),
    )
});

static SHARED_STREAM_MESSAGES_IN_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "shared_stream",
                "messages_in_total",
                "Total number of messages accepted by the shared stream runtime from the internal shared ingest pipeline",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create shared stream messages in counter vec"),
    )
});

static SHARED_STREAM_MESSAGES_OUT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "shared_stream",
                "messages_out_total",
                "Total number of successful shared stream publish events into the fan-out hub",
            ),
            &[FLOW_INSTANCE_LABEL, KEY_LABEL],
        )
        .expect("create shared stream messages out counter vec"),
    )
});

pub fn running() -> &'static IntGaugeVec {
    &SHARED_STREAM_RUNNING
}

pub fn active_subscribers() -> &'static IntGaugeVec {
    &SHARED_STREAM_ACTIVE_SUBSCRIBERS
}

pub fn starts_total() -> &'static IntCounterVec {
    &SHARED_STREAM_STARTS_TOTAL
}

pub fn errors_total() -> &'static IntCounterVec {
    &SHARED_STREAM_ERRORS_TOTAL
}

pub fn messages_in_total() -> &'static IntCounterVec {
    &SHARED_STREAM_MESSAGES_IN_TOTAL
}

pub fn messages_out_total() -> &'static IntCounterVec {
    &SHARED_STREAM_MESSAGES_OUT_TOTAL
}
