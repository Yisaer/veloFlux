#![allow(clippy::expect_used)] // prometheus metric registration — Lazy::new closures run once at startup

use once_cell::sync::Lazy;
use prometheus::{exponential_buckets, HistogramVec, IntCounterVec, IntGaugeVec};

use crate::common::{
    histogram_opts, opts, register_collector, FLOW_INSTANCE_LABEL, METRIC_LABEL, PROCESSOR_ID_LABEL,
};

const STREAM_NAME_LABEL: &str = "stream_name";

macro_rules! counter_vec {
    ($static_name:ident, $metric_name:literal, $help:literal) => {
        static $static_name: Lazy<IntCounterVec> = Lazy::new(|| {
            register_collector(
                IntCounterVec::new(
                    opts("shared_stream_processor", $metric_name, $help),
                    &[FLOW_INSTANCE_LABEL, STREAM_NAME_LABEL, PROCESSOR_ID_LABEL],
                )
                .expect("create shared stream processor counter vec"),
            )
        });
    };
}

counter_vec!(
    RECORDS_IN_TOTAL,
    "records_in_total",
    "Rows received by shared stream processors"
);
counter_vec!(
    RECORDS_OUT_TOTAL,
    "records_out_total",
    "Rows emitted by shared stream processors"
);
counter_vec!(
    ERRORS_TOTAL,
    "errors_total",
    "Errors observed by shared stream processors"
);
counter_vec!(
    BACKPRESSURE_WAITS_TOTAL,
    "send_backpressure_waits_total",
    "Backpressure wait ticks in shared stream processors"
);

static CUSTOM_COUNTER_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "shared_stream_processor",
                "custom_counter_total",
                "Counter metrics reported by shared stream processors",
            ),
            &[
                FLOW_INSTANCE_LABEL,
                STREAM_NAME_LABEL,
                PROCESSOR_ID_LABEL,
                METRIC_LABEL,
            ],
        )
        .expect("create shared stream processor custom counter vec"),
    )
});

static CUSTOM_GAUGE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_collector(
        IntGaugeVec::new(
            opts(
                "shared_stream_processor",
                "custom_gauge",
                "Gauge metrics reported by shared stream processors",
            ),
            &[
                FLOW_INSTANCE_LABEL,
                STREAM_NAME_LABEL,
                PROCESSOR_ID_LABEL,
                METRIC_LABEL,
            ],
        )
        .expect("create shared stream processor gauge vec"),
    )
});

static HANDLE_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    let buckets = exponential_buckets(0.000_001, 2.0, 20)
        .expect("create shared stream processor duration buckets");
    register_collector(
        HistogramVec::new(
            histogram_opts(
                "shared_stream_processor",
                "handle_duration_seconds",
                "Time spent handling one shared stream processor input batch",
            )
            .buckets(buckets),
            &[FLOW_INSTANCE_LABEL, STREAM_NAME_LABEL, PROCESSOR_ID_LABEL],
        )
        .expect("create shared stream processor duration histogram vec"),
    )
});

pub fn records_in_total() -> &'static IntCounterVec {
    &RECORDS_IN_TOTAL
}

pub fn records_out_total() -> &'static IntCounterVec {
    &RECORDS_OUT_TOTAL
}

pub fn errors_total() -> &'static IntCounterVec {
    &ERRORS_TOTAL
}

pub fn custom_gauge() -> &'static IntGaugeVec {
    &CUSTOM_GAUGE
}

pub fn custom_counter_total() -> &'static IntCounterVec {
    &CUSTOM_COUNTER_TOTAL
}

pub fn handle_duration_seconds() -> &'static HistogramVec {
    &HANDLE_DURATION_SECONDS
}

pub fn send_backpressure_waits_total() -> &'static IntCounterVec {
    &BACKPRESSURE_WAITS_TOTAL
}
