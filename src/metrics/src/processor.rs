#![allow(clippy::expect_used)] // prometheus metric registration — Lazy::new closures run once at startup

use once_cell::sync::Lazy;
use prometheus::{exponential_buckets, HistogramVec, IntCounterVec, IntGaugeVec};

use crate::common::{
    histogram_opts, opts, register_collector, FLOW_INSTANCE_LABEL, METRIC_LABEL, PIPELINE_ID_LABEL,
    PROCESSOR_ID_LABEL,
};

static PROCESSOR_RECORDS_IN_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "processor",
                "records_in_total",
                "Rows received by processors",
            ),
            &[FLOW_INSTANCE_LABEL, PIPELINE_ID_LABEL, PROCESSOR_ID_LABEL],
        )
        .expect("create processor records_in counter vec"),
    )
});

static PROCESSOR_RECORDS_OUT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "processor",
                "records_out_total",
                "Rows emitted by processors",
            ),
            &[FLOW_INSTANCE_LABEL, PIPELINE_ID_LABEL, PROCESSOR_ID_LABEL],
        )
        .expect("create processor records_out counter vec"),
    )
});

static PROCESSOR_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts("processor", "errors_total", "Errors observed by processors"),
            &[FLOW_INSTANCE_LABEL, PIPELINE_ID_LABEL, PROCESSOR_ID_LABEL],
        )
        .expect("create processor errors counter vec"),
    )
});

static PROCESSOR_CUSTOM_GAUGE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_collector(
        IntGaugeVec::new(
            opts(
                "processor",
                "custom_gauge",
                "Gauge metrics reported by processors",
            ),
            &[
                FLOW_INSTANCE_LABEL,
                PIPELINE_ID_LABEL,
                PROCESSOR_ID_LABEL,
                METRIC_LABEL,
            ],
        )
        .expect("create processor custom gauge vec"),
    )
});

static PROCESSOR_CUSTOM_COUNTER_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "processor",
                "custom_counter_total",
                "Counter metrics reported by processors",
            ),
            &[
                FLOW_INSTANCE_LABEL,
                PIPELINE_ID_LABEL,
                PROCESSOR_ID_LABEL,
                METRIC_LABEL,
            ],
        )
        .expect("create processor custom counter vec"),
    )
});

static PROCESSOR_HANDLE_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    let buckets = exponential_buckets(0.000_001, 2.0, 20).expect("create handle duration buckets");
    register_collector(
        HistogramVec::new(
            histogram_opts(
                "processor",
                "handle_duration_seconds",
                "Time spent handling one processor input batch",
            )
            .buckets(buckets),
            &[FLOW_INSTANCE_LABEL, PIPELINE_ID_LABEL, PROCESSOR_ID_LABEL],
        )
        .expect("create processor handle duration histogram vec"),
    )
});

static PROCESSOR_SEND_BACKPRESSURE_WAITS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_collector(
        IntCounterVec::new(
            opts(
                "processor",
                "send_backpressure_waits_total",
                "Number of cooperative backpressure sleep ticks taken while sending processor data",
            ),
            &[FLOW_INSTANCE_LABEL, PIPELINE_ID_LABEL, PROCESSOR_ID_LABEL],
        )
        .expect("create processor send backpressure waits counter vec"),
    )
});

pub fn records_in_total() -> &'static IntCounterVec {
    &PROCESSOR_RECORDS_IN_TOTAL
}

pub fn records_out_total() -> &'static IntCounterVec {
    &PROCESSOR_RECORDS_OUT_TOTAL
}

pub fn errors_total() -> &'static IntCounterVec {
    &PROCESSOR_ERRORS_TOTAL
}

pub fn custom_gauge() -> &'static IntGaugeVec {
    &PROCESSOR_CUSTOM_GAUGE
}

pub fn custom_counter_total() -> &'static IntCounterVec {
    &PROCESSOR_CUSTOM_COUNTER_TOTAL
}

pub fn handle_duration_seconds() -> &'static HistogramVec {
    &PROCESSOR_HANDLE_DURATION_SECONDS
}

pub fn send_backpressure_waits_total() -> &'static IntCounterVec {
    &PROCESSOR_SEND_BACKPRESSURE_WAITS_TOTAL
}
