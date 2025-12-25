use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::pipeline::{EventtimeOptions, PipelineOptions, PlanCacheOptions};
use flow::planner::sink::{
    NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
};
use flow::{
    Catalog, ColumnSchema, ConcreteDatatype, EventtimeDefinition, PipelineRegistries,
    SinkEncoderConfig,
};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn explain_pipeline_with_eventtime_enabled_prints_plans() {
    let stream_name = "stream";
    let schema = flow::Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(flow::Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "event_ts".to_string(),
            ConcreteDatatype::Int64(flow::Int64Type),
        ),
    ]);

    let catalog = Catalog::new();
    catalog.upsert(
        StreamDefinition::new(
            stream_name.to_string(),
            Arc::new(schema),
            StreamProps::Mock(MockStreamProps::default()),
            StreamDecoderConfig::json(),
        )
        .with_eventtime(EventtimeDefinition::new(
            "event_ts".to_string(),
            "unixtimestamp_ms".to_string(),
        )),
    );

    let registries = PipelineRegistries::new_with_builtin();

    let options = PipelineOptions {
        plan_cache: PlanCacheOptions { enabled: false },
        eventtime: EventtimeOptions {
            enabled: true,
            late_tolerance: Duration::from_secs(5),
        },
    };

    let sink_connector = PipelineSinkConnector::new(
        "nop_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("nop_sink", sink_connector);

    let explain = flow::explain_pipeline_with_options(
        "SELECT sum(a), lag(a) FROM stream GROUP BY tumblingwindow('ss', 10)",
        vec![sink],
        &catalog,
        flow::shared_stream_registry(),
        &registries,
        &options,
    )
    .expect("explain pipeline");

    let rendered = explain.to_pretty_string();
    println!("{rendered}");
    assert!(rendered.contains("Logical Plan Explain:"));
    assert!(rendered.contains("Physical Plan Explain:"));
    assert!(rendered.contains("PhysicalEventtimeWatermark"));
    assert!(rendered.contains("mode=event_time"));
    assert!(rendered.contains("lateToleranceMs=5000"));
    assert!(rendered.contains("eventtime.column=event_ts"));
    assert!(rendered.contains("eventtime.type=unixtimestamp_ms"));
    assert!(rendered.contains("eventtime.index=1"));
}
