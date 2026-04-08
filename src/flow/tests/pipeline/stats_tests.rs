//! Integration tests for pipeline stats behavior.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, Value};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::model::batch_from_columns_simple;
use flow::pipeline::MemorySinkProps;
use flow::FlowInstance;
use flow::{
    CreatePipelineRequest, NopSinkProps, PipelineDefinition, PipelineError, PipelineStopMode,
    SinkDefinition, SinkProps, SinkType,
};
use serde_json::json;
use std::sync::Arc;
use tokio::time::Duration;

use super::common::{
    declare_memory_input_output_topics, install_memory_stream_schema, make_memory_topics,
    publish_input_collection, recv_next_json,
};

async fn create_mock_json_stream(instance: &FlowInstance, stream_name: &str) {
    let schema = Schema::new(vec![ColumnSchema::new(
        stream_name.to_string(),
        "x".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]);
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create mock stream");
}

fn build_nop_pipeline(pipeline_id: &str, sql: &str) -> PipelineDefinition {
    PipelineDefinition::new(
        pipeline_id.to_string(),
        sql,
        vec![SinkDefinition::new(
            "nop_sink",
            SinkType::Nop,
            SinkProps::Nop(NopSinkProps::default()),
        )],
    )
}

#[tokio::test]
async fn collect_pipeline_stats_returns_base_fields_for_running_pipeline() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    create_mock_json_stream(&instance, "stats_stream").await;

    let pipeline_id = "stats_running_pipeline";
    let pipeline = build_nop_pipeline(pipeline_id, "SELECT x FROM stats_stream");
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .expect("start pipeline");

    let stats = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect("collect stats for running pipeline");
    assert!(
        !stats.is_empty(),
        "running pipeline should expose at least one processor stats entry"
    );
    assert!(
        stats
            .iter()
            .all(|entry| entry.stats.error_count == 0 && entry.stats.last_error.is_none()),
        "fresh running pipeline should not report processor errors"
    );
    assert!(
        stats.iter().all(|entry| {
            entry.stats.records_in == 0
                && entry.stats.records_out == 0
                && entry.stats.custom.is_empty()
        }),
        "idle pipeline should expose zeroed base stats without custom metrics"
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

#[tokio::test]
async fn collect_pipeline_stats_returns_error_for_stopped_pipeline() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    create_mock_json_stream(&instance, "stats_stopped_stream").await;

    let pipeline_id = "stats_stopped_pipeline";
    let pipeline = build_nop_pipeline(pipeline_id, "SELECT x FROM stats_stopped_stream");
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .expect("start pipeline");
    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline");

    let err = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect_err("stopped pipeline should not return stats");
    match err {
        PipelineError::Runtime(message) => {
            assert!(
                message.contains("is not running"),
                "unexpected stopped-pipeline stats error: {message}"
            );
        }
        other => panic!("unexpected stats error for stopped pipeline: {other:?}"),
    }

    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

#[tokio::test]
async fn collect_pipeline_stats_reset_after_runtime_rebuild() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_stats",
        "collect_pipeline_stats_reset_after_runtime_rebuild",
    );
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);

    let schema_hint = vec![(
        "x".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )];
    install_memory_stream_schema(&instance, &input_topic, &schema_hint).await;

    let pipeline_id = "stats_rebuild_pipeline";
    let pipeline = PipelineDefinition::new(
        pipeline_id.to_string(),
        "SELECT x FROM stream",
        vec![SinkDefinition::new(
            "memory_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    instance
        .start_pipeline(pipeline_id)
        .expect("start pipeline before stats collection");

    let batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "x".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )])
    .expect("build input batch");
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(batch),
        Duration::from_secs(5),
    )
    .await;

    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(actual, json!([{"x": 1}, {"x": 2}, {"x": 3}]));

    let stats_before = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect("collect stats before rebuild");
    assert!(
        stats_before
            .iter()
            .any(|entry| entry.stats.records_in > 0 || entry.stats.records_out > 0),
        "processed pipeline should expose non-zero counters before rebuild: {stats_before:#?}"
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline before rebuild");
    instance
        .start_pipeline(pipeline_id)
        .expect("restart pipeline to rebuild runtime");

    let stats_after = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect("collect stats after rebuild");
    assert!(
        !stats_after.is_empty(),
        "rebuilt pipeline should still expose processor stats entries"
    );
    assert!(
        stats_after.iter().all(|entry| {
            entry.stats.records_in == 0
                && entry.stats.records_out == 0
                && entry.stats.error_count == 0
                && entry.stats.last_error.is_none()
                && entry.stats.custom.is_empty()
        }),
        "runtime rebuild should reset in-memory stats counters: {stats_after:#?}"
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop rebuilt pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}
