//! Integration tests for the SamplerProcessor.
//!
//! These tests verify that the sampler is correctly inserted into physical plans
//! and that the stream definition stores sampler config correctly.

use datatypes::{ColumnSchema, Schema};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::planner::sink::{
    NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
};
use flow::processor::processor_builder::PlanProcessor;
use flow::processor::SamplerConfig;
use flow::FlowInstance;
use std::sync::Arc;
use std::time::Duration;

async fn create_stream_with_sampler(
    instance: &FlowInstance,
    stream_name: &str,
    sampler_interval: Duration,
) {
    let schema_columns = vec![ColumnSchema::new(
        stream_name.to_string(),
        "x".to_string(),
        datatypes::ConcreteDatatype::Int64(datatypes::Int64Type),
    )];
    let schema = Schema::new(schema_columns);
    let mut definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    definition = definition.with_sampler(SamplerConfig::new(sampler_interval));
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream with sampler");
}

async fn create_stream_without_sampler(instance: &FlowInstance, stream_name: &str) {
    let schema_columns = vec![ColumnSchema::new(
        stream_name.to_string(),
        "x".to_string(),
        datatypes::ConcreteDatatype::Int64(datatypes::Int64Type),
    )];
    let schema = Schema::new(schema_columns);
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");
}

/// Test that a stream with sampler config produces a pipeline with SamplerProcessor.
#[tokio::test]
async fn test_pipeline_with_sampler_has_sampler_processor() {
    let instance = FlowInstance::new();
    create_stream_with_sampler(&instance, "sampled_stream", Duration::from_millis(100)).await;

    let connector = PipelineSinkConnector::new(
        "test_sink",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("output_sink", connector).with_forward_to_result(true);

    let pipeline = instance
        .build_pipeline("SELECT x FROM sampled_stream", vec![sink])
        .expect("pipeline creation should succeed");

    // Check that there's a Sampler processor in the middle processors
    let has_sampler = pipeline
        .middle_processors
        .iter()
        .any(|p| matches!(p, PlanProcessor::Sampler(_)));

    assert!(has_sampler, "Expected Sampler processor in pipeline");
}

/// Test that a stream without sampler config does NOT have SamplerProcessor.
#[tokio::test]
async fn test_pipeline_without_sampler_has_no_sampler_processor() {
    let instance = FlowInstance::new();
    create_stream_without_sampler(&instance, "normal_stream").await;

    let connector = PipelineSinkConnector::new(
        "test_sink",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("output_sink", connector).with_forward_to_result(true);

    let pipeline = instance
        .build_pipeline("SELECT x FROM normal_stream", vec![sink])
        .expect("pipeline creation should succeed");

    // Check that there's NO Sampler processor
    let has_sampler = pipeline
        .middle_processors
        .iter()
        .any(|p| matches!(p, PlanProcessor::Sampler(_)));

    assert!(
        !has_sampler,
        "Expected no Sampler processor in pipeline, but found one"
    );
}

/// Test that SamplerConfig is correctly stored in StreamDefinition.
#[tokio::test]
async fn test_stream_definition_stores_sampler_config() {
    let instance = FlowInstance::new();
    let sampler_interval = Duration::from_secs(5);

    create_stream_with_sampler(&instance, "test_stream", sampler_interval).await;

    let stream_info = instance
        .get_stream("test_stream")
        .await
        .expect("get stream");

    let sampler = stream_info
        .definition
        .sampler()
        .expect("stream should have sampler config");

    assert_eq!(sampler.interval, sampler_interval);
}

/// Test that stream without sampler has None for sampler config.
#[tokio::test]
async fn test_stream_definition_without_sampler() {
    let instance = FlowInstance::new();
    create_stream_without_sampler(&instance, "no_sampler_stream").await;

    let stream_info = instance
        .get_stream("no_sampler_stream")
        .await
        .expect("get stream");

    assert!(
        stream_info.definition.sampler().is_none(),
        "Stream without sampler config should return None"
    );
}
