//! Integration tests for non-table-driven pipeline behaviors.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::planner::sink::{
    NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
};
use flow::processor::{SamplerConfig, StreamData};
use flow::FlowInstance;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

async fn create_stream_with_sampler(
    instance: &FlowInstance,
    stream_name: &str,
    sampler_interval: Duration,
) {
    let schema_columns = vec![ColumnSchema::new(
        stream_name.to_string(),
        "x".to_string(),
        ConcreteDatatype::Int64(Int64Type),
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

/// Test execution of the "latest" samler strategy.
/// We send 5 values within a single interval window (200ms).
/// Expected result: Only ONE output value, which is the 5th (latest) value.
#[tokio::test]
async fn test_sampler_execution_latest_strategy() {
    let instance = FlowInstance::new_default();
    let interval = Duration::from_millis(200);
    // Create stream with 200ms sampler interval
    create_stream_with_sampler(&instance, "latest_stream", interval).await;

    let connector = PipelineSinkConnector::new(
        "test_sink",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("output_sink", connector).with_forward_to_result(true);

    let mut pipeline = instance
        .build_pipeline("SELECT x FROM latest_stream", vec![sink])
        .expect("pipeline creation should succeed");

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send 5 JSON messages as raw bytes: {"x": 1}, ..., {"x": 5}
    for i in 1..=5 {
        let payload = format!(r#"{{"x": {}}}"#, i).into_bytes();
        pipeline
            .send_stream_data("latest_stream", StreamData::bytes(payload))
            .await
            .expect("send data");
        // Tiny sleep to ensure order but still well within 200ms interval
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");

    // We expect EXACTLY one output (the sampled value)
    let result = timeout(Duration::from_millis(500), output.recv())
        .await
        .expect("wait for sampled output")
        .expect("sampled output missing");

    // Verify content: should be value 5
    match result {
        StreamData::EncodedBytes { payload, .. } => {
            let json: serde_json::Value = serde_json::from_slice(&payload).expect("json");
            // Output format depends on sink, typically [{"x": 5}]
            let rows = json.as_array().expect("output is array");
            assert_eq!(rows.len(), 1, "expected 1 row in batch");
            assert_eq!(rows[0]["x"], 5, "Expected latest value 5");
        }
        other => panic!("expected EncodedBytes, got {}", other.description()),
    }

    // Ensure no more outputs come (sampler should wait for next interval)
    let extra = timeout(Duration::from_millis(100), output.recv()).await;
    assert!(
        extra.is_err(),
        "Expected no extra outputs, sampler should have throttled intermediate values"
    );

    // Verify stats: Sampler should have 5 inputs and 1 output
    let stats = pipeline.processor_stats();
    let _ = stats
        .iter()
        .find(|s| {
            let snap = s.snapshot().stats;
            // Identify sampler by behavior: 5 in, 1 out (and ideally name)
            // Or just check if name contains "sampler"
            s.processor_id.to_lowercase().contains("sampler")
                && snap.records_in == 5
                && snap.records_out == 1
        })
        .expect("Sampler stats not found or incorrect (expected 5 in, 1 out)");

    // Attempt to close pipeline, but don't block indefinitely if it hangs (test artifact)
    let _ = timeout(
        Duration::from_millis(500),
        pipeline.close(Duration::from_secs(1)),
    )
    .await;
}
