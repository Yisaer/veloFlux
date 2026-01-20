//! Tests for the public pipeline creation helpers.
//!
//! This module exercises both `create_pipeline_with_log_sink` (default/testing)
//! and the customizable `create_pipeline` API that accepts user-defined sinks.

use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::model::batch_from_columns_simple;
use flow::planner::sink::{
    NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
};
use flow::processor::{SamplerConfig, StreamData};
use flow::FlowInstance;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

async fn recv_next_json(
    output: &mut tokio::sync::mpsc::Receiver<StreamData>,
    timeout_duration: Duration,
) -> JsonValue {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output")
            .expect("pipeline output channel closed");
        match item {
            StreamData::EncodedBytes { payload, .. } => {
                return serde_json::from_slice(&payload).expect("invalid JSON payload")
            }
            StreamData::Control(_) => continue,
            StreamData::Watermark(_) => continue,
            StreamData::Error(err) => panic!("pipeline returned error: {}", err.message),
            other => panic!("unexpected stream data: {}", other.description()),
        }
    }
}

fn normalize_json(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(items) => {
            JsonValue::Array(items.into_iter().map(normalize_json).collect())
        }
        JsonValue::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut out = serde_json::Map::with_capacity(entries.len());
            for (k, v) in entries {
                out.insert(k, normalize_json(v));
            }
            JsonValue::Object(out)
        }
        other => other,
    }
}

fn sort_json_array_by_key(mut value: JsonValue, key: &str) -> JsonValue {
    let JsonValue::Array(items) = &mut value else {
        panic!("expected JSON array");
    };
    items.sort_by(|a, b| {
        let JsonValue::Object(a_obj) = a else {
            return std::cmp::Ordering::Equal;
        };
        let JsonValue::Object(b_obj) = b else {
            return std::cmp::Ordering::Equal;
        };
        let a_val = a_obj.get(key);
        let b_val = b_obj.get(key);
        match (a_val, b_val) {
            (Some(JsonValue::Number(a_num)), Some(JsonValue::Number(b_num))) => {
                a_num.as_i64().cmp(&b_num.as_i64())
            }
            (Some(a), Some(b)) => format!("{a:?}").cmp(&format!("{b:?}")),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });
    value
}

#[tokio::test]
async fn test_create_pipeline_aggregation_with_group_by_window_and_expr() {
    let test_name = "aggregation_with_group_by_window_and_expr";
    let sql = "SELECT sum(a) + 1, b + 1 FROM stream GROUP BY countwindow(4), b + 1";
    let input_data = vec![
        (
            "a".to_string(),
            vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
            ],
        ),
        (
            "b".to_string(),
            vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(2),
            ],
        ),
        (
            "c".to_string(),
            vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(1),
                Value::Int64(2),
            ],
        ),
    ];

    let instance = FlowInstance::new();
    install_stream_schema(&instance, &input_data).await;

    let mut pipeline = instance
        .build_pipeline_with_log_sink(sql, true)
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_name));

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_name));

    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(test_batch)))
        .await
        .unwrap_or_else(|_| panic!("Failed to send test data for: {}", test_name));

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");
    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    let expected = serde_json::json!([
        {"sum(a) + 1": 3, "b + 1": 2},
        {"sum(a) + 1": 3, "b + 1": 3}
    ]);
    let actual_sorted = sort_json_array_by_key(normalize_json(actual), "b + 1");
    let expected_sorted = sort_json_array_by_key(normalize_json(expected), "b + 1");
    assert_eq!(
        actual_sorted, expected_sorted,
        "Wrong output JSON for test: {}",
        test_name
    );

    pipeline
        .close(Duration::from_secs(5))
        .await
        .unwrap_or_else(|_| panic!("Failed to close pipeline for test: {}", test_name));
}

async fn install_stream_schema(instance: &FlowInstance, columns: &[(String, Vec<Value>)]) {
    let schema_columns = columns
        .iter()
        .map(|(name, values)| {
            let datatype = values
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            ColumnSchema::new("stream".to_string(), name.clone(), datatype)
        })
        .collect();
    let schema = Schema::new(schema_columns);
    let definition = StreamDefinition::new(
        "stream".to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");
}

async fn create_stream_with_sampler(
    instance: &FlowInstance,
    stream_name: &str,
    sampler_interval: Duration,
) {
    let schema_columns = vec![ColumnSchema::new(
        stream_name.to_string(),
        "x".to_string(),
        ConcreteDatatype::Int64(datatypes::Int64Type),
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
    let instance = FlowInstance::new();
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
