//! Table-driven tests for batching behavior (encoder flush) in pipelines.

use datatypes::Value;
use flow::connector::MemoryData;
use flow::model::batch_from_columns_simple;
use flow::pipeline::MemorySinkProps;
use flow::pipeline::PipelineDefinition;
use flow::planner::sink::CommonSinkProps;
use flow::FlowInstance;
use flow::{
    CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkEncoderConfig, SinkProps, SinkType,
};
use serde_json::Value as JsonValue;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::timeout;
use tokio::time::Duration;

use super::common::{
    declare_memory_input_output_topics, install_memory_stream_schema, make_memory_topics,
    publish_input_collection, recv_next_json,
};

struct BatchCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    sink_common: CommonSinkProps,
    expected_batches: Vec<JsonValue>,
}

async fn run_batch_case(case: BatchCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) = make_memory_topics("pipeline_batching", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    )
    .with_common_props(case.sink_common);
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));
    instance
        .start_pipeline(&pipeline_id)
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let columns = case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    for expected in case.expected_batches {
        let actual = recv_next_json(&mut output, timeout_duration).await;
        assert_eq!(
            actual, expected,
            "Wrong output JSON batch for test: {}",
            case.name
        );
    }

    // Stop after reading expected batches to avoid relying on stop/close to flush the final batch.
    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

struct BatchBytesCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    sink_common: CommonSinkProps,
    encoder: SinkEncoderConfig,
    expected_batches: Vec<&'static [u8]>,
}

async fn recv_next_bytes(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) -> Vec<u8> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output");

        match item {
            Ok(MemoryData::Bytes(payload)) => return payload.as_ref().to_vec(),
            Ok(MemoryData::Collection(_)) => panic!("unexpected collection payload on bytes topic"),
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => panic!("pipeline output topic closed"),
        }
    }
}

async fn run_batch_bytes_case(case: BatchBytesCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) = make_memory_topics("pipeline_batching_transform", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    )
    .with_common_props(case.sink_common)
    .with_encoder(case.encoder);
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));
    instance
        .start_pipeline(&pipeline_id)
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let columns = case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    for expected in case.expected_batches {
        let actual = recv_next_bytes(&mut output, timeout_duration).await;
        assert_eq!(
            actual.as_slice(),
            expected,
            "Wrong output bytes batch for test: {}",
            case.name
        );
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

#[tokio::test]
async fn pipeline_batching_table_driven() {
    let cases = vec![BatchCase {
        name: "flushes_by_count_then_by_duration",
        sql: "SELECT a FROM stream",
        input_data: vec![(
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )],
        sink_common: CommonSinkProps {
            batch_count: Some(2),
            // Ensure the final partial batch is flushed without needing stop/close.
            batch_duration: Some(Duration::from_millis(50)),
        },
        expected_batches: vec![
            serde_json::json!([{"a": 1}, {"a": 2}]),
            serde_json::json!([{"a": 3}]),
        ],
    }];

    for case in cases {
        run_batch_case(case).await;
    }
}

#[tokio::test]
async fn pipeline_encoder_transform_table_driven() {
    let cases = vec![BatchBytesCase {
        name: "streaming_encoder_flushes_transformed_json_bytes",
        sql: "SELECT a + 1 AS c, b + 10 AS d FROM stream",
        input_data: vec![
            (
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(3), Value::Int64(5)],
            ),
            (
                "b".to_string(),
                vec![Value::Int64(2), Value::Int64(4), Value::Int64(6)],
            ),
        ],
        sink_common: CommonSinkProps {
            batch_count: Some(2),
            // Ensure the final partial batch is flushed without relying on stop/close.
            batch_duration: Some(Duration::from_millis(50)),
        },
        encoder: SinkEncoderConfig::json_with_transform_template(
            r#"{"x":{{ json(.row.c) }},"y":{{ json(.row.d) }} }"#,
        ),
        expected_batches: vec![
            br#"[{"x":2,"y":12 },{"x":4,"y":14 }]"#,
            br#"[{"x":6,"y":16 }]"#,
        ],
    }];

    for case in cases {
        run_batch_bytes_case(case).await;
    }
}
