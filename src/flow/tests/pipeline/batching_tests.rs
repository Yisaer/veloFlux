//! Table-driven tests for batching behavior (encoder flush) in pipelines.

use datatypes::Value;
use flow::model::batch_from_columns_simple;
use flow::pipeline::MemorySinkProps;
use flow::pipeline::PipelineDefinition;
use flow::planner::plan_cache::PlanCacheInputs;
use flow::planner::sink::CommonSinkProps;
use flow::FlowInstance;
use flow::{PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use serde_json::Value as JsonValue;
use tokio::time::Duration;

use super::common::{
    declare_memory_input_output_topics, install_memory_stream_schema, make_memory_topics,
    memory_registry, publish_input_collection, recv_next_json,
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

    let instance = FlowInstance::new();
    let registry = memory_registry(&instance);
    let (input_topic, output_topic) = make_memory_topics("pipeline_batching", case.name);
    declare_memory_input_output_topics(&registry, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &case.input_data).await;

    let mut output = registry
        .open_subscribe_bytes(&output_topic)
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
        .create_pipeline_with_plan_cache(
            pipeline,
            PlanCacheInputs {
                pipeline_raw_json: String::new(),
                streams_raw_json: Vec::new(),
                snapshot: None,
            },
        )
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
    publish_input_collection(&registry, &input_topic, Box::new(batch), timeout_duration).await;

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
