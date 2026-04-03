//! Table-driven tests for pipeline creation helpers.

use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::MemoryTopicKind;
use flow::model::{batch_from_columns_simple, Message, RecordBatch, Tuple};
use flow::pipeline::{MemorySinkProps, PipelineDefinition, SourceDefinition, SourceInputConfig};
use flow::planner::sink::{CommonSinkProps, SinkOutputConfig};
use flow::Collection;
use flow::FlowInstance;
use flow::SinkEncoderConfig;
use flow::{CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::time::Duration;

use super::common::ColumnCheck;
use super::common::{
    assert_no_json_output, build_expected_json, declare_memory_input_output_topics,
    install_memory_stream_schema, install_memory_stream_schema_with_name, make_memory_topics,
    normalize_json, publish_input_collection, recv_next_json,
};
use super::common::{declare_memory_input_output_topics_with_output_kind, recv_next_collection};

struct TestCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>, // checks for specific columns by name
}

async fn run_test_case(test_case: TestCase) {
    println!("Running test: {}", test_case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) =
        make_memory_topics("pipeline_table_driven_queries", test_case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &test_case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        test_case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_case.name));

    instance
        .start_pipeline(&pipeline_id)
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", test_case.name));

    let columns = test_case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();

    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        timeout_duration,
    )
    .await;
    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(
        test_case.expected_columns,
        test_case.column_checks.len(),
        "expected_columns should match column_checks for JSON comparison (test: {})",
        test_case.name
    );
    let expected: JsonValue =
        build_expected_json(test_case.expected_rows, &test_case.column_checks);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong output JSON for test: {}",
        test_case.name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", test_case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", test_case.name));
}

struct PublishedRowSpec {
    keys_order: Vec<&'static str>,
    values: Vec<Value>,
}

struct SourceLayoutTestCase {
    name: &'static str,
    sql: &'static str,
    stream_schema_hint: Vec<(String, Vec<Value>)>, // (column_name, sample values)
    published_rows: Vec<PublishedRowSpec>,
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>,
}

async fn run_source_layout_test_case(test_case: SourceLayoutTestCase) {
    println!("Running test: {}", test_case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_table_driven_memory_collection_sources_layout_normalize",
        test_case.name,
    );
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &test_case.stream_schema_hint).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        test_case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_case.name));

    instance
        .start_pipeline(&pipeline_id)
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", test_case.name));

    let topic_source: Arc<str> = Arc::<str>::from(input_topic.as_str());
    let rows = test_case
        .published_rows
        .iter()
        .map(|row| {
            assert_eq!(
                row.keys_order.len(),
                row.values.len(),
                "keys_order length must match values length"
            );
            let keys: Vec<Arc<str>> = row
                .keys_order
                .iter()
                .map(|k| Arc::<str>::from(*k))
                .collect();
            let values: Vec<Arc<Value>> = row.values.iter().cloned().map(Arc::new).collect();
            let msg = Arc::new(Message::new_shared_keys(
                Arc::clone(&topic_source),
                Arc::from(keys),
                values,
            ));
            Tuple::new(vec![msg])
        })
        .collect::<Vec<_>>();

    let test_batch = RecordBatch::new(rows)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        timeout_duration,
    )
    .await;

    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(
        test_case.expected_columns,
        test_case.column_checks.len(),
        "expected_columns should match column_checks for JSON comparison (test: {})",
        test_case.name
    );
    let expected: JsonValue =
        build_expected_json(test_case.expected_rows, &test_case.column_checks);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong output JSON for test: {}",
        test_case.name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", test_case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", test_case.name));
}

struct CollectionSinkTestCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_message_count: usize,
    expected_affiliate_none: bool,
    expected_columns: Vec<ColumnCheck>, // order matters
}

async fn run_collection_sink_test_case(test_case: CollectionSinkTestCase) {
    println!("Running test: {}", test_case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) =
        make_memory_topics("pipeline_table_driven_collection_sinks", test_case.name);
    declare_memory_input_output_topics_with_output_kind(
        &instance,
        &input_topic,
        &output_topic,
        MemoryTopicKind::Collection,
    );
    install_memory_stream_schema(&instance, &input_topic, &test_case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_collection(&output_topic)
        .expect("subscribe output collection");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        test_case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()))],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_case.name));

    instance
        .start_pipeline(&pipeline_id)
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", test_case.name));

    let columns = test_case
        .input_data
        .clone()
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();

    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        timeout_duration,
    )
    .await;

    let got = recv_next_collection(&mut output, timeout_duration).await;
    assert_eq!(
        got.num_rows(),
        test_case.expected_rows,
        "row count mismatch (test: {})",
        test_case.name
    );

    for (row_idx, tuple) in got.rows().iter().enumerate() {
        if test_case.expected_affiliate_none {
            assert!(tuple.affiliate().is_none(), "affiliate should be empty");
        }

        assert_eq!(
            tuple.messages().len(),
            test_case.expected_message_count,
            "message count mismatch"
        );

        let msg = &tuple.messages()[0];
        assert_eq!(
            msg.source(),
            "",
            "message source should be empty (downstream memory source normalizes it)"
        );

        let entries: Vec<(&str, datatypes::Value)> =
            msg.entries().map(|(k, v)| (k, v.clone())).collect();
        let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
        let expected_keys: Vec<&str> = test_case
            .expected_columns
            .iter()
            .map(|c| c.expected_name.as_str())
            .collect();
        assert_eq!(keys, expected_keys, "column order mismatch");

        assert_eq!(
            entries.len(),
            test_case.expected_columns.len(),
            "unexpected column count"
        );

        for (col_idx, check) in test_case.expected_columns.iter().enumerate() {
            let got_name = entries[col_idx].0;
            assert_eq!(
                got_name,
                check.expected_name.as_str(),
                "column name mismatch"
            );
            let expected_value = check.expected_values[row_idx].clone();
            assert_eq!(entries[col_idx].1, expected_value, "value mismatch");
        }
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", test_case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", test_case.name));
}

struct RowDiffJsonCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    sink_common: CommonSinkProps,
    encoder: SinkEncoderConfig,
    output: SinkOutputConfig,
    expected: JsonValue,
}

async fn run_row_diff_json_case(case: RowDiffJsonCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) = make_memory_topics("pipeline_row_diff_json", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema_with_name(
        &instance,
        &input_topic,
        case.source_name,
        &case.input_data,
    )
    .await;

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
    .with_encoder(case.encoder)
    .with_output(case.output);
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
        .map(|(col_name, values)| (case.source_name.to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(
        normalize_json(actual),
        normalize_json(case.expected),
        "Wrong row diff JSON for test: {}",
        case.name
    );

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

struct OmitIfEmptyJsonCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    schema_hint: Vec<(String, Vec<Value>)>,
    input_batches: Vec<Vec<(String, Vec<Value>)>>,
    sink_common: CommonSinkProps,
    encoder: SinkEncoderConfig,
    output: SinkOutputConfig,
    expected_outputs: Vec<Option<JsonValue>>,
}

async fn run_omit_if_empty_json_case(case: OmitIfEmptyJsonCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) = make_memory_topics("pipeline_omit_if_empty_json", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema_with_name(
        &instance,
        &input_topic,
        case.source_name,
        &case.schema_hint,
    )
    .await;

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
    .with_encoder(case.encoder)
    .with_output(case.output);
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));

    instance
        .start_pipeline(&pipeline_id)
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    assert_eq!(
        case.input_batches.len(),
        case.expected_outputs.len(),
        "input_batches and expected_outputs length mismatch for test: {}",
        case.name
    );

    for (batch_idx, (input_data, expected_output)) in case
        .input_batches
        .into_iter()
        .zip(case.expected_outputs.into_iter())
        .enumerate()
    {
        let columns = input_data
            .into_iter()
            .map(|(col_name, values)| (case.source_name.to_string(), col_name, values))
            .collect();
        let batch = batch_from_columns_simple(columns).unwrap_or_else(|_| {
            panic!(
                "Failed to create test RecordBatch for: {} batch={}",
                case.name, batch_idx
            )
        });

        publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

        match expected_output {
            Some(expected) => {
                let actual = recv_next_json(&mut output, timeout_duration).await;
                assert_eq!(
                    normalize_json(actual),
                    normalize_json(expected),
                    "Wrong omit_if_empty JSON for test: {} batch={}",
                    case.name,
                    batch_idx
                );
            }
            None => {
                assert_no_json_output(&mut output, Duration::from_millis(300)).await;
            }
        }
    }

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

struct SourceOnChangeJsonCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    schema_hint: Vec<(String, Vec<Value>)>,
    source_input: SourceInputConfig,
    input_rows: Vec<JsonValue>,
    expected_outputs: Vec<Option<JsonValue>>,
}

async fn install_memory_json_stream_schema_with_name(
    instance: &FlowInstance,
    input_topic: &str,
    source_name: &str,
    columns: &[(String, Vec<Value>)],
) {
    let schema_columns = columns
        .iter()
        .map(|(name, values)| {
            let datatype = values
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            ColumnSchema::new(source_name.to_string(), name.clone(), datatype)
        })
        .collect();
    let schema = Schema::new(schema_columns);
    let definition = StreamDefinition::new(
        source_name.to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create json memory stream");
}

async fn run_source_on_change_json_case(case: SourceOnChangeJsonCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) = make_memory_topics("pipeline_source_on_change", case.name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");
    install_memory_json_stream_schema_with_name(
        &instance,
        &input_topic,
        case.source_name,
        &case.schema_hint,
    )
    .await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    );
    let pipeline =
        PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink]).with_sources(vec![
            SourceDefinition::new(case.source_name).with_input(case.source_input),
        ]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));

    instance
        .start_pipeline(&pipeline_id)
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    assert_eq!(
        case.input_rows.len(),
        case.expected_outputs.len(),
        "input_rows and expected_outputs length mismatch for test: {}",
        case.name
    );

    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for memory source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");

    for (row_idx, (input_row, expected_output)) in case
        .input_rows
        .into_iter()
        .zip(case.expected_outputs.into_iter())
        .enumerate()
    {
        let payload = serde_json::to_vec(&input_row).unwrap_or_else(|_| {
            panic!(
                "Failed to encode test input row as JSON for: {} row={}",
                case.name, row_idx
            )
        });
        publisher.publish_bytes(payload).unwrap_or_else(|_| {
            panic!(
                "Failed to publish input bytes for: {} row={}",
                case.name, row_idx
            )
        });

        match expected_output {
            Some(expected) => {
                let actual = recv_next_json(&mut output, timeout_duration).await;
                assert_eq!(
                    normalize_json(actual),
                    normalize_json(expected),
                    "Wrong source on_change JSON for test: {} row={}",
                    case.name,
                    row_idx
                );
            }
            None => {
                assert_no_json_output(&mut output, Duration::from_millis(300)).await;
            }
        }
    }

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

#[tokio::test]
async fn pipeline_table_driven_queries() {
    let test_cases = vec![
        TestCase {
            name: "wildcard",
            sql: "SELECT * FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "c".to_string(),
                    vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)],
                ),
            ],
            expected_rows: 3,
            expected_columns: 3,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                },
                ColumnCheck {
                    expected_name: "c".to_string(),
                    expected_values: vec![
                        Value::Int64(1000),
                        Value::Int64(2000),
                        Value::Int64(3000),
                    ],
                },
            ],
        },
        TestCase {
            name: "simple_projection",
            sql: "SELECT a + 1, b + 2 FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "c".to_string(),
                    vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)],
                ),
            ],
            expected_rows: 3,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a + 1".to_string(),
                    expected_values: vec![Value::Int64(11), Value::Int64(21), Value::Int64(31)],
                },
                ColumnCheck {
                    expected_name: "b + 2".to_string(),
                    expected_values: vec![Value::Int64(102), Value::Int64(202), Value::Int64(302)],
                },
            ],
        },
        TestCase {
            name: "projection_with_alias",
            sql: "SELECT a, a + b + c AS abc FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "c".to_string(),
                    vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)],
                ),
            ],
            expected_rows: 3,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "abc".to_string(),
                    expected_values: vec![
                        Value::Int64(1110),
                        Value::Int64(2220),
                        Value::Int64(3330),
                    ],
                },
            ],
        },
        TestCase {
            name: "by_index_projection_with_alias",
            sql: "SELECT a AS a1 FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            )],
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "a1".to_string(),
                expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            }],
        },
        TestCase {
            name: "simple_filter",
            sql: "SELECT a, b FROM stream WHERE a > 15",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(200), Value::Int64(300)],
                },
            ],
        },
        TestCase {
            name: "filter_with_projection",
            sql: "SELECT a + 5, b * 2 FROM stream WHERE a > 15",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a + 5".to_string(),
                    expected_values: vec![Value::Int64(25), Value::Int64(35)],
                },
                ColumnCheck {
                    expected_name: "b * 2".to_string(),
                    expected_values: vec![Value::Int64(400), Value::Int64(600)],
                },
            ],
        },
        TestCase {
            name: "filter_no_matches",
            sql: "SELECT a, b FROM stream WHERE a > 100",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 0,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![],
                },
            ],
        },
        TestCase {
            name: "filter_all_match",
            sql: "SELECT a FROM stream WHERE a > 5",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            )],
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "a".to_string(),
                expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            }],
        },
        TestCase {
            name: "alias_can_be_used_in_select_and_where",
            sql: "SELECT a + 1 AS b1, b1 + 1 AS c1 FROM stream WHERE b1 > 1 AND c1 > 1",
            input_data: vec![("a".to_string(), vec![Value::Int64(0), Value::Int64(1), Value::Int64(2)])],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "b1".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(3)],
                },
                ColumnCheck {
                    expected_name: "c1".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(4)],
                },
            ],
        },
        TestCase {
            name: "order_by_projection_alias_non_aggregate",
            sql: "SELECT a + 1 AS b FROM stream ORDER BY b DESC",
            input_data: vec![("a".to_string(), vec![Value::Int64(0), Value::Int64(2), Value::Int64(1)])],
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "b".to_string(),
                expected_values: vec![Value::Int64(3), Value::Int64(2), Value::Int64(1)],
            }],
        },
        TestCase {
            name: "order_by_group_key_alias_with_countwindow",
            sql: "SELECT b AS k, sum(a) AS s FROM stream GROUP BY countwindow(4), b ORDER BY k",
            input_data: vec![
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
                        Value::Int64(2),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "k".to_string(),
                    expected_values: vec![Value::Int64(1), Value::Int64(2)],
                },
                ColumnCheck {
                    expected_name: "s".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(2)],
                },
            ],
        },
        TestCase {
            name: "order_by_aggregate_alias_with_countwindow",
            sql: "SELECT b, sum(a) AS s FROM stream GROUP BY countwindow(4), b ORDER BY s DESC",
            input_data: vec![
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
                        Value::Int64(2),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(1)],
                },
                ColumnCheck {
                    expected_name: "s".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(1)],
                },
            ],
        },
        TestCase {
            name: "having_filters_groups_countwindow",
            sql: "SELECT sum(a) AS s, b FROM stream GROUP BY countwindow(4), b HAVING sum(a) > 2 AND sum(a) < 10",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(5),
                        Value::Int64(0),
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
            ],
            expected_rows: 1,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "s".to_string(),
                    expected_values: vec![Value::Int64(5)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(2)],
                },
            ],
        },
        TestCase {
            name: "having_filters_all_groups_emits_empty",
            sql: "SELECT sum(a) AS s FROM stream GROUP BY countwindow(4) HAVING sum(a) > 100",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Int64(4),
                ],
            )],
            expected_rows: 0,
            expected_columns: 0,
            column_checks: vec![],
        },
        TestCase {
            name: "aggregation_group_by_window_and_expr_order_by",
            sql: "SELECT sum(a) + 1, b + 1 FROM stream GROUP BY countwindow(4), b + 1 ORDER BY b + 1 ASC",
            input_data: vec![
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
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "sum(a) + 1".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(3)],
                },
                ColumnCheck {
                    expected_name: "b + 1".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(3)],
                },
            ],
        },
    ];

    for test_case in test_cases {
        run_test_case(test_case).await;
    }
}

#[tokio::test]
async fn pipeline_row_diff_json_table_driven() {
    let cases = vec![
        RowDiffJsonCase {
            name: "emits_sparse_object_for_unchanged_columns",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(10)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {},
                {"a": 2}
            ]),
        },
        RowDiffJsonCase {
            name: "preserves_changed_to_null",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Null, Value::Null],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(10)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": null},
                {}
            ]),
        },
        RowDiffJsonCase {
            name: "respects_tracked_column_subset",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2},
                {"a": 3, "b": 11}
            ]),
        },
        RowDiffJsonCase {
            name: "row_diff_with_batch_keeps_row_diff_and_rewrites_to_streaming_encoder",
            source_name: "stream_ab",
            sql: "SELECT a, b FROM stream_ab",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ],
            sink_common: CommonSinkProps {
                batch_count: Some(16),
                batch_duration: Some(Duration::from_millis(50)),
            },
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2},
                {"a": 3, "b": 11}
            ]),
        },
        RowDiffJsonCase {
            name: "splits_partial_late_materialization_between_row_diff_and_encoder",
            source_name: "stream",
            sql: "SELECT a, b, flag AS c FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
                (
                    "flag".to_string(),
                    vec![Value::Int64(100), Value::Int64(101), Value::Int64(102)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]),
            expected: serde_json::json!([
                {"a": 1, "b": 10, "c": 100},
                {"a": 2, "c": 101},
                {"a": 3, "b": 11, "c": 102}
            ]),
        },
        RowDiffJsonCase {
            name: "splits_partial_late_materialization_between_row_diff_and_encoder_with_aliases",
            source_name: "stream",
            sql: "SELECT a AS x, b AS y, flag AS z FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
                (
                    "flag".to_string(),
                    vec![Value::Int64(100), Value::Int64(101), Value::Int64(102)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["y"]),
            expected: serde_json::json!([
                {"x": 1, "y": 10, "z": 100},
                {"x": 2, "z": 101},
                {"x": 3, "y": 11, "z": 102}
            ]),
        },
        RowDiffJsonCase {
            name: "supports_alias_in_by_index_row_diff_rewrite",
            source_name: "stream",
            sql: "SELECT a AS x FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
            )],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"x": 1},
                {},
                {"x": 2}
            ]),
        },
        RowDiffJsonCase {
            name: "works_with_computed_affiliate_column",
            source_name: "stream",
            sql: "SELECT a, a + 1 AS x FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
            )],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["x"]),
            expected: serde_json::json!([
                {"a": 1, "x": 2},
                {"a": 1},
                {"a": 2, "x": 3}
            ]),
        },
        RowDiffJsonCase {
            name: "supports_mixed_alias_partial_by_index_row_diff_rewrite",
            source_name: "stream_ab",
            sql: "SELECT a AS x, b + 1 AS y FROM stream_ab",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(11), Value::Int64(11)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"x": 1, "y": 11},
                {"y": 12},
                {"x": 2}
            ]),
        },
        RowDiffJsonCase {
            name: "template_transform_still_sees_dense_row_on_delta_branch",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(10)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json_with_transform_template("{{ json(.row) }}"),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": null, "b": null},
                {"a": 2, "b": null}
            ]),
        },
    ];

    for case in cases {
        run_row_diff_json_case(case).await;
    }
}

#[tokio::test]
async fn pipeline_omit_if_empty_json_table_driven() {
    let cases = vec![
        OmitIfEmptyJsonCase {
            name: "full_non_empty_collection_is_forwarded",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            schema_hint: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
            ],
            input_batches: vec![vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
            ]],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::default().with_omit_if_empty(true),
            expected_outputs: vec![Some(serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2, "b": 20},
                {"a": 3, "b": 30}
            ]))],
        },
        OmitIfEmptyJsonCase {
            name: "full_empty_collection_is_suppressed",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            schema_hint: vec![
                ("a".to_string(), vec![Value::Int64(1)]),
                ("b".to_string(), vec![Value::Int64(10)]),
            ],
            input_batches: vec![vec![("a".to_string(), vec![]), ("b".to_string(), vec![])]],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::default().with_omit_if_empty(true),
            expected_outputs: vec![None],
        },
        OmitIfEmptyJsonCase {
            name: "delta_non_empty_collection_is_forwarded",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            schema_hint: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ],
            input_batches: vec![vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ]],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]).with_omit_if_empty(true),
            expected_outputs: vec![Some(serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2},
                {"a": 3, "b": 11}
            ]))],
        },
        OmitIfEmptyJsonCase {
            name: "delta_repeated_same_row_is_suppressed_after_first_emit",
            source_name: "stream",
            sql: "SELECT a FROM stream",
            schema_hint: vec![("a".to_string(), vec![Value::Int64(1)])],
            input_batches: vec![
                vec![("a".to_string(), vec![Value::Int64(1)])],
                vec![("a".to_string(), vec![Value::Int64(1)])],
                vec![("a".to_string(), vec![Value::Int64(1)])],
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta().with_omit_if_empty(true),
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"a": 1}
                ])),
                None,
                None,
            ],
        },
    ];

    for case in cases {
        run_omit_if_empty_json_case(case).await;
    }
}

#[tokio::test]
async fn pipeline_source_on_change_json_table_driven() {
    let cases = vec![
        SourceOnChangeJsonCase {
            name: "explicit_columns_filters_rows_within_single_collection",
            source_name: "stream",
            sql: "SELECT speed, rpm FROM stream",
            schema_hint: vec![
                (
                    "speed".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(20)],
                ),
                (
                    "rpm".to_string(),
                    vec![Value::Int64(1000), Value::Int64(1000), Value::Int64(2000)],
                ),
            ],
            source_input: SourceInputConfig::on_change_with_columns(["speed", "rpm"]),
            input_rows: vec![
                serde_json::json!({"speed": 10, "rpm": 1000}),
                serde_json::json!({"speed": 10, "rpm": 1000}),
                serde_json::json!({"speed": 20, "rpm": 2000}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 10, "rpm": 1000}
                ])),
                None,
                Some(serde_json::json!([
                    {"speed": 20, "rpm": 2000}
                ])),
            ],
        },
        SourceOnChangeJsonCase {
            name: "hidden_tracked_column_controls_emission",
            source_name: "stream",
            sql: "SELECT speed FROM stream",
            schema_hint: vec![
                (
                    "speed".to_string(),
                    vec![Value::Int64(10), Value::Int64(11), Value::Int64(12)],
                ),
                (
                    "rpm".to_string(),
                    vec![Value::Int64(1000), Value::Int64(1000), Value::Int64(2000)],
                ),
            ],
            source_input: SourceInputConfig::on_change_with_columns(["rpm"]),
            input_rows: vec![
                serde_json::json!({"speed": 10, "rpm": 1000}),
                serde_json::json!({"speed": 11, "rpm": 1000}),
                serde_json::json!({"speed": 12, "rpm": 2000}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 10}
                ])),
                None,
                Some(serde_json::json!([
                    {"speed": 12}
                ])),
            ],
        },
        SourceOnChangeJsonCase {
            name: "on_change_without_columns_tracks_all_top_level_columns",
            source_name: "stream",
            sql: "SELECT speed FROM stream",
            schema_hint: vec![
                (
                    "speed".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(10)],
                ),
                (
                    "rpm".to_string(),
                    vec![Value::Int64(1000), Value::Int64(1000), Value::Int64(1000)],
                ),
                (
                    "gear".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(2)],
                ),
            ],
            source_input: SourceInputConfig::on_change(),
            input_rows: vec![
                serde_json::json!({"speed": 10, "rpm": 1000, "gear": 1}),
                serde_json::json!({"speed": 10, "rpm": 1000, "gear": 2}),
                serde_json::json!({"speed": 10, "rpm": 1000, "gear": 2}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 10}
                ])),
                Some(serde_json::json!([
                    {"speed": 10}
                ])),
                None,
            ],
        },
        SourceOnChangeJsonCase {
            name: "state_is_preserved_across_batches",
            source_name: "stream",
            sql: "SELECT speed FROM stream",
            schema_hint: vec![("speed".to_string(), vec![Value::Int64(1), Value::Int64(2)])],
            source_input: SourceInputConfig::on_change_with_columns(["speed"]),
            input_rows: vec![
                serde_json::json!({"speed": 1}),
                serde_json::json!({"speed": 1}),
                serde_json::json!({"speed": 2}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 1}
                ])),
                None,
                Some(serde_json::json!([
                    {"speed": 2}
                ])),
            ],
        },
    ];

    for case in cases {
        run_source_on_change_json_case(case).await;
    }
}

struct MixedConsumersJsonCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    delta_columns: &'static [&'static str],
    expected_regular: JsonValue,
    expected_delta: JsonValue,
}

async fn run_mixed_consumers_json_case(case: MixedConsumersJsonCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, regular_output_topic) =
        make_memory_topics("pipeline_mixed_consumers_json_regular", case.name);
    let (_, delta_output_topic) =
        make_memory_topics("pipeline_mixed_consumers_json_delta", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &regular_output_topic);
    instance
        .declare_memory_topic(
            &delta_output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare delta output memory topic");
    install_memory_stream_schema_with_name(
        &instance,
        &input_topic,
        case.source_name,
        &case.input_data,
    )
    .await;

    let mut regular_output = instance
        .open_memory_subscribe_bytes(&regular_output_topic)
        .expect("subscribe regular output bytes");
    let mut delta_output = instance
        .open_memory_subscribe_bytes(&delta_output_topic)
        .expect("subscribe delta output bytes");

    let pipeline_id = format!("pipe_{}", regular_output_topic);
    let regular_sink = SinkDefinition::new(
        "mem_sink_regular",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(regular_output_topic.clone())),
    )
    .with_encoder(SinkEncoderConfig::json());
    let delta_sink = SinkDefinition::new(
        "mem_sink_delta",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(delta_output_topic.clone())),
    )
    .with_encoder(SinkEncoderConfig::json())
    .with_output(SinkOutputConfig::delta_with_columns(
        case.delta_columns.iter().copied(),
    ));
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        case.sql,
        vec![regular_sink, delta_sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));

    instance
        .start_pipeline(&pipeline_id)
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let columns = case
        .input_data
        .into_iter()
        .map(|(col_name, values)| (case.source_name.to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let actual_regular = recv_next_json(&mut regular_output, timeout_duration).await;
    assert_eq!(
        normalize_json(actual_regular),
        normalize_json(case.expected_regular),
        "Wrong regular JSON for test: {}",
        case.name
    );

    let actual_delta = recv_next_json(&mut delta_output, timeout_duration).await;
    assert_eq!(
        normalize_json(actual_delta),
        normalize_json(case.expected_delta),
        "Wrong delta JSON for test: {}",
        case.name
    );

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

#[tokio::test]
async fn pipeline_mixed_consumers_json_table_driven() {
    let cases = vec![
        MixedConsumersJsonCase {
            name: "shared_project_with_encoder_branch_and_row_diff_branch",
            source_name: "stream_ab",
            sql: "SELECT a, b FROM stream_ab",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ],
            delta_columns: &["b"],
            expected_regular: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2, "b": 10},
                {"a": 3, "b": 11}
            ]),
            expected_delta: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2},
                {"a": 3, "b": 11}
            ]),
        },
        MixedConsumersJsonCase {
            name: "shared_project_with_encoder_branch_and_row_diff_branch_preserves_aliases",
            source_name: "stream_ab",
            sql: "SELECT a AS x, b + 1 AS y FROM stream_ab",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(11), Value::Int64(11)],
                ),
            ],
            delta_columns: &["y"],
            expected_regular: serde_json::json!([
                {"x": 1, "y": 11},
                {"x": 1, "y": 12},
                {"x": 2, "y": 12}
            ]),
            expected_delta: serde_json::json!([
                {"x": 1, "y": 11},
                {"x": 1, "y": 12},
                {"x": 2}
            ]),
        },
    ];

    for case in cases {
        run_mixed_consumers_json_case(case).await;
    }
}

#[tokio::test]
async fn pipeline_table_driven_memory_collection_sources_layout_normalize() {
    let test_cases = vec![SourceLayoutTestCase {
        name: "layout_normalize_reorders_and_fills_missing_with_null",
        sql: "SELECT a, b FROM stream",
        stream_schema_hint: vec![
            ("a".to_string(), vec![Value::Int64(1)]),
            ("b".to_string(), vec![Value::Int64(2)]),
        ],
        published_rows: vec![
            // Wrong message source + swapped order: [b, a].
            PublishedRowSpec {
                keys_order: vec!["b", "a"],
                values: vec![Value::Int64(2), Value::Int64(1)],
            },
            // Only `a` exists -> `b` should be filled as NULL.
            PublishedRowSpec {
                keys_order: vec!["a"],
                values: vec![Value::Int64(100)],
            },
        ],
        expected_rows: 2,
        expected_columns: 2,
        column_checks: vec![
            ColumnCheck {
                expected_name: "a".to_string(),
                expected_values: vec![Value::Int64(1), Value::Int64(100)],
            },
            ColumnCheck {
                expected_name: "b".to_string(),
                expected_values: vec![Value::Int64(2), Value::Null],
            },
        ],
    }];

    for test_case in test_cases {
        run_source_layout_test_case(test_case).await;
    }
}

#[tokio::test]
async fn pipeline_table_driven_collection_sinks() {
    let test_cases = vec![CollectionSinkTestCase {
        name: "select_star_with_alias_materializes_single_message",
        sql: "SELECT *, a AS x FROM stream",
        input_data: vec![
            (
                "a".to_string(),
                vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            ),
            (
                "b".to_string(),
                vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
            ),
        ],
        expected_rows: 3,
        expected_message_count: 1,
        expected_affiliate_none: true,
        expected_columns: vec![
            ColumnCheck {
                expected_name: "a".to_string(),
                expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            },
            ColumnCheck {
                expected_name: "b".to_string(),
                expected_values: vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
            },
            ColumnCheck {
                expected_name: "x".to_string(),
                expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            },
        ],
    }];

    for test_case in test_cases {
        run_collection_sink_test_case(test_case).await;
    }
}
