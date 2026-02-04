//! Table-driven tests for pipeline creation helpers.

use datatypes::Value;
use flow::connector::MemoryTopicKind;
use flow::model::{batch_from_columns_simple, Message, RecordBatch, Tuple};
use flow::pipeline::MemorySinkProps;
use flow::pipeline::PipelineDefinition;
use flow::planner::plan_cache::PlanCacheInputs;
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
    build_expected_json, declare_memory_input_output_topics, install_memory_stream_schema,
    make_memory_topics, normalize_json, publish_input_collection, recv_next_json,
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

    let instance = FlowInstance::default();
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
        .create_pipeline(CreatePipelineRequest::new(pipeline).with_plan_cache_inputs(
            PlanCacheInputs {
                pipeline_raw_json: String::new(),
                streams_raw_json: Vec::new(),
                snapshot: None,
            },
        ))
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

    let instance = FlowInstance::default();
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
        .create_pipeline(CreatePipelineRequest::new(pipeline).with_plan_cache_inputs(
            PlanCacheInputs {
                pipeline_raw_json: String::new(),
                streams_raw_json: Vec::new(),
                snapshot: None,
            },
        ))
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

    let instance = FlowInstance::default();
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
        .create_pipeline(CreatePipelineRequest::new(pipeline).with_plan_cache_inputs(
            PlanCacheInputs {
                pipeline_raw_json: String::new(),
                streams_raw_json: Vec::new(),
                snapshot: None,
            },
        ))
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
