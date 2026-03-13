//! Table-driven tests for built-in stateful functions.

use datatypes::Value;
use flow::model::batch_from_columns_simple;
use flow::pipeline::MemorySinkProps;
use flow::pipeline::PipelineDefinition;
use flow::FlowInstance;
use flow::{CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use serde_json::Value as JsonValue;
use tokio::time::Duration;

use super::common::ColumnCheck;
use super::common::{
    build_expected_json, declare_memory_input_output_topics, install_memory_stream_schema,
    make_memory_topics, normalize_json, publish_input_collection, recv_next_json,
};

struct ExpectedCollection {
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>,
}

struct StatefulCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    expected_outputs: Vec<ExpectedCollection>,
    wait_after_send: Duration,
    close_before_read: bool,
}

async fn run_stateful_case(case: StatefulCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ));
    let (input_topic, output_topic) =
        make_memory_topics("stateful_function_table_driven", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
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
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        Duration::from_secs(5),
    )
    .await;

    tokio::time::sleep(case.wait_after_send).await;

    if case.close_before_read {
        instance
            .stop_pipeline(
                &pipeline_id,
                PipelineStopMode::Quick,
                Duration::from_secs(5),
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case.name));
    }

    let timeout_duration = Duration::from_secs(5);
    for expected in case.expected_outputs {
        let actual: JsonValue = recv_next_json(&mut output, timeout_duration).await;
        assert_eq!(
            expected.expected_columns,
            expected.column_checks.len(),
            "expected_columns should match column_checks for JSON comparison (test: {})",
            case.name
        );
        let expected_json: JsonValue =
            build_expected_json(expected.expected_rows, &expected.column_checks);
        assert_eq!(
            normalize_json(actual),
            normalize_json(expected_json),
            "Wrong output JSON for test: {}",
            case.name
        );
    }

    if !case.close_before_read {
        instance
            .stop_pipeline(
                &pipeline_id,
                PipelineStopMode::Quick,
                Duration::from_secs(5),
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case.name));
    }

    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

#[tokio::test]
async fn stateful_function_table_driven() {
    let cases = vec![
        StatefulCase {
            name: "aggregation_sum_countwindow",
            sql: "SELECT sum(a) FROM stream GROUP BY countwindow(2)",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(10),
                    Value::Int64(20),
                    Value::Int64(30),
                    Value::Int64(40),
                    Value::Int64(50),
                ],
            )],
            expected_outputs: vec![
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "sum(a)".to_string(),
                        expected_values: vec![Value::Int64(30)],
                    }],
                },
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "sum(a)".to_string(),
                        expected_values: vec![Value::Int64(70)],
                    }],
                },
            ],
            wait_after_send: Duration::from_millis(200),
            close_before_read: false,
        },
        StatefulCase {
            name: "last_row_countwindow",
            sql: "SELECT last_row(a) FROM stream GROUP BY countwindow(2)",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(10),
                    Value::Int64(20),
                    Value::Int64(30),
                    Value::Int64(40),
                ],
            )],
            expected_outputs: vec![
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "last_row(a)".to_string(),
                        expected_values: vec![Value::Int64(20)],
                    }],
                },
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "last_row(a)".to_string(),
                        expected_values: vec![Value::Int64(40)],
                    }],
                },
            ],
            wait_after_send: Duration::from_millis(200),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_basic",
            sql: "SELECT lag(a) AS prev FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 3,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "prev".to_string(),
                    expected_values: vec![Value::Null, Value::Int64(1), Value::Int64(2)],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_dedup_two_columns",
            sql: "SELECT lag(a) AS p1, lag(a) AS p2 FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 3,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "p1".to_string(),
                        expected_values: vec![Value::Null, Value::Int64(1), Value::Int64(2)],
                    },
                    ColumnCheck {
                        expected_name: "p2".to_string(),
                        expected_values: vec![Value::Null, Value::Int64(1), Value::Int64(2)],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_filter_false_returns_visible_lag_value",
            sql: "SELECT lag(a) FILTER (WHERE flag = 1) AS prev FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(0),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "prev".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int64(30),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_in_where_filter",
            sql: "SELECT a FROM stream WHERE lag(a) > 1",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Int64(4),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 2,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(4)],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_filter_partition_by_uses_visible_lag_per_partition",
            sql: "SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) AS prev FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Int64(50),
                        Value::Int64(60),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                    ],
                ),
                (
                    "k1".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 6,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "prev".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Null,
                        Value::Int64(40),
                        Value::Int64(40),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_filter_depends_on_prior_stateful_bool_output",
            sql: "SELECT lag(flag) AS prev_flag, lag(a) FILTER (WHERE lag(flag)) AS prev_a FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "prev_flag".to_string(),
                        expected_values: vec![
                            Value::Null,
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Bool(true),
                        ],
                    },
                    ColumnCheck {
                        expected_name: "prev_a".to_string(),
                        expected_values: vec![
                            Value::Null,
                            Value::Null,
                            Value::Int64(20),
                            Value::Int64(20),
                        ],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "project_expression_uses_visible_lag_value",
            sql: "SELECT a + lag(b) FILTER (WHERE flag = 1) AS v FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Int64(4),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(0),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "v".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(12),
                        Value::Int64(13),
                        Value::Int64(34),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_offset_ignore_null",
            sql: "SELECT lag(a, 2, true) AS prev FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(10), Value::Null, Value::Int64(30), Value::Int64(40)],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "prev".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int64(10),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "latest_tracks_last_non_null_with_filter",
            sql: "SELECT latest(a) FILTER (WHERE flag = 1) AS latest_a FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Null,
                        Value::Int64(10),
                        Value::Null,
                        Value::Int64(20),
                        Value::Int64(30),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(0),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "latest_a".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(20),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "latest_partition_by_keeps_state_per_partition",
            sql: "SELECT latest(a) OVER (PARTITION BY k1) AS latest_a FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Null,
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Null,
                    ],
                ),
                (
                    "k1".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 6,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "latest_a".to_string(),
                    expected_values: vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(10),
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Int64(40),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "changed_col_emits_only_on_changes",
            sql: "SELECT changed_col(true, a) FILTER (WHERE flag = 1) AS delta FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Null,
                        Value::Int64(2),
                        Value::Int64(3),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 6,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "delta".to_string(),
                    expected_values: vec![
                        Value::Int64(1),
                        Value::Null,
                        Value::Int64(2),
                        Value::Null,
                        Value::Null,
                        Value::Int64(3),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "changed_col_ignore_null_false_treats_null_as_change",
            sql: "SELECT changed_col(false, a) AS delta FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(1),
                    Value::Int64(1),
                    Value::Null,
                    Value::Null,
                    Value::Int64(2),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "delta".to_string(),
                    expected_values: vec![
                        Value::Int64(1),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int64(2),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "changed_col_filter_false_always_returns_null",
            sql: "SELECT changed_col(true, a) FILTER (WHERE flag = 1) AS delta FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Int64(4),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(0),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "delta".to_string(),
                    expected_values: vec![
                        Value::Int64(1),
                        Value::Null,
                        Value::Null,
                        Value::Int64(4),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "had_changed_detects_any_input_change",
            sql: "SELECT had_changed(true, a, b) FILTER (WHERE flag = 1) AS changed FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::String("x".to_string()),
                        Value::String("x".to_string()),
                        Value::String("y".to_string()),
                        Value::String("y".to_string()),
                        Value::Null,
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "changed".to_string(),
                    expected_values: vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(true),
                        Value::Bool(false),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "had_changed_ignore_null_false_null_affects_state",
            sql: "SELECT had_changed(false, a, b) AS changed FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Null,
                        Value::Null,
                        Value::Int64(1),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::String("x".to_string()),
                        Value::String("x".to_string()),
                        Value::String("x".to_string()),
                        Value::Null,
                        Value::Null,
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "changed".to_string(),
                    expected_values: vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(true),
                        Value::Bool(true),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "had_changed_filter_false_returns_false_and_does_not_advance",
            sql: "SELECT had_changed(true, a, b) FILTER (WHERE flag = 1) AS changed FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                        Value::Int64(3),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::String("x".to_string()),
                        Value::String("y".to_string()),
                        Value::String("y".to_string()),
                        Value::String("y".to_string()),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "changed".to_string(),
                    expected_values: vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(true),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "nested_inside_aggregate_countwindow",
            sql: "SELECT sum(a), last_row(lag(a)) FROM stream GROUP BY countwindow(4)",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(10),
                    Value::Int64(20),
                    Value::Int64(30),
                    Value::Int64(40),
                    Value::Int64(50),
                    Value::Int64(60),
                    Value::Int64(70),
                    Value::Int64(80),
                ],
            )],
            expected_outputs: vec![
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 2,
                    column_checks: vec![
                        ColumnCheck {
                            expected_name: "sum(a)".to_string(),
                            expected_values: vec![Value::Int64(100)],
                        },
                        ColumnCheck {
                            expected_name: "last_row(lag(a))".to_string(),
                            expected_values: vec![Value::Int64(30)],
                        },
                    ],
                },
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 2,
                    column_checks: vec![
                        ColumnCheck {
                            expected_name: "sum(a)".to_string(),
                            expected_values: vec![Value::Int64(260)],
                        },
                        ColumnCheck {
                            expected_name: "last_row(lag(a))".to_string(),
                            expected_values: vec![Value::Int64(70)],
                        },
                    ],
                },
            ],
            wait_after_send: Duration::from_millis(200),
            close_before_read: true,
        },
    ];

    for case in cases {
        run_stateful_case(case).await;
    }
}
