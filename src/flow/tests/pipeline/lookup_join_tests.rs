//! Pipeline integration tests for stream-to-table lookup joins.

use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MemoryMockTableProps, StreamDecoderConfig, TableProps};
use flow::connector::MemoryData;
use flow::model::batch_from_columns_simple;
use flow::pipeline::{MemorySinkProps, PipelineDefinition};
use flow::{CreatePipelineRequest, FlowInstance, PipelineError, PipelineStopMode, SinkDefinition};
use flow::{SinkProps, SinkType, TableDefinition as FlowTableDefinition};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{timeout, Duration};

use super::common::{
    declare_memory_input_output_topics, install_memory_stream_schema, make_memory_topics,
    normalize_json, publish_input_collection,
};

struct LookupTableCase {
    name: &'static str,
    columns: Vec<(&'static str, ConcreteDatatype)>,
    rows: Vec<JsonValue>,
}

struct LookupJoinCase {
    name: &'static str,
    sql: &'static str,
    stream_columns: Vec<(String, Vec<Value>)>,
    tables: Vec<LookupTableCase>,
    expected: JsonValue,
}

fn create_lookup_table(
    instance: &FlowInstance,
    table: LookupTableCase,
) -> impl std::future::Future<Output = ()> + '_ {
    async move {
        let schema = Arc::new(Schema::new(
            table
                .columns
                .iter()
                .map(|(name, datatype)| {
                    ColumnSchema::new(
                        table.name.to_string(),
                        (*name).to_string(),
                        datatype.clone(),
                    )
                })
                .collect(),
        ));
        let definition = FlowTableDefinition::new(
            table.name.to_string(),
            schema,
            TableProps::MemoryMock(MemoryMockTableProps { rows: table.rows }),
            StreamDecoderConfig::json(),
        );
        instance
            .create_table(definition)
            .await
            .expect("create lookup table");
    }
}

async fn collect_json_rows(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    expected_rows: usize,
    timeout_duration: Duration,
) -> JsonValue {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let mut rows = Vec::new();

    while rows.len() < expected_rows {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for lookup join output");
        match item {
            Ok(MemoryData::Bytes(payload)) => {
                let value: JsonValue =
                    serde_json::from_slice(payload.as_ref()).expect("invalid JSON output");
                match value {
                    JsonValue::Array(items) => rows.extend(items),
                    other => rows.push(other),
                }
            }
            Ok(MemoryData::Collection(_)) => {
                panic!("unexpected collection payload on bytes output topic")
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => panic!("pipeline output topic closed"),
        }
    }

    JsonValue::Array(rows)
}

async fn assert_no_extra_json_output(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) {
    loop {
        match timeout(timeout_duration, output.recv()).await {
            Err(_) => return,
            Ok(Ok(MemoryData::Bytes(payload))) => panic!(
                "unexpected extra lookup join output: {}",
                String::from_utf8_lossy(payload.as_ref())
            ),
            Ok(Ok(MemoryData::Collection(_))) => {
                panic!("unexpected collection payload on bytes output topic")
            }
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => return,
        }
    }
}

async fn run_lookup_join_case(case: LookupJoinCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_lookup_join", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &case.stream_columns).await;
    for table in case.tables {
        create_lookup_table(&instance, table).await;
    }

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{output_topic}");
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        case.sql,
        vec![SinkDefinition::new(
            "lookup_join_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create lookup join pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start lookup join pipeline");

    let input_columns = case
        .stream_columns
        .iter()
        .map(|(column, values)| ("stream".to_string(), column.clone(), values.clone()))
        .collect();
    let input = Box::new(
        batch_from_columns_simple(input_columns).expect("build lookup join input collection"),
    );
    let expected_rows = case.expected.as_array().expect("expected JSON array").len();
    publish_input_collection(&instance, &input_topic, input, Duration::from_secs(5)).await;

    let actual = collect_json_rows(&mut output, expected_rows, Duration::from_secs(5)).await;
    assert_eq!(
        normalize_json(actual),
        normalize_json(case.expected),
        "wrong lookup join output for test: {}",
        case.name
    );
    assert_no_extra_json_output(&mut output, Duration::from_millis(200)).await;

    match instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
    {
        Ok(()) => {}
        Err(PipelineError::Runtime(error)) if error.contains("Timeout waiting for data") => {}
        Err(error) => panic!("stop lookup join pipeline: {error}"),
    }
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete lookup join pipeline");
}

fn json_row(entries: &[(&str, JsonValue)]) -> JsonValue {
    JsonValue::Object(
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect(),
    )
}

fn large_lookup_case() -> LookupJoinCase {
    let rows = (0..2049)
        .map(|index| {
            json_row(&[
                ("id", 1.into()),
                ("a_value", JsonValue::String(format!("value-{index}"))),
            ])
        })
        .collect();
    let expected = (0..2049)
        .map(|index| {
            json_row(&[
                ("stream.id", 1.into()),
                (
                    "table_a.a_value",
                    JsonValue::String(format!("value-{index}")),
                ),
            ])
        })
        .collect();
    LookupJoinCase {
        name: "lookup_output_is_split_at_1024_rows",
        sql: "SELECT stream.id, table_a.a_value FROM stream INNER JOIN table_a ON stream.id = table_a.id",
        stream_columns: vec![("id".to_string(), vec![Value::Int64(1)])],
        tables: vec![LookupTableCase {
            name: "table_a",
            columns: vec![
                ("id", ConcreteDatatype::Int64(datatypes::Int64Type)),
                (
                    "a_value",
                    ConcreteDatatype::String(datatypes::StringType),
                ),
            ],
            rows,
        }],
        expected: JsonValue::Array(expected),
    }
}

#[tokio::test]
async fn pipeline_lookup_join_table_driven() {
    let cases = vec![
        LookupJoinCase {
            name: "single_key_with_unmatched_stream_row",
            sql: "SELECT stream.id, table_a.a_value FROM stream INNER JOIN table_a ON stream.id = table_a.id",
            stream_columns: vec![(
                "id".to_string(),
                vec![Value::Int64(1), Value::Int64(2)],
            )],
            tables: vec![LookupTableCase {
                name: "table_a",
                columns: vec![
                    ("id", ConcreteDatatype::Int64(datatypes::Int64Type)),
                    (
                        "a_value",
                        ConcreteDatatype::String(datatypes::StringType),
                    ),
                ],
                rows: vec![json_row(&[("id", 1.into()), ("a_value", "one".into())])],
            }],
            expected: serde_json::json!([{"stream.id": 1, "table_a.a_value": "one"}]),
        },
        LookupJoinCase {
            name: "multiple_equality_keys",
            sql: "SELECT stream.k1, stream.k2, table_a.a_value FROM stream INNER JOIN table_a ON stream.k1 = table_a.k1 AND stream.k2 = table_a.k2",
            stream_columns: vec![
                ("k1".to_string(), vec![Value::Int64(1), Value::Int64(1)]),
                ("k2".to_string(), vec![Value::Int64(1), Value::Int64(2)]),
            ],
            tables: vec![LookupTableCase {
                name: "table_a",
                columns: vec![
                    ("k1", ConcreteDatatype::Int64(datatypes::Int64Type)),
                    ("k2", ConcreteDatatype::Int64(datatypes::Int64Type)),
                    (
                        "a_value",
                        ConcreteDatatype::String(datatypes::StringType),
                    ),
                ],
                rows: vec![
                    json_row(&[
                        ("k1", 1.into()),
                        ("k2", 1.into()),
                        ("a_value", "matched".into()),
                    ]),
                    json_row(&[
                        ("k1", 1.into()),
                        ("k2", 3.into()),
                        ("a_value", "not_matched".into()),
                    ]),
                ],
            }],
            expected: serde_json::json!([{
                "stream.k1": 1,
                "stream.k2": 1,
                "table_a.a_value": "matched"
            }]),
        },
        LookupJoinCase {
            name: "one_to_many_table_matches",
            sql: "SELECT stream.id, table_a.a_value FROM stream INNER JOIN table_a ON stream.id = table_a.id",
            stream_columns: vec![("id".to_string(), vec![Value::Int64(1)])],
            tables: vec![LookupTableCase {
                name: "table_a",
                columns: vec![
                    ("id", ConcreteDatatype::Int64(datatypes::Int64Type)),
                    (
                        "a_value",
                        ConcreteDatatype::String(datatypes::StringType),
                    ),
                ],
                rows: vec![
                    json_row(&[("id", 1.into()), ("a_value", "first".into())]),
                    json_row(&[("id", 1.into()), ("a_value", "second".into())]),
                ],
            }],
            expected: serde_json::json!([
                {"stream.id": 1, "table_a.a_value": "first"},
                {"stream.id": 1, "table_a.a_value": "second"}
            ]),
        },
        LookupJoinCase {
            name: "multiple_lookup_joins",
            sql: "SELECT stream.id, stream.code, table_a.a_value, table_b.b_value FROM stream INNER JOIN table_a ON stream.id = table_a.id INNER JOIN table_b ON stream.code = table_b.code",
            stream_columns: vec![
                ("id".to_string(), vec![Value::Int64(1)]),
                ("code".to_string(), vec![Value::Int64(10)]),
            ],
            tables: vec![
                LookupTableCase {
                    name: "table_a",
                    columns: vec![
                        ("id", ConcreteDatatype::Int64(datatypes::Int64Type)),
                        (
                            "a_value",
                            ConcreteDatatype::String(datatypes::StringType),
                        ),
                    ],
                    rows: vec![json_row(&[("id", 1.into()), ("a_value", "a".into())])],
                },
                LookupTableCase {
                    name: "table_b",
                    columns: vec![
                        ("code", ConcreteDatatype::Int64(datatypes::Int64Type)),
                        (
                            "b_value",
                            ConcreteDatatype::String(datatypes::StringType),
                        ),
                    ],
                    rows: vec![json_row(&[("code", 10.into()), ("b_value", "b".into())])],
                },
            ],
            expected: serde_json::json!([{
                "stream.id": 1,
                "stream.code": 10,
                "table_a.a_value": "a",
                "table_b.b_value": "b"
            }]),
        },
        large_lookup_case(),
    ];

    for case in cases {
        run_lookup_join_case(case).await;
    }
}
