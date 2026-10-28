//! Source-qualified, per-event FROM evaluation with NULL placeholders.
use super::common::recv_next_json;
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{
    MemoryStreamProps, MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
};
use flow::connector::{MemoryData, MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{MemorySinkProps, PipelineDefinition, PipelineOptions};
use flow::processor::SamplerConfig;
use flow::{
    CreatePipelineRequest, ExplainPipelineTarget, FlowInstance, PipelineStopMode, SinkDefinition,
    SinkProps, SinkType,
};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::time::Duration;

fn integers(names: &[&str]) -> Vec<(String, ConcreteDatatype)> {
    names
        .iter()
        .map(|name| ((*name).to_string(), ConcreteDatatype::Int64(Int64Type)))
        .collect()
}

async fn input(
    instance: &FlowInstance,
    name: &str,
    columns: Vec<(String, ConcreteDatatype)>,
    shared: bool,
    sampler: Option<SamplerConfig>,
) {
    let schema = Arc::new(Schema::new(
        columns
            .into_iter()
            .map(|(column, dtype)| ColumnSchema::new(name.to_string(), column, dtype))
            .collect(),
    ));
    let props = if shared {
        StreamProps::Mock(MockStreamProps::default())
    } else {
        instance
            .declare_memory_topic(name, MemoryTopicKind::Bytes, DEFAULT_MEMORY_PUBSUB_CAPACITY)
            .unwrap();
        StreamProps::Memory(MemoryStreamProps::new(name))
    };
    let mut definition = StreamDefinition::new(name, schema, props, StreamDecoderConfig::json());
    if let Some(sampler) = sampler {
        definition = definition.with_sampler(sampler);
    }
    instance.create_stream(definition, shared).await.unwrap();
}

fn pipeline(id: &str, sql: &str, output: &str, slice: bool) -> PipelineDefinition {
    PipelineDefinition::new(
        id,
        sql,
        vec![SinkDefinition::new(
            "output",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output)),
        )
        .with_encoder(
            flow::planner::sink::SinkEncoderConfig::json().with_json_omit_null_columns(false),
        )],
    )
    .with_options(PipelineOptions {
        shared_slice_projection_enabled: Some(slice),
        ..PipelineOptions::default()
    })
}

fn output(instance: &FlowInstance) -> tokio::sync::broadcast::Receiver<MemoryData> {
    instance
        .declare_memory_topic(
            "out",
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .unwrap();
    instance.open_memory_subscribe_bytes("out").unwrap()
}

async fn send(instance: &FlowInstance, name: &str, row: Value, shared: bool) {
    let bytes = serde_json::to_vec(&row).unwrap();
    if shared {
        instance
            .send_shared_mock_stream_payload(name, bytes)
            .await
            .unwrap();
    } else {
        instance
            .wait_for_memory_subscribers(name, MemoryTopicKind::Bytes, 1, Duration::from_secs(5))
            .await
            .unwrap();
        instance
            .open_memory_publisher_bytes(name)
            .unwrap()
            .publish_bytes(bytes)
            .unwrap();
    }
}

async fn wait_shared(
    instance: &FlowInstance,
    subscribers: usize,
    columns: usize,
) -> flow::shared_stream::SharedStreamInfo {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let info = instance
                .get_stream("faults")
                .await
                .unwrap()
                .shared_info
                .unwrap();
            if info.subscriber_count == subscribers && info.decoding_columns.len() == columns {
                break info;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn multi_source_aligns_fields_and_shared_slots() {
    for (shared, slice) in [(false, false), (true, false), (true, true)] {
        let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime(
            "default", None,
        ))
        .unwrap();
        input(
            &instance,
            "faults",
            integers(&["unused", "device_id", "fault_code"]),
            shared,
            None,
        )
        .await;
        input(
            &instance,
            "messages",
            integers(&["msg_type", "unused", "device_id", "payload"]),
            shared,
            None,
        )
        .await;
        let mut output = output(&instance);
        let sql = "SELECT faults.device_id AS fault_device, messages.device_id AS message_device, fault_code, msg_type, payload FROM faults, messages";
        let definition = pipeline("combined", sql, "out", slice);
        let explain = instance
            .explain_pipeline(ExplainPipelineTarget::Definition(&definition))
            .unwrap();
        println!("SQL: {sql}\n{}", explain.to_pretty_string());
        if !shared {
            assert!(!explain.physical.to_json().to_string().contains("unused"));
        }
        instance
            .create_pipeline(CreatePipelineRequest::new(definition))
            .unwrap();
        instance.start_pipeline("combined").await.unwrap();
        for (source, row, expected) in [
            (
                "faults",
                json!({"device_id":1,"fault_code":7,"unused":999}),
                json!([{ "fault_device":1,"message_device":null,"fault_code":7,"msg_type":null,"payload":null }]),
            ),
            (
                "messages",
                json!({"device_id":2,"msg_type":2,"payload":42,"unused":999}),
                json!([{ "fault_device":null,"message_device":2,"fault_code":null,"msg_type":2,"payload":42 }]),
            ),
        ] {
            send(&instance, source, row, shared).await;
            assert_eq!(
                recv_next_json(&mut output, Duration::from_secs(5)).await,
                expected
            );
        }
        if shared {
            let info = wait_shared(&instance, 1, 2).await;
            assert_eq!(
                info.decoding_columns
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>(),
                ["device_id".to_string(), "fault_code".to_string()]
                    .into_iter()
                    .collect()
            );
            instance
                .create_pipeline(CreatePipelineRequest::new(pipeline(
                    "direct",
                    "SELECT unused FROM faults",
                    "out",
                    slice,
                )))
                .unwrap();
            instance.start_pipeline("direct").await.unwrap();
            wait_shared(&instance, 2, 3).await;
            instance
                .stop_pipeline("direct", PipelineStopMode::Quick, Duration::from_secs(5))
                .await
                .unwrap();
            wait_shared(&instance, 1, 2).await;
            send(
                &instance,
                "faults",
                json!({"device_id":3,"fault_code":8,"unused":999}),
                shared,
            )
            .await;
            assert_eq!(
                recv_next_json(&mut output, Duration::from_secs(5)).await,
                json!([{ "fault_device":3,"message_device":null,"fault_code":8,"msg_type":null,"payload":null }])
            );
        }
        instance
            .stop_pipeline("combined", PipelineStopMode::Quick, Duration::from_secs(5))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn multi_source_distinguishes_types_and_missing_sources() {
    for (sql, rows, expected) in [
        ("SELECT faults.ts AS fault_ts, messages.ts AS message_ts, payload FROM faults, messages", vec![("faults", json!({"ts":7})), ("messages", json!({"ts":"later","payload":42}))], vec![json!([{ "fault_ts":7,"message_ts":null,"payload":null }]), json!([{ "fault_ts":null,"message_ts":"later","payload":42 }])]),
        ("SELECT payload FROM faults, messages", vec![("faults", json!({"ts":7})), ("messages", json!({"ts":"later","payload":42}))], vec![json!([{ "payload":null }]), json!([{ "payload":42 }])]),
        ("SELECT faults.ts AS fault_ts, messages.ts AS message_ts, payload FROM faults, messages", vec![("messages", json!([{"ts":"first","payload":1},{"ts":"second","payload":2}]))], vec![json!([{ "fault_ts":null,"message_ts":"first","payload":1 }, { "fault_ts":null,"message_ts":"second","payload":2 }])]),
    ] {
        let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime("default", None)).unwrap();
        input(&instance, "faults", integers(&["ts"]), false, None).await;
        input(&instance, "messages", vec![("ts".to_string(), ConcreteDatatype::String(datatypes::StringType)), ("payload".to_string(), ConcreteDatatype::Int64(Int64Type))], false, None).await;
        let mut output = output(&instance);
        instance.create_pipeline(CreatePipelineRequest::new(pipeline("typed", sql, "out", false))).unwrap();
        instance.start_pipeline("typed").await.unwrap();
        for ((name, row), expected) in rows.into_iter().zip(expected) {
            send(&instance, name, row, false).await;
            assert_eq!(recv_next_json(&mut output, Duration::from_secs(5)).await, expected);
        }
        instance.stop_pipeline("typed", PipelineStopMode::Quick, Duration::from_secs(5)).await.unwrap();
    }
}

#[tokio::test]
async fn multi_source_fault_triggers_message_window() {
    for (sql, expected) in [
        ("SELECT messages.device_id AS captured_device, payload FROM faults, messages WHERE msg_type = 2 GROUP BY slidingwindow('ms', 0, 500) OVER (WHEN fault_code = 7 PARTITION BY coalesce(faults.device_id, messages.device_id))", json!([{ "captured_device":1,"payload":42 }])),
        ("SELECT sum(payload) AS captured FROM faults, messages WHERE msg_type = 2 GROUP BY slidingwindow('ms', 0, 500) OVER (WHEN fault_code = 7 PARTITION BY coalesce(faults.device_id, messages.device_id))", json!([{ "captured":42 }])),
    ] {
    let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .unwrap();
    input(
        &instance,
        "faults",
        integers(&["device_id", "fault_code"]),
        false,
        None,
    )
    .await;
    input(
        &instance,
        "messages",
        integers(&["device_id", "msg_type", "payload"]),
        false,
        None,
    )
    .await;
    let mut output = output(&instance);

    let definition = pipeline("capture", sql, "out", false);
    let explain = instance.explain_pipeline(ExplainPipelineTarget::Definition(&definition)).unwrap();
    assert_eq!(explain.physical.to_json().to_string().contains("PhysicalStreamingAggregation"), sql.contains("sum(payload)"));
    instance.create_pipeline(CreatePipelineRequest::new(definition)).unwrap();
    instance.start_pipeline("capture").await.unwrap();
    send(
        &instance,
        "faults",
        json!({"device_id":1,"fault_code":7}),
        false,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    send(
        &instance,
        "messages",
        json!({"device_id":2,"msg_type":2,"payload":99}),
        false,
    )
    .await;
    send(
        &instance,
        "messages",
        json!({"device_id":1,"msg_type":2,"payload":42}),
        false,
    )
    .await;
    assert_eq!(
        recv_next_json(&mut output, Duration::from_secs(5)).await,
        expected
    );
    instance
        .stop_pipeline("capture", PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .unwrap();
}
}

#[tokio::test]
async fn multi_source_preserves_nested_field_indexes() {
    for shared in [false, true] {
        let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime(
            "default", None,
        ))
        .unwrap();
        input(&instance, "other", integers(&["id"]), shared, None).await;
        let payload = ConcreteDatatype::Struct(datatypes::StructType::new(Arc::new(vec![
            datatypes::StructField::new(
                "first".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                true,
            ),
            datatypes::StructField::new(
                "second".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                true,
            ),
        ])));
        input(
            &instance,
            "nested",
            vec![
                ("payload".to_string(), payload),
                (
                    "items".to_string(),
                    ConcreteDatatype::List(datatypes::ListType::new(Arc::new(
                        ConcreteDatatype::Int64(Int64Type),
                    ))),
                ),
            ],
            shared,
            None,
        )
        .await;
        let mut output = output(&instance);
        let invalid = pipeline(
            "invalid_nested",
            "SELECT payload->missing FROM other, nested",
            "out",
            false,
        );
        assert!(instance
            .explain_pipeline(ExplainPipelineTarget::Definition(&invalid))
            .is_err());
        instance
            .create_pipeline(CreatePipelineRequest::new(pipeline(
                "nested_fields",
                "SELECT payload->second AS selected, items[1] AS selected_item FROM other, nested",
                "out",
                false,
            )))
            .unwrap();
        instance.start_pipeline("nested_fields").await.unwrap();
        send(&instance, "other", json!({"id":1}), shared).await;
        assert_eq!(
            recv_next_json(&mut output, Duration::from_secs(5)).await,
            json!([{ "selected":null, "selected_item":null }])
        );
        send(
            &instance,
            "nested",
            json!({"payload":{"first":11,"second":22},"items":[33,44]}),
            shared,
        )
        .await;
        assert_eq!(
            recv_next_json(&mut output, Duration::from_secs(5)).await,
            json!([{ "selected":22, "selected_item":44 }])
        );
        instance
            .stop_pipeline(
                "nested_fields",
                PipelineStopMode::Quick,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn multi_source_latest_sampler_does_not_sample_other_input() {
    let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .unwrap();
    input(
        &instance,
        "faults",
        integers(&["value"]),
        false,
        Some(SamplerConfig::new(Duration::from_millis(500))),
    )
    .await;
    input(&instance, "messages", integers(&["value"]), false, None).await;
    let mut output = output(&instance);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline(
            "sampled",
            "SELECT faults.value AS a, messages.value AS b FROM faults, messages",
            "out",
            false,
        )))
        .unwrap();
    instance.start_pipeline("sampled").await.unwrap();
    tokio::task::yield_now().await;
    for value in [1, 2, 3] {
        send(&instance, "faults", json!({"value":value}), false).await;
    }
    for value in [10, 20] {
        send(&instance, "messages", json!({"value":value}), false).await;
    }
    let mut actual = Vec::new();
    for _ in 0..3 {
        actual.push(
            recv_next_json(&mut output, Duration::from_secs(5))
                .await
                .to_string(),
        );
    }
    let mut expected = vec![
        json!([{ "a":null,"b":10 }]).to_string(),
        json!([{ "a":null,"b":20 }]).to_string(),
        json!([{ "a":3,"b":null }]).to_string(),
    ];
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    instance
        .stop_pipeline("sampled", PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test]
async fn multi_source_aggregation_consumes_both_inputs() {
    let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .unwrap();
    input(&instance, "faults", integers(&["value"]), false, None).await;
    input(&instance, "messages", integers(&["value"]), false, None).await;
    let mut output = output(&instance);
    let sql = "SELECT sum(coalesce(faults.value, messages.value)) AS total FROM faults, messages GROUP BY countwindow(2)";
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline(
            "summed", sql, "out", false,
        )))
        .unwrap();
    instance.start_pipeline("summed").await.unwrap();
    send(&instance, "faults", json!({"value":1}), false).await;
    send(&instance, "messages", json!({"value":10}), false).await;
    assert_eq!(
        recv_next_json(&mut output, Duration::from_secs(5)).await,
        json!([{ "total":11 }])
    );
    instance
        .stop_pipeline("summed", PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test]
async fn multi_source_wildcard_and_row_only_three_inputs() {
    for shared in [false, true] {
        for (sql, wildcard) in [
            ("SELECT * FROM a, b, c", true),
            ("SELECT 1 AS one FROM a, b, c", false),
        ] {
            let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime(
                "default", None,
            ))
            .unwrap();
            for (name, columns) in [
                ("a", vec!["a2", "a1"]),
                ("b", vec!["b1"]),
                ("c", vec!["c1"]),
            ] {
                input(&instance, name, integers(&columns), shared, None).await;
            }
            let mut output = output(&instance);
            instance
                .create_pipeline(CreatePipelineRequest::new(pipeline(
                    "wildcard", sql, "out", false,
                )))
                .unwrap();
            instance.start_pipeline("wildcard").await.unwrap();
            for (name, value, expected) in [
                (
                    "a",
                    json!([{"a1":1,"a2":2},{"a1":3,"a2":4}]),
                    json!([{"a2":2,"a1":1,"b1":null,"c1":null},{"a2":4,"a1":3,"b1":null,"c1":null}]),
                ),
                (
                    "b",
                    json!({"b1":5}),
                    json!([{"a2":null,"a1":null,"b1":5,"c1":null}]),
                ),
                (
                    "c",
                    json!({"c1":6}),
                    json!([{"a2":null,"a1":null,"b1":null,"c1":6}]),
                ),
            ] {
                let row_count = if name == "a" { 2 } else { 1 };
                send(&instance, name, value, shared).await;
                let expected = if wildcard {
                    expected
                } else {
                    Value::Array(vec![json!({"one":1}); row_count])
                };
                assert_eq!(
                    recv_next_json(&mut output, Duration::from_secs(5)).await,
                    expected,
                    "{sql}, shared={shared}"
                );
            }
            send(&instance, "a", json!([]), shared).await;
            assert_eq!(
                recv_next_json(&mut output, Duration::from_secs(5)).await,
                json!([])
            );
            instance
                .stop_pipeline("wildcard", PipelineStopMode::Quick, Duration::from_secs(5))
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn multi_source_file_checkpoint_restores_both_cursors() {
    use flow::checkpoint::{CheckpointStore, InMemoryCheckpointStore};
    use std::io::Write;
    let directory = tempfile::tempdir().unwrap();
    let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .unwrap();
    let store = Arc::new(InMemoryCheckpointStore::new());
    instance.set_checkpoint_store(store.clone());
    for (name, value) in [("left_file", "left-before"), ("right_file", "right-before")] {
        let path = directory.path().join(name);
        std::fs::write(&path, value).unwrap();
        let schema = Arc::new(Schema::new(
            ["line", "filename"]
                .into_iter()
                .map(|field| {
                    ColumnSchema::new(
                        name.to_string(),
                        field.to_string(),
                        ConcreteDatatype::String(datatypes::StringType),
                    )
                })
                .collect(),
        ));
        instance
            .create_stream(
                StreamDefinition::new(
                    name,
                    schema,
                    StreamProps::File(flow::catalog::FileStreamProps::new(path.to_string_lossy())),
                    StreamDecoderConfig::new("file_line", Default::default()),
                ),
                false,
            )
            .await
            .unwrap();
    }
    let mut output = output(&instance);
    let mut definition = pipeline(
        "checkpointed",
        "SELECT left_file.line AS l, right_file.line AS r FROM left_file, right_file",
        "out",
        false,
    );
    definition = definition.with_options(PipelineOptions {
        checkpoint_enabled: true,
        ..Default::default()
    });
    instance
        .create_pipeline(CreatePipelineRequest::new(definition))
        .unwrap();
    instance.start_pipeline("checkpointed").await.unwrap();
    let mut before = vec![
        recv_next_json(&mut output, Duration::from_secs(5))
            .await
            .to_string(),
        recv_next_json(&mut output, Duration::from_secs(5))
            .await
            .to_string(),
    ];
    before.sort();
    let mut expected = vec![
        json!([{"l":"left-before","r":null}]).to_string(),
        json!([{"l":null,"r":"right-before"}]).to_string(),
    ];
    expected.sort();
    assert_eq!(before, expected);
    instance
        .stop_pipeline(
            "checkpointed",
            PipelineStopMode::Graceful,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let manifest = store
        .load_latest(instance.id(), "checkpointed")
        .unwrap()
        .unwrap();
    assert_eq!(
        manifest
            .operator_snapshots
            .iter()
            .filter(|snapshot| snapshot.operator_kind == "datasource")
            .count(),
        2
    );
    for (name, value) in [("left_file", "left-after"), ("right_file", "right-after")] {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(directory.path().join(name))
            .unwrap();
        file.write_all(value.as_bytes()).unwrap();
        file.sync_all().unwrap();
    }
    instance.start_pipeline("checkpointed").await.unwrap();
    let mut after = vec![
        recv_next_json(&mut output, Duration::from_secs(5))
            .await
            .to_string(),
        recv_next_json(&mut output, Duration::from_secs(5))
            .await
            .to_string(),
    ];
    after.sort();
    let mut expected = vec![
        json!([{"l":"left-after","r":null}]).to_string(),
        json!([{"l":null,"r":"right-after"}]).to_string(),
    ];
    expected.sort();
    assert_eq!(after, expected);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), output.recv())
            .await
            .is_err()
    );
    instance
        .stop_pipeline(
            "checkpointed",
            PipelineStopMode::Graceful,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let next = store
        .load_latest(instance.id(), "checkpointed")
        .unwrap()
        .unwrap();
    assert!(next.checkpoint_id > manifest.checkpoint_id);
}

#[tokio::test]
async fn multi_source_checkpoint_failure_does_not_commit_unfenced_sources() {
    let instance = FlowInstance::new(flow::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .unwrap();
    let store = Arc::new(flow::checkpoint::InMemoryCheckpointStore::new());
    instance.set_checkpoint_store(store.clone());
    input(&instance, "a", integers(&["a1"]), false, None).await;
    input(&instance, "b", integers(&["b1"]), false, None).await;
    let mut output = output(&instance);
    let definition =
        pipeline("unfenced", "SELECT * FROM a, b", "out", false).with_options(PipelineOptions {
            checkpoint_enabled: true,
            ..Default::default()
        });
    instance
        .create_pipeline(CreatePipelineRequest::new(definition))
        .unwrap();
    instance.start_pipeline("unfenced").await.unwrap();
    send(&instance, "a", json!({"a1":1}), false).await;
    assert_eq!(
        recv_next_json(&mut output, Duration::from_secs(5)).await,
        json!([{"a1":1,"b1":null}])
    );
    assert!(instance
        .stop_pipeline(
            "unfenced",
            PipelineStopMode::Graceful,
            Duration::from_secs(2)
        )
        .await
        .is_err());
    assert!(store.is_empty());
}
