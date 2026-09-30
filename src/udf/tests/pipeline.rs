use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, Value};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{
    MemoryData, MemoryTopicKind, SharedCollection, DEFAULT_MEMORY_PUBSUB_CAPACITY,
};
use flow::model::batch_from_columns_simple;
use flow::pipeline::{MemorySinkProps, PipelineDefinition};
use flow::{
    CreatePipelineRequest, FlowInstance, PipelineStopMode, SinkDefinition, SinkProps, SinkType,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use udf::WasmEngine;

static TOPIC_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Build a real batch-based WASM UDF fixture.
fn batch_wasm_module() -> Vec<u8> {
    wat::parse_str(
        r#"
        (module
            (import "host" "alloc" (func $alloc (param i32) (result i32)))

            (memory (export "memory") 1 256)
            (data (i32.const 0) "\00\00\00\00")

            (data (i32.const 128) "{\"abi_version\":1,\"name\":\"test_udf\",\"description\":\"identity\",\"args\":[{\"name\":\"x\",\"type\":\"any\"}],\"return_type\":\"any\",\"execution_mode\":\"batch\"}")

            (func (export "udf_metadata_ptr") (result i32)
                i32.const 128)
            (func (export "udf_metadata_len") (result i32)
                i32.const 140)

            (data (i32.const 32768) "{\"values\":[1,2,3]}")

            (func (export "udf_batch") (param $input_ptr i32) (param $input_len i32) (result i32)
                (local $buf i32)
                i32.const 18
                i32.const 4
                i32.add
                call $alloc
                local.set $buf

                local.get $buf
                i32.const 18
                i32.store

                local.get $buf
                i32.const 4
                i32.add
                i32.const 32768
                i32.const 18
                memory.copy

                local.get $buf
                i32.const 4
                i32.add)
        )
        "#,
    )
    .expect("parse identity WASM module")
    .into()
}

fn make_topics() -> (String, String) {
    let id = TOPIC_COUNTER.fetch_add(1, Ordering::Relaxed);
    (
        format!("tests.wasm_udf_pipeline.{id}.input"),
        format!("tests.wasm_udf_pipeline.{id}.output"),
    )
}

async fn install_stream_schema(instance: &FlowInstance, input_topic: &str) {
    let schema = Schema::new(vec![ColumnSchema::new(
        "stream".to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]);
    let stream = StreamDefinition::new(
        "stream".to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        StreamDecoderConfig::new("none".to_string(), JsonMap::new()),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create input stream");
}

async fn receive_json(output: &mut tokio::sync::broadcast::Receiver<MemoryData>) -> JsonValue {
    let payload = timeout(Duration::from_secs(10), async {
        loop {
            match output.recv().await.expect("receive output") {
                MemoryData::Bytes(bytes) => break bytes,
                MemoryData::Collection(_) => panic!("expected JSON bytes output"),
            }
        }
    })
    .await
    .expect("timeout waiting for pipeline output");
    serde_json::from_slice(&payload).expect("parse pipeline JSON output")
}

#[tokio::test]
async fn wasm_udf_pipeline_uses_real_batch_wasm_execution() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");

    let engine = WasmEngine::new().expect("create WASM engine");
    let wasm_udf = engine
        .instantiate("test_udf", &batch_wasm_module())
        .expect("instantiate WASM UDF");
    let registry = instance.custom_func_registry();
    instance.set_custom_func_registry(
        registry
            .clone_and_add(Arc::new(wasm_udf))
            .expect("register WASM UDF"),
    );

    let (input_topic, output_topic) = make_topics();
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output topic");
    install_stream_schema(&instance, &input_topic).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output topic");
    let pipeline_id = format!(
        "wasm_udf_pipeline_{}",
        TOPIC_COUNTER.load(Ordering::Relaxed)
    );
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT test_udf(a) + test_udf(a) AS result FROM stream",
        vec![SinkDefinition::new(
            "memory_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    let input = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )])
    .expect("build input batch");
    let publisher = instance
        .open_memory_publisher_collection(&input_topic)
        .expect("open input publisher");
    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Collection,
            1,
            Duration::from_secs(10),
        )
        .await
        .expect("wait for input subscriber");
    publisher
        .publish_collection(SharedCollection::from_box(Box::new(input)))
        .expect("publish input");

    let actual = receive_json(&mut output).await;
    assert_eq!(
        actual,
        serde_json::json!([
            { "result": 2 },
            { "result": 4 },
            { "result": 6 }
        ])
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            Duration::from_secs(10),
        )
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}
