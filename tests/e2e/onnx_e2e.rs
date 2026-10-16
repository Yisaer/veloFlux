use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use flow::connector::MemoryData;
use reqwest::StatusCode;
use std::io::{Cursor, Write};
use std::time::Duration;
use tokio::time::timeout;
use zip::ZipWriter;

const MNIST_MODEL: &[u8] = include_bytes!("../../src/manager/src/model_testdata/mnist-8.onnx");
const EXPECTED_PROBABILITIES: [f32; 10] = [
    -1.0895445346832275,
    0.023687202483415604,
    0.05233169347047806,
    0.27945414185523987,
    0.19593794643878937,
    0.518629252910614,
    -0.24499107897281647,
    0.20446670055389404,
    -0.021566495299339294,
    0.06969335675239563,
];

fn model_bundle() -> Vec<u8> {
    let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default();
    archive.start_file("mnist_8/model.onnx", options).unwrap();
    archive.write_all(MNIST_MODEL).unwrap();
    archive.start_file("mnist_8/model.yaml", options).unwrap();
    archive.write_all(b"name: mnist_8\n").unwrap();
    for index in 1..=3 {
        archive
            .start_file(format!("mnist_8/test_vectors/input-{index}.json"), options)
            .unwrap();
        archive.write_all(b"{}").unwrap();
        archive
            .start_file(
                format!("mnist_8/test_vectors/expected_output-{index}.json"),
                options,
            )
            .unwrap();
        archive.write_all(b"{}").unwrap();
    }
    archive.finish().unwrap().into_inner()
}

fn protobuf_varint(mut value: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
    encoded
}

fn protobuf_field(tag: u32, value: &[u8]) -> Vec<u8> {
    let mut encoded = protobuf_varint((tag << 3 | 2) as u64);
    encoded.extend(protobuf_varint(value.len() as u64));
    encoded.extend(value);
    encoded
}

fn protobuf_varint_field(tag: u32, value: u64) -> Vec<u8> {
    let mut encoded = protobuf_varint((tag << 3) as u64);
    encoded.extend(protobuf_varint(value));
    encoded
}

fn identity_model(first_dimension: Vec<u8>, graph_name: &str) -> Vec<u8> {
    fn string_field(tag: u32, value: &str) -> Vec<u8> {
        protobuf_field(tag, value.as_bytes())
    }

    let fixed_dimension = protobuf_varint_field(1, 3);
    let mut shape = protobuf_field(1, &first_dimension);
    shape.extend(protobuf_field(1, &fixed_dimension));

    let mut tensor_type = protobuf_varint_field(1, 1);
    tensor_type.extend(protobuf_field(2, &shape));
    let type_proto = protobuf_field(1, &tensor_type);
    let mut input_info = string_field(1, "input");
    input_info.extend(protobuf_field(2, &type_proto));
    let mut output_info = string_field(1, "output");
    output_info.extend(protobuf_field(2, &type_proto));

    let mut node = string_field(1, "input");
    node.extend(string_field(2, "output"));
    node.extend(string_field(3, graph_name));
    node.extend(string_field(4, "Identity"));

    let mut graph = protobuf_field(1, &node);
    graph.extend(string_field(2, graph_name));
    graph.extend(protobuf_field(11, &input_info));
    graph.extend(protobuf_field(12, &output_info));

    let opset = protobuf_varint_field(2, 13);
    let mut model = protobuf_varint_field(1, 8);
    model.extend(protobuf_field(7, &graph));
    model.extend(protobuf_field(8, &opset));
    model
}

fn static_2x3_identity_model() -> Vec<u8> {
    identity_model(protobuf_varint_field(1, 2), "static_2x3_identity")
}

fn dynamic_2x3_identity_model() -> Vec<u8> {
    identity_model(protobuf_field(2, b"batch"), "dynamic_2x3_identity")
}

fn mnist_input() -> Vec<f32> {
    let mut input = vec![0.0; 28 * 28];
    input[14 * 28 + 14] = 1.0;
    input
}

async fn wait_for_model_prepared(
    http: &reqwest::Client,
    manager_base: &str,
    model_name: &str,
    expected: bool,
) -> serde_json::Value {
    for _ in 0..50 {
        let response = http
            .get(format!("{manager_base}/models/{model_name}/status"))
            .send()
            .await
            .expect("get model runtime status");
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.expect("decode model status");
        if body["prepared"] == expected {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("model '{model_name}' did not reach prepared={expected}");
}

fn identity_model_bundle(model_name: &str, model: &[u8]) -> Vec<u8> {
    let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default();
    archive
        .start_file(format!("{model_name}/model.onnx"), options)
        .unwrap();
    archive.write_all(model).unwrap();
    archive
        .start_file(format!("{model_name}/model.yaml"), options)
        .unwrap();
    archive
        .write_all(format!("name: {model_name}\n").as_bytes())
        .unwrap();
    for index in 1..=3 {
        archive
            .start_file(
                format!("{model_name}/test_vectors/input-{index}.json"),
                options,
            )
            .unwrap();
        archive.write_all(b"{}").unwrap();
        archive
            .start_file(
                format!("{model_name}/test_vectors/expected_output-{index}.json"),
                options,
            )
            .unwrap();
        archive.write_all(b"{}").unwrap();
    }
    archive.finish().unwrap().into_inner()
}

fn registry_rebuild_udf_module() -> Vec<u8> {
    wat::parse_str(
        r#"
        (module
            (import "host" "alloc" (func $alloc (param i32) (result i32)))
            (memory (export "memory") 1 256)
            (data (i32.const 128) "{\"abi_version\":1,\"name\":\"onnx_fvt_udf\",\"description\":\"registry rebuild fixture\",\"args\":[],\"return_type\":\"any\",\"execution_mode\":\"batch\"}")

            (func (export "udf_metadata_ptr") (result i32)
                i32.const 128)
            (func (export "udf_metadata_len") (result i32)
                i32.const 135)
            (func (export "udf_batch") (param i32 i32) (result i32)
                i32.const 0)
        )
        "#,
    )
    .expect("parse registry rebuild UDF module")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn onnx_model_stream_pipeline_fvt() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let injector = instance.clone();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");
    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{addr}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let upload = reqwest::multipart::Part::bytes(model_bundle())
        .file_name("mnist_8.zip")
        .mime_str("application/zip")
        .unwrap();
    let model_response = http
        .post(format!("{manager_base}/models"))
        .multipart(reqwest::multipart::Form::new().part("file", upload))
        .send()
        .await
        .expect("upload model");
    assert_eq!(model_response.status(), StatusCode::OK);
    let model: serde_json::Value = model_response.json().await.unwrap();
    assert_eq!(model["name"], "mnist_8");
    assert_eq!(model["test_vector_count"], 3);
    let model_status = http
        .get(format!("{manager_base}/models/mnist_8/status"))
        .send()
        .await
        .expect("get initial model status");
    assert_eq!(model_status.status(), StatusCode::OK);
    let model_status: serde_json::Value = model_status.json().await.unwrap();
    assert_eq!(model_status["prepared"], false);
    assert_eq!(model_status["prepare_count"], 0);

    let udf_upload = reqwest::multipart::Form::new()
        .text("name", "onnx_fvt_udf")
        .text("revision", "1")
        .part(
            "wasm_file",
            reqwest::multipart::Part::bytes(registry_rebuild_udf_module())
                .file_name("onnx_fvt_udf.wasm")
                .mime_str("application/wasm")
                .unwrap(),
        );
    let udf_response = http
        .post(format!("{manager_base}/udfs/upload"))
        .multipart(udf_upload)
        .send()
        .await
        .expect("upload registry rebuild UDF");
    assert_eq!(udf_response.status(), StatusCode::OK);

    let export_response = http
        .get(format!(
            "{manager_base}/storage/export?bundle_version=onnx-registry-rebuild"
        ))
        .send()
        .await
        .expect("export storage for registry rebuild");
    assert_eq!(export_response.status(), StatusCode::OK);
    let export_archive = export_response
        .bytes()
        .await
        .expect("read registry rebuild export");
    let import_response = http
        .post(format!("{manager_base}/storage/import"))
        .multipart(
            reqwest::multipart::Form::new().part(
                "file",
                reqwest::multipart::Part::bytes(export_archive.to_vec())
                    .file_name("onnx-registry-rebuild.zip")
                    .mime_str("application/zip")
                    .unwrap(),
            ),
        )
        .send()
        .await
        .expect("import storage for registry rebuild");
    assert_eq!(import_response.status(), StatusCode::OK);

    let stream_name = format!("onnx_fvt_stream_{}", random_suffix());
    let output_topic = format!("onnx_fvt_output_{}", random_suffix());
    let pipeline_id = format!("onnx_fvt_pipeline_{}", random_suffix());

    let stream_response = http
        .post(format!("{manager_base}/streams"))
        .json(&serde_json::json!({
            "name": stream_name,
            "revision": 1,
            "type": "mock",
            "schema": {
                "type": "json",
                "props": {
                    "columns": [{"name": "image", "data_type": "list", "element": {"name": "element", "data_type": "float32"}}]
                }
            },
            "props": {},
            "shared": true,
            "decoder": {"type": "json", "props": {}}
        }))
        .send()
        .await
        .expect("create stream");
    assert_eq!(stream_response.status(), StatusCode::CREATED);

    let topic_response = http
        .post(format!("{manager_base}/memory/topics"))
        .json(&serde_json::json!({
            "topic": output_topic,
            "revision": 1,
            "kind": "bytes"
        }))
        .send()
        .await
        .expect("create output topic");
    assert_eq!(topic_response.status(), StatusCode::CREATED);

    let mut output = injector
        .open_memory_subscribe_bytes(&output_topic)
        .expect("open output subscription");
    let pipeline_response = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipeline_id,
            "revision": 1,
            "sql": format!("SELECT onnx('mnist_8', image) AS result FROM {stream_name}"),
            "sinks": [{
                "id": "onnx_fvt_output",
                "type": "memory",
                "props": {"topic": output_topic},
                "encoder": {"type": "json", "props": {}}
            }]
        }))
        .send()
        .await
        .expect("create pipeline");
    assert_eq!(pipeline_response.status(), StatusCode::CREATED);

    let start_response = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline");
    assert_eq!(start_response.status(), StatusCode::OK);
    let model_status = wait_for_model_prepared(&http, &manager_base, "mnist_8", true).await;
    assert_eq!(model_status["prepare_count"], 1);

    injector
        .send_shared_mock_stream_payload(
            &stream_name,
            serde_json::to_vec(&serde_json::json!({"image": mnist_input()})).unwrap(),
        )
        .await
        .expect("send model input");

    let payload = timeout(Duration::from_secs(10), async {
        loop {
            match output.recv().await.expect("receive output") {
                MemoryData::Bytes(payload) => break payload,
                MemoryData::Collection(_) => continue,
            }
        }
    })
    .await
    .expect("timeout waiting for ONNX output");
    let actual: serde_json::Value = serde_json::from_slice(&payload).expect("decode output JSON");
    let probabilities = actual[0]["result"]
        .as_object()
        .and_then(|fields| fields.values().next())
        .and_then(serde_json::Value::as_array)
        .expect("result should contain one array output");
    assert_eq!(probabilities.len(), EXPECTED_PROBABILITIES.len());
    for (actual, expected) in probabilities.iter().zip(EXPECTED_PROBABILITIES) {
        let actual = actual.as_f64().expect("probability should be numeric") as f32;
        assert!(
            (actual - expected).abs() <= 1e-5,
            "actual={actual}, expected={expected}"
        );
    }

    let second_output_topic = format!("onnx_fvt_output_second_{}", random_suffix());
    let second_pipeline_id = format!("onnx_fvt_pipeline_second_{}", random_suffix());
    let second_topic_response = http
        .post(format!("{manager_base}/memory/topics"))
        .json(&serde_json::json!({
            "topic": second_output_topic,
            "revision": 1,
            "kind": "bytes"
        }))
        .send()
        .await
        .expect("create second output topic");
    assert_eq!(second_topic_response.status(), StatusCode::CREATED);
    let second_pipeline_response = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": second_pipeline_id,
            "revision": 1,
            "sql": format!("SELECT onnx('mnist_8', image) AS result FROM {stream_name}"),
            "sinks": [{
                "id": "onnx_fvt_output_second",
                "type": "memory",
                "props": {"topic": second_output_topic},
                "encoder": {"type": "json", "props": {}}
            }]
        }))
        .send()
        .await
        .expect("create second pipeline");
    assert_eq!(second_pipeline_response.status(), StatusCode::CREATED);
    let second_start_response = http
        .post(format!(
            "{manager_base}/pipelines/{second_pipeline_id}/start"
        ))
        .send()
        .await
        .expect("start second pipeline");
    assert_eq!(second_start_response.status(), StatusCode::OK);
    let model_status = wait_for_model_prepared(&http, &manager_base, "mnist_8", true).await;
    assert_eq!(model_status["prepare_count"], 1);

    let stop_response = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop pipeline");
    assert_eq!(stop_response.status(), StatusCode::OK);
    let model_status = wait_for_model_prepared(&http, &manager_base, "mnist_8", true).await;
    assert_eq!(model_status["prepare_count"], 1);

    let second_stop_response = http
        .post(format!(
            "{manager_base}/pipelines/{second_pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop second pipeline");
    assert_eq!(second_stop_response.status(), StatusCode::OK);
    let model_status = wait_for_model_prepared(&http, &manager_base, "mnist_8", false).await;
    assert_eq!(model_status["prepare_count"], 1);

    for (resource, path) in [
        ("pipeline", format!("/pipelines/{pipeline_id}")),
        (
            "second pipeline",
            format!("/pipelines/{second_pipeline_id}"),
        ),
        ("stream", format!("/streams/{stream_name}")),
        ("model", "/models/mnist_8".to_string()),
    ] {
        let response = http
            .delete(format!("{manager_base}{path}"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("delete {resource}: {error}"));
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "delete {resource} failed: {}",
            response.text().await.unwrap_or_default()
        );
    }
    server.abort();
}

async fn run_flat_array_input_fvt(
    model_name: &str,
    model: Vec<u8>,
    valid_values: &[f32],
    invalid_values: &[f32],
) {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let injector = instance.clone();
    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");
    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{addr}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let upload = reqwest::multipart::Part::bytes(identity_model_bundle(model_name, &model))
        .file_name(format!("{model_name}.zip"))
        .mime_str("application/zip")
        .unwrap();
    let model_response = http
        .post(format!("{manager_base}/models"))
        .multipart(reqwest::multipart::Form::new().part("file", upload))
        .send()
        .await
        .expect("upload identity model");
    assert_eq!(model_response.status(), StatusCode::OK);

    let stream_name = format!("onnx_identity_stream_{}", random_suffix());
    let output_topic = format!("onnx_identity_output_{}", random_suffix());
    let pipeline_id = format!("onnx_identity_pipeline_{}", random_suffix());
    let array_schema = serde_json::json!({
        "name": "pairs",
        "data_type": "list",
        "element": {
            "name": "element",
            "data_type": "float32"
        }
    });
    let stream_response = http
        .post(format!("{manager_base}/streams"))
        .json(&serde_json::json!({
            "name": stream_name,
            "revision": 1,
            "type": "mock",
            "schema": {
                "type": "json",
                "props": {"columns": [array_schema]}
            },
            "props": {},
            "shared": true,
            "decoder": {"type": "json", "props": {}}
        }))
        .send()
        .await
        .expect("create flat array stream");
    assert_eq!(stream_response.status(), StatusCode::CREATED);

    let topic_response = http
        .post(format!("{manager_base}/memory/topics"))
        .json(&serde_json::json!({
            "topic": output_topic,
            "revision": 1,
            "kind": "bytes"
        }))
        .send()
        .await
        .expect("create output topic");
    assert_eq!(topic_response.status(), StatusCode::CREATED);

    let mut output = injector
        .open_memory_subscribe_bytes(&output_topic)
        .expect("open output subscription");
    let pipeline_response = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipeline_id,
            "revision": 1,
            "sql": format!(
                "SELECT onnx('{model_name}', pairs) AS result FROM {stream_name}"
            ),
            "sinks": [{
                "id": "identity_output",
                "type": "memory",
                "props": {"topic": output_topic},
                "encoder": {"type": "json", "props": {}}
            }]
        }))
        .send()
        .await
        .expect("create identity pipeline");
    let pipeline_status = pipeline_response.status();
    let pipeline_body = pipeline_response.text().await.unwrap_or_default();
    assert_eq!(pipeline_status, StatusCode::CREATED, "{pipeline_body}");
    let start_response = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start identity pipeline");
    assert_eq!(start_response.status(), StatusCode::OK);

    let model_status = wait_for_model_prepared(&http, &manager_base, model_name, true).await;
    assert_eq!(model_status["prepare_count"], 1);

    // Send multiple independent rows through one runtime. A regression that prepares the
    // model from eval_row would increase prepare_count for these rows.
    for _ in 0..16 {
        injector
            .send_shared_mock_stream_payload(
                &stream_name,
                serde_json::to_vec(&serde_json::json!({
                    "pairs": valid_values
                }))
                .unwrap(),
            )
            .await
            .expect("send repeated identity input");
    }
    let mut repeated_outputs = Vec::new();
    timeout(Duration::from_secs(10), async {
        while repeated_outputs.len() < 16 {
            match output
                .recv()
                .await
                .expect("receive repeated identity output")
            {
                MemoryData::Bytes(payload) => {
                    let rows: Vec<serde_json::Value> =
                        serde_json::from_slice(&payload).expect("decode repeated identity output");
                    repeated_outputs.extend(rows);
                }
                MemoryData::Collection(_) => continue,
            }
        }
    })
    .await
    .expect("timeout waiting for repeated identity outputs");
    assert_eq!(repeated_outputs.len(), 16);
    for row in &repeated_outputs {
        let result = row["result"]
            .as_object()
            .and_then(|fields| fields.values().next())
            .expect("result should contain one ONNX output");
        assert_eq!(result, &serde_json::json!(valid_values));
    }
    let model_status = wait_for_model_prepared(&http, &manager_base, model_name, true).await;
    assert_eq!(model_status["prepare_count"], 1);

    injector
        .send_shared_mock_stream_payload(
            &stream_name,
            serde_json::to_vec(&serde_json::json!({
                "pairs": invalid_values
            }))
            .unwrap(),
        )
        .await
        .expect("send invalid flat array input");
    match timeout(Duration::from_millis(500), output.recv()).await {
        Err(_) => {}
        Ok(Ok(MemoryData::Bytes(payload))) => {
            let actual: serde_json::Value =
                serde_json::from_slice(&payload).expect("decode invalid-input output");
            assert_eq!(actual, serde_json::json!([]));
        }
        Ok(Ok(MemoryData::Collection(_))) => {
            panic!("invalid flat array input produced a collection output")
        }
        Ok(Err(error)) => panic!("output subscription failed: {error}"),
    }

    injector
        .send_shared_mock_stream_payload(
            &stream_name,
            serde_json::to_vec(&serde_json::json!({
                "pairs": valid_values
            }))
            .unwrap(),
        )
        .await
        .expect("send valid flat array input");
    let payload = timeout(Duration::from_secs(10), async {
        loop {
            match output.recv().await.expect("receive output") {
                MemoryData::Bytes(payload) => break payload,
                MemoryData::Collection(_) => continue,
            }
        }
    })
    .await
    .expect("timeout waiting for identity output");
    let actual: serde_json::Value = serde_json::from_slice(&payload).expect("decode output JSON");
    let result = actual[0]["result"]
        .as_object()
        .and_then(|fields| fields.values().next())
        .expect("result should contain one ONNX output");
    assert_eq!(result, &serde_json::json!(valid_values));

    let stop_response = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop identity pipeline");
    assert_eq!(stop_response.status(), StatusCode::OK);
    let model_status = wait_for_model_prepared(&http, &manager_base, model_name, false).await;
    assert_eq!(model_status["prepare_count"], 1);
    for (resource, path) in [
        ("pipeline", format!("/pipelines/{pipeline_id}")),
        ("stream", format!("/streams/{stream_name}")),
        ("model", format!("/models/{model_name}")),
    ] {
        let response = http
            .delete(format!("{manager_base}{path}"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("delete {resource}: {error}"));
        assert_eq!(response.status(), StatusCode::OK);
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn onnx_static_2x3_flat_array_input_fvt() {
    run_flat_array_input_fvt(
        "identity_static_2x3",
        static_2x3_identity_model(),
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        &[1.0, 2.0, 3.0, 4.0],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn onnx_dynamic_2x3_flat_array_input_fvt() {
    run_flat_array_input_fvt(
        "identity_dynamic_2x3",
        dynamic_2x3_identity_model(),
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        &[1.0, 2.0, 3.0, 4.0],
    )
    .await;
}
