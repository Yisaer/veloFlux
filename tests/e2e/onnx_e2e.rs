use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use flow::connector::MemoryData;
use reqwest::StatusCode;
use std::io::{Cursor, Write};
use std::time::Duration;
use tokio::time::timeout;
use zip::ZipWriter;

const MNIST_MODEL: &[u8] = include_bytes!("../../src/manager/src/model_testdata/mnist-8.onnx");
const IDENTITY_MODEL: &[u8] =
    include_bytes!("../../src/manager/src/model_testdata/identity-4.onnx");
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
    archive
        .write_all(
            b"name: mnist_8
inputs:
  - name: image
    data_type: list
    element:
      name: element
      data_type: float32
outputs:
  - name: probabilities
    data_type: list
    element:
      name: element
      data_type: float32
",
        )
        .unwrap();
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

fn mnist_input() -> Vec<f32> {
    let mut input = vec![0.0; 28 * 28];
    input[14 * 28 + 14] = 1.0;
    input
}

fn identity_model_bundle() -> Vec<u8> {
    let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default();
    archive
        .start_file("identity_4/model.onnx", options)
        .unwrap();
    archive.write_all(IDENTITY_MODEL).unwrap();
    archive
        .start_file("identity_4/model.yaml", options)
        .unwrap();
    archive
        .write_all(
            br#"name: identity_4
inputs:
  - name: pairs
    data_type: list
    cardinality: 2
    element:
      name: pair
      data_type: struct
      fields:
        - name: left
          data_type: float32
        - name: right
          data_type: float32
outputs:
  - name: pairs
    data_type: list
    cardinality: 2
    element:
      name: pair
      data_type: struct
      fields:
        - name: left
          data_type: float32
        - name: right
          data_type: float32
"#,
        )
        .unwrap();
    for index in 1..=3 {
        archive
            .start_file(
                format!("identity_4/test_vectors/input-{index}.json"),
                options,
            )
            .unwrap();
        archive.write_all(b"{}").unwrap();
        archive
            .start_file(
                format!("identity_4/test_vectors/expected_output-{index}.json"),
                options,
            )
            .unwrap();
        archive.write_all(b"{}").unwrap();
    }
    archive.finish().unwrap().into_inner()
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
    let probabilities = actual[0]["result"]["probabilities"]
        .as_array()
        .expect("result.probabilities should be an array");
    assert_eq!(probabilities.len(), EXPECTED_PROBABILITIES.len());
    for (actual, expected) in probabilities.iter().zip(EXPECTED_PROBABILITIES) {
        let actual = actual.as_f64().expect("probability should be numeric") as f32;
        assert!(
            (actual - expected).abs() <= 1e-5,
            "actual={actual}, expected={expected}"
        );
    }

    let stop_response = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop pipeline");
    assert_eq!(stop_response.status(), StatusCode::OK);

    for (resource, path) in [
        ("pipeline", format!("/pipelines/{pipeline_id}")),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn onnx_composite_input_cardinality_fvt() {
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

    let upload = reqwest::multipart::Part::bytes(identity_model_bundle())
        .file_name("identity_4.zip")
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
    let composite_schema = serde_json::json!({
        "name": "pairs",
        "data_type": "list",
        "cardinality": 2,
        "element": {
            "name": "pair",
            "data_type": "struct",
            "fields": [
                {"name": "left", "data_type": "float32"},
                {"name": "right", "data_type": "float32"}
            ]
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
                "props": {"columns": [composite_schema]}
            },
            "props": {},
            "shared": true,
            "decoder": {"type": "json", "props": {}}
        }))
        .send()
        .await
        .expect("create composite stream");
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
            "sql": format!("SELECT onnx('identity_4', pairs) AS result FROM {stream_name}"),
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

    injector
        .send_shared_mock_stream_payload(
            &stream_name,
            serde_json::to_vec(&serde_json::json!({
                "pairs": [
                    {"left": 1.0, "right": 2.0},
                    {"left": 3.0, "right": 4.0},
                    {"left": 5.0, "right": 6.0}
                ]
            }))
            .unwrap(),
        )
        .await
        .expect("send invalid cardinality input");
    match timeout(Duration::from_millis(500), output.recv()).await {
        Err(_) => {}
        Ok(Ok(MemoryData::Bytes(payload))) => {
            let actual: serde_json::Value =
                serde_json::from_slice(&payload).expect("decode invalid-input output");
            assert_eq!(actual, serde_json::json!([]));
        }
        Ok(Ok(MemoryData::Collection(_))) => {
            panic!("invalid cardinality input produced a collection output")
        }
        Ok(Err(error)) => panic!("output subscription failed: {error}"),
    }

    injector
        .send_shared_mock_stream_payload(
            &stream_name,
            serde_json::to_vec(&serde_json::json!({
                "pairs": [
                    {"left": 1.0, "right": 2.0},
                    {"left": 3.0, "right": 4.0}
                ]
            }))
            .unwrap(),
        )
        .await
        .expect("send valid cardinality input");
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
    assert_eq!(
        actual[0]["result"]["pairs"],
        serde_json::json!([
            {"left": 1.0, "right": 2.0},
            {"left": 3.0, "right": 4.0}
        ])
    );

    let stop_response = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop identity pipeline");
    assert_eq!(stop_response.status(), StatusCode::OK);
    for (resource, path) in [
        ("pipeline", format!("/pipelines/{pipeline_id}")),
        ("stream", format!("/streams/{stream_name}")),
        ("model", "/models/identity_4".to_string()),
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
