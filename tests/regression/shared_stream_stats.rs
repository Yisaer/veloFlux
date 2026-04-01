use super::{bind_manager_listener_or_skip, default_flow_instances, random_suffix};
use reqwest::StatusCode;
use sdk::{PipelineCreateRequest, StreamCreateRequest};
use serde_json::Value as JsonValue;
use std::time::Duration;

fn worker_flow_instance_spec(id: &str) -> manager::FlowInstanceSpec {
    manager::FlowInstanceSpec {
        id: id.to_string(),
        backend: manager::FlowInstanceBackendKind::WorkerProcess,
        worker_addr: Some("http://127.0.0.1:1".to_string()),
        metrics_addr: Some("http://127.0.0.1:2".to_string()),
        profile_addr: Some("http://127.0.0.1:3".to_string()),
        ..manager::FlowInstanceSpec::default()
    }
}

fn records_value(entry: &JsonValue, field: &str) -> u64 {
    entry["stats"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("missing numeric stats field {field} in {entry}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_default_to_default_instance_in_single_instance_deployments() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let manager_instance = manager::new_default_flow_instance();
    let injector = manager_instance.clone();

    let Some(manager_listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let manager_addr = manager_listener
        .local_addr()
        .expect("manager listener addr");

    let manager_server = tokio::spawn(async move {
        manager::start_server_with_listener(
            manager_listener,
            manager_instance,
            storage,
            default_flow_instances(),
            Vec::new(),
        )
        .await
        .expect("start manager server");
    });

    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build test http client");
    let manager_base = format!("http://{manager_addr}");
    let stream_name = format!("single_shared_stream_stats_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .send()
        .await
        .expect("create shared stream request");
    assert_eq!(create_stream_resp.status(), StatusCode::CREATED);

    let pipeline_id = format!("single_shared_stream_pipeline_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&PipelineCreateRequest::nop(pipeline_id.clone(), sql))
        .send()
        .await
        .expect("create default pipeline request");
    assert_eq!(create_pipeline_resp.status(), StatusCode::CREATED);

    let start_pipeline_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start default pipeline request");
    assert_eq!(start_pipeline_resp.status(), StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(200)).await;
    injector
        .send_shared_mock_stream_payload(&stream_name, br#"{"value": 42}"#.as_ref())
        .await
        .expect("inject payload into default shared mock stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let stats = loop {
        let stats_resp = http
            .get(format!("{manager_base}/streams/{stream_name}/shared/stats"))
            .send()
            .await
            .expect("get shared stream stats for default instance");
        assert_eq!(stats_resp.status(), StatusCode::OK);
        let stats: JsonValue = stats_resp
            .json()
            .await
            .expect("decode shared stream stats for default instance");
        let processors = stats["processors"]
            .as_array()
            .expect("processors should be an array");
        if processors.iter().any(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| id.ends_with("PhysicalDecoder_1"))
                && records_value(entry, "records_in") > 0
        }) {
            break stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "default shared stream stats did not reflect runtime activity before timeout: {stats}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(stats["stream"], stream_name);
    assert_eq!(stats["flow_instance_id"], "default");
    assert_eq!(stats["status"], "running");

    let stop_pipeline_resp = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop default pipeline request");
    assert_eq!(stop_pipeline_resp.status(), StatusCode::OK);

    let delete_pipeline_resp = http
        .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete default pipeline request");
    assert_eq!(delete_pipeline_resp.status(), StatusCode::OK);

    let delete_stream_resp = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete shared stream request");
    assert_eq!(delete_stream_resp.status(), StatusCode::OK);

    manager_server.abort();
    let _ = manager_server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_require_explicit_flow_instance_id_in_multi_instance_deployments() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let manager_instance = manager::new_default_flow_instance();

    let worker_runtime_spec = manager::FlowInstanceSpec {
        id: "worker_a".to_string(),
        backend: manager::FlowInstanceBackendKind::InProcess,
        ..manager::FlowInstanceSpec::default()
    };
    let worker_instance =
        manager::build_in_process_flow_instance(&worker_runtime_spec, None).expect("build worker");
    let worker_injector = worker_instance.clone();

    let Some(worker_listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let worker_addr = worker_listener.local_addr().expect("worker listener addr");
    let worker_server = tokio::spawn(async move {
        manager::start_flow_worker_with_listener(worker_listener, worker_instance)
            .await
            .expect("start worker server");
    });

    let Some(manager_listener) = bind_manager_listener_or_skip().await else {
        worker_server.abort();
        let _ = worker_server.await;
        return;
    };
    let manager_addr = manager_listener
        .local_addr()
        .expect("manager listener addr");

    let mut flow_instances = default_flow_instances();
    flow_instances.push(worker_flow_instance_spec("worker_a"));
    let worker_endpoints = vec![("worker_a".to_string(), format!("http://{worker_addr}"))];

    let manager_server = tokio::spawn(async move {
        manager::start_server_with_listener(
            manager_listener,
            manager_instance,
            storage,
            flow_instances,
            worker_endpoints,
        )
        .await
        .expect("start manager server");
    });

    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build test http client");
    let manager_base = format!("http://{manager_addr}");
    let stream_name = format!("worker_shared_stream_stats_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .send()
        .await
        .expect("create shared stream request");
    let create_stream_status = create_stream_resp.status();
    let create_stream_body = create_stream_resp.text().await.unwrap_or_default();
    assert!(
        create_stream_status.is_success(),
        "create shared stream failed: status={} body={}",
        create_stream_status,
        create_stream_body
    );

    let pipeline_id = format!("worker_shared_stream_pipeline_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(
            &PipelineCreateRequest::nop(pipeline_id.clone(), sql).with_flow_instance_id("worker_a"),
        )
        .send()
        .await
        .expect("create worker pipeline request");
    let create_pipeline_status = create_pipeline_resp.status();
    let create_pipeline_body = create_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        create_pipeline_status.is_success(),
        "create worker pipeline failed: status={} body={}",
        create_pipeline_status,
        create_pipeline_body
    );

    let start_pipeline_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start worker pipeline request");
    let start_pipeline_status = start_pipeline_resp.status();
    let start_pipeline_body = start_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        start_pipeline_status.is_success(),
        "start worker pipeline failed: status={} body={}",
        start_pipeline_status,
        start_pipeline_body
    );

    tokio::time::sleep(Duration::from_millis(200)).await;
    worker_injector
        .send_shared_mock_stream_payload(&stream_name, br#"{"value": 7}"#.as_ref())
        .await
        .expect("inject payload into worker shared mock stream");

    let missing_target_resp = http
        .get(format!("{manager_base}/streams/{stream_name}/shared/stats"))
        .send()
        .await
        .expect("shared stream stats without flow_instance_id");
    let missing_target_status = missing_target_resp.status();
    let missing_target_body = missing_target_resp.text().await.unwrap_or_default();
    assert_eq!(missing_target_status, StatusCode::BAD_REQUEST);
    assert!(
        missing_target_body.contains("flow_instance_id is required"),
        "unexpected body: {missing_target_body}"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let stats = loop {
        let stats_resp = http
            .get(format!(
                "{manager_base}/streams/{stream_name}/shared/stats?flow_instance_id=worker_a"
            ))
            .send()
            .await
            .expect("get shared stream stats for worker_a");
        assert_eq!(stats_resp.status(), StatusCode::OK);
        let stats: JsonValue = stats_resp
            .json()
            .await
            .expect("decode shared stream stats for worker_a");
        let processors = stats["processors"]
            .as_array()
            .expect("processors should be an array");
        if processors.iter().any(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| id.ends_with("PhysicalDecoder_1"))
                && records_value(entry, "records_in") > 0
        }) {
            break stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker shared stream stats did not reflect runtime activity before timeout: {stats}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(stats["stream"], stream_name);
    assert_eq!(stats["flow_instance_id"], "worker_a");
    assert_eq!(stats["status"], "running");

    let stop_pipeline_resp = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop worker pipeline request");
    assert_eq!(stop_pipeline_resp.status(), StatusCode::OK);

    let delete_pipeline_resp = http
        .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete worker pipeline request");
    assert_eq!(delete_pipeline_resp.status(), StatusCode::OK);

    let delete_stream_resp = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete stream request");
    assert_eq!(delete_stream_resp.status(), StatusCode::OK);

    manager_server.abort();
    let _ = manager_server.await;
    worker_server.abort();
    let _ = worker_server.await;
}
