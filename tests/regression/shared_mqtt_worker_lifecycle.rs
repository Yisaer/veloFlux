use super::{bind_manager_listener_or_skip, default_flow_instances, random_suffix};
use flow::connector::SharedMqttClientConfig;
use reqwest::StatusCode;

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

fn shared_mqtt_cfg(key: &str) -> SharedMqttClientConfig {
    SharedMqttClientConfig {
        key: key.to_string(),
        broker_url: "tcp://127.0.0.1:1883".to_string(),
        topic: "fleet/+/telemetry".to_string(),
        client_id: format!("client_{key}"),
        qos: 0,
        max_packet_size: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_shared_mqtt_client_also_drops_worker_runtime_client() {
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
    let key = format!("shared_{}", random_suffix());
    let cfg = shared_mqtt_cfg(&key);
    worker_instance
        .create_shared_mqtt_client(cfg.clone())
        .await
        .expect("seed worker shared mqtt client");

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
    let worker_base = format!("http://{worker_addr}");

    let create_resp = http
        .post(format!("{manager_base}/mqtt/clients"))
        .json(&cfg)
        .send()
        .await
        .expect("create shared mqtt client via manager");
    let create_status = create_resp.status();
    let create_body = create_resp.text().await.unwrap_or_default();
    assert!(
        create_status.is_success(),
        "create failed: status={} body={}",
        create_status,
        create_body
    );

    let delete_resp = http
        .delete(format!("{manager_base}/mqtt/clients/{key}"))
        .send()
        .await
        .expect("delete shared mqtt client via manager");
    assert_eq!(delete_resp.status(), StatusCode::NO_CONTENT);

    let worker_delete_resp = http
        .delete(format!("{worker_base}/internal/v1/mqtt/clients/{key}"))
        .send()
        .await
        .expect("delete shared mqtt client directly in worker");
    let worker_delete_status = worker_delete_resp.status();
    let worker_delete_body = worker_delete_resp
        .text()
        .await
        .expect("read worker delete response");
    assert_eq!(worker_delete_status, StatusCode::NOT_FOUND);
    assert_eq!(
        worker_delete_body,
        format!("shared mqtt client {key} not found")
    );

    manager_server.abort();
    let _ = manager_server.await;
    worker_server.abort();
    let _ = worker_server.await;
}
