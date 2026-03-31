use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::connector::{ConnectorError, SharedMqttClientConfig};
use storage::StorageError;

use crate::pipeline::AppState;
use crate::storage_bridge::{mqtt_config_from_stored, stored_mqtt_from_config};
use crate::worker::client::DeleteSharedMqttClientError;

fn validate_shared_mqtt_config(cfg: &SharedMqttClientConfig) -> Result<(), String> {
    if cfg.key.trim().is_empty() {
        return Err("shared mqtt client key must not be empty".to_string());
    }
    if cfg.broker_url.trim().is_empty() {
        return Err(format!(
            "shared mqtt client {} broker_url must not be empty",
            cfg.key
        ));
    }
    if cfg.topic.trim().is_empty() {
        return Err(format!(
            "shared mqtt client {} topic must not be empty",
            cfg.key
        ));
    }
    if cfg.client_id.trim().is_empty() {
        return Err(format!(
            "shared mqtt client {} client_id must not be empty",
            cfg.key
        ));
    }
    Ok(())
}

pub(crate) fn shared_mqtt_config_eq(
    left: &SharedMqttClientConfig,
    right: &SharedMqttClientConfig,
) -> bool {
    left.key == right.key
        && left.broker_url == right.broker_url
        && left.topic == right.topic
        && left.client_id == right.client_id
        && left.qos == right.qos
        && left.max_packet_size == right.max_packet_size
}

fn storage_conflict_message(key: &str, existing: &SharedMqttClientConfig) -> String {
    format!(
        "shared mqtt client {key} already exists with different config: broker_url={}, topic={}, client_id={}, qos={}, max_packet_size={:?}",
        existing.broker_url,
        existing.topic,
        existing.client_id,
        existing.qos,
        existing.max_packet_size
    )
}

pub async fn create_shared_mqtt_client_handler(
    State(state): State<AppState>,
    Json(req): Json<SharedMqttClientConfig>,
) -> impl IntoResponse {
    if let Err(err) = validate_shared_mqtt_config(&req) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let key = req.key.trim().to_string();
    let mut storage_created = false;
    match state.storage.get_mqtt_config(&key) {
        Ok(Some(stored)) => {
            let existing = mqtt_config_from_stored(&stored);
            if !shared_mqtt_config_eq(&existing, &req) {
                return (
                    StatusCode::CONFLICT,
                    storage_conflict_message(&key, &existing),
                )
                    .into_response();
            }
        }
        Ok(None) => match state
            .storage
            .create_mqtt_config(stored_mqtt_from_config(&req))
        {
            Ok(()) => {
                storage_created = true;
            }
            Err(StorageError::AlreadyExists(_)) => {
                return (
                    StatusCode::CONFLICT,
                    format!("shared mqtt client {key} already exists"),
                )
                    .into_response();
            }
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to persist shared mqtt client {key}: {err}"),
                )
                    .into_response();
            }
        },
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read shared mqtt client {key}: {err}"),
            )
                .into_response();
        }
    }

    let mut created_instances: Vec<std::sync::Arc<flow::FlowInstance>> = Vec::new();
    for (instance_id, instance) in state.instances.instances_snapshot() {
        if let Some(existing) = instance.get_shared_mqtt_client(&key) {
            if !shared_mqtt_config_eq(&existing, &req) {
                for created in &created_instances {
                    let _ = created.drop_shared_mqtt_client(&key);
                }
                if storage_created {
                    let _ = state.storage.delete_mqtt_config(&key);
                }
                return (
                    StatusCode::CONFLICT,
                    format!(
                        "shared mqtt client {key} already exists in runtime instance {instance_id} with different config"
                    ),
                )
                    .into_response();
            }
            continue;
        }
        if let Err(err) = instance.create_shared_mqtt_client(req.clone()).await {
            for created in &created_instances {
                let _ = created.drop_shared_mqtt_client(&key);
            }
            if storage_created {
                let _ = state.storage.delete_mqtt_config(&key);
            }
            return (
                StatusCode::BAD_REQUEST,
                format!("create shared mqtt client {key} in runtime instance {instance_id}: {err}"),
            )
                .into_response();
        }
        created_instances.push(instance);
    }

    let status = if storage_created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    (status, Json(req)).into_response()
}

pub async fn list_shared_mqtt_clients_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_mqtt_configs() {
        Ok(configs) => {
            let mut items = configs
                .into_iter()
                .map(|stored| mqtt_config_from_stored(&stored))
                .collect::<Vec<_>>();
            items.sort_by(|a, b| a.key.cmp(&b.key));
            Json(items).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list shared mqtt clients: {err}"),
        )
            .into_response(),
    }
}

pub async fn get_shared_mqtt_client_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    match state.storage.get_mqtt_config(&key) {
        Ok(Some(stored)) => {
            (StatusCode::OK, Json(mqtt_config_from_stored(&stored))).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("shared mqtt client {key} not found"),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read shared mqtt client {key}: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_shared_mqtt_client_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let stored = match state.storage.get_mqtt_config(&key) {
        Ok(Some(stored)) => stored,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                format!("shared mqtt client {key} not found"),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read shared mqtt client {key}: {err}"),
            )
                .into_response();
        }
    };

    for (instance_id, instance) in state.instances.instances_snapshot() {
        if let Err(err) = instance.can_drop_shared_mqtt_client(&key) {
            match err {
                flow::FlowInstanceError::Connector(ConnectorError::NotFound(_)) => {}
                flow::FlowInstanceError::Connector(ConnectorError::ResourceBusy(_)) => {
                    return (
                        StatusCode::CONFLICT,
                        format!(
                            "shared mqtt client {key} is still in use in runtime instance {instance_id}"
                        ),
                    )
                        .into_response();
                }
                other => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "drop shared mqtt client {key} in runtime instance {instance_id}: {other}"
                        ),
                    )
                        .into_response();
                }
            }
        }
    }

    for (instance_id, worker) in state.workers.iter() {
        if let Err(err) = worker.can_delete_shared_mqtt_client(&key).await {
            match err {
                DeleteSharedMqttClientError::NotFound => {}
                DeleteSharedMqttClientError::Conflict(_) => {
                    return (
                        StatusCode::CONFLICT,
                        format!(
                            "shared mqtt client {key} is still in use in worker instance {instance_id}"
                        ),
                    )
                        .into_response();
                }
                DeleteSharedMqttClientError::Other(other) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "drop shared mqtt client {key} in worker instance {instance_id}: {other}"
                        ),
                    )
                        .into_response();
                }
            }
        }
    }

    let _stored = stored;
    for (instance_id, instance) in state.instances.instances_snapshot() {
        if let Err(err) = instance.drop_shared_mqtt_client(&key) {
            match err {
                flow::FlowInstanceError::Connector(ConnectorError::NotFound(_)) => {}
                flow::FlowInstanceError::Connector(ConnectorError::ResourceBusy(_)) => {
                    return (
                        StatusCode::CONFLICT,
                        format!(
                            "shared mqtt client {key} is still in use in runtime instance {instance_id}"
                        ),
                    )
                        .into_response();
                }
                other => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "drop shared mqtt client {key} in runtime instance {instance_id}: {other}"
                        ),
                    )
                        .into_response();
                }
            }
        }
    }

    for (instance_id, worker) in state.workers.iter() {
        if let Err(err) = worker.delete_shared_mqtt_client(&key).await {
            match err {
                DeleteSharedMqttClientError::NotFound => {}
                DeleteSharedMqttClientError::Conflict(_) => {
                    return (
                        StatusCode::CONFLICT,
                        format!(
                            "shared mqtt client {key} is still in use in worker instance {instance_id}"
                        ),
                    )
                        .into_response();
                }
                DeleteSharedMqttClientError::Other(other) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "drop shared mqtt client {key} in worker instance {instance_id}: {other}"
                        ),
                    )
                        .into_response();
                }
            }
        }
    }

    match state.storage.delete_mqtt_config(&key) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(StorageError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            format!("shared mqtt client {key} not found"),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to delete shared mqtt client {key}: {err}"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{create_shared_mqtt_client_handler, delete_shared_mqtt_client_handler};
    use crate::pipeline::AppState;
    use axum::{
        Json,
        body::to_bytes,
        extract::{Path, State},
        http::StatusCode,
        response::IntoResponse,
        routing::{delete, get},
    };
    use flow::connector::SharedMqttClientConfig;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::net::TcpListener;

    fn worker_flow_instance_spec(id: &str) -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: id.to_string(),
            backend: crate::FlowInstanceBackendKind::WorkerProcess,
            worker_addr: Some("http://127.0.0.1:1".to_string()),
            metrics_addr: Some("http://127.0.0.1:2".to_string()),
            profile_addr: Some("http://127.0.0.1:3".to_string()),
            ..crate::FlowInstanceSpec::default()
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

    async fn conflict_worker_server(
        delete_called: Arc<AtomicBool>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        async fn can_delete_handler() -> impl IntoResponse {
            (
                StatusCode::CONFLICT,
                "shared mqtt client shared is still in use",
            )
        }

        async fn delete_handler(State(called): State<Arc<AtomicBool>>) -> impl IntoResponse {
            called.store(true, Ordering::Release);
            StatusCode::NO_CONTENT
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake worker listener");
        let addr = listener.local_addr().expect("fake worker addr");
        let app = axum::Router::new()
            .route(
                "/internal/v1/mqtt/clients/:key/can-delete",
                get(can_delete_handler),
            )
            .route("/internal/v1/mqtt/clients/:key", delete(delete_handler))
            .with_state(delete_called);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .expect("serve fake worker");
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn delete_shared_mqtt_client_does_not_drop_local_runtime_when_worker_preflight_conflicts()
    {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
        let instance = crate::new_default_flow_instance();

        let delete_called = Arc::new(AtomicBool::new(false));
        let (worker_url, worker_handle) = conflict_worker_server(delete_called.clone()).await;

        let state = AppState::new(
            instance,
            storage,
            vec![
                crate::FlowInstanceSpec {
                    id: "default".to_string(),
                    backend: crate::FlowInstanceBackendKind::InProcess,
                    ..crate::FlowInstanceSpec::default()
                },
                worker_flow_instance_spec("worker_a"),
            ],
            vec![("worker_a".to_string(), worker_url)],
        )
        .expect("build app state");

        let cfg = shared_mqtt_cfg("shared");
        let create_resp = create_shared_mqtt_client_handler(State(state.clone()), Json(cfg))
            .await
            .into_response();
        assert_eq!(create_resp.status(), StatusCode::CREATED);

        let local_instance = state
            .local_instance("default")
            .expect("default local runtime instance");

        let delete_resp =
            delete_shared_mqtt_client_handler(State(state), Path("shared".to_string()))
                .await
                .into_response();
        assert_eq!(delete_resp.status(), StatusCode::CONFLICT);

        let body = to_bytes(delete_resp.into_body(), 64 * 1024)
            .await
            .expect("read delete body");
        assert!(
            String::from_utf8(body.to_vec())
                .expect("utf8 delete body")
                .contains("worker instance worker_a"),
            "expected conflict body to mention worker instance"
        );

        assert!(
            local_instance.get_shared_mqtt_client("shared").is_some(),
            "local runtime should keep shared mqtt client when worker preflight conflicts"
        );
        assert!(
            !delete_called.load(Ordering::Acquire),
            "manager should stop before issuing worker delete after preflight conflict"
        );

        worker_handle.abort();
        let _ = worker_handle.await;
    }
}
