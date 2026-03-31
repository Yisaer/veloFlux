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

fn shared_mqtt_config_eq(left: &SharedMqttClientConfig, right: &SharedMqttClientConfig) -> bool {
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
