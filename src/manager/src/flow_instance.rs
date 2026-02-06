use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::pipeline::AppState;
use crate::storage_bridge;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use storage::{StorageError, StoredFlowInstance, StoredPipeline};

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateFlowInstanceRequest {
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct FlowInstanceItem {
    pub id: String,
}

pub async fn create_flow_instance_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateFlowInstanceRequest>,
) -> impl IntoResponse {
    let id = req.id.trim().to_string();
    if id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "flow instance id must not be empty".to_string(),
        )
            .into_response();
    }
    if id == DEFAULT_FLOW_INSTANCE_ID {
        return (
            StatusCode::CONFLICT,
            "flow instance default already exists".to_string(),
        )
            .into_response();
    }

    let stored = StoredFlowInstance { id: id.clone() };
    match state.storage.create_flow_instance(stored) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("flow instance {id} already exists"),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist flow instance {id}: {err}"),
            )
                .into_response();
        }
    }

    let instance = state.instances.create_dedicated_instance(&id);
    if let Err(err) =
        storage_bridge::hydrate_instance_from_storage(state.storage.as_ref(), &instance).await
    {
        let _ = state.storage.delete_flow_instance(&id);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to initialize flow instance {id}: {err}"),
        )
            .into_response();
    }

    if state.instances.insert(id.clone(), instance).is_err() {
        let _ = state.storage.delete_flow_instance(&id);
        return (
            StatusCode::CONFLICT,
            format!("flow instance {id} already exists"),
        )
            .into_response();
    }

    (StatusCode::CREATED, Json(FlowInstanceItem { id })).into_response()
}

pub async fn list_flow_instances_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_flow_instances() {
        Ok(mut items) => {
            items.sort_by(|a, b| a.id.cmp(&b.id));
            let payload = items
                .into_iter()
                .map(|item| FlowInstanceItem { id: item.id })
                .collect::<Vec<_>>();
            Json(payload).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list flow instances: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_flow_instance_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let id = id.trim().to_string();
    if id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "flow instance id must not be empty".to_string(),
        )
            .into_response();
    }
    if id == DEFAULT_FLOW_INSTANCE_ID {
        return (
            StatusCode::CONFLICT,
            "flow instance default cannot be deleted".to_string(),
        )
            .into_response();
    }

    let mut pipelines = Vec::new();
    let stored_pipelines: Vec<StoredPipeline> = match state.storage.list_pipelines() {
        Ok(items) => items,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to list pipelines: {err}"),
            )
                .into_response();
        }
    };
    for stored in stored_pipelines {
        let req = match storage_bridge::pipeline_request_from_stored(&stored) {
            Ok(req) => req,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("decode stored pipeline {}: {err}", stored.id),
                )
                    .into_response();
            }
        };
        if req.flow_instance_id.as_deref() == Some(id.as_str()) {
            pipelines.push(req.id);
        }
    }
    if !pipelines.is_empty() {
        pipelines.sort();
        return (
            StatusCode::CONFLICT,
            format!(
                "flow instance {id} still referenced by pipelines: {}",
                pipelines.join(", ")
            ),
        )
            .into_response();
    }

    match state.storage.delete_flow_instance(&id) {
        Ok(()) => {
            let _ = state.instances.remove(&id);
            (StatusCode::OK, format!("flow instance {id} deleted")).into_response()
        }
        Err(StorageError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            format!("flow instance {id} not found"),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to delete flow instance {id}: {err}"),
        )
            .into_response(),
    }
}
