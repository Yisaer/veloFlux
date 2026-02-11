use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::storage_bridge;
use crate::worker::WorkerDesiredState;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use flow::pipeline::{PipelineError, PipelineStopMode};
use std::collections::HashMap;
use std::time::Duration;
use storage::{StorageError, StoredPipelineDesiredState, StoredPipelineRunState};
use tokio::sync::TryAcquireError;

use super::context::build_pipeline_context_payload;
use super::remote::apply_pipeline_in_worker;
use super::spec::{build_pipeline_definition, status_label, validate_create_request};
use super::state::AppState;
use super::types::{
    BuildPipelineContextResponse, CollectStatsQuery, CreatePipelineRequest, CreatePipelineResponse,
    GetPipelineResponse, ListPipelineItem, StopPipelineQuery, UpsertPipelineRequest,
};

fn parse_stop_mode(mode: &str) -> Result<PipelineStopMode, String> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "" | "quick" => Ok(PipelineStopMode::Quick),
        "graceful" => Ok(PipelineStopMode::Graceful),
        other => Err(format!("unsupported stop mode: {other}")),
    }
}

fn busy_response(id: &str) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        format!("pipeline {id} is busy processing another command"),
    )
        .into_response()
}

fn stored_state_label(state: Option<StoredPipelineRunState>) -> String {
    match state.map(|s| s.desired_state) {
        Some(StoredPipelineDesiredState::Running) => "running".to_string(),
        _ => "stopped".to_string(),
    }
}

fn canonical_flow_instance_id(value: Option<&str>) -> Result<String, String> {
    let id = value.unwrap_or(DEFAULT_FLOW_INSTANCE_ID).trim();
    if id.is_empty() {
        return Err("flow_instance_id must not be empty".to_string());
    }
    Ok(id.to_string())
}

async fn resolve_pipeline_spec(
    state: &AppState,
    pipeline_id: &str,
) -> Result<(String, CreatePipelineRequest), axum::response::Response> {
    let stored = match state.storage.get_pipeline(pipeline_id) {
        Ok(Some(pipeline)) => pipeline,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("pipeline {pipeline_id} not found"),
            )
                .into_response());
        }
        Err(err) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {pipeline_id} from storage: {err}"),
            )
                .into_response());
        }
    };

    let mut req = match storage_bridge::pipeline_request_from_stored(&stored) {
        Ok(req) => req,
        Err(err) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode stored pipeline {pipeline_id}: {err}"),
            )
                .into_response());
        }
    };
    let flow_instance_id = canonical_flow_instance_id(req.flow_instance_id.as_deref())
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err).into_response())?;
    req.flow_instance_id = Some(flow_instance_id.clone());

    if !state.is_declared_instance(&flow_instance_id) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "pipeline {pipeline_id} references undeclared flow instance {flow_instance_id}"
            ),
        )
            .into_response());
    }
    Ok((flow_instance_id, req))
}

pub async fn create_pipeline_handler(
    State(state): State<AppState>,
    Json(req): Json<CreatePipelineRequest>,
) -> impl IntoResponse {
    let mut req = req;
    let flow_instance_id = match canonical_flow_instance_id(req.flow_instance_id.as_deref()) {
        Ok(id) => id,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    req.flow_instance_id = Some(flow_instance_id.clone());

    if let Err(err) = validate_create_request(&req) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let _permit = match state.try_acquire_pipeline_op(&req.id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&req.id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    if flow_instance_id != DEFAULT_FLOW_INSTANCE_ID
        && !state.is_declared_instance(&flow_instance_id)
    {
        return (
            StatusCode::BAD_REQUEST,
            format!("flow instance {flow_instance_id} is not declared by config"),
        )
            .into_response();
    }

    let stored = match storage_bridge::stored_pipeline_from_request(&req) {
        Ok(stored) => stored,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match state.storage.create_pipeline(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("pipeline {} already exists", req.id),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist pipeline {}: {err}", req.id),
            )
                .into_response();
        }
    }

    if flow_instance_id != DEFAULT_FLOW_INSTANCE_ID {
        let Some(worker) = state.worker(&flow_instance_id) else {
            return (
                StatusCode::BAD_REQUEST,
                format!("flow instance {flow_instance_id} is not declared by config"),
            )
                .into_response();
        };
        let apply_result = match apply_pipeline_in_worker(
            &worker,
            &state.instances,
            state.storage.as_ref(),
            &req.id,
            &req,
            &stored.raw_json,
            WorkerDesiredState::Stopped,
        )
        .await
        {
            Ok(result) => result,
            Err(resp) => {
                tracing::error!(
                    pipeline_id = %req.id,
                    flow_instance_id = %flow_instance_id,
                    "pipeline persisted but failed to apply to worker"
                );
                return resp;
            }
        };

        tracing::info!(
            pipeline_id = %stored.id,
            flow_instance_id = %flow_instance_id,
            "pipeline created (remote)"
        );
        return (
            StatusCode::CREATED,
            Json(CreatePipelineResponse {
                id: stored.id,
                status: apply_result.status,
            }),
        )
            .into_response();
    }

    let instance = match state.instances.get(&flow_instance_id) {
        Some(instance) => instance,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                format!("flow instance {flow_instance_id} is not declared by config"),
            )
                .into_response();
        }
    };

    let encoder_registry = instance.encoder_registry();
    let definition =
        match build_pipeline_definition(&req, encoder_registry.as_ref(), instance.as_ref()) {
            Ok(def) => def,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };

    let build_result = match instance.create_pipeline(flow::CreatePipelineRequest::new(definition))
    {
        Ok(result) => result,
        Err(PipelineError::AlreadyExists(_)) => {
            let _ = state.storage.delete_pipeline(&stored.id);
            return (
                StatusCode::CONFLICT,
                format!("pipeline {} already exists", req.id),
            )
                .into_response();
        }
        Err(err) => {
            let _ = state.storage.delete_pipeline(&stored.id);
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to create pipeline {}: {err}", req.id),
            )
                .into_response();
        }
    };

    let snapshot = build_result.snapshot;
    tracing::info!(pipeline_id = %snapshot.definition.id(), "pipeline created");
    (
        StatusCode::CREATED,
        Json(CreatePipelineResponse {
            id: snapshot.definition.id().to_string(),
            status: status_label(snapshot.status),
        }),
    )
        .into_response()
}

pub async fn upsert_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpsertPipelineRequest>,
) -> impl IntoResponse {
    let id = id.trim().to_string();
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    let old_pipeline = match state.storage.get_pipeline(&id) {
        Ok(pipeline) => pipeline,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {id} from storage: {err}"),
            )
                .into_response();
        }
    };

    let flow_instance_id = match old_pipeline.as_ref() {
        Some(stored) => match storage_bridge::pipeline_request_from_stored(stored) {
            Ok(req) => match canonical_flow_instance_id(req.flow_instance_id.as_deref()) {
                Ok(id) => id,
                Err(err) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("invalid stored flow_instance_id for pipeline {id}: {err}"),
                    )
                        .into_response();
                }
            },
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to decode stored pipeline {id}: {err}"),
                )
                    .into_response();
            }
        },
        None => DEFAULT_FLOW_INSTANCE_ID.to_string(),
    };

    let create_req = CreatePipelineRequest {
        id: id.clone(),
        flow_instance_id: Some(flow_instance_id),
        sql: req.sql,
        sinks: req.sinks,
        options: req.options,
    };
    if let Err(err) = validate_create_request(&create_req) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let old_desired_state = match state.storage.get_pipeline_run_state(&id) {
        Ok(Some(state)) => state.desired_state,
        Ok(None) => StoredPipelineDesiredState::Stopped,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {id} run state from storage: {err}"),
            )
                .into_response();
        }
    };

    if create_req
        .flow_instance_id
        .as_deref()
        .unwrap_or(DEFAULT_FLOW_INSTANCE_ID)
        != DEFAULT_FLOW_INSTANCE_ID
    {
        let flow_instance_id = create_req
            .flow_instance_id
            .clone()
            .unwrap_or_else(|| DEFAULT_FLOW_INSTANCE_ID.to_string());
        if !state.is_declared_instance(&flow_instance_id) {
            return (
                StatusCode::BAD_REQUEST,
                format!("flow instance {flow_instance_id} is not declared by config"),
            )
                .into_response();
        }

        if old_pipeline.is_some() {
            let _ = state.storage.delete_pipeline(&id);
        }

        let stored = match storage_bridge::stored_pipeline_from_request(&create_req) {
            Ok(stored) => stored,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };
        match state.storage.create_pipeline(stored.clone()) {
            Ok(()) => {}
            Err(StorageError::AlreadyExists(_)) => {
                return (
                    StatusCode::CONFLICT,
                    format!("pipeline {id} already exists"),
                )
                    .into_response();
            }
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to persist pipeline {id}: {err}"),
                )
                    .into_response();
            }
        }

        let desired_state = match old_desired_state {
            StoredPipelineDesiredState::Running => WorkerDesiredState::Running,
            StoredPipelineDesiredState::Stopped => WorkerDesiredState::Stopped,
        };

        if matches!(old_desired_state, StoredPipelineDesiredState::Running)
            && state
                .storage
                .put_pipeline_run_state(StoredPipelineRunState {
                    pipeline_id: id.clone(),
                    desired_state: StoredPipelineDesiredState::Running,
                })
                .is_err()
        {
            // Best effort; keep running intent if we cannot persist.
        }

        let Some(worker) = state.worker(&flow_instance_id) else {
            return (
                StatusCode::BAD_REQUEST,
                format!("flow instance {flow_instance_id} is not declared by config"),
            )
                .into_response();
        };
        let apply_result = match apply_pipeline_in_worker(
            &worker,
            &state.instances,
            state.storage.as_ref(),
            &id,
            &create_req,
            &stored.raw_json,
            desired_state,
        )
        .await
        {
            Ok(result) => result,
            Err(resp) => {
                if matches!(old_desired_state, StoredPipelineDesiredState::Running) {
                    let _ = state
                        .storage
                        .put_pipeline_run_state(StoredPipelineRunState {
                            pipeline_id: id.clone(),
                            desired_state: StoredPipelineDesiredState::Stopped,
                        });
                }
                tracing::error!(
                    pipeline_id = %id,
                    flow_instance_id = %flow_instance_id,
                    "pipeline persisted but failed to apply to worker"
                );
                return resp;
            }
        };

        tracing::info!(
            pipeline_id = %id,
            flow_instance_id = %flow_instance_id,
            "pipeline upserted (remote)"
        );
        return Json(CreatePipelineResponse {
            id,
            status: apply_result.status,
        })
        .into_response();
    }

    let flow_instance_id = create_req
        .flow_instance_id
        .as_deref()
        .unwrap_or(DEFAULT_FLOW_INSTANCE_ID);
    let instance = match state.instances.get(flow_instance_id) {
        Some(instance) => instance,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("pipeline {id} references undeclared flow instance {flow_instance_id}"),
            )
                .into_response();
        }
    };

    let encoder_registry = instance.encoder_registry();
    let definition = match build_pipeline_definition(
        &create_req,
        encoder_registry.as_ref(),
        instance.as_ref(),
    ) {
        Ok(definition) => definition,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    if let Err(err) =
        instance.explain_pipeline(flow::ExplainPipelineTarget::Definition(&definition))
    {
        return (
            StatusCode::BAD_REQUEST,
            format!("invalid pipeline spec: {err}"),
        )
            .into_response();
    }

    if old_pipeline.is_some() {
        match instance.delete_pipeline(&id).await {
            Ok(_) | Err(PipelineError::NotFound(_)) => {}
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("failed to delete pipeline {id} in runtime: {err}"),
                )
                    .into_response();
            }
        }
        match state.storage.delete_pipeline(&id) {
            Ok(_) | Err(StorageError::NotFound(_)) => {}
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to delete pipeline {id} from storage: {err}"),
                )
                    .into_response();
            }
        }
    }

    let stored = match storage_bridge::stored_pipeline_from_request(&create_req) {
        Ok(stored) => stored,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match state.storage.create_pipeline(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("pipeline {id} already exists"),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist pipeline {id}: {err}"),
            )
                .into_response();
        }
    }

    if let Err(err) = instance.create_pipeline(flow::CreatePipelineRequest::new(definition)) {
        let _ = state.storage.delete_pipeline(&id);
        return (
            StatusCode::BAD_REQUEST,
            format!("failed to create pipeline {id}: {err}"),
        )
            .into_response();
    }

    if matches!(old_desired_state, StoredPipelineDesiredState::Running) {
        if let Err(err) = state
            .storage
            .put_pipeline_run_state(StoredPipelineRunState {
                pipeline_id: id.clone(),
                desired_state: StoredPipelineDesiredState::Running,
            })
        {
            let _ = instance.delete_pipeline(&id).await;
            let _ = state.storage.delete_pipeline(&id);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist pipeline {id} desired state: {err}"),
            )
                .into_response();
        }

        if let Err(err) = instance.start_pipeline(&id) {
            tracing::error!(
                pipeline_id = %id,
                error = %err,
                "failed to start pipeline after upsert, leaving stopped"
            );
            let _ = state
                .storage
                .put_pipeline_run_state(StoredPipelineRunState {
                    pipeline_id: id.clone(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                });
        }
    }

    let status = stored_state_label(state.storage.get_pipeline_run_state(&id).unwrap_or(None));
    Json(CreatePipelineResponse { id, status }).into_response()
}

pub async fn get_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let stored = match state.storage.get_pipeline(&id) {
        Ok(Some(pipeline)) => pipeline,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {id} from storage: {err}"),
            )
                .into_response();
        }
    };

    let spec = match storage_bridge::pipeline_request_from_stored(&stored) {
        Ok(spec) => spec,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode stored pipeline {id}: {err}"),
            )
                .into_response();
        }
    };
    let mut spec = spec;
    let flow_instance_id = match canonical_flow_instance_id(spec.flow_instance_id.as_deref()) {
        Ok(id) => id,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid stored flow_instance_id for pipeline {id}: {err}"),
            )
                .into_response();
        }
    };
    spec.flow_instance_id = Some(flow_instance_id);

    let run_state = match state.storage.get_pipeline_run_state(&id) {
        Ok(state) => state,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {id} run state from storage: {err}"),
            )
                .into_response();
        }
    };

    Json(GetPipelineResponse {
        id: id.clone(),
        status: stored_state_label(run_state),
        spec,
    })
    .into_response()
}

pub async fn build_pipeline_context_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let (_flow_instance_id, pipeline_req) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    let (streams, shared_mqtt_clients, memory_topics) = match build_pipeline_context_payload(
        &state.instances,
        state.storage.as_ref(),
        &id,
        &pipeline_req,
    ) {
        Ok(payload) => payload,
        Err(resp) => return *resp,
    };

    (
        StatusCode::OK,
        Json(BuildPipelineContextResponse {
            pipeline: pipeline_req,
            streams,
            shared_mqtt_clients,
            memory_topics,
        }),
    )
        .into_response()
}

pub async fn explain_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let (flow_instance_id, _) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    if flow_instance_id == DEFAULT_FLOW_INSTANCE_ID {
        let instance = state
            .instances
            .get(DEFAULT_FLOW_INSTANCE_ID)
            .expect("default instance missing");
        let explain = match instance.explain_pipeline(flow::ExplainPipelineTarget::Id(&id)) {
            Ok(explain) => explain,
            Err(PipelineError::NotFound(_)) => {
                return (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response();
            }
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("failed to explain pipeline {id}: {err}"),
                )
                    .into_response();
            }
        };

        let mut response = explain.to_pretty_string().into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        return response;
    }

    let worker = match state.worker(&flow_instance_id) {
        Some(worker) => worker,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("flow instance {flow_instance_id} is not declared by config"),
            )
                .into_response();
        }
    };
    match worker.explain_pipeline(&id).await {
        Ok(text) => {
            let mut response = text.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            response
        }
        Err(err) if err == "not_found" => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (StatusCode::BAD_REQUEST, err).into_response(),
    }
}

pub async fn collect_pipeline_stats_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<CollectStatsQuery>,
) -> impl IntoResponse {
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    let (flow_instance_id, _) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    let timeout = Duration::from_millis(query.timeout_ms);
    if flow_instance_id == DEFAULT_FLOW_INSTANCE_ID {
        let instance = state
            .instances
            .get(DEFAULT_FLOW_INSTANCE_ID)
            .expect("default instance missing");
        return match instance.collect_pipeline_stats(&id, timeout).await {
            Ok(stats) => {
                let stats = stats
                    .into_iter()
                    .filter(|entry| {
                        entry.processor_id != "control_source"
                            && !entry.processor_id.starts_with("PhysicalResultCollect_")
                    })
                    .collect::<Vec<_>>();
                (StatusCode::OK, Json(stats)).into_response()
            }
            Err(PipelineError::NotFound(_)) => {
                (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
            }
            Err(PipelineError::Runtime(err))
                if err == flow::ProcessorError::Timeout.to_string() =>
            {
                (
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("collect stats timeout for pipeline {id}"),
                )
                    .into_response()
            }
            Err(err) => (
                StatusCode::BAD_REQUEST,
                format!("failed to collect pipeline {id} stats: {err}"),
            )
                .into_response(),
        };
    }

    let worker = match state.worker(&flow_instance_id) {
        Some(worker) => worker,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("flow instance {flow_instance_id} is not declared by config"),
            )
                .into_response();
        }
    };
    match worker.collect_stats(&id, query.timeout_ms).await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(err) if err == "not_found" => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) if err == "timeout" => (
            StatusCode::GATEWAY_TIMEOUT,
            format!("collect stats timeout for pipeline {id}"),
        )
            .into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, err).into_response(),
    }
}

pub async fn start_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let (flow_instance_id, pipeline_req) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    if let Err(err) = state
        .storage
        .put_pipeline_run_state(StoredPipelineRunState {
            pipeline_id: id.clone(),
            desired_state: StoredPipelineDesiredState::Running,
        })
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist pipeline {id} desired state: {err}"),
        )
            .into_response();
    }

    if flow_instance_id == DEFAULT_FLOW_INSTANCE_ID {
        let instance = state
            .instances
            .get(DEFAULT_FLOW_INSTANCE_ID)
            .expect("default instance missing");
        return match instance.start_pipeline(&id) {
            Ok(_) => {
                tracing::info!(pipeline_id = %id, "pipeline started");
                (StatusCode::OK, format!("pipeline {id} started")).into_response()
            }
            Err(PipelineError::NotFound(_)) => {
                let _ = state
                    .storage
                    .put_pipeline_run_state(StoredPipelineRunState {
                        pipeline_id: id.clone(),
                        desired_state: StoredPipelineDesiredState::Stopped,
                    });
                (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
            }
            Err(err) => {
                let _ = state
                    .storage
                    .put_pipeline_run_state(StoredPipelineRunState {
                        pipeline_id: id.clone(),
                        desired_state: StoredPipelineDesiredState::Stopped,
                    });
                (
                    StatusCode::BAD_REQUEST,
                    format!("failed to start pipeline {id}: {err}"),
                )
                    .into_response()
            }
        };
    }

    let worker = match state.worker(&flow_instance_id) {
        Some(worker) => worker,
        None => {
            let _ = state
                .storage
                .put_pipeline_run_state(StoredPipelineRunState {
                    pipeline_id: id.clone(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                });
            return (
                StatusCode::BAD_REQUEST,
                format!("flow instance {flow_instance_id} is not declared by config"),
            )
                .into_response();
        }
    };

    match worker.start_pipeline(&id).await {
        Ok(()) => (StatusCode::OK, format!("pipeline {id} started")).into_response(),
        Err(err) if err == "not_found" => {
            // Best-effort: apply pipeline before retrying start.
            let stored = match state.storage.get_pipeline(&id) {
                Ok(Some(p)) => p,
                Ok(None) => {
                    let _ = state
                        .storage
                        .put_pipeline_run_state(StoredPipelineRunState {
                            pipeline_id: id.clone(),
                            desired_state: StoredPipelineDesiredState::Stopped,
                        });
                    return (StatusCode::NOT_FOUND, format!("pipeline {id} not found"))
                        .into_response();
                }
                Err(err) => {
                    let _ = state
                        .storage
                        .put_pipeline_run_state(StoredPipelineRunState {
                            pipeline_id: id.clone(),
                            desired_state: StoredPipelineDesiredState::Stopped,
                        });
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to read pipeline {id} from storage: {err}"),
                    )
                        .into_response();
                }
            };
            if let Err(resp) = apply_pipeline_in_worker(
                &worker,
                &state.instances,
                state.storage.as_ref(),
                &id,
                &pipeline_req,
                &stored.raw_json,
                WorkerDesiredState::Running,
            )
            .await
            {
                let _ = state
                    .storage
                    .put_pipeline_run_state(StoredPipelineRunState {
                        pipeline_id: id.clone(),
                        desired_state: StoredPipelineDesiredState::Stopped,
                    });
                return resp;
            }
            (StatusCode::OK, format!("pipeline {id} started")).into_response()
        }
        Err(err) => {
            let _ = state
                .storage
                .put_pipeline_run_state(StoredPipelineRunState {
                    pipeline_id: id.clone(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                });
            (
                StatusCode::BAD_REQUEST,
                format!("failed to start pipeline {id}: {err}"),
            )
                .into_response()
        }
    }
}

pub async fn stop_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<StopPipelineQuery>,
) -> impl IntoResponse {
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let (flow_instance_id, _) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    let mode = match parse_stop_mode(&query.mode) {
        Ok(mode) => mode,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let timeout = Duration::from_millis(query.timeout_ms);

    if let Err(err) = state
        .storage
        .put_pipeline_run_state(StoredPipelineRunState {
            pipeline_id: id.clone(),
            desired_state: StoredPipelineDesiredState::Stopped,
        })
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist pipeline {id} desired state: {err}"),
        )
            .into_response();
    }

    if flow_instance_id == DEFAULT_FLOW_INSTANCE_ID {
        let instance = state
            .instances
            .get(DEFAULT_FLOW_INSTANCE_ID)
            .expect("default instance missing");
        return match instance.stop_pipeline(&id, mode, timeout).await {
            Ok(_) => {
                tracing::info!(
                    pipeline_id = %id,
                    mode = %query.mode,
                    timeout_ms = query.timeout_ms,
                    "pipeline stopped"
                );
                (StatusCode::OK, format!("pipeline {id} stopped")).into_response()
            }
            Err(PipelineError::NotFound(_)) => {
                (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
            }
            Err(err) => (
                StatusCode::BAD_REQUEST,
                format!("failed to stop pipeline {id}: {err}"),
            )
                .into_response(),
        };
    }

    let worker = match state.worker(&flow_instance_id) {
        Some(worker) => worker,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                format!("flow instance {flow_instance_id} is not declared by config"),
            )
                .into_response();
        }
    };
    match worker.stop_pipeline(&id).await {
        Ok(()) => (StatusCode::OK, format!("pipeline {id} stopped")).into_response(),
        Err(err) if err == "not_found" => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to stop pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let (flow_instance_id, _) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    if flow_instance_id == DEFAULT_FLOW_INSTANCE_ID {
        let instance = state
            .instances
            .get(DEFAULT_FLOW_INSTANCE_ID)
            .expect("default instance missing");
        match instance.delete_pipeline(&id).await {
            Ok(_) => {}
            Err(PipelineError::NotFound(_)) => {
                return (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response();
            }
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("failed to delete pipeline {id}: {err}"),
                )
                    .into_response();
            }
        }
    } else if let Some(worker) = state.worker(&flow_instance_id) {
        let _ = worker.delete_pipeline(&id).await;
    }

    if let Err(err) = state.storage.delete_pipeline(&id) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to remove pipeline {id} from storage: {err}"),
        )
            .into_response();
    }
    (StatusCode::OK, format!("pipeline {id} deleted")).into_response()
}

pub async fn list_pipelines(State(state): State<AppState>) -> impl IntoResponse {
    let mut runtime_status = HashMap::new();
    for (_, instance) in state.instances.instances_snapshot() {
        for snapshot in instance.list_pipelines() {
            runtime_status.insert(
                snapshot.definition.id().to_string(),
                status_label(snapshot.status),
            );
        }
    }
    for (id, worker) in state.workers.iter() {
        match worker.list_pipelines().await {
            Ok(items) => {
                for item in items {
                    runtime_status.insert(item.id, item.status);
                }
            }
            Err(err) => {
                tracing::error!(
                    flow_instance_id = %id,
                    error = %err,
                    "failed to list pipelines from worker"
                );
            }
        }
    }

    match state.storage.list_pipelines() {
        Ok(entries) => {
            let mut list = Vec::with_capacity(entries.len());
            for entry in entries {
                let mut spec = match storage_bridge::pipeline_request_from_stored(&entry) {
                    Ok(req) => req,
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("decode stored pipeline {}: {err}", entry.id),
                        )
                            .into_response();
                    }
                };
                let flow_instance_id =
                    match canonical_flow_instance_id(spec.flow_instance_id.as_deref()) {
                        Ok(id) => id,
                        Err(err) => {
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!(
                                    "invalid stored flow_instance_id for pipeline {}: {err}",
                                    entry.id
                                ),
                            )
                                .into_response();
                        }
                    };
                spec.flow_instance_id = Some(flow_instance_id.clone());

                let status = runtime_status
                    .get(&entry.id)
                    .cloned()
                    .unwrap_or_else(|| "stopped".to_string());
                list.push(ListPipelineItem {
                    id: entry.id,
                    status,
                    flow_instance_id,
                });
            }
            list.sort_by(|a, b| a.id.cmp(&b.id));
            Json(list).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list pipelines: {err}"),
        )
            .into_response(),
    }
}
