use super::protocol::{
    WorkerApplyPipelineRequest, WorkerApplyPipelineResponse, WorkerDesiredState,
    WorkerPipelineListItem,
};
use crate::pipeline::{build_pipeline_definition, status_label, validate_create_request};
use crate::stream::{
    CreateStreamRequest, build_schema_from_request, build_stream_decoder, build_stream_props,
    validate_memory_stream_topic, validate_stream_decoder_config,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::{delete, get, post},
};
use flow::pipeline::{PipelineError, PipelineStopMode};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct FlowWorkerState {
    instance: Arc<flow::FlowInstance>,
}

impl FlowWorkerState {
    pub fn new(instance: flow::FlowInstance) -> Self {
        Self {
            instance: Arc::new(instance),
        }
    }
}

pub fn build_worker_app(state: FlowWorkerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/internal/v1/pipelines", get(list_pipelines))
        .route("/internal/v1/pipelines/apply", post(apply_pipeline))
        .route("/internal/v1/pipelines/:id", delete(delete_pipeline))
        .route("/internal/v1/pipelines/:id/start", post(start_pipeline))
        .route("/internal/v1/pipelines/:id/stop", post(stop_pipeline))
        .route("/internal/v1/pipelines/:id/explain", get(explain_pipeline))
        .route(
            "/internal/v1/pipelines/:id/stats",
            get(collect_pipeline_stats),
        )
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn list_pipelines(State(state): State<FlowWorkerState>) -> impl IntoResponse {
    let items = state
        .instance
        .list_pipelines()
        .into_iter()
        .map(|snapshot| WorkerPipelineListItem {
            id: snapshot.definition.id().to_string(),
            status: status_label(snapshot.status),
            streams: snapshot.streams,
        })
        .collect::<Vec<_>>();
    (StatusCode::OK, Json(items))
}

async fn apply_pipeline(
    State(state): State<FlowWorkerState>,
    Json(req): Json<WorkerApplyPipelineRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_create_request(&req.pipeline) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    for topic in &req.memory_topics {
        let kind = match topic.kind {
            storage::StoredMemoryTopicKind::Bytes => flow::connector::MemoryTopicKind::Bytes,
            storage::StoredMemoryTopicKind::Collection => {
                flow::connector::MemoryTopicKind::Collection
            }
        };
        if let Err(err) = state
            .instance
            .declare_memory_topic(&topic.topic, kind, topic.capacity)
        {
            return (
                StatusCode::BAD_REQUEST,
                format!("declare memory topic {}: {err}", topic.topic),
            )
                .into_response();
        }
    }

    for cfg in &req.shared_mqtt_clients {
        if state.instance.get_shared_mqtt_client(&cfg.key).is_some() {
            continue;
        }
        if let Err(err) = state.instance.create_shared_mqtt_client(cfg.clone()).await {
            return (
                StatusCode::BAD_REQUEST,
                format!("create shared mqtt client {}: {err}", cfg.key),
            )
                .into_response();
        }
    }

    for stream in req.streams.values() {
        if let Err(resp) = ensure_stream_installed(state.instance.as_ref(), stream).await {
            return resp;
        }
    }

    let _ = state.instance.delete_pipeline(&req.pipeline.id).await;

    let encoder_registry = state.instance.encoder_registry();
    let definition = match build_pipeline_definition(
        &req.pipeline,
        encoder_registry.as_ref(),
        state.instance.as_ref(),
    ) {
        Ok(def) => def,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let create_req = flow::CreatePipelineRequest::new(definition);

    let result = match state.instance.create_pipeline(create_req) {
        Ok(result) => result,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("create pipeline {}: {err}", req.pipeline.id),
            )
                .into_response();
        }
    };

    if matches!(req.desired_state, WorkerDesiredState::Running)
        && let Err(err) = state.instance.start_pipeline(&req.pipeline.id)
    {
        return (
            StatusCode::BAD_REQUEST,
            format!("start pipeline {}: {err}", req.pipeline.id),
        )
            .into_response();
    }

    let status = status_label(result.snapshot.status);
    (StatusCode::OK, Json(WorkerApplyPipelineResponse { status })).into_response()
}

async fn ensure_stream_installed(
    instance: &flow::FlowInstance,
    req: &CreateStreamRequest,
) -> Result<(), axum::response::Response> {
    if instance.get_stream(&req.name).await.is_ok() {
        return Ok(());
    }

    let schema = match build_schema_from_request(req) {
        Ok(schema) => schema,
        Err(err) => return Err((StatusCode::BAD_REQUEST, err).into_response()),
    };
    let stream_props = match build_stream_props(&req.stream_type, &req.props) {
        Ok(props) => props,
        Err(err) => return Err((StatusCode::BAD_REQUEST, err).into_response()),
    };
    let decoder_registry = instance.decoder_registry();
    let decoder = match build_stream_decoder(req, decoder_registry.as_ref()) {
        Ok(config) => config,
        Err(err) => return Err((StatusCode::BAD_REQUEST, err).into_response()),
    };
    if let Err(err) = validate_stream_decoder_config(req, &decoder) {
        return Err((StatusCode::BAD_REQUEST, err).into_response());
    }
    if let flow::catalog::StreamProps::Memory(memory_props) = &stream_props
        && let Err(err) = validate_memory_stream_topic(req, memory_props)
    {
        return Err((StatusCode::BAD_REQUEST, err).into_response());
    }

    let mut definition = flow::catalog::StreamDefinition::new(
        req.name.clone(),
        Arc::new(schema),
        stream_props,
        decoder,
    );
    if let Some(cfg) = &req.eventtime {
        definition = definition.with_eventtime(flow::catalog::EventtimeDefinition::new(
            cfg.column.clone(),
            cfg.eventtime_type.clone(),
        ));
    }
    if let Some(sampler) = &req.sampler {
        definition = definition.with_sampler(sampler.clone());
    }

    match instance.create_stream(definition, req.shared).await {
        Ok(_) => Ok(()),
        Err(err) => Err((StatusCode::BAD_REQUEST, err.to_string()).into_response()),
    }
}

async fn start_pipeline(
    State(state): State<FlowWorkerState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.instance.start_pipeline(&id) {
        Ok(_) => (StatusCode::OK, format!("pipeline {id} started")).into_response(),
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to start pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

async fn stop_pipeline(
    State(state): State<FlowWorkerState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let timeout = Duration::from_millis(5_000);
    match state
        .instance
        .stop_pipeline(&id, PipelineStopMode::Quick, timeout)
        .await
    {
        Ok(_) => (StatusCode::OK, format!("pipeline {id} stopped")).into_response(),
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to stop pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

async fn delete_pipeline(
    State(state): State<FlowWorkerState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.instance.delete_pipeline(&id).await {
        Ok(_) => (StatusCode::OK, format!("pipeline {id} deleted")).into_response(),
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to delete pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

async fn explain_pipeline(
    State(state): State<FlowWorkerState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let explain = match state
        .instance
        .explain_pipeline(flow::ExplainPipelineTarget::Id(&id))
    {
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
    response
}

#[derive(Deserialize)]
#[serde(default)]
struct CollectStatsQuery {
    timeout_ms: u64,
}

impl Default for CollectStatsQuery {
    fn default() -> Self {
        Self { timeout_ms: 5_000 }
    }
}

async fn collect_pipeline_stats(
    State(state): State<FlowWorkerState>,
    Path(id): Path<String>,
    Query(query): Query<CollectStatsQuery>,
) -> impl IntoResponse {
    let timeout = Duration::from_millis(query.timeout_ms);
    match state.instance.collect_pipeline_stats(&id, timeout).await {
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
        Err(PipelineError::Runtime(err)) if err == flow::ProcessorError::Timeout.to_string() => (
            StatusCode::GATEWAY_TIMEOUT,
            json!({"error": format!("collect stats timeout for pipeline {id}")}).to_string(),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to collect pipeline {id} stats: {err}"),
        )
            .into_response(),
    }
}
