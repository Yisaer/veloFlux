use super::protocol::{
    WorkerApplyPipelineRequest, WorkerApplyPipelineResponse, WorkerDesiredState,
    WorkerPipelineListItem,
};
use crate::mqtt_client::shared_mqtt_config_eq;
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
use flow::connector::{ConnectorError, SharedMqttClientConfig};
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
        .route(
            "/internal/v1/mqtt/clients/:key",
            delete(delete_shared_mqtt_client),
        )
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

    for stream in req.streams.values() {
        if let Err(resp) = ensure_stream_installed(state.instance.as_ref(), stream).await {
            return resp;
        }
    }

    let _ = state.instance.delete_pipeline(&req.pipeline.id).await;

    if let Err(err) =
        reconcile_shared_mqtt_clients(state.instance.as_ref(), &req.shared_mqtt_clients).await
    {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

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

async fn reconcile_shared_mqtt_clients(
    instance: &flow::FlowInstance,
    shared_mqtt_clients: &[SharedMqttClientConfig],
) -> Result<(), String> {
    for cfg in shared_mqtt_clients {
        if let Some(existing) = instance.get_shared_mqtt_client(&cfg.key) {
            if shared_mqtt_config_eq(&existing, cfg) {
                continue;
            }
            instance
                .drop_shared_mqtt_client(&cfg.key)
                .map_err(|err| format!("replace shared mqtt client {}: {err}", cfg.key))?;
        }

        instance
            .create_shared_mqtt_client(cfg.clone())
            .await
            .map_err(|err| format!("create shared mqtt client {}: {err}", cfg.key))?;
    }
    Ok(())
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

async fn delete_shared_mqtt_client(
    State(state): State<FlowWorkerState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    match state.instance.drop_shared_mqtt_client(&key) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(flow::FlowInstanceError::Connector(ConnectorError::NotFound(_))) => (
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

#[cfg(test)]
mod tests {
    use super::{FlowWorkerState, build_worker_app, reconcile_shared_mqtt_clients};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use flow::connector::SharedMqttClientConfig;
    use tower::util::ServiceExt;

    fn shared_mqtt_cfg(key: &str, topic: &str, client_id: &str) -> SharedMqttClientConfig {
        SharedMqttClientConfig {
            key: key.to_string(),
            broker_url: "tcp://127.0.0.1:1883".to_string(),
            topic: topic.to_string(),
            client_id: client_id.to_string(),
            qos: 0,
            max_packet_size: None,
        }
    }

    #[tokio::test]
    async fn reconcile_shared_mqtt_clients_replaces_stale_config() {
        let instance = crate::new_default_flow_instance();
        let original = shared_mqtt_cfg("shared", "topic/old", "client_old");
        let updated = shared_mqtt_cfg("shared", "topic/new", "client_new");

        instance
            .create_shared_mqtt_client(original)
            .await
            .expect("create original shared mqtt client");

        reconcile_shared_mqtt_clients(&instance, std::slice::from_ref(&updated))
            .await
            .expect("reconcile shared mqtt client");

        let actual = instance
            .get_shared_mqtt_client("shared")
            .expect("shared mqtt client present after reconcile");
        assert_eq!(actual.key, updated.key);
        assert_eq!(actual.broker_url, updated.broker_url);
        assert_eq!(actual.topic, updated.topic);
        assert_eq!(actual.client_id, updated.client_id);
        assert_eq!(actual.qos, updated.qos);
        assert_eq!(actual.max_packet_size, updated.max_packet_size);
    }

    #[tokio::test]
    async fn delete_shared_mqtt_client_endpoint_returns_not_found_after_drop() {
        let instance = crate::new_default_flow_instance();
        let cfg = shared_mqtt_cfg("shared", "topic/a", "client_a");
        instance
            .create_shared_mqtt_client(cfg)
            .await
            .expect("create shared mqtt client");

        let app = build_worker_app(FlowWorkerState::new(instance));

        let first = app
            .clone()
            .oneshot(
                Request::delete("/internal/v1/mqtt/clients/shared")
                    .body(Body::empty())
                    .expect("first request"),
            )
            .await
            .expect("first response");
        assert_eq!(first.status(), StatusCode::NO_CONTENT);

        let second = app
            .oneshot(
                Request::delete("/internal/v1/mqtt/clients/shared")
                    .body(Body::empty())
                    .expect("second request"),
            )
            .await
            .expect("second response");
        assert_eq!(second.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(second.into_body(), 64 * 1024)
            .await
            .expect("read second body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 body"),
            "shared mqtt client shared not found"
        );
    }
}
