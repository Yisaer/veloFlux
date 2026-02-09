use crate::{MQTT_QOS, storage_bridge};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::IntoResponse,
};
use base64::Engine;
use flow::EncoderRegistry;
use flow::FlowInstance;
use flow::connector::SharedMqttClientConfig;
use flow::pipeline::{
    KuksaSinkProps, MemorySinkProps, MqttSinkProps, NopSinkProps, PipelineDefinition,
    PipelineError, PipelineOptions, PipelineStatus, PipelineStopMode, PlanCacheOptions,
    SinkDefinition, SinkProps, SinkType,
};
use flow::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use parser::SelectStmt;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use storage::{StorageError, StorageManager, StoredPipelineDesiredState, StoredPipelineRunState};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::FlowInstanceSpec;
use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstances};
use crate::worker_client::FlowWorkerClient;
use crate::worker_protocol::{
    WorkerApplyPipelineRequest, WorkerDesiredState, WorkerMemoryTopicSpec,
};

#[derive(Clone)]
pub struct AppState {
    pub instances: FlowInstances,
    pub storage: Arc<StorageManager>,
    pub workers: Arc<HashMap<String, FlowWorkerClient>>,
    pub declared_extra_instances: Arc<BTreeSet<String>>,
    pipeline_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}

impl AppState {
    pub fn new(
        instance: FlowInstance,
        storage: StorageManager,
        extra_flow_instances: Vec<FlowInstanceSpec>,
        extra_flow_worker_endpoints: Vec<(String, String)>,
    ) -> Result<Self, String> {
        let instances = FlowInstances::new(instance);
        let storage = Arc::new(storage);
        let mut declared_extra = BTreeSet::new();
        let mut workers = HashMap::new();
        let state = Self {
            instances,
            storage,
            workers: Arc::new(HashMap::new()),
            declared_extra_instances: Arc::new(BTreeSet::new()),
            pipeline_op_locks: Arc::new(Mutex::new(HashMap::new())),
        };

        for spec in extra_flow_instances {
            let id = spec.id.trim();
            if id.is_empty() {
                return Err("extra_flow_instances contains an empty id".to_string());
            }
            if id == DEFAULT_FLOW_INSTANCE_ID {
                return Err("extra_flow_instances must not include default".to_string());
            }
            if !declared_extra.insert(id.to_string()) {
                return Err(format!("duplicate flow instance id in config: {id}"));
            }
        }

        for (id, base_url) in extra_flow_worker_endpoints {
            if !declared_extra.contains(&id) {
                return Err(format!(
                    "flow worker endpoint provided for undeclared instance: {id}"
                ));
            }
            workers.insert(id, FlowWorkerClient::new(base_url));
        }
        for id in &declared_extra {
            if !workers.contains_key(id) {
                return Err(format!("missing flow worker endpoint for instance: {id}"));
            }
        }

        Ok(Self {
            workers: Arc::new(workers),
            declared_extra_instances: Arc::new(declared_extra),
            ..state
        })
    }

    pub fn is_declared_instance(&self, id: &str) -> bool {
        id == DEFAULT_FLOW_INSTANCE_ID || self.declared_extra_instances.contains(id)
    }

    pub fn worker(&self, id: &str) -> Option<FlowWorkerClient> {
        self.workers.get(id).cloned()
    }

    pub async fn bootstrap_from_storage(&self) -> Result<(), String> {
        crate::storage_bridge::hydrate_runtime_from_storage(self.storage.as_ref(), &self.instances)
            .await?;
        self.hydrate_workers_from_storage().await?;
        Ok(())
    }

    pub async fn try_acquire_pipeline_op(
        &self,
        pipeline_id: &str,
    ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        let semaphore = {
            let mut guard = self.pipeline_op_locks.lock().await;
            guard
                .entry(pipeline_id.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(1)))
                .clone()
        };
        semaphore.try_acquire_owned()
    }
}

async fn response_to_message(resp: axum::response::Response) -> String {
    let (parts, body) = resp.into_parts();
    let status = parts.status;
    let bytes = axum::body::to_bytes(body, 64 * 1024)
        .await
        .unwrap_or_default();
    let body = String::from_utf8_lossy(&bytes);
    format!("status={status} body={}", body.trim())
}

async fn apply_pipeline_to_worker_with_retry(
    worker: &FlowWorkerClient,
    req: &WorkerApplyPipelineRequest,
) -> Result<crate::worker_protocol::WorkerApplyPipelineResponse, String> {
    let mut backoff_ms = 50u64;
    let mut last_err = None;
    for attempt in 1..=8 {
        match worker.apply_pipeline(req).await {
            Ok(resp) => return Ok(resp),
            Err(err) => {
                let retryable = err.starts_with("worker request failed:");
                last_err = Some(err);
                if !retryable || attempt == 8 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(500);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "worker apply failed".to_string()))
}

impl AppState {
    async fn hydrate_workers_from_storage(&self) -> Result<(), String> {
        if self.workers.is_empty() {
            return Ok(());
        }

        let pipelines = self
            .storage
            .list_pipelines()
            .map_err(|e| format!("list pipelines from storage: {e}"))?;

        let mut applied = 0usize;
        for pipeline in pipelines {
            let req = match storage_bridge::pipeline_request_from_stored(&pipeline) {
                Ok(req) => req,
                Err(err) => {
                    tracing::error!(pipeline_id = %pipeline.id, error = %err, "failed to decode stored pipeline");
                    continue;
                }
            };

            let flow_instance_id = req
                .flow_instance_id
                .clone()
                .unwrap_or_else(|| DEFAULT_FLOW_INSTANCE_ID.to_string());
            if flow_instance_id == DEFAULT_FLOW_INSTANCE_ID {
                continue;
            }
            if !self.is_declared_instance(&flow_instance_id) {
                tracing::warn!(
                    pipeline_id = %pipeline.id,
                    flow_instance_id = %flow_instance_id,
                    "skipping worker pipeline hydrate: flow instance not declared by config"
                );
                continue;
            }

            let desired_state = match self
                .storage
                .get_pipeline_run_state(&pipeline.id)
                .map_err(|e| format!("read pipeline run state {}: {e}", pipeline.id))?
            {
                Some(state)
                    if matches!(state.desired_state, StoredPipelineDesiredState::Running) =>
                {
                    WorkerDesiredState::Running
                }
                _ => WorkerDesiredState::Stopped,
            };

            let (streams, shared_mqtt_clients, memory_topics) =
                match build_pipeline_context_payload(self, &pipeline.id, &req) {
                    Ok(payload) => payload,
                    Err(resp) => {
                        let msg = response_to_message(*resp).await;
                        tracing::error!(
                            pipeline_id = %pipeline.id,
                            flow_instance_id = %flow_instance_id,
                            error = %msg,
                            "failed to build worker apply context from storage"
                        );
                        continue;
                    }
                };

            let Some(worker) = self.worker(&flow_instance_id) else {
                tracing::error!(
                    pipeline_id = %pipeline.id,
                    flow_instance_id = %flow_instance_id,
                    "missing worker client for declared instance"
                );
                continue;
            };

            let worker_req = WorkerApplyPipelineRequest {
                pipeline: req.clone(),
                pipeline_raw_json: pipeline.raw_json.clone(),
                streams,
                shared_mqtt_clients,
                memory_topics,
                desired_state,
            };

            match apply_pipeline_to_worker_with_retry(&worker, &worker_req).await {
                Ok(resp) => {
                    applied += 1;
                    tracing::info!(
                        pipeline_id = %pipeline.id,
                        flow_instance_id = %flow_instance_id,
                        status = %resp.status,
                        "hydrated pipeline into worker"
                    );
                }
                Err(err) => {
                    tracing::error!(
                        pipeline_id = %pipeline.id,
                        flow_instance_id = %flow_instance_id,
                        error = %err,
                        "failed to hydrate pipeline into worker"
                    );
                }
            }
        }

        tracing::info!(count = applied, "worker pipeline hydration completed");
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreatePipelineRequest {
    pub id: String,
    #[serde(default)]
    pub flow_instance_id: Option<String>,
    pub sql: String,
    #[serde(default)]
    pub sinks: Vec<CreatePipelineSinkRequest>,
    #[serde(default)]
    pub options: PipelineOptionsRequest,
}

#[derive(Deserialize, Serialize)]
pub struct UpsertPipelineRequest {
    pub sql: String,
    #[serde(default)]
    pub sinks: Vec<CreatePipelineSinkRequest>,
    #[serde(default)]
    pub options: PipelineOptionsRequest,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct PipelineOptionsRequest {
    #[serde(rename = "data_channel_capacity")]
    pub data_channel_capacity: usize,
    #[serde(rename = "plan_cache")]
    pub plan_cache: PlanCacheOptionsRequest,
    #[serde(default)]
    pub eventtime: EventtimeOptionsRequest,
}

impl Default for PipelineOptionsRequest {
    fn default() -> Self {
        Self {
            data_channel_capacity: 16,
            plan_cache: PlanCacheOptionsRequest::default(),
            eventtime: EventtimeOptionsRequest::default(),
        }
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct EventtimeOptionsRequest {
    pub enabled: bool,
    pub late_tolerance_ms: u64,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct PlanCacheOptionsRequest {
    pub enabled: bool,
}

#[derive(Serialize)]
pub struct CreatePipelineResponse {
    pub id: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct ListPipelineItem {
    pub id: String,
    pub status: String,
    pub flow_instance_id: String,
}

#[derive(Serialize)]
pub struct GetPipelineResponse {
    pub id: String,
    pub status: String,
    pub spec: CreatePipelineRequest,
}

#[derive(Serialize)]
pub struct BuildPipelineContextResponse {
    pub pipeline: CreatePipelineRequest,
    pub streams: BTreeMap<String, crate::stream::CreateStreamRequest>,
    pub shared_mqtt_clients: Vec<SharedMqttClientConfig>,
    pub memory_topics: Vec<WorkerMemoryTopicSpec>,
}

#[derive(Deserialize)]
#[serde(default)]
pub(crate) struct CollectStatsQuery {
    timeout_ms: u64,
}

impl Default for CollectStatsQuery {
    fn default() -> Self {
        Self { timeout_ms: 5_000 }
    }
}

#[derive(Deserialize)]
#[serde(default)]
pub(crate) struct StopPipelineQuery {
    mode: String,
    timeout_ms: u64,
}

impl Default for StopPipelineQuery {
    fn default() -> Self {
        Self {
            mode: "quick".to_string(),
            timeout_ms: 5_000,
        }
    }
}

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

#[derive(Deserialize, Serialize, Clone)]
pub struct CreatePipelineSinkRequest {
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub sink_type: String,
    #[serde(default)]
    pub props: SinkPropsRequest,
    #[serde(rename = "common_sink_props", default)]
    pub common: CommonSinkPropsRequest,
    #[serde(default)]
    pub encoder: EncoderConfigRequest,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct EncoderConfigRequest {
    #[serde(rename = "type")]
    pub encode_type: String,
    pub props: JsonMap<String, JsonValue>,
}

impl EncoderConfigRequest {
    fn new(encode_type: impl Into<String>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            encode_type: encode_type.into(),
            props,
        }
    }
}

impl Default for EncoderConfigRequest {
    fn default() -> Self {
        Self::new("json", JsonMap::new())
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct SinkPropsRequest {
    #[serde(flatten)]
    fields: JsonMap<String, JsonValue>,
}

impl SinkPropsRequest {
    fn to_value(&self) -> JsonValue {
        JsonValue::Object(self.fields.clone())
    }
}

impl CommonSinkPropsRequest {
    fn to_common_props(&self) -> CommonSinkProps {
        let duration = self.batch_duration_ms.map(Duration::from_millis);
        CommonSinkProps {
            batch_count: self.batch_count,
            batch_duration: duration,
        }
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct MqttSinkPropsRequest {
    pub broker_url: Option<String>,
    pub topic: Option<String>,
    pub qos: Option<u8>,
    pub retain: Option<bool>,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct NopSinkPropsRequest {
    pub log: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct MemorySinkPropsRequest {
    pub topic: String,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct CommonSinkPropsRequest {
    #[serde(rename = "batch_count")]
    pub batch_count: Option<usize>,
    #[serde(rename = "batch_duration")]
    pub batch_duration_ms: Option<u64>,
}

#[derive(Deserialize)]
struct KuksaSinkPropsRequest {
    pub addr: Option<String>,
    #[serde(rename = "vss_path")]
    pub vss_path: Option<String>,
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
        let apply_result = match apply_pipeline_in_worker(
            &state,
            &flow_instance_id,
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

        if let Some(plan_cache) = apply_result.plan_cache.as_ref()
            && !plan_cache.hit
            && let Some(b64) = plan_cache.logical_plan_ir_b64.as_deref()
        {
            match base64::engine::general_purpose::STANDARD.decode(b64) {
                Ok(bytes) => match storage_bridge::build_plan_snapshot(
                    state.storage.as_ref(),
                    &stored.id,
                    &stored.raw_json,
                    &plan_cache.streams,
                    bytes,
                )
                .and_then(|record| {
                    state
                        .storage
                        .put_plan_snapshot(record)
                        .map_err(|e| e.to_string())
                }) {
                    Ok(()) => {}
                    Err(err) => {
                        tracing::error!(
                            pipeline_id = %stored.id,
                            error = %err,
                            "failed to persist plan cache snapshot for remote pipeline"
                        );
                    }
                },
                Err(err) => {
                    tracing::error!(
                        pipeline_id = %stored.id,
                        error = %err,
                        "failed to decode worker plan cache logical IR"
                    );
                }
            }
        }

        tracing::info!(pipeline_id = %stored.id, flow_instance_id = %flow_instance_id, "pipeline created (remote)");
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

    let build_result = match instance.create_pipeline(
        flow::CreatePipelineRequest::new(definition).with_plan_cache_inputs(
            flow::planner::plan_cache::PlanCacheInputs {
                pipeline_raw_json: stored.raw_json.clone(),
                streams_raw_json: Vec::new(),
                snapshot: None,
            },
        ),
    ) {
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

    let logical_ir = build_result
        .plan_cache
        .and_then(|result| result.logical_plan_ir);
    if let Some(logical_ir) = logical_ir {
        match storage_bridge::build_plan_snapshot(
            state.storage.as_ref(),
            &stored.id,
            &stored.raw_json,
            &build_result.snapshot.streams,
            logical_ir,
        )
        .and_then(|record| {
            state
                .storage
                .put_plan_snapshot(record)
                .map_err(|e| e.to_string())
        }) {
            Ok(()) => {}
            Err(err) => {
                let _ = instance.delete_pipeline(&stored.id).await;
                let _ = state.storage.delete_pipeline(&stored.id);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "pipeline {} created but failed to persist plan cache: {err}",
                        req.id
                    ),
                )
                    .into_response();
            }
        }
    }

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

        let apply_result = match apply_pipeline_in_worker(
            &state,
            &flow_instance_id,
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

        if let Some(plan_cache) = apply_result.plan_cache.as_ref()
            && !plan_cache.hit
            && let Some(b64) = plan_cache.logical_plan_ir_b64.as_deref()
        {
            match base64::engine::general_purpose::STANDARD.decode(b64) {
                Ok(bytes) => match storage_bridge::build_plan_snapshot(
                    state.storage.as_ref(),
                    &stored.id,
                    &stored.raw_json,
                    &plan_cache.streams,
                    bytes,
                )
                .and_then(|record| {
                    state
                        .storage
                        .put_plan_snapshot(record)
                        .map_err(|e| e.to_string())
                }) {
                    Ok(()) => {}
                    Err(err) => tracing::error!(
                        pipeline_id = %stored.id,
                        error = %err,
                        "failed to persist plan cache snapshot for remote pipeline"
                    ),
                },
                Err(err) => tracing::error!(
                    pipeline_id = %stored.id,
                    error = %err,
                    "failed to decode worker plan cache logical IR"
                ),
            }
        }

        tracing::info!(pipeline_id = %id, flow_instance_id = %flow_instance_id, "pipeline upserted (remote)");
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

    let build_result = match instance.create_pipeline(
        flow::CreatePipelineRequest::new(definition).with_plan_cache_inputs(
            flow::planner::plan_cache::PlanCacheInputs {
                pipeline_raw_json: stored.raw_json.clone(),
                streams_raw_json: Vec::new(),
                snapshot: None,
            },
        ),
    ) {
        Ok(result) => result,
        Err(err) => {
            let _ = state.storage.delete_pipeline(&id);
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to create pipeline {id}: {err}"),
            )
                .into_response();
        }
    };

    let logical_ir = build_result
        .plan_cache
        .and_then(|result| result.logical_plan_ir);
    if let Some(logical_ir) = logical_ir {
        match storage_bridge::build_plan_snapshot(
            state.storage.as_ref(),
            &stored.id,
            &stored.raw_json,
            &build_result.snapshot.streams,
            logical_ir,
        )
        .and_then(|record| {
            state
                .storage
                .put_plan_snapshot(record)
                .map_err(|e| e.to_string())
        }) {
            Ok(()) => {}
            Err(err) => {
                let _ = instance.delete_pipeline(&id).await;
                let _ = state.storage.delete_pipeline(&id);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("pipeline {id} created but failed to persist plan cache: {err}"),
                )
                    .into_response();
            }
        }
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

fn select_stmt_from_sql(
    instance: &FlowInstance,
    pipeline_id: &str,
    sql: &str,
) -> Result<SelectStmt, String> {
    parser::parse_sql_with_registries(
        sql,
        instance.aggregate_registry(),
        instance.stateful_registry(),
    )
    .map_err(|err| format!("parse pipeline {pipeline_id} sql: {err}"))
}

fn connector_keys_from_pipeline_sinks(
    pipeline_id: &str,
    sinks: &[CreatePipelineSinkRequest],
) -> Result<BTreeSet<String>, String> {
    let mut keys = BTreeSet::new();
    for sink in sinks {
        if !sink.sink_type.eq_ignore_ascii_case("mqtt") {
            continue;
        }
        let mqtt_props: MqttSinkPropsRequest = serde_json::from_value(sink.props.to_value())
            .map_err(|err| format!("decode pipeline {pipeline_id} mqtt sink props: {err}"))?;
        if let Some(key) = mqtt_props.connector_key
            && !key.trim().is_empty()
        {
            keys.insert(key);
        }
    }
    Ok(keys)
}

fn connector_key_from_stream(
    req: &crate::stream::CreateStreamRequest,
) -> Result<Option<String>, String> {
    if !req.stream_type.eq_ignore_ascii_case("mqtt") {
        return Ok(None);
    }
    let mqtt_props: crate::stream::MqttStreamPropsRequest =
        serde_json::from_value(JsonValue::Object(req.props.fields.clone()))
            .map_err(|err| format!("decode mqtt stream {} props: {err}", req.name))?;
    Ok(mqtt_props
        .connector_key
        .filter(|key| !key.trim().is_empty()))
}

type PipelineContextPayload = (
    BTreeMap<String, crate::stream::CreateStreamRequest>,
    Vec<SharedMqttClientConfig>,
    Vec<WorkerMemoryTopicSpec>,
);
type PipelineContextError = Box<axum::response::Response>;

fn build_pipeline_context_payload(
    state: &AppState,
    pipeline_id: &str,
    pipeline_req: &CreatePipelineRequest,
) -> Result<PipelineContextPayload, PipelineContextError> {
    let default_instance = state.instances.default_instance();
    let select_stmt =
        select_stmt_from_sql(default_instance.as_ref(), pipeline_id, &pipeline_req.sql)
            .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;

    let mut stream_names = select_stmt
        .source_infos
        .iter()
        .map(|source| source.name.clone())
        .collect::<Vec<_>>();
    stream_names.sort();
    stream_names.dedup();

    let mut connector_keys = connector_keys_from_pipeline_sinks(pipeline_id, &pipeline_req.sinks)
        .map_err(|err| {
        Box::new(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to inspect pipeline {pipeline_id} sink props: {err}"),
            )
                .into_response(),
        )
    })?;

    let mut streams = BTreeMap::new();
    let mut memory_topics = BTreeSet::new();
    for stream_name in stream_names {
        let stored_stream = match state.storage.get_stream(&stream_name) {
            Ok(Some(stream)) => stream,
            Ok(None) => {
                return Err(Box::new(
                    (
                        StatusCode::BAD_REQUEST,
                        format!("stream {stream_name} missing from storage"),
                    )
                        .into_response(),
                ));
            }
            Err(err) => {
                return Err(Box::new(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to read stream {stream_name} from storage: {err}"),
                    )
                        .into_response(),
                ));
            }
        };

        let stream_req: crate::stream::CreateStreamRequest =
            match serde_json::from_str(&stored_stream.raw_json) {
                Ok(req) => req,
                Err(err) => {
                    return Err(Box::new(
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("decode stored stream {stream_name}: {err}"),
                        )
                            .into_response(),
                    ));
                }
            };

        match connector_key_from_stream(&stream_req) {
            Ok(Some(key)) => {
                connector_keys.insert(key);
            }
            Ok(None) => {}
            Err(err) => {
                return Err(Box::new(
                    (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
                ));
            }
        }

        if stream_req.stream_type.eq_ignore_ascii_case("memory") {
            let props: crate::stream::MemoryStreamPropsRequest =
                serde_json::from_value(JsonValue::Object(stream_req.props.fields.clone()))
                    .map_err(|err| {
                        Box::new(
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("decode memory stream {stream_name} props: {err}"),
                            )
                                .into_response(),
                        )
                    })?;
            if let Some(topic) = props.topic.filter(|t| !t.trim().is_empty()) {
                memory_topics.insert(topic);
            }
        }

        streams.insert(stream_name, stream_req);
    }

    for sink in &pipeline_req.sinks {
        if !sink.sink_type.eq_ignore_ascii_case("memory") {
            continue;
        }
        let props: MemorySinkPropsRequest =
            serde_json::from_value(sink.props.to_value()).map_err(|err| {
                Box::new(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("decode pipeline {pipeline_id} memory sink props: {err}"),
                    )
                        .into_response(),
                )
            })?;
        let topic = props.topic.trim();
        if !topic.is_empty() {
            memory_topics.insert(topic.to_string());
        }
    }

    let mut shared_mqtt_clients = Vec::with_capacity(connector_keys.len());
    for key in connector_keys {
        let stored = match state.storage.get_mqtt_config(&key) {
            Ok(Some(cfg)) => cfg,
            Ok(None) => {
                return Err(Box::new(
                    (
                        StatusCode::BAD_REQUEST,
                        format!("shared mqtt client config {key} missing from storage"),
                    )
                        .into_response(),
                ));
            }
            Err(err) => {
                return Err(Box::new(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "failed to read shared mqtt client config {key} from storage: {err}"
                        ),
                    )
                        .into_response(),
                ));
            }
        };

        let cfg: SharedMqttClientConfig =
            serde_json::from_str(&stored.raw_json).map_err(|err| {
                Box::new(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("decode stored shared mqtt client config {key}: {err}"),
                    )
                        .into_response(),
                )
            })?;
        shared_mqtt_clients.push(cfg);
    }

    let mut memory_topic_specs = Vec::with_capacity(memory_topics.len());
    for topic in memory_topics {
        let stored = match state.storage.get_memory_topic(&topic) {
            Ok(Some(topic)) => topic,
            Ok(None) => {
                return Err(Box::new(
                    (
                        StatusCode::BAD_REQUEST,
                        format!("memory topic {topic} missing from storage"),
                    )
                        .into_response(),
                ));
            }
            Err(err) => {
                return Err(Box::new(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to read memory topic {topic} from storage: {err}"),
                    )
                        .into_response(),
                ));
            }
        };
        memory_topic_specs.push(WorkerMemoryTopicSpec {
            topic: stored.topic,
            kind: stored.kind,
            capacity: stored.capacity,
        });
    }

    Ok((streams, shared_mqtt_clients, memory_topic_specs))
}

async fn apply_pipeline_in_worker(
    state: &AppState,
    flow_instance_id: &str,
    pipeline_id: &str,
    pipeline_req: &CreatePipelineRequest,
    pipeline_raw_json: &str,
    desired_state: WorkerDesiredState,
) -> Result<crate::worker_protocol::WorkerApplyPipelineResponse, axum::response::Response> {
    let worker = state.worker(flow_instance_id).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!("flow instance {flow_instance_id} is not declared by config"),
        )
            .into_response()
    })?;

    let (streams, shared_mqtt_clients, memory_topics) =
        build_pipeline_context_payload(state, pipeline_id, pipeline_req).map_err(|resp| *resp)?;

    let req = WorkerApplyPipelineRequest {
        pipeline: pipeline_req.clone(),
        pipeline_raw_json: pipeline_raw_json.to_string(),
        streams,
        shared_mqtt_clients,
        memory_topics,
        desired_state,
    };
    worker
        .apply_pipeline(&req)
        .await
        .map_err(|err| (StatusCode::BAD_REQUEST, err).into_response())
}

pub async fn build_pipeline_context_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let (_flow_instance_id, pipeline_req) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    let (streams, shared_mqtt_clients, memory_topics) =
        match build_pipeline_context_payload(&state, &id, &pipeline_req) {
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
                &state,
                &flow_instance_id,
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
                tracing::error!(flow_instance_id = %id, error = %err, "failed to list pipelines from worker");
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

pub(crate) fn validate_create_request(req: &CreatePipelineRequest) -> Result<(), String> {
    if req.id.trim().is_empty() {
        return Err("pipeline id must not be empty".to_string());
    }
    if req.sql.trim().is_empty() {
        return Err("pipeline sql must not be empty".to_string());
    }
    if req.sinks.is_empty() {
        return Err("pipeline must define at least one sink".to_string());
    }
    if req.options.data_channel_capacity == 0 {
        return Err("options.data_channel_capacity must be greater than 0".to_string());
    }
    Ok(())
}

pub(crate) fn build_pipeline_definition(
    req: &CreatePipelineRequest,
    encoder_registry: &EncoderRegistry,
    instance: &flow::FlowInstance,
) -> Result<PipelineDefinition, String> {
    let mut sinks = Vec::with_capacity(req.sinks.len());
    for (index, sink_req) in req.sinks.iter().enumerate() {
        let sink_id = sink_req
            .id
            .clone()
            .unwrap_or_else(|| format!("{}_sink_{index}", req.id));
        let sink_type = sink_req.sink_type.to_ascii_lowercase();
        let sink_definition = match sink_type.as_str() {
            "mqtt" => {
                let mqtt_props: MqttSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid mqtt sink props: {err}"))?;
                let broker = mqtt_props
                    .broker_url
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| "mqtt sink requires broker_url".to_string())?;
                let topic = mqtt_props
                    .topic
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| "mqtt sink requires topic".to_string())?;
                let qos = mqtt_props.qos.unwrap_or(MQTT_QOS);
                let retain = mqtt_props.retain.unwrap_or(false);

                let mut props = MqttSinkProps::new(broker, topic, qos).with_retain(retain);
                if let Some(client_id) = mqtt_props.client_id {
                    props = props.with_client_id(client_id);
                }
                if let Some(connector_key) = mqtt_props.connector_key {
                    props = props.with_connector_key(connector_key);
                }
                SinkDefinition::new(sink_id.clone(), SinkType::Mqtt, SinkProps::Mqtt(props))
            }
            "nop" => {
                let nop_props: NopSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid nop sink props: {err}"))?;
                SinkDefinition::new(
                    sink_id.clone(),
                    SinkType::Nop,
                    SinkProps::Nop(NopSinkProps {
                        log: nop_props.log.unwrap_or(false),
                    }),
                )
            }
            "kuksa" => {
                let kuksa_props: KuksaSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid kuksa sink props: {err}"))?;
                let addr = kuksa_props
                    .addr
                    .ok_or_else(|| "kuksa sink props missing addr".to_string())?;
                let vss_path = kuksa_props
                    .vss_path
                    .ok_or_else(|| "kuksa sink props missing vss_path".to_string())?;
                SinkDefinition::new(
                    sink_id.clone(),
                    SinkType::Kuksa,
                    SinkProps::Kuksa(KuksaSinkProps { addr, vss_path }),
                )
            }
            "memory" => {
                let memory_props: MemorySinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid memory sink props: {err}"))?;
                let topic = memory_props.topic;
                if topic.trim().is_empty() {
                    return Err("memory sink requires topic".to_string());
                }

                let expects_collection = sink_req.encoder.encode_type.eq_ignore_ascii_case("none");
                let expected_kind = if expects_collection {
                    flow::connector::MemoryTopicKind::Collection
                } else {
                    flow::connector::MemoryTopicKind::Bytes
                };
                let actual_kind = instance
                    .memory_topic_kind(&topic)
                    .ok_or_else(|| format!("memory topic `{topic}` not declared"))?;
                if actual_kind != expected_kind {
                    return Err(format!(
                        "memory topic `{topic}` kind mismatch: expected {}, got {}",
                        expected_kind, actual_kind
                    ));
                }

                SinkDefinition::new(
                    sink_id.clone(),
                    SinkType::Memory,
                    SinkProps::Memory(MemorySinkProps::new(topic)),
                )
            }
            other => return Err(format!("unsupported sink type: {other}")),
        };

        let sink_definition = match sink_definition.sink_type {
            SinkType::Kuksa => {
                let encoder_config = SinkEncoderConfig::new("none", JsonMap::new());
                sink_definition.with_encoder(encoder_config)
            }
            SinkType::Memory if sink_req.encoder.encode_type.eq_ignore_ascii_case("none") => {
                let encoder_config = SinkEncoderConfig::new("none", sink_req.encoder.props.clone());
                sink_definition.with_encoder(encoder_config)
            }
            _ => {
                let encoder_kind = sink_req.encoder.encode_type.clone();
                if !encoder_registry.is_registered(&encoder_kind) {
                    return Err(format!("encoder kind `{encoder_kind}` not registered"));
                }
                let encoder_config =
                    SinkEncoderConfig::new(encoder_kind, sink_req.encoder.props.clone());
                sink_definition.with_encoder(encoder_config)
            }
        }
        .with_common_props(sink_req.common.to_common_props());
        sinks.push(sink_definition);
    }
    let options = PipelineOptions {
        data_channel_capacity: req.options.data_channel_capacity,
        plan_cache: PlanCacheOptions {
            enabled: req.options.plan_cache.enabled,
        },
        eventtime: flow::pipeline::EventtimeOptions {
            enabled: req.options.eventtime.enabled,
            late_tolerance: Duration::from_millis(req.options.eventtime.late_tolerance_ms),
        },
    };
    Ok(PipelineDefinition::new(req.id.clone(), req.sql.clone(), sinks).with_options(options))
}

pub(crate) fn status_label(status: PipelineStatus) -> String {
    match status {
        PipelineStatus::Stopped => "stopped".to_string(),
        PipelineStatus::Running => "running".to_string(),
    }
}
