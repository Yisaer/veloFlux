use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use std::collections::BTreeSet;
use storage::{
    MetadataExportSnapshot, StoredMemoryTopic, StoredMqttClientConfig, StoredPipelineRunState,
};
use tokio::sync::TryAcquireError;

use crate::audit::ResourceMutationLog;
use crate::export::{
    ExportBundleV1, ExportMemoryTopic, ExportPipelineRunState, build_export_bundle,
};
use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::pipeline::{AppState, CreatePipelineRequest, validate_create_request};
use crate::storage_bridge;
use crate::stream::CreateStreamRequest;

#[derive(Serialize)]
pub struct ImportStorageResponse {
    pub applied_to_runtime: bool,
    pub imported_resource_counts: ImportResourceCounts,
    pub previous_bundle: ExportBundleV1,
}

#[derive(Serialize)]
pub struct ImportResourceCounts {
    pub memory_topics: usize,
    pub shared_mqtt_clients: usize,
    pub streams: usize,
    pub pipelines: usize,
    pub pipeline_run_states: usize,
}

pub async fn import_storage_handler(
    State(state): State<AppState>,
    Json(bundle): Json<ExportBundleV1>,
) -> impl IntoResponse {
    let audit = ResourceMutationLog::new("storage", "import", "metadata_bundle", None);
    let _import_export_permit = match state.try_acquire_import_export_op() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return import_export_busy_response(),
        Err(TryAcquireError::Closed) => {
            let err = "import/export operation guard closed".to_string();
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    let snapshot = match validate_and_build_snapshot(&bundle) {
        Ok(snapshot) => snapshot,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let previous_bundle = match build_export_bundle(state.storage.as_ref()) {
        Ok(bundle) => bundle,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    let imported_resource_counts = ImportResourceCounts {
        memory_topics: snapshot.memory_topics.len(),
        shared_mqtt_clients: snapshot.mqtt_configs.len(),
        streams: snapshot.streams.len(),
        pipelines: snapshot.pipelines.len(),
        pipeline_run_states: snapshot.pipeline_run_states.len(),
    };

    if let Err(err) = state.storage.replace_metadata_snapshot(snapshot) {
        let err = format!("replace metadata snapshot in storage: {err}");
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }

    audit.log_success();
    (
        StatusCode::OK,
        Json(ImportStorageResponse {
            applied_to_runtime: false,
            imported_resource_counts,
            previous_bundle,
        }),
    )
        .into_response()
}

fn validate_and_build_snapshot(bundle: &ExportBundleV1) -> Result<MetadataExportSnapshot, String> {
    let mut memory_topics = Vec::with_capacity(bundle.resources.memory_topics.len());
    let mut memory_topic_names = BTreeSet::new();
    for topic in &bundle.resources.memory_topics {
        validate_memory_topic(topic, &mut memory_topic_names)?;
        memory_topics.push(StoredMemoryTopic {
            topic: topic.topic.trim().to_string(),
            kind: topic.kind.clone(),
            capacity: topic.capacity,
        });
    }

    let mut mqtt_configs = Vec::with_capacity(bundle.resources.shared_mqtt_clients.len());
    let mut mqtt_keys = BTreeSet::new();
    for cfg in &bundle.resources.shared_mqtt_clients {
        let key = cfg.key.trim();
        if key.is_empty() {
            return Err("shared mqtt client key must not be empty".to_string());
        }
        if !mqtt_keys.insert(key.to_string()) {
            return Err(format!("duplicate shared mqtt client key in bundle: {key}"));
        }
        let raw_json = serde_json::to_string(cfg)
            .map_err(|err| format!("serialize shared mqtt client config {key}: {err}"))?;
        mqtt_configs.push(StoredMqttClientConfig {
            key: cfg.key.clone(),
            raw_json,
        });
    }

    let mut streams = Vec::with_capacity(bundle.resources.streams.len());
    let mut stream_names = BTreeSet::new();
    for req in &bundle.resources.streams {
        validate_stream_request(req, &mut stream_names)?;
        streams.push(storage_bridge::stored_stream_from_request(req)?);
    }

    let mut pipelines = Vec::with_capacity(bundle.resources.pipelines.len());
    let mut pipeline_ids = BTreeSet::new();
    for req in &bundle.resources.pipelines {
        let normalized = normalize_pipeline_request(req)?;
        let id = normalized.id.clone();
        if !pipeline_ids.insert(id.clone()) {
            return Err(format!("duplicate pipeline id in bundle: {id}"));
        }
        pipelines.push(storage_bridge::stored_pipeline_from_request(&normalized)?);
    }

    let mut pipeline_run_states = Vec::with_capacity(bundle.resources.pipeline_run_states.len());
    let mut state_ids = BTreeSet::new();
    for run_state in &bundle.resources.pipeline_run_states {
        validate_pipeline_run_state(run_state, &pipeline_ids, &mut state_ids)?;
        pipeline_run_states.push(StoredPipelineRunState {
            pipeline_id: run_state.pipeline_id.clone(),
            desired_state: run_state.desired_state.clone(),
        });
    }

    Ok(MetadataExportSnapshot {
        streams,
        pipelines,
        pipeline_run_states,
        mqtt_configs,
        memory_topics,
    })
}

fn validate_memory_topic(
    topic: &ExportMemoryTopic,
    names: &mut BTreeSet<String>,
) -> Result<(), String> {
    let topic_name = topic.topic.trim();
    if topic_name.is_empty() {
        return Err("memory topic name must not be empty".to_string());
    }
    if topic.capacity == 0 {
        return Err(format!(
            "memory topic {} capacity must be greater than 0",
            topic.topic
        ));
    }
    if !names.insert(topic_name.to_string()) {
        return Err(format!("duplicate memory topic in bundle: {topic_name}"));
    }
    Ok(())
}

fn validate_stream_request(
    req: &CreateStreamRequest,
    stream_names: &mut BTreeSet<String>,
) -> Result<(), String> {
    if req.name.trim().is_empty() {
        return Err("stream name must not be empty".to_string());
    }
    if !stream_names.insert(req.name.clone()) {
        return Err(format!("duplicate stream name in bundle: {}", req.name));
    }
    Ok(())
}

fn normalize_pipeline_request(
    req: &CreatePipelineRequest,
) -> Result<CreatePipelineRequest, String> {
    let mut normalized = req.clone();
    normalized.flow_instance_id =
        Some(canonical_flow_instance_id(req.flow_instance_id.as_deref())?);
    validate_create_request(&normalized)?;
    Ok(normalized)
}

fn validate_pipeline_run_state(
    run_state: &ExportPipelineRunState,
    pipeline_ids: &BTreeSet<String>,
    state_ids: &mut BTreeSet<String>,
) -> Result<(), String> {
    if run_state.pipeline_id.trim().is_empty() {
        return Err("pipeline_run_state pipeline_id must not be empty".to_string());
    }
    if !state_ids.insert(run_state.pipeline_id.clone()) {
        return Err(format!(
            "duplicate pipeline_run_state entry in bundle: {}",
            run_state.pipeline_id
        ));
    }
    if !pipeline_ids.contains(&run_state.pipeline_id) {
        return Err(format!(
            "pipeline_run_state references missing pipeline: {}",
            run_state.pipeline_id
        ));
    }
    Ok(())
}

fn canonical_flow_instance_id(value: Option<&str>) -> Result<String, String> {
    let id = value.unwrap_or(DEFAULT_FLOW_INSTANCE_ID).trim();
    if id.is_empty() {
        return Err("flow_instance_id must not be empty".to_string());
    }
    Ok(id.to_string())
}

fn import_export_busy_response() -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        "another import/export command is in progress".to_string(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstanceBackendKind, FlowInstanceSpec};
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use serde_json::{Value as JsonValue, json};
    use storage::{StorageManager, StoredMemoryTopicKind, StoredPipelineDesiredState};
    use tempfile::tempdir;

    fn sample_default_instance_spec() -> FlowInstanceSpec {
        FlowInstanceSpec {
            id: DEFAULT_FLOW_INSTANCE_ID.to_string(),
            backend: FlowInstanceBackendKind::InProcess,
            ..FlowInstanceSpec::default()
        }
    }

    fn sample_stream_request(name: &str) -> CreateStreamRequest {
        serde_json::from_value(json!({
            "name": name,
            "type": "mqtt",
            "schema": {
                "type": "json",
                "props": {
                    "columns": [
                        { "name": "value", "data_type": "int64" }
                    ]
                }
            },
            "props": {
                "broker_url": "mqtt://localhost:1883",
                "topic": format!("{name}/topic"),
                "qos": 0
            },
            "shared": false,
            "decoder": {
                "type": "json",
                "props": {}
            }
        }))
        .expect("deserialize stream request")
    }

    fn sample_pipeline_request(id: &str, stream_name: &str) -> CreatePipelineRequest {
        serde_json::from_value(json!({
            "id": id,
            "flow_instance_id": DEFAULT_FLOW_INSTANCE_ID,
            "sql": format!("SELECT * FROM {stream_name}"),
            "sinks": [
                {
                    "id": format!("{id}_sink_0"),
                    "type": "nop",
                    "props": { "log": false },
                    "common_sink_props": {},
                    "encoder": { "type": "json", "props": {} }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request")
    }

    fn sample_bundle(
        stream_name: &str,
        pipeline_id: &str,
        mqtt_key: &str,
        topic_name: &str,
    ) -> ExportBundleV1 {
        ExportBundleV1 {
            exported_at: 1_700_000_000,
            resources: crate::export::ExportResources {
                memory_topics: vec![ExportMemoryTopic {
                    topic: topic_name.to_string(),
                    kind: StoredMemoryTopicKind::Bytes,
                    capacity: 16,
                }],
                shared_mqtt_clients: vec![flow::connector::SharedMqttClientConfig {
                    key: mqtt_key.to_string(),
                    broker_url: "tcp://localhost:1883".to_string(),
                    topic: format!("{mqtt_key}/topic"),
                    client_id: format!("{mqtt_key}_client"),
                    qos: 1,
                }],
                streams: vec![sample_stream_request(stream_name)],
                pipelines: vec![sample_pipeline_request(pipeline_id, stream_name)],
                pipeline_run_states: vec![ExportPipelineRunState {
                    pipeline_id: pipeline_id.to_string(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                }],
            },
        }
    }

    #[tokio::test]
    async fn import_storage_handler_replaces_snapshot_and_returns_previous_bundle() {
        let dir = tempdir().expect("create tempdir");
        let storage = StorageManager::new(dir.path()).expect("create storage");
        let old_bundle = sample_bundle("stream_old", "pipe_old", "mqtt_old", "topic_old");
        let new_bundle = sample_bundle("stream_new", "pipe_new", "mqtt_new", "topic_new");

        storage
            .replace_metadata_snapshot(
                validate_and_build_snapshot(&old_bundle).expect("build old snapshot"),
            )
            .expect("seed old snapshot");

        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
            Vec::new(),
        )
        .expect("create app state");

        let response = import_storage_handler(State(state.clone()), Json(new_bundle.clone()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode import response");

        assert_eq!(json["applied_to_runtime"], false);
        assert_eq!(json["imported_resource_counts"]["memory_topics"], 1);
        assert_eq!(json["imported_resource_counts"]["shared_mqtt_clients"], 1);
        assert_eq!(json["imported_resource_counts"]["streams"], 1);
        assert_eq!(json["imported_resource_counts"]["pipelines"], 1);
        assert_eq!(json["imported_resource_counts"]["pipeline_run_states"], 1);
        assert!(json["previous_bundle"]["exported_at"].as_u64().unwrap() > 0);
        assert_eq!(
            json["previous_bundle"]["resources"],
            serde_json::to_value(&old_bundle.resources).expect("serialize old resources")
        );

        assert!(state.storage.get_stream("stream_old").unwrap().is_none());
        assert!(state.storage.get_pipeline("pipe_old").unwrap().is_none());
        assert!(state.storage.get_mqtt_config("mqtt_old").unwrap().is_none());
        assert!(
            state
                .storage
                .get_memory_topic("topic_old")
                .unwrap()
                .is_none()
        );
        assert!(
            state
                .storage
                .get_pipeline_run_state("pipe_old")
                .unwrap()
                .is_none()
        );

        assert!(state.storage.get_stream("stream_new").unwrap().is_some());
        assert!(state.storage.get_pipeline("pipe_new").unwrap().is_some());
        assert!(state.storage.get_mqtt_config("mqtt_new").unwrap().is_some());
        assert!(
            state
                .storage
                .get_memory_topic("topic_new")
                .unwrap()
                .is_some()
        );
        assert!(
            state
                .storage
                .get_pipeline_run_state("pipe_new")
                .unwrap()
                .is_some()
        );

        let final_bundle =
            build_export_bundle(state.storage.as_ref()).expect("build final storage bundle");
        assert_eq!(
            serde_json::to_value(&final_bundle.resources).expect("serialize final resources"),
            serde_json::to_value(&new_bundle.resources).expect("serialize new resources")
        );
    }
}
