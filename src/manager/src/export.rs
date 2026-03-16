use axum::{
    Json,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::IntoResponse,
};
use flow::connector::SharedMqttClientConfig;
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use storage::{StorageManager, StoredMemoryTopicKind, StoredPipelineDesiredState};

use crate::pipeline::{AppState, CreatePipelineRequest};
use crate::stream::CreateStreamRequest;

#[derive(Serialize)]
pub struct ExportBundleV1 {
    pub bundle_version: u32,
    pub exported_at: u64,
    pub resources: ExportResources,
}

#[derive(Serialize)]
pub struct ExportResources {
    pub memory_topics: Vec<ExportMemoryTopic>,
    pub shared_mqtt_clients: Vec<SharedMqttClientConfig>,
    pub streams: Vec<CreateStreamRequest>,
    pub pipelines: Vec<CreatePipelineRequest>,
    pub pipeline_run_states: Vec<ExportPipelineRunState>,
}

#[derive(Serialize)]
pub struct ExportMemoryTopic {
    pub topic: String,
    pub kind: StoredMemoryTopicKind,
    pub capacity: usize,
}

#[derive(Serialize)]
pub struct ExportPipelineRunState {
    pub pipeline_id: String,
    pub desired_state: StoredPipelineDesiredState,
}

pub async fn export_storage_handler(State(state): State<AppState>) -> impl IntoResponse {
    let bundle = match build_export_bundle(state.storage.as_ref()) {
        Ok(bundle) => bundle,
        Err(err) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    let filename = format!("veloflux-metadata-export-{}.json", bundle.exported_at);
    let disposition = format!("attachment; filename=\"{filename}\"");
    let disposition = match HeaderValue::from_str(&disposition) {
        Ok(value) => value,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build export response header: {err}"),
            )
                .into_response();
        }
    };

    ([(header::CONTENT_DISPOSITION, disposition)], Json(bundle)).into_response()
}

fn build_export_bundle(storage: &StorageManager) -> Result<ExportBundleV1, String> {
    let snapshot = storage
        .export_metadata_snapshot()
        .map_err(|err| format!("read export snapshot from storage: {err}"))?;

    let mut memory_topics = snapshot
        .memory_topics
        .into_iter()
        .map(|topic| ExportMemoryTopic {
            topic: topic.topic,
            kind: topic.kind,
            capacity: topic.capacity,
        })
        .collect::<Vec<_>>();
    memory_topics.sort_by(|a, b| a.topic.cmp(&b.topic));

    let mut shared_mqtt_clients = Vec::with_capacity(snapshot.mqtt_configs.len());
    for stored in snapshot.mqtt_configs {
        let cfg: SharedMqttClientConfig =
            serde_json::from_str(&stored.raw_json).map_err(|err| {
                format!(
                    "decode stored shared mqtt client config {}: {err}",
                    stored.key
                )
            })?;
        if cfg.key != stored.key {
            return Err(format!(
                "stored shared mqtt client config {} key mismatch in raw_json: {}",
                stored.key, cfg.key
            ));
        }
        shared_mqtt_clients.push(cfg);
    }
    shared_mqtt_clients.sort_by(|a, b| a.key.cmp(&b.key));

    let mut streams = Vec::with_capacity(snapshot.streams.len());
    for stored in snapshot.streams {
        let req: CreateStreamRequest = serde_json::from_str(&stored.raw_json)
            .map_err(|err| format!("decode stored stream {}: {err}", stored.id))?;
        if req.name != stored.id {
            return Err(format!(
                "stored stream {} name mismatch in raw_json: {}",
                stored.id, req.name
            ));
        }
        streams.push(req);
    }
    streams.sort_by(|a, b| a.name.cmp(&b.name));

    let mut pipelines = Vec::with_capacity(snapshot.pipelines.len());
    for stored in snapshot.pipelines {
        let req: CreatePipelineRequest = serde_json::from_str(&stored.raw_json)
            .map_err(|err| format!("decode stored pipeline {}: {err}", stored.id))?;
        if req.id != stored.id {
            return Err(format!(
                "stored pipeline {} id mismatch in raw_json: {}",
                stored.id, req.id
            ));
        }
        pipelines.push(req);
    }
    pipelines.sort_by(|a, b| a.id.cmp(&b.id));

    let mut pipeline_run_states = snapshot
        .pipeline_run_states
        .into_iter()
        .map(|state| ExportPipelineRunState {
            pipeline_id: state.pipeline_id,
            desired_state: state.desired_state,
        })
        .collect::<Vec<_>>();
    pipeline_run_states.sort_by(|a, b| a.pipeline_id.cmp(&b.pipeline_id));

    let exported_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("compute export timestamp: {err}"))?
        .as_secs();

    Ok(ExportBundleV1 {
        bundle_version: 1,
        exported_at,
        resources: ExportResources {
            memory_topics,
            shared_mqtt_clients,
            streams,
            pipelines,
            pipeline_run_states,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstanceBackendKind, FlowInstanceSpec};
    use crate::pipeline::AppState;
    use crate::storage_bridge::{
        stored_mqtt_from_config, stored_pipeline_from_request, stored_stream_from_request,
    };
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::http::{StatusCode, header};
    use serde_json::{Value as JsonValue, json};
    use storage::{
        StorageManager, StoredMemoryTopic, StoredMemoryTopicKind, StoredPipelineDesiredState,
        StoredPipelineRunState,
    };
    use tempfile::tempdir;

    fn sample_stream_request() -> CreateStreamRequest {
        serde_json::from_value(json!({
            "name": "stream_1",
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
                "topic": "input/topic",
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

    fn sample_pipeline_request() -> CreatePipelineRequest {
        serde_json::from_value(json!({
            "id": "pipe_1",
            "flow_instance_id": DEFAULT_FLOW_INSTANCE_ID,
            "sql": "SELECT * FROM stream_1",
            "sinks": [
                {
                    "id": "pipe_1_sink_0",
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

    fn sample_default_instance_spec() -> FlowInstanceSpec {
        FlowInstanceSpec {
            id: DEFAULT_FLOW_INSTANCE_ID.to_string(),
            backend: FlowInstanceBackendKind::InProcess,
            ..FlowInstanceSpec::default()
        }
    }

    #[tokio::test]
    async fn export_storage_handler_returns_bundle_json() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let stream = sample_stream_request();
        let pipeline = sample_pipeline_request();
        let mqtt = SharedMqttClientConfig {
            key: "shared_a".to_string(),
            broker_url: "tcp://localhost:1883".to_string(),
            topic: "foo/bar".to_string(),
            client_id: "client_a".to_string(),
            qos: 1,
        };
        let memory_topic = StoredMemoryTopic {
            topic: "topic_1".to_string(),
            kind: StoredMemoryTopicKind::Bytes,
            capacity: 16,
        };
        let run_state = StoredPipelineRunState {
            pipeline_id: pipeline.id.clone(),
            desired_state: StoredPipelineDesiredState::Running,
        };

        storage
            .create_stream(stored_stream_from_request(&stream).unwrap())
            .unwrap();
        storage
            .create_pipeline(stored_pipeline_from_request(&pipeline).unwrap())
            .unwrap();
        storage
            .create_mqtt_config(stored_mqtt_from_config(&mqtt))
            .unwrap();
        storage.create_memory_topic(memory_topic).unwrap();
        storage.put_pipeline_run_state(run_state).unwrap();

        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
            Vec::new(),
        )
        .unwrap();

        let response = export_storage_handler(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let disposition = response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .expect("content disposition header")
            .to_str()
            .expect("header as str");
        assert!(disposition.starts_with("attachment; filename=\"veloflux-metadata-export-"));
        assert!(disposition.ends_with(".json\""));

        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode export bundle");

        assert_eq!(json["bundle_version"], 1);
        assert!(json["exported_at"].as_u64().unwrap() > 0);
        assert_eq!(
            json["resources"]["memory_topics"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            json["resources"]["shared_mqtt_clients"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(json["resources"]["streams"].as_array().unwrap().len(), 1);
        assert_eq!(json["resources"]["pipelines"].as_array().unwrap().len(), 1);
        assert_eq!(
            json["resources"]["pipeline_run_states"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(json["resources"]["streams"][0]["name"], "stream_1");
        assert_eq!(json["resources"]["pipelines"][0]["id"], "pipe_1");
        assert_eq!(
            json["resources"]["pipeline_run_states"][0]["desired_state"],
            "Running"
        );
    }
}
