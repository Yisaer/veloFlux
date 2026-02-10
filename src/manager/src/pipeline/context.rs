use axum::http::StatusCode;
use axum::response::IntoResponse;
use flow::FlowInstance;
use flow::connector::SharedMqttClientConfig;
use parser::SelectStmt;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};

use crate::instances::FlowInstances;
use crate::worker::WorkerMemoryTopicSpec;
use storage::StorageManager;

use super::types::{
    CreatePipelineRequest, CreatePipelineSinkRequest, MemorySinkPropsRequest, MqttSinkPropsRequest,
};

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

pub(super) type PipelineContextPayload = (
    BTreeMap<String, crate::stream::CreateStreamRequest>,
    Vec<SharedMqttClientConfig>,
    Vec<WorkerMemoryTopicSpec>,
);
pub(super) type PipelineContextError = Box<axum::response::Response>;

pub(super) fn build_pipeline_context_payload(
    instances: &FlowInstances,
    storage: &StorageManager,
    pipeline_id: &str,
    pipeline_req: &CreatePipelineRequest,
) -> Result<PipelineContextPayload, PipelineContextError> {
    let default_instance = instances.default_instance();
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
        let stored_stream = match storage.get_stream(&stream_name) {
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
        let stored = match storage.get_mqtt_config(&key) {
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
        let stored = match storage.get_memory_topic(&topic) {
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
