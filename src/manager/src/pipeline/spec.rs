use crate::MQTT_QOS;
use flow::EncoderRegistry;
use flow::pipeline::{
    KuksaSinkProps, MemorySinkProps, MqttSinkProps, NopSinkProps, PipelineDefinition,
    PipelineOptions, PipelineStatus, SinkDefinition, SinkProps, SinkType,
};
use flow::planner::sink::{SinkEncoderConfig, SinkEncoderKind};
use serde::Deserialize;
use serde_json::Map as JsonMap;
use std::time::Duration;

use super::types::{
    CreatePipelineRequest, EncoderTransformRequest, MemorySinkPropsRequest, MqttSinkPropsRequest,
    NopSinkPropsRequest,
};

#[derive(Deserialize)]
struct KuksaSinkPropsRequest {
    pub addr: Option<String>,
    #[serde(rename = "vss_path")]
    pub vss_path: Option<String>,
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

        let mut encoder_config = match sink_definition.sink_type {
            SinkType::Kuksa => SinkEncoderConfig::new("none", JsonMap::new()),
            SinkType::Memory if sink_req.encoder.encode_type.eq_ignore_ascii_case("none") => {
                SinkEncoderConfig::new("none", sink_req.encoder.props.clone())
            }
            _ => {
                let encoder_kind = sink_req.encoder.encode_type.clone();
                if !encoder_registry.is_registered(&encoder_kind) {
                    return Err(format!("encoder kind `{encoder_kind}` not registered"));
                }
                SinkEncoderConfig::new(encoder_kind, sink_req.encoder.props.clone())
            }
        };

        encoder_config =
            apply_encoder_transform_request(encoder_config, sink_req.encoder.transform.as_ref());
        encoder_config
            .validate()
            .map_err(|err| format!("invalid encoder config for sink `{sink_id}`: {err}"))?;

        let sink_definition = sink_definition
            .with_encoder(encoder_config)
            .with_common_props(sink_req.common.to_common_props());
        sinks.push(sink_definition);
    }
    let options = PipelineOptions {
        data_channel_capacity: req.options.data_channel_capacity,
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

fn apply_encoder_transform_request(
    encoder_config: SinkEncoderConfig,
    transform: Option<&EncoderTransformRequest>,
) -> SinkEncoderConfig {
    let Some(transform) = transform else {
        return encoder_config;
    };

    if matches!(encoder_config.kind(), SinkEncoderKind::None) {
        return encoder_config;
    }

    encoder_config.with_transform_template(transform.template.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::FlowInstance;
    use flow::connector::{DEFAULT_MEMORY_PUBSUB_CAPACITY, MemoryTopicKind};
    use serde_json::json;

    fn test_instance() -> FlowInstance {
        FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
            "default", None,
        ))
    }

    fn sample_request_with_encoder(encoder: serde_json::Value) -> CreatePipelineRequest {
        serde_json::from_value(json!({
            "id": "pipe_1",
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": encoder
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

    #[tokio::test]
    async fn build_pipeline_definition_wires_encoder_transform_request() {
        let instance = test_instance();
        let request = sample_request_with_encoder(json!({
            "type": "json",
            "transform": {
                "template": "{\"x\":{{ json(.row.a) }} }"
            }
        }));

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];

        assert_eq!(sink.encoder.kind_str(), "json");
        assert_eq!(sink.encoder.transform_kind(), Some("template"));
        assert_eq!(
            sink.encoder.transform_template(),
            Some("{\"x\":{{ json(.row.a) }} }")
        );
    }

    #[tokio::test]
    async fn build_pipeline_definition_ignores_transform_when_encoder_none() {
        let instance = test_instance();
        let topic = "sink_none_transform";
        instance
            .declare_memory_topic(
                topic,
                MemoryTopicKind::Collection,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare collection memory topic");
        let request = serde_json::from_value(json!({
            "id": "pipe_1",
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "memory",
                    "props": { "topic": topic },
                    "encoder": {
                        "type": "none",
                        "transform": {
                            "template": "{\"x\":{{ json(.row.a) }} }"
                        }
                    }
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
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];

        assert_eq!(sink.encoder.kind_str(), "none");
        assert_eq!(sink.encoder.transform_kind(), None);
        assert_eq!(sink.encoder.transform_template(), None);
    }

    #[test]
    fn create_pipeline_request_rejects_non_object_encoder_transform() {
        let result = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json",
                        "transform": "oops"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }));

        assert!(
            result.is_err(),
            "non-object transform should fail deserialization"
        );
    }

    #[test]
    fn create_pipeline_request_rejects_missing_template_in_encoder_transform() {
        let result = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json",
                        "transform": {
                            "tpl": "{\"x\":{{ json(.row.a) }} }"
                        }
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }));

        assert!(
            result.is_err(),
            "missing template should fail deserialization"
        );
    }
}
