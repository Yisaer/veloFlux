use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstances};
use crate::pipeline::{CreatePipelineRequest, build_pipeline_definition};
use crate::stream::{
    CreateStreamRequest, build_schema_from_request, build_stream_decoder, build_stream_props,
    validate_memory_stream_topic, validate_stream_decoder_config,
};
use flow::catalog::EventtimeDefinition;
use flow::catalog::StreamDefinition;
use flow::connector::SharedMqttClientConfig;
use flow::pipeline::PipelineDefinition;
use flow::{DecoderRegistry, EncoderRegistry};
use std::sync::Arc;
use storage::{
    StorageManager, StoredMemoryTopicKind, StoredMqttClientConfig, StoredPipeline, StoredStream,
};

/// Serialize a create-stream request for storage.
pub fn stored_stream_from_request(req: &CreateStreamRequest) -> Result<StoredStream, String> {
    let raw_json =
        serde_json::to_string(req).map_err(|err| format!("serialize stream request: {err}"))?;
    Ok(StoredStream {
        id: req.name.clone(),
        raw_json,
    })
}

/// Rebuild a StreamDefinition from stored raw JSON.
pub fn stream_definition_from_stored(
    stored: &StoredStream,
    decoder_registry: &DecoderRegistry,
) -> Result<StreamDefinition, String> {
    let req: CreateStreamRequest = serde_json::from_str(&stored.raw_json)
        .map_err(|err| format!("decode stored stream {}: {err}", stored.id))?;
    let schema = build_schema_from_request(&req)?;
    let props = build_stream_props(&req.stream_type, &req.props)?;
    let decoder = build_stream_decoder(&req, decoder_registry)?;
    let mut definition = StreamDefinition::new(req.name.clone(), Arc::new(schema), props, decoder);
    if let Some(cfg) = &req.eventtime {
        definition = definition.with_eventtime(EventtimeDefinition::new(
            cfg.column.clone(),
            cfg.eventtime_type.clone(),
        ));
    }
    if let Some(sampler) = &req.sampler {
        definition = definition.with_sampler(sampler.clone());
    }
    Ok(definition)
}

/// Serialize a create-pipeline request for storage.
pub fn stored_pipeline_from_request(req: &CreatePipelineRequest) -> Result<StoredPipeline, String> {
    let raw_json =
        serde_json::to_string(req).map_err(|err| format!("serialize pipeline request: {err}"))?;
    Ok(StoredPipeline {
        id: req.id.clone(),
        raw_json,
    })
}

/// Rebuild a PipelineDefinition from stored raw JSON.
pub fn pipeline_definition_from_stored(
    stored: &StoredPipeline,
    encoder_registry: &EncoderRegistry,
    instance: &flow::FlowInstance,
) -> Result<PipelineDefinition, String> {
    let req = pipeline_request_from_stored(stored)?;
    build_pipeline_definition(&req, encoder_registry, instance)
}

pub fn pipeline_request_from_stored(
    stored: &StoredPipeline,
) -> Result<CreatePipelineRequest, String> {
    let mut req: CreatePipelineRequest = serde_json::from_str(&stored.raw_json)
        .map_err(|err| format!("decode stored pipeline {}: {err}", stored.id))?;
    let instance_id = req
        .flow_instance_id
        .as_deref()
        .map(|val| val.trim().to_string())
        .filter(|val| !val.is_empty())
        .unwrap_or_else(|| DEFAULT_FLOW_INSTANCE_ID.to_string());
    req.flow_instance_id = Some(instance_id);
    Ok(req)
}

pub fn mqtt_config_from_stored(stored: &StoredMqttClientConfig) -> SharedMqttClientConfig {
    serde_json::from_str(&stored.raw_json).unwrap_or_else(|_| SharedMqttClientConfig {
        key: stored.key.clone(),
        broker_url: String::new(),
        topic: String::new(),
        client_id: String::new(),
        qos: 0,
    })
}

pub fn stored_mqtt_from_config(cfg: &SharedMqttClientConfig) -> StoredMqttClientConfig {
    let raw_json = serde_json::to_string(cfg).unwrap_or_default();
    StoredMqttClientConfig {
        key: cfg.key.clone(),
        raw_json,
    }
}

/// Load persisted resources into the running FlowInstance.
async fn hydrate_instance_globals_from_storage(
    storage: &StorageManager,
    instance: &flow::FlowInstance,
) -> Result<(), String> {
    for topic in storage.list_memory_topics().map_err(|e| e.to_string())? {
        let kind = match topic.kind {
            StoredMemoryTopicKind::Bytes => flow::connector::MemoryTopicKind::Bytes,
            StoredMemoryTopicKind::Collection => flow::connector::MemoryTopicKind::Collection,
        };
        if let Err(err) = instance.declare_memory_topic(&topic.topic, kind, topic.capacity) {
            tracing::error!(topic = %topic.topic, error = %err, "failed to restore memory topic");
        }
    }

    for cfg in storage.list_mqtt_configs().map_err(|e| e.to_string())? {
        if let Err(err) = instance
            .create_shared_mqtt_client(mqtt_config_from_stored(&cfg))
            .await
        {
            tracing::error!(key = %cfg.key, error = %err, "failed to restore shared mqtt client");
        }
    }

    for stream in storage.list_streams().map_err(|e| e.to_string())? {
        if let Err(err) = restore_stream(stream.clone(), instance).await {
            tracing::error!(stream_id = %stream.id, error = %err, "failed to restore stream");
        }
    }
    Ok(())
}

async fn hydrate_pipelines_into_instances_from_storage(
    storage: &StorageManager,
    instances: &FlowInstances,
) -> Result<(), String> {
    for pipeline in storage.list_pipelines().map_err(|e| e.to_string())? {
        if let Err(err) = restore_pipeline(pipeline.clone(), storage, instances).await {
            tracing::error!(pipeline_id = %pipeline.id, error = %err, "failed to restore pipeline");
        }
    }
    Ok(())
}

async fn restore_stream(stream: StoredStream, instance: &flow::FlowInstance) -> Result<(), String> {
    let decoder_registry = instance.decoder_registry();
    let def = stream_definition_from_stored(&stream, decoder_registry.as_ref())?;
    let req = serde_json::from_str::<CreateStreamRequest>(&stream.raw_json)
        .map_err(|err| format!("decode stored stream {}: {err}", stream.id))?;
    let shared = req.shared;
    validate_stream_decoder_config(&req, def.decoder())?;
    if let flow::catalog::StreamProps::Memory(memory_props) = def.props() {
        validate_memory_stream_topic(&req, memory_props)?;
    }
    instance
        .create_stream(def, shared)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn restore_pipeline(
    pipeline: StoredPipeline,
    storage: &StorageManager,
    instances: &FlowInstances,
) -> Result<(), String> {
    let req = pipeline_request_from_stored(&pipeline)?;
    let flow_instance_id = req
        .flow_instance_id
        .as_deref()
        .unwrap_or(DEFAULT_FLOW_INSTANCE_ID);

    let Some(instance) = instances.get(flow_instance_id) else {
        tracing::warn!(
            pipeline_id = %pipeline.id,
            flow_instance_id = %flow_instance_id,
            "skipping pipeline restore: flow instance not available in this process"
        );
        return Ok(());
    };

    let encoder_registry = instance.encoder_registry();
    let def =
        pipeline_definition_from_stored(&pipeline, encoder_registry.as_ref(), instance.as_ref())?;
    instance
        .create_pipeline(flow::CreatePipelineRequest::new(def))
        .map_err(|e| e.to_string())?;

    match storage
        .get_pipeline_run_state(&pipeline.id)
        .map_err(|e| e.to_string())?
    {
        Some(state)
            if matches!(
                state.desired_state,
                storage::StoredPipelineDesiredState::Running
            ) =>
        {
            if let Err(err) = instance.start_pipeline(&pipeline.id) {
                tracing::error!(
                    pipeline_id = %pipeline.id,
                    error = %err,
                    "failed to auto-start pipeline"
                );
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) async fn hydrate_runtime_from_storage(
    storage: &StorageManager,
    instances: &FlowInstances,
) -> Result<(), String> {
    for (_, instance) in instances.instances_snapshot() {
        hydrate_instance_globals_from_storage(storage, instance.as_ref()).await?;
    }
    hydrate_pipelines_into_instances_from_storage(storage, instances).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::CreateStreamRequest;
    use flow::FlowInstance;
    use serde_json::{Map as JsonMap, Value as JsonValue, json};
    use tempfile::tempdir;

    fn sample_stream_request(name: &str) -> CreateStreamRequest {
        let schema_props: JsonMap<String, JsonValue> = json!({
            "columns": [
                {"name":"value","data_type":"int64"}
            ]
        })
        .as_object()
        .unwrap()
        .clone();

        let props_fields: JsonMap<String, JsonValue> = json!({
            "broker_url": "mqtt://localhost:1883",
            "topic": "in",
            "qos": 0
        })
        .as_object()
        .unwrap()
        .clone();

        CreateStreamRequest {
            name: name.to_string(),
            stream_type: "mqtt".to_string(),
            schema: crate::stream::SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: schema_props,
            },
            props: crate::stream::StreamPropsRequest {
                fields: props_fields,
            },
            shared: false,
            decoder: crate::stream::DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    // Plan-cache behavior is intentionally tested in the flow crate. The manager's storage bridge
    // only restores pipelines from their persisted SQL specification.
    #[tokio::test]
    async fn load_storage_skips_invalid_streams() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let instance = FlowInstance::new_default();

        // 1. Create a GOOD stream
        let good_req = sample_stream_request("good_stream");
        let good_stored = stored_stream_from_request(&good_req).unwrap();
        storage.create_stream(good_stored.clone()).unwrap();

        // 2. Create a BAD stream (manually insert invalid JSON)
        let bad_stored = StoredStream {
            id: "bad_stream".to_string(),
            raw_json: "{ invalid json".to_string(),
        };
        storage.create_stream(bad_stored).unwrap();

        // 3. Load. This should NOT return Err because stream restore errors are logged and skipped.
        hydrate_instance_globals_from_storage(&storage, &instance)
            .await
            .expect("hydrate instance globals from storage");

        // 4. Verify GOOD stream exists, BAD stream does not
        let streams = instance.list_streams().await.unwrap();
        assert!(streams.iter().any(|s| s.definition.id() == "good_stream"));
        assert!(!streams.iter().any(|s| s.definition.id() == "bad_stream"));
    }
}
