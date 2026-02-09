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

fn fnv1a_64_hex(input: &str) -> String {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

pub(crate) fn build_plan_snapshot(
    storage: &StorageManager,
    pipeline_id: &str,
    pipeline_raw_json: &str,
    stream_ids: &[String],
    logical_plan_ir: Vec<u8>,
) -> Result<storage::StoredPlanSnapshot, String> {
    let pipeline_json_hash = fnv1a_64_hex(pipeline_raw_json);
    let mut stream_json_hashes = Vec::with_capacity(stream_ids.len());
    for stream_id in stream_ids {
        let stream = storage
            .get_stream(stream_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("stream {stream_id} missing from storage"))?;
        stream_json_hashes.push((stream_id.clone(), fnv1a_64_hex(&stream.raw_json)));
    }
    stream_json_hashes.sort_by(|a, b| a.0.cmp(&b.0));

    let flow_build_id = build_info::build_id();
    let fingerprint = format!("{pipeline_json_hash}:{flow_build_id}");

    Ok(storage::StoredPlanSnapshot {
        pipeline_id: pipeline_id.to_string(),
        fingerprint,
        pipeline_json_hash,
        stream_json_hashes,
        flow_build_id,
        logical_plan_ir,
    })
}

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

    let stored_snapshot = storage
        .get_plan_snapshot(&pipeline.id)
        .map_err(|e| e.to_string())?;
    let (plan_cache_snapshot, streams_raw_json) = match stored_snapshot.as_ref() {
        Some(snapshot) => {
            let mut streams_raw_json = Vec::with_capacity(snapshot.stream_json_hashes.len());
            for (stream_id, _) in &snapshot.stream_json_hashes {
                let stream = storage
                    .get_stream(stream_id)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("stream {stream_id} missing from storage"))?;
                streams_raw_json.push((stream_id.clone(), stream.raw_json));
            }
            (
                Some(flow::planner::plan_cache::PlanSnapshotRecord {
                    pipeline_json_hash: snapshot.pipeline_json_hash.clone(),
                    stream_json_hashes: snapshot.stream_json_hashes.clone(),
                    flow_build_id: snapshot.flow_build_id.clone(),
                    logical_plan_ir: snapshot.logical_plan_ir.clone(),
                }),
                streams_raw_json,
            )
        }
        None => (None, Vec::new()),
    };

    let result = instance
        .create_pipeline(
            flow::CreatePipelineRequest::new(def).with_plan_cache_inputs(
                flow::planner::plan_cache::PlanCacheInputs {
                    pipeline_raw_json: pipeline.raw_json.clone(),
                    streams_raw_json,
                    snapshot: plan_cache_snapshot,
                },
            ),
        )
        .map_err(|e| e.to_string())?;

    let logical_ir = result.plan_cache.and_then(|result| result.logical_plan_ir);
    if let Some(logical_ir) = logical_ir {
        let stored_snapshot = build_plan_snapshot(
            storage,
            &pipeline.id,
            &pipeline.raw_json,
            &result.snapshot.streams,
            logical_ir,
        )?;
        storage
            .put_plan_snapshot(stored_snapshot)
            .map_err(|e| e.to_string())?;
    }

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
    use crate::instances::FlowInstances;
    use crate::pipeline::CreatePipelineRequest;
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

    fn sample_pipeline_request(
        id: &str,
        sql: &str,
        plan_cache_enabled: bool,
    ) -> CreatePipelineRequest {
        serde_json::from_value(json!({
            "id": id,
            "sql": sql,
            "sinks": [{
                "type": "mqtt",
                "props": {
                    "broker_url": "mqtt://localhost:1883",
                    "topic": "out",
                    "qos": 0
                }
            }],
            "options": {
                "plan_cache": {"enabled": plan_cache_enabled}
            }
        }))
        .expect("pipeline request json")
    }

    async fn install_stream_into_instance(
        instance: &FlowInstance,
        stream_req: &CreateStreamRequest,
    ) -> StoredStream {
        let stored = stored_stream_from_request(stream_req).expect("stored stream");
        let def = stream_definition_from_stored(&stored, instance.decoder_registry().as_ref())
            .expect("stream definition");
        instance
            .create_stream(def, stream_req.shared)
            .await
            .expect("create stream");
        stored
    }

    #[tokio::test]
    async fn plan_cache_hit_skips_sql_parse() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let instance = FlowInstance::new_default();
        let instances = FlowInstances::new(instance);

        let stream_req = sample_stream_request("s1");
        let stored_stream = stored_stream_from_request(&stream_req).unwrap();
        storage.create_stream(stored_stream.clone()).unwrap();

        // Create a pipeline with invalid SQL but plan cache enabled.
        let pipe_req = sample_pipeline_request("p1", "SELECT FROM", true);
        let stored_pipeline = stored_pipeline_from_request(&pipe_req).unwrap();
        storage.create_pipeline(stored_pipeline.clone()).unwrap();

        // Prepare a valid logical plan IR from a separate instance; this IR will be used to create
        // the pipeline without parsing stored_pipeline.sql.
        let ir_instance = FlowInstance::new_with_id("test-ir", None);
        let _ = install_stream_into_instance(&ir_instance, &stream_req).await;
        let valid_req = sample_pipeline_request("p_tmp", "SELECT value FROM s1", true);
        let valid_def = crate::pipeline::build_pipeline_definition(
            &valid_req,
            ir_instance.encoder_registry().as_ref(),
            &ir_instance,
        )
        .unwrap();
        let ir_result = ir_instance
            .create_pipeline(
                flow::CreatePipelineRequest::new(valid_def).with_plan_cache_inputs(
                    flow::planner::plan_cache::PlanCacheInputs {
                        pipeline_raw_json: String::new(),
                        streams_raw_json: Vec::new(),
                        snapshot: None,
                    },
                ),
            )
            .unwrap();
        let logical_ir = ir_result
            .plan_cache
            .and_then(|result| result.logical_plan_ir)
            .expect("plan cache miss should return logical IR");
        ir_instance
            .delete_pipeline("p_tmp")
            .await
            .expect("delete p_tmp");

        // Sanity: the cached IR must be sufficient to build a pipeline without parsing SQL.
        let check_instance = FlowInstance::new_with_id("test-check", None);
        let _ = install_stream_into_instance(&check_instance, &stream_req).await;
        let check_def = pipeline_definition_from_stored(
            &stored_pipeline,
            check_instance.encoder_registry().as_ref(),
            &check_instance,
        )
        .unwrap();
        check_instance
            .create_pipeline(
                flow::CreatePipelineRequest::new(check_def).with_plan_cache_inputs(
                    flow::planner::plan_cache::PlanCacheInputs {
                        pipeline_raw_json: stored_pipeline.raw_json.clone(),
                        streams_raw_json: vec![(
                            stored_stream.id.clone(),
                            stored_stream.raw_json.clone(),
                        )],
                        snapshot: Some(flow::planner::plan_cache::PlanSnapshotRecord {
                            pipeline_json_hash: fnv1a_64_hex(&stored_pipeline.raw_json),
                            stream_json_hashes: vec![(
                                stored_stream.id.clone(),
                                fnv1a_64_hex(&stored_stream.raw_json),
                            )],
                            flow_build_id: build_info::build_id(),
                            logical_plan_ir: logical_ir.clone(),
                        }),
                    },
                ),
            )
            .expect("rehydrate pipeline from logical IR");

        let cached = build_plan_snapshot(
            &storage,
            &stored_pipeline.id,
            &stored_pipeline.raw_json,
            &vec!["s1".to_string()],
            logical_ir,
        )
        .unwrap();
        storage.put_plan_snapshot(cached).unwrap();

        // This should succeed even though the stored SQL is invalid, proving we did not parse it.
        hydrate_runtime_from_storage(&storage, &instances)
            .await
            .unwrap();
        assert!(
            instances
                .default_instance()
                .list_pipelines()
                .iter()
                .any(|p| p.definition.id() == "p1")
        );
    }

    #[tokio::test]
    async fn plan_cache_miss_writes_snapshot() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let instance = FlowInstance::new_default();
        let instances = FlowInstances::new(instance);

        let stream_req = sample_stream_request("s1");
        let stored_stream = stored_stream_from_request(&stream_req).unwrap();
        storage.create_stream(stored_stream.clone()).unwrap();

        let pipe_req = sample_pipeline_request("p1", "SELECT value FROM s1", true);
        let stored_pipeline = stored_pipeline_from_request(&pipe_req).unwrap();
        storage.create_pipeline(stored_pipeline.clone()).unwrap();

        assert!(storage.get_plan_snapshot("p1").unwrap().is_none());

        hydrate_runtime_from_storage(&storage, &instances)
            .await
            .unwrap();

        let snapshot = storage
            .get_plan_snapshot("p1")
            .unwrap()
            .expect("snapshot written");
        assert_eq!(snapshot.pipeline_id, "p1");
        assert_eq!(
            snapshot.pipeline_json_hash,
            fnv1a_64_hex(&stored_pipeline.raw_json)
        );
        assert!(!snapshot.logical_plan_ir.is_empty());
        assert!(
            instances
                .default_instance()
                .list_pipelines()
                .iter()
                .any(|p| p.definition.id() == "p1")
        );
    }
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
