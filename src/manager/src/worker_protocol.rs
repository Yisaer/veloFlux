use crate::pipeline::CreatePipelineRequest;
use crate::stream::CreateStreamRequest;
use flow::connector::SharedMqttClientConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerDesiredState {
    Stopped,
    Running,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WorkerMemoryTopicSpec {
    pub topic: String,
    pub kind: storage::StoredMemoryTopicKind,
    pub capacity: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WorkerApplyPipelineRequest {
    pub pipeline: CreatePipelineRequest,
    /// Pipeline raw JSON as stored in metadata storage.
    pub pipeline_raw_json: String,
    /// Referenced streams required to build the pipeline in the worker.
    pub streams: BTreeMap<String, CreateStreamRequest>,
    /// Shared MQTT client configs required by referenced streams/sinks.
    pub shared_mqtt_clients: Vec<SharedMqttClientConfig>,
    /// Memory topics required by referenced streams/sinks.
    pub memory_topics: Vec<WorkerMemoryTopicSpec>,
    pub desired_state: WorkerDesiredState,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WorkerPlanCacheResult {
    pub hit: bool,
    /// Base64-encoded logical plan IR bytes (only returned on cache miss).
    pub logical_plan_ir_b64: Option<String>,
    /// Stream ids referenced by the built pipeline.
    pub streams: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WorkerApplyPipelineResponse {
    pub status: String,
    pub plan_cache: Option<WorkerPlanCacheResult>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WorkerPipelineListItem {
    pub id: String,
    pub status: String,
    pub streams: Vec<String>,
}
