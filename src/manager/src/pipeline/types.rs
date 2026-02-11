use crate::worker::WorkerMemoryTopicSpec;
use flow::connector::SharedMqttClientConfig;
use flow::planner::sink::CommonSinkProps;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;
use std::time::Duration;

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
    #[serde(default)]
    pub eventtime: EventtimeOptionsRequest,
}

impl Default for PipelineOptionsRequest {
    fn default() -> Self {
        Self {
            data_channel_capacity: 16,
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
    pub(crate) timeout_ms: u64,
}

impl Default for CollectStatsQuery {
    fn default() -> Self {
        Self { timeout_ms: 5_000 }
    }
}

#[derive(Deserialize)]
#[serde(default)]
pub(crate) struct StopPipelineQuery {
    pub(crate) mode: String,
    pub(crate) timeout_ms: u64,
}

impl Default for StopPipelineQuery {
    fn default() -> Self {
        Self {
            mode: "quick".to_string(),
            timeout_ms: 5_000,
        }
    }
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
    pub(super) fn to_value(&self) -> JsonValue {
        JsonValue::Object(self.fields.clone())
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

impl CommonSinkPropsRequest {
    pub(super) fn to_common_props(&self) -> CommonSinkProps {
        let duration = self.batch_duration_ms.map(Duration::from_millis);
        CommonSinkProps {
            batch_count: self.batch_count,
            batch_duration: duration,
        }
    }
}
