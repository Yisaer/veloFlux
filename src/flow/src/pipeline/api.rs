use crate::catalog::Catalog;
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig, SinkOutputConfig};
use crate::PipelineRegistries;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use std::time::Duration;

/// Errors that can occur when mutating pipeline definitions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PipelineError {
    #[error("pipeline already exists: {0}")]
    AlreadyExists(String),
    #[error("pipeline not found: {0}")]
    NotFound(String),
    #[error("pipeline build error: {0}")]
    BuildFailure(String),
    #[error("pipeline runtime error: {0}")]
    Runtime(String),
}

/// Supported sink types for pipeline outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkType {
    /// MQTT sink.
    Mqtt,
    /// No-op sink that discards payloads.
    Nop,
    /// Kuksa sink that updates VSS paths via kuksa.val.v2.
    Kuksa,
    /// Memory sink that publishes to an in-process pub/sub topic.
    Memory,
}

/// Sink configuration payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkProps {
    /// MQTT sink configuration.
    Mqtt(MqttSinkProps),
    /// No-op sink config.
    Nop(NopSinkProps),
    /// Kuksa sink config.
    Kuksa(KuksaSinkProps),
    /// Memory sink config.
    Memory(MemorySinkProps),
}

/// Runtime state for pipeline execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Stopped,
    Running,
}

#[derive(Debug, Clone)]
pub struct CreatePipelineRequest {
    pub definition: PipelineDefinition,
}

impl CreatePipelineRequest {
    pub fn new(definition: PipelineDefinition) -> Self {
        Self { definition }
    }
}

#[derive(Debug, Clone)]
pub struct CreatePipelineResult {
    pub snapshot: PipelineSnapshot,
}

pub enum ExplainPipelineTarget<'a> {
    Id(&'a str),
    Definition(&'a PipelineDefinition),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStopMode {
    Graceful,
    Quick,
}

/// Concrete MQTT sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttSinkProps {
    pub broker_url: String,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
    pub max_packet_size: Option<usize>,
}

/// Concrete Nop sink configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NopSinkProps {
    pub log: bool,
}

/// Concrete Kuksa sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KuksaSinkProps {
    pub addr: String,
    pub vss_path: String,
}

/// Concrete memory sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySinkProps {
    pub topic: String,
}

impl MemorySinkProps {
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
        }
    }
}

impl MqttSinkProps {
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>, qos: u8) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            retain: false,
            client_id: None,
            connector_key: None,
            max_packet_size: None,
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    pub fn with_connector_key(mut self, connector_key: impl Into<String>) -> Self {
        self.connector_key = Some(connector_key.into());
        self
    }

    pub fn with_max_packet_size(mut self, max_packet_size: usize) -> Self {
        self.max_packet_size = Some(max_packet_size);
        self
    }
}

/// Sink definition for a pipeline.
#[derive(Debug, Clone)]
pub struct SinkDefinition {
    pub sink_id: String,
    pub sink_type: SinkType,
    pub props: SinkProps,
    pub common: CommonSinkProps,
    pub encoder: SinkEncoderConfig,
    pub output: SinkOutputConfig,
}

impl SinkDefinition {
    pub fn new(sink_id: impl Into<String>, sink_type: SinkType, props: SinkProps) -> Self {
        let sink_id_str = sink_id.into();
        Self {
            sink_id: sink_id_str.clone(),
            sink_type,
            props,
            common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::default(),
        }
    }

    pub fn with_common_props(mut self, common: CommonSinkProps) -> Self {
        self.common = common;
        self
    }

    pub fn with_encoder(mut self, encoder: SinkEncoderConfig) -> Self {
        self.encoder = encoder;
        self
    }

    pub fn with_output(mut self, output: SinkOutputConfig) -> Self {
        self.output = output;
        self
    }
}

/// Source binding definition for a pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDefinition {
    pub stream: String,
    pub input: SourceInputConfig,
}

impl SourceDefinition {
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            input: SourceInputConfig::default(),
        }
    }

    pub fn with_input(mut self, input: SourceInputConfig) -> Self {
        self.input = input;
        self
    }
}

/// Source-side input behavior configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceInputConfig {
    pub mode: SourceInputMode,
    pub on_change: Option<SourceOnChangeConfig>,
}

impl SourceInputConfig {
    pub fn new(mode: SourceInputMode) -> Self {
        Self {
            mode,
            on_change: None,
        }
    }

    pub fn on_change() -> Self {
        Self::new(SourceInputMode::OnChange)
    }

    pub fn on_change_with_columns(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::on_change().with_on_change_columns(columns)
    }

    pub fn is_on_change(&self) -> bool {
        matches!(self.mode, SourceInputMode::OnChange)
    }

    pub fn with_on_change_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.mode = SourceInputMode::OnChange;
        self.on_change = Some(SourceOnChangeConfig {
            columns: Some(columns.into_iter().map(Into::into).collect()),
        });
        self
    }

    pub fn on_change_columns(&self) -> Option<&[String]> {
        self.on_change
            .as_ref()
            .and_then(|cfg| cfg.columns.as_deref())
    }
}

/// Input delivery mode for a source branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceInputMode {
    #[default]
    Full,
    OnChange,
}

impl SourceInputMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceInputMode::Full => "full",
            SourceInputMode::OnChange => "on_change",
        }
    }
}

/// On-change-specific source input configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceOnChangeConfig {
    pub columns: Option<Vec<String>>,
}

/// Pipeline definition referencing SQL + sinks.
#[derive(Debug, Clone)]
pub struct PipelineDefinition {
    id: String,
    sql: String,
    sources: Vec<SourceDefinition>,
    sinks: Vec<SinkDefinition>,
    options: PipelineOptions,
}

impl PipelineDefinition {
    pub fn new(id: impl Into<String>, sql: impl Into<String>, sinks: Vec<SinkDefinition>) -> Self {
        Self {
            id: id.into(),
            sql: sql.into(),
            sources: Vec::new(),
            sinks,
            options: PipelineOptions::default(),
        }
    }

    pub fn with_sources(mut self, sources: Vec<SourceDefinition>) -> Self {
        self.sources = sources;
        self
    }

    pub fn with_options(mut self, options: PipelineOptions) -> Self {
        self.options = options;
        self
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn sources(&self) -> &[SourceDefinition] {
        &self.sources
    }

    pub fn sinks(&self) -> &[SinkDefinition] {
        &self.sinks
    }

    pub fn options(&self) -> &PipelineOptions {
        &self.options
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineOptions {
    pub data_channel_capacity: usize,
    pub eventtime: EventtimeOptions,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            data_channel_capacity: crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY,
            eventtime: EventtimeOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventtimeOptions {
    pub enabled: bool,
    pub late_tolerance: Duration,
}

impl Default for EventtimeOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            late_tolerance: Duration::ZERO,
        }
    }
}

/// User-facing view of a pipeline entry.
#[derive(Debug, Clone)]
pub struct PipelineSnapshot {
    pub definition: Arc<PipelineDefinition>,
    pub streams: Vec<String>,
    pub status: PipelineStatus,
}

/// Stores all registered pipelines and manages their lifecycle.
pub(crate) struct PipelineManager {
    pub(super) pipelines: RwLock<HashMap<String, super::internal::ManagedPipeline>>,
    pub(super) catalog: Arc<Catalog>,
    pub(super) context: super::PipelineContext,
    pub(super) registries: PipelineRegistries,
}
