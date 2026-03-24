use crate::connector::sink::kuksa::KuksaSinkConfig;
use crate::connector::sink::memory::MemorySinkConfig;
use crate::connector::sink::mqtt::MqttSinkConfig;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::fmt;
use std::time::Duration;

/// Declarative description of a sink processor in the logical/physical plans.
#[derive(Clone)]
pub struct PipelineSink {
    pub sink_id: String,
    pub forward_to_result: bool,
    pub common: CommonSinkProps,
    pub connector: PipelineSinkConnector,
}

impl PipelineSink {
    /// Create a new sink descriptor with the provided connector configuration.
    pub fn new(sink_id: impl Into<String>, connector: PipelineSinkConnector) -> Self {
        Self {
            sink_id: sink_id.into(),
            forward_to_result: false,
            common: CommonSinkProps::default(),
            connector,
        }
    }

    /// Configure whether this sink should forward records to the result collector.
    pub fn with_forward_to_result(mut self, forward: bool) -> Self {
        self.forward_to_result = forward;
        self
    }

    pub fn with_common_props(mut self, common: CommonSinkProps) -> Self {
        self.common = common;
        self
    }
}

impl fmt::Debug for PipelineSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineSink")
            .field("sink_id", &self.sink_id)
            .field("forward_to_result", &self.forward_to_result)
            .field("common", &self.common)
            .field("connector", &self.connector)
            .finish()
    }
}

/// Declarative description of a connector bound to a sink.
#[derive(Clone)]
pub struct PipelineSinkConnector {
    pub connector_id: String,
    pub connector: SinkConnectorConfig,
    pub encoder: SinkEncoderConfig,
}

impl PipelineSinkConnector {
    pub fn new(
        connector_id: impl Into<String>,
        connector: SinkConnectorConfig,
        encoder: SinkEncoderConfig,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            connector,
            encoder,
        }
    }
}

impl fmt::Debug for PipelineSinkConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineSinkConnector")
            .field("connector_id", &self.connector_id)
            .field("connector", &self.connector)
            .field("encoder", &self.encoder)
            .finish()
    }
}

/// Configuration for supported sink connectors.
#[derive(Clone, Debug)]
pub enum SinkConnectorConfig {
    Mqtt(MqttSinkConfig),
    Nop(NopSinkConfig),
    Kuksa(KuksaSinkConfig),
    Memory(MemorySinkConfig),
    Custom(CustomSinkConnectorConfig),
}

impl SinkConnectorConfig {
    pub fn kind(&self) -> &str {
        match self {
            SinkConnectorConfig::Mqtt(_) => "mqtt",
            SinkConnectorConfig::Nop(_) => "nop",
            SinkConnectorConfig::Kuksa(_) => "kuksa",
            SinkConnectorConfig::Memory(_) => "memory",
            SinkConnectorConfig::Custom(custom) => custom.kind.as_str(),
        }
    }

    pub fn custom_settings(&self) -> Option<&JsonValue> {
        match self {
            SinkConnectorConfig::Custom(custom) => Some(&custom.settings),
            _ => None,
        }
    }
}

/// JSON-based payload for custom connectors.
#[derive(Clone, Debug)]
pub struct CustomSinkConnectorConfig {
    pub kind: String,
    pub settings: JsonValue,
}

/// Configuration for a no-op sink connector.
#[derive(Clone, Debug, Default)]
pub struct NopSinkConfig {
    pub log: bool,
}

/// Configuration for supported sink encoders.
#[derive(Clone, Debug, PartialEq)]
pub struct SinkEncoderConfig {
    kind: SinkEncoderKind,
    props: JsonMap<String, JsonValue>,
    transform: Option<SinkEncoderTransformConfig>,
}

/// Supported encoder kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkEncoderKind {
    Json,
    None,
    Custom(String),
}

/// Supported encoder transform kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkEncoderTransformConfig {
    Template { template: String },
}

impl SinkEncoderTransformConfig {
    pub fn kind_str(&self) -> &'static str {
        match self {
            SinkEncoderTransformConfig::Template { .. } => "template",
        }
    }

    pub fn template(&self) -> &str {
        match self {
            SinkEncoderTransformConfig::Template { template } => template.as_str(),
        }
    }
}

impl SinkEncoderKind {
    pub fn as_str(&self) -> &str {
        match self {
            SinkEncoderKind::Json => "json",
            SinkEncoderKind::None => "none",
            SinkEncoderKind::Custom(kind) => kind.as_str(),
        }
    }
}

impl From<String> for SinkEncoderKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "json" => SinkEncoderKind::Json,
            "none" => SinkEncoderKind::None,
            other => SinkEncoderKind::Custom(other.to_string()),
        }
    }
}

impl From<&str> for SinkEncoderKind {
    fn from(value: &str) -> Self {
        match value {
            "json" => SinkEncoderKind::Json,
            "none" => SinkEncoderKind::None,
            other => SinkEncoderKind::Custom(other.to_string()),
        }
    }
}

impl SinkEncoderConfig {
    pub fn new(kind: impl Into<SinkEncoderKind>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            kind: kind.into(),
            props,
            transform: None,
        }
    }

    pub fn json() -> Self {
        Self::new(SinkEncoderKind::Json, JsonMap::new())
    }

    pub fn json_with_transform_template(template: impl Into<String>) -> Self {
        Self::json().with_transform_template(template)
    }

    pub fn kind(&self) -> &SinkEncoderKind {
        &self.kind
    }

    pub fn kind_str(&self) -> &str {
        self.kind.as_str()
    }

    pub fn props(&self) -> &JsonMap<String, JsonValue> {
        &self.props
    }

    pub fn transform(&self) -> Option<&SinkEncoderTransformConfig> {
        self.transform.as_ref()
    }

    pub fn with_transform_template(mut self, template: impl Into<String>) -> Self {
        self.transform = Some(SinkEncoderTransformConfig::Template {
            template: template.into(),
        });
        self
    }

    pub fn transform_template(&self) -> Option<&str> {
        self.transform
            .as_ref()
            .map(SinkEncoderTransformConfig::template)
    }

    pub fn transform_kind(&self) -> Option<&'static str> {
        self.transform
            .as_ref()
            .map(SinkEncoderTransformConfig::kind_str)
    }

    pub fn with_transform(mut self, transform: SinkEncoderTransformConfig) -> Self {
        self.transform = Some(transform);
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        let Some(_transform) = self.transform.as_ref() else {
            return Ok(());
        };

        if !matches!(self.kind, SinkEncoderKind::Json) {
            return Err(format!(
                "encoder transform is only supported for encoder.type=json, got `{}`",
                self.kind.as_str()
            ));
        }

        Ok(())
    }
}

/// Common sink-level properties (batching, etc.).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommonSinkProps {
    pub batch_count: Option<usize>,
    pub batch_duration: Option<Duration>,
}

impl CommonSinkProps {
    pub fn is_batching_enabled(&self) -> bool {
        self.batch_count.is_some() || self.batch_duration.is_some()
    }
}
