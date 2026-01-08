//! Memory sink connector that publishes to in-process pub/sub topics.

use super::{SinkConnector, SinkConnectorError};
use crate::connector::memory_pubsub::{
    registry as memory_pubsub_registry, MemoryPubSubRegistry, MemoryTopicKind, SharedCollection,
};
use crate::model::Collection;
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct MemorySinkConfig {
    pub sink_name: String,
    pub topic: String,
    pub kind: MemoryTopicKind,
}

impl MemorySinkConfig {
    pub fn new(
        sink_name: impl Into<String>,
        topic: impl Into<String>,
        kind: MemoryTopicKind,
    ) -> Self {
        Self {
            sink_name: sink_name.into(),
            topic: topic.into(),
            kind,
        }
    }
}

pub struct MemorySinkConnector {
    id: String,
    config: MemorySinkConfig,
    publisher: Option<crate::connector::MemoryPublisher>,
    registry: MemoryPubSubRegistry,
}

impl MemorySinkConnector {
    pub fn new(id: impl Into<String>, config: MemorySinkConfig) -> Self {
        Self::with_registry(id, config, memory_pubsub_registry().clone())
    }

    pub fn with_registry(
        id: impl Into<String>,
        config: MemorySinkConfig,
        registry: MemoryPubSubRegistry,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            publisher: None,
            registry,
        }
    }

    fn ensure_publisher(
        &mut self,
    ) -> Result<&crate::connector::MemoryPublisher, SinkConnectorError> {
        if self.publisher.is_none() {
            let publisher = match self.config.kind {
                MemoryTopicKind::Bytes => self
                    .registry
                    .open_publisher_bytes(&self.config.topic)
                    .map_err(|err| {
                        SinkConnectorError::Other(format!("memory pubsub open: {err}"))
                    })?,
                MemoryTopicKind::Collection => self
                    .registry
                    .open_publisher_collection(&self.config.topic)
                    .map_err(|err| {
                        SinkConnectorError::Other(format!("memory pubsub open: {err}"))
                    })?,
            };
            self.publisher = Some(publisher);
        }
        self.publisher
            .as_ref()
            .ok_or_else(|| SinkConnectorError::Other("memory pubsub publisher missing".to_string()))
    }
}

#[async_trait]
impl SinkConnector for MemorySinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        let _ = self.ensure_publisher()?;
        Ok(())
    }

    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        if self.config.kind != MemoryTopicKind::Bytes {
            return Err(SinkConnectorError::Other(format!(
                "connector `{}` expected collection payloads for topic `{}`",
                self.id, self.config.topic
            )));
        }
        let publisher = self.ensure_publisher()?;
        publisher
            .publish_bytes(Bytes::copy_from_slice(payload))
            .map_err(|err| SinkConnectorError::Other(err.to_string()))?;
        Ok(())
    }

    async fn send_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        if self.config.kind != MemoryTopicKind::Collection {
            return Err(SinkConnectorError::Other(format!(
                "connector `{}` expected bytes payloads for topic `{}`",
                self.id, self.config.topic
            )));
        }
        let publisher = self.ensure_publisher()?;
        let shared = SharedCollection::from_box(collection.clone_box());
        publisher
            .publish_collection(shared)
            .map_err(|err| SinkConnectorError::Other(err.to_string()))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.publisher = None;
        Ok(())
    }
}
