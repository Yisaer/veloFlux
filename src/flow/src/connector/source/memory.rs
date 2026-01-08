//! Memory source connector that subscribes to in-process pub/sub topics.

use crate::connector::memory_pubsub::{
    registry as memory_pubsub_registry, MemoryData, MemoryPubSubRegistry, MemoryTopicKind,
};
use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};
use crate::model::Collection;
use futures::stream::StreamExt;
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

#[derive(Debug, Clone)]
pub struct MemorySourceConfig {
    pub topic: String,
    pub kind: MemoryTopicKind,
}

impl MemorySourceConfig {
    pub fn new(topic: impl Into<String>, kind: MemoryTopicKind) -> Self {
        Self {
            topic: topic.into(),
            kind,
        }
    }

    pub fn with_kind(mut self, kind: MemoryTopicKind) -> Self {
        self.kind = kind;
        self
    }
}

pub struct MemorySourceConnector {
    id: String,
    config: MemorySourceConfig,
    subscribed: bool,
    registry: MemoryPubSubRegistry,
}

impl MemorySourceConnector {
    pub fn new(id: impl Into<String>, config: MemorySourceConfig) -> Self {
        Self::with_registry(id, config, memory_pubsub_registry().clone())
    }

    pub fn with_registry(
        id: impl Into<String>,
        config: MemorySourceConfig,
        registry: MemoryPubSubRegistry,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            subscribed: false,
            registry,
        }
    }
}

impl SourceConnector for MemorySourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        if self.subscribed {
            return Err(ConnectorError::AlreadySubscribed(self.id.clone()));
        }

        let receiver = match self.config.kind {
            MemoryTopicKind::Bytes => self
                .registry
                .open_subscribe_bytes(&self.config.topic)
                .map_err(|err| {
                    ConnectorError::Other(format!("memory pubsub subscribe error: {err}"))
                })?,
            MemoryTopicKind::Collection => self
                .registry
                .open_subscribe_collection(&self.config.topic)
                .map_err(|err| {
                    ConnectorError::Other(format!("memory pubsub subscribe error: {err}"))
                })?,
        };
        self.subscribed = true;

        let connector_id = self.id.clone();
        let topic = self.config.topic.clone();
        let kind = self.config.kind;
        let stream = BroadcastStream::new(receiver).filter_map(move |item| {
            let connector_id = connector_id.clone();
            let topic = topic.clone();
            async move {
                match item {
                    Ok(MemoryData::Bytes(payload)) => {
                        if kind != MemoryTopicKind::Bytes {
                            tracing::error!(connector_id = %connector_id, topic = %topic, "memory source received Bytes for non-bytes topic");
                            return None;
                        }
                        Some(Ok(ConnectorEvent::Payload(payload.to_vec())))
                    }
                    Ok(MemoryData::Collection(collection)) => {
                        if kind != MemoryTopicKind::Collection {
                            tracing::error!(connector_id = %connector_id, topic = %topic, "memory source received Collection for non-collection topic");
                            return None;
                        }
                        Some(Ok(ConnectorEvent::Collection(
                            Box::new(collection) as Box<dyn Collection>
                        )))
                    }
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                        tracing::error!(
                            connector_id = %connector_id,
                            topic = %topic,
                            skipped = skipped,
                            "memory source lagged"
                        );
                        None
                    }
                }
            }
        });

        tracing::info!(connector_id = %self.id, topic = %self.config.topic, "memory source starting");
        Ok(Box::pin(stream))
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        tracing::info!(connector_id = %self.id, topic = %self.config.topic, "memory source closed");
        Ok(())
    }
}
