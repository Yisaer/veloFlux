//! Memory source connector that subscribes to in-process pub/sub topics.

use crate::connector::memory_pubsub::{MemoryData, MemoryPubSubRegistry, MemoryTopicKind};
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
    pub fn new(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{ConnectorEvent, SourceConnector};
    use tokio::time::{timeout, Duration};

    async fn recv_next_event(
        stream: &mut crate::connector::ConnectorStream,
    ) -> Result<ConnectorEvent, ConnectorError> {
        timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for connector event")
            .expect("connector stream ended unexpectedly")
    }

    #[tokio::test]
    async fn memory_source_bytes_connector_subscribes_only_to_bytes_topics() {
        let registry = MemoryPubSubRegistry::new();

        registry
            .declare_topic("memory_source_bytes_topic", MemoryTopicKind::Bytes, 4)
            .expect("declare bytes topic");
        let publisher = registry
            .open_publisher_bytes("memory_source_bytes_topic")
            .expect("open bytes publisher");

        let mut connector = MemorySourceConnector::new(
            "memory_source_bytes_connector",
            MemorySourceConfig::new("memory_source_bytes_topic", MemoryTopicKind::Bytes),
            registry.clone(),
        );
        let mut stream = connector.subscribe().expect("subscribe bytes connector");

        publisher
            .publish_bytes("hello")
            .expect("publish bytes payload");

        match recv_next_event(&mut stream)
            .await
            .expect("receive bytes connector event")
        {
            ConnectorEvent::Payload(payload) => assert_eq!(payload, b"hello"),
            other => panic!("expected bytes payload, got {other:?}"),
        }

        registry
            .declare_topic(
                "memory_source_collection_topic",
                MemoryTopicKind::Collection,
                4,
            )
            .expect("declare collection topic");
        let mut mismatched_connector = MemorySourceConnector::new(
            "memory_source_mismatched_bytes_connector",
            MemorySourceConfig::new("memory_source_collection_topic", MemoryTopicKind::Bytes),
            registry,
        );
        let err = match mismatched_connector.subscribe() {
            Ok(_) => panic!("bytes connector should reject collection topic"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("kind mismatch"),
            "unexpected subscribe error: {err}"
        );
    }

    #[tokio::test]
    async fn memory_source_bytes_connector_continues_after_broadcast_lag() {
        let registry = MemoryPubSubRegistry::new();
        registry
            .declare_topic("memory_source_lag_topic", MemoryTopicKind::Bytes, 1)
            .expect("declare lag topic");
        let publisher = registry
            .open_publisher_bytes("memory_source_lag_topic")
            .expect("open lag publisher");

        let mut connector = MemorySourceConnector::new(
            "memory_source_lag_connector",
            MemorySourceConfig::new("memory_source_lag_topic", MemoryTopicKind::Bytes),
            registry,
        );
        let mut stream = connector.subscribe().expect("subscribe lag connector");

        publisher.publish_bytes("first").expect("publish first");
        publisher.publish_bytes("second").expect("publish second");
        publisher.publish_bytes("third").expect("publish third");

        match recv_next_event(&mut stream)
            .await
            .expect("receive post-lag event")
        {
            ConnectorEvent::Payload(payload) => assert_eq!(payload, b"third"),
            other => panic!("expected latest payload after lag, got {other:?}"),
        }

        publisher.publish_bytes("fourth").expect("publish fourth");

        match recv_next_event(&mut stream)
            .await
            .expect("receive event after lag recovery")
        {
            ConnectorEvent::Payload(payload) => assert_eq!(payload, b"fourth"),
            other => panic!("expected payload after lag recovery, got {other:?}"),
        }
    }
}
