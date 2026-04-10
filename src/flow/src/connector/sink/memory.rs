//! Memory sink connector that publishes to in-process pub/sub topics.

use super::{SinkConnector, SinkConnectorError};
use crate::connector::memory_pubsub::{MemoryPubSubRegistry, MemoryTopicKind, SharedCollection};
use crate::model::Collection;
use async_trait::async_trait;
use bytes::Bytes;
use std::any::Any;

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

pub enum MemorySinkConnector {
    Bytes(MemoryBytesSinkConnector),
    Collection(MemoryCollectionSinkConnector),
}

impl MemorySinkConnector {
    pub fn new(
        id: impl Into<String>,
        config: MemorySinkConfig,
        registry: MemoryPubSubRegistry,
    ) -> Self {
        match config.kind {
            MemoryTopicKind::Bytes => MemorySinkConnector::Bytes(MemoryBytesSinkConnector::new(
                id,
                config.topic,
                registry,
            )),
            MemoryTopicKind::Collection => MemorySinkConnector::Collection(
                MemoryCollectionSinkConnector::new(id, config, registry),
            ),
        }
    }
}

pub struct MemoryBytesSinkConnector {
    id: String,
    topic: String,
    registry: MemoryPubSubRegistry,
    publisher: Option<crate::connector::MemoryPublisher>,
}

impl MemoryBytesSinkConnector {
    fn new(
        id: impl Into<String>,
        topic: impl Into<String>,
        registry: MemoryPubSubRegistry,
    ) -> Self {
        Self {
            id: id.into(),
            topic: topic.into(),
            registry,
            publisher: None,
        }
    }

    fn ensure_publisher(
        &mut self,
    ) -> Result<&crate::connector::MemoryPublisher, SinkConnectorError> {
        if self.publisher.is_none() {
            let publisher = self
                .registry
                .open_publisher_bytes(&self.topic)
                .map_err(|err| SinkConnectorError::Other(format!("memory pubsub open: {err}")))?;
            self.publisher = Some(publisher);
        }
        self.publisher
            .as_ref()
            .ok_or_else(|| SinkConnectorError::Other("memory pubsub publisher missing".to_string()))
    }
}

#[async_trait]
impl SinkConnector for MemoryBytesSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        let _ = self.ensure_publisher()?;
        Ok(())
    }

    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        let publisher = self.ensure_publisher()?;
        publisher
            .publish_bytes(Bytes::copy_from_slice(payload))
            .map_err(|err| SinkConnectorError::Other(err.to_string()))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.publisher = None;
        Ok(())
    }
}

pub struct MemoryCollectionSinkConnector {
    id: String,
    topic: String,
    registry: MemoryPubSubRegistry,
    publisher: Option<crate::connector::MemoryPublisher>,
}

impl MemoryCollectionSinkConnector {
    fn new(
        id: impl Into<String>,
        config: MemorySinkConfig,
        registry: MemoryPubSubRegistry,
    ) -> Self {
        Self {
            id: id.into(),
            topic: config.topic,
            registry,
            publisher: None,
        }
    }

    fn ensure_publisher(
        &mut self,
    ) -> Result<&crate::connector::MemoryPublisher, SinkConnectorError> {
        if self.publisher.is_none() {
            let publisher = self
                .registry
                .open_publisher_collection(&self.topic)
                .map_err(|err| SinkConnectorError::Other(format!("memory pubsub open: {err}")))?;
            self.publisher = Some(publisher);
        }
        self.publisher
            .as_ref()
            .ok_or_else(|| SinkConnectorError::Other("memory pubsub publisher missing".to_string()))
    }
}

#[async_trait]
impl SinkConnector for MemoryCollectionSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        let _ = self.ensure_publisher()?;
        Ok(())
    }

    async fn send(&mut self, _payload: &[u8]) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Other(format!(
            "connector `{}` does not support bytes payloads for collection topic `{}`",
            self.id, self.topic
        )))
    }

    async fn send_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        let shared =
            if let Some(shared) = (collection as &dyn Any).downcast_ref::<SharedCollection>() {
                shared.clone()
            } else {
                SharedCollection::from_box(collection.clone_box())
            };

        let publisher = self.ensure_publisher()?;
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

#[async_trait]
impl SinkConnector for MemorySinkConnector {
    fn id(&self) -> &str {
        match self {
            MemorySinkConnector::Bytes(inner) => inner.id(),
            MemorySinkConnector::Collection(inner) => inner.id(),
        }
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        match self {
            MemorySinkConnector::Bytes(inner) => inner.ready().await,
            MemorySinkConnector::Collection(inner) => inner.ready().await,
        }
    }

    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        match self {
            MemorySinkConnector::Bytes(inner) => inner.send(payload).await,
            MemorySinkConnector::Collection(inner) => inner.send(payload).await,
        }
    }

    async fn send_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        match self {
            MemorySinkConnector::Bytes(inner) => inner.send_collection(collection).await,
            MemorySinkConnector::Collection(inner) => inner.send_collection(collection).await,
        }
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        match self {
            MemorySinkConnector::Bytes(inner) => inner.close().await,
            MemorySinkConnector::Collection(inner) => inner.close().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{MemoryData, DEFAULT_MEMORY_PUBSUB_CAPACITY};
    use crate::model::batch_from_columns_simple;
    use datatypes::Value;
    use tokio::time::{timeout, Duration};

    fn sample_collection() -> SharedCollection {
        let batch = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2)],
        )])
        .expect("build collection");
        SharedCollection::from_box(Box::new(batch))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_bytes_sink_connector_publishes_bytes_and_rejects_collection_payloads() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "memory_bytes_sink_connector_contract";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare bytes topic");

        let mut connector = MemorySinkConnector::new(
            "mem_bytes",
            MemorySinkConfig::new("mem_bytes", topic, MemoryTopicKind::Bytes),
            registry.clone(),
        );
        connector.ready().await.expect("ready bytes connector");

        connector
            .send(b"without_subscribers")
            .await
            .expect("bytes send should succeed without subscribers");

        let err = connector
            .send_collection(&sample_collection())
            .await
            .expect_err("bytes connector should reject collection payloads");
        assert!(
            err.to_string()
                .contains("does not support collection payloads"),
            "unexpected collection rejection error: {err}"
        );

        let mut output = registry
            .open_subscribe_bytes(topic)
            .expect("subscribe bytes topic");
        connector
            .send(b"hello_bytes")
            .await
            .expect("publish bytes payload");

        let received = timeout(Duration::from_secs(2), output.recv())
            .await
            .expect("receive bytes timeout")
            .expect("receive bytes payload");
        match received {
            MemoryData::Bytes(bytes) => assert_eq!(bytes.as_ref(), b"hello_bytes"),
            MemoryData::Collection(_) => panic!("expected bytes payload"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_collection_sink_connector_publishes_collections_and_rejects_bytes_payloads() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "memory_collection_sink_connector_contract";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Collection,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare collection topic");

        let mut connector = MemorySinkConnector::new(
            "mem_collection",
            MemorySinkConfig::new("mem_collection", topic, MemoryTopicKind::Collection),
            registry.clone(),
        );
        connector.ready().await.expect("ready collection connector");

        let collection = sample_collection();
        connector
            .send_collection(&collection)
            .await
            .expect("collection send should succeed without subscribers");

        let err = connector
            .send(b"unexpected_bytes")
            .await
            .expect_err("collection connector should reject bytes payloads");
        assert!(
            err.to_string()
                .contains("does not support bytes payloads for collection topic"),
            "unexpected bytes rejection error: {err}"
        );

        let mut output = registry
            .open_subscribe_collection(topic)
            .expect("subscribe collection topic");
        connector
            .send_collection(&collection)
            .await
            .expect("publish collection payload");

        let received = timeout(Duration::from_secs(2), output.recv())
            .await
            .expect("receive collection timeout")
            .expect("receive collection payload");
        match received {
            MemoryData::Collection(shared) => {
                assert_eq!(shared.num_rows(), 2, "collection row count mismatch");
            }
            MemoryData::Bytes(_) => panic!("expected collection payload"),
        }
    }
}
