//! Memory sink connector that publishes to in-process pub/sub topics.

use super::{SinkConnector, SinkConnectorError};
use crate::connector::memory_pubsub::{
    registry as memory_pubsub_registry, MemoryPubSubRegistry, MemoryTopicKind, SharedCollection,
};
use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::planner::physical::output_schema::{OutputSchema, OutputValueGetter};
use async_trait::async_trait;
use bytes::Bytes;
use datatypes::Value;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MemorySinkConfig {
    pub sink_name: String,
    pub topic: String,
    pub kind: MemoryTopicKind,
    pub collection_output_schema: Option<OutputSchema>,
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
            collection_output_schema: None,
        }
    }

    pub fn with_collection_output_schema(mut self, schema: OutputSchema) -> Self {
        self.collection_output_schema = Some(schema);
        self
    }
}

pub enum MemorySinkConnector {
    Bytes(MemoryBytesSinkConnector),
    Collection(MemoryCollectionSinkConnector),
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
    output_schema: Option<OutputSchema>,
    keys: Option<Arc<[Arc<str>]>>,
    message_source: Arc<str>,
}

impl MemoryCollectionSinkConnector {
    fn new(
        id: impl Into<String>,
        config: MemorySinkConfig,
        registry: MemoryPubSubRegistry,
    ) -> Self {
        let topic = config.topic;
        Self {
            id: id.into(),
            message_source: Arc::<str>::from(topic.as_str()),
            topic,
            registry,
            publisher: None,
            output_schema: config.collection_output_schema,
            keys: None,
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

    fn ensure_keys(
        &mut self,
        output_schema: &OutputSchema,
    ) -> Result<Arc<[Arc<str>]>, SinkConnectorError> {
        if self.keys.is_none() {
            let keys: Vec<Arc<str>> = output_schema
                .columns
                .iter()
                .map(|col| Arc::clone(&col.name))
                .collect();
            self.keys = Some(Arc::from(keys));
        }

        Ok(Arc::clone(self.keys.as_ref().expect("initialized above")))
    }

    fn get_output_schema(&self) -> Result<OutputSchema, SinkConnectorError> {
        self.output_schema.clone().ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "memory collection sink `{}` missing output schema (planner bug)",
                self.id
            ))
        })
    }
}

#[async_trait]
impl SinkConnector for MemoryCollectionSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        let schema = self.get_output_schema()?;
        let _ = self.ensure_publisher()?;
        let _ = self.ensure_keys(&schema)?;
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
        let output_schema = self.get_output_schema()?;
        let keys = self.ensure_keys(&output_schema)?;

        let mut missing = BTreeSet::<String>::new();
        let mut rows = Vec::with_capacity(collection.num_rows());
        for tuple in collection.rows() {
            let mut values = Vec::with_capacity(output_schema.columns.len());
            for col in output_schema.columns.iter() {
                let value: Option<Arc<Value>> = match &col.getter {
                    OutputValueGetter::Affiliate { column_name } => tuple
                        .affiliate()
                        .and_then(|aff| aff.value(column_name.as_ref()))
                        .map(|v| Arc::new(v.clone())),
                    OutputValueGetter::MessageByName {
                        source_name,
                        column_name,
                    } => message_value_by_name(tuple, source_name.as_ref(), column_name.as_ref()),
                };

                values.push(value.unwrap_or_else(|| {
                    missing.insert(format!("{} (getter={:?})", col.name.as_ref(), col.getter));
                    Arc::new(Value::Null)
                }));
            }

            let msg = Arc::new(Message::new_shared_keys(
                Arc::clone(&self.message_source),
                Arc::clone(&keys),
                values,
            ));
            rows.push(Tuple::with_timestamp(Arc::from(vec![msg]), tuple.timestamp));
        }

        if !missing.is_empty() {
            tracing::warn!(
                sink_id = %self.id,
                topic = %self.topic,
                missing_columns = ?missing,
                "memory sink filled NULL for missing columns"
            );
        }

        let batch = RecordBatch::new(rows)
            .map_err(|err| SinkConnectorError::Other(format!("invalid record batch: {err}")))?;
        let shared = SharedCollection::from_box(Box::new(batch));

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

fn message_value_by_name(tuple: &Tuple, source: &str, column: &str) -> Option<Arc<Value>> {
    for msg in tuple.messages().iter() {
        if msg.source() != source {
            continue;
        }
        if let Some((_, value)) = msg.entry_by_name(column) {
            return Some(Arc::clone(value));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_value_by_name_scans_all_messages_for_source() {
        let msg1 = Arc::new(Message::new(
            Arc::<str>::from("stream"),
            vec![Arc::<str>::from("a"), Arc::<str>::from("b")],
            vec![Arc::new(Value::Int64(1)), Arc::new(Value::Int64(2))],
        ));
        let msg2 = Arc::new(Message::new(
            Arc::<str>::from("stream"),
            vec![Arc::<str>::from("x")],
            vec![Arc::new(Value::Int64(10))],
        ));
        let tuple = Tuple::new(vec![msg1, msg2]);

        let got = message_value_by_name(&tuple, "stream", "x").expect("x exists in msg2");
        assert_eq!(*got, Value::Int64(10));
    }
}
