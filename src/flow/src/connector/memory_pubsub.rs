//! In-process memory pub/sub used by memory source/sink connectors.

use crate::model::Collection;
use bytes::Bytes;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

pub const DEFAULT_MEMORY_PUBSUB_CAPACITY: usize = 1024;

/// A cheaply cloneable `Collection` wrapper suitable for broadcast fan-out.
#[derive(Clone)]
pub struct SharedCollection(Arc<dyn Collection>);

impl SharedCollection {
    pub fn new(inner: Arc<dyn Collection>) -> Self {
        Self(inner)
    }

    pub fn from_box(inner: Box<dyn Collection>) -> Self {
        Self(Arc::from(inner))
    }

    pub fn inner(&self) -> &Arc<dyn Collection> {
        &self.0
    }
}

impl std::fmt::Debug for SharedCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedCollection").finish()
    }
}

impl Collection for SharedCollection {
    fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    fn rows(&self) -> &[crate::model::Tuple] {
        self.0.rows()
    }

    fn slice(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Box<dyn Collection>, crate::model::CollectionError> {
        self.0.slice(start, end)
    }

    fn take(
        &self,
        indices: &[usize],
    ) -> Result<Box<dyn Collection>, crate::model::CollectionError> {
        self.0.take(indices)
    }

    fn apply_projection(
        &self,
        fields: &[crate::planner::physical::PhysicalProjectField],
    ) -> Result<Box<dyn Collection>, crate::model::CollectionError> {
        self.0.apply_projection(fields)
    }

    fn apply_filter(
        &self,
        filter_expr: &crate::expr::ScalarExpr,
    ) -> Result<Box<dyn Collection>, crate::model::CollectionError> {
        self.0.apply_filter(filter_expr)
    }

    fn clone_box(&self) -> Box<dyn Collection> {
        Box::new(self.clone())
    }

    fn into_rows(
        self: Box<Self>,
    ) -> Result<Vec<crate::model::Tuple>, crate::model::CollectionError> {
        self.0.clone_box().into_rows()
    }
}

/// Payload types that can be published via the memory pub/sub topics.
#[derive(Clone, Debug)]
pub enum MemoryData {
    Bytes(Bytes),
    Collection(SharedCollection),
}

#[derive(thiserror::Error, Debug)]
pub enum MemoryPubSubError {
    #[error("topic must not be empty")]
    InvalidTopic,
}

#[derive(Clone)]
pub struct MemoryPubSubRegistry {
    topics: Arc<RwLock<HashMap<String, TopicEntry>>>,
}

#[derive(Clone)]
struct TopicEntry {
    sender: broadcast::Sender<MemoryData>,
    capacity: usize,
}

impl MemoryPubSubRegistry {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn publisher(
        &self,
        topic: &str,
        capacity: Option<usize>,
    ) -> Result<broadcast::Sender<MemoryData>, MemoryPubSubError> {
        Ok(self.ensure_topic(topic, capacity)?.sender)
    }

    pub fn subscribe(
        &self,
        topic: &str,
        capacity: Option<usize>,
    ) -> Result<broadcast::Receiver<MemoryData>, MemoryPubSubError> {
        Ok(self.ensure_topic(topic, capacity)?.sender.subscribe())
    }

    fn ensure_topic(
        &self,
        topic: &str,
        capacity: Option<usize>,
    ) -> Result<TopicEntry, MemoryPubSubError> {
        if topic.trim().is_empty() {
            return Err(MemoryPubSubError::InvalidTopic);
        }
        let cap = capacity.unwrap_or(DEFAULT_MEMORY_PUBSUB_CAPACITY).max(1);

        {
            let guard = self.topics.read().expect("memory pubsub registry poisoned");
            if let Some(entry) = guard.get(topic) {
                return Ok(entry.clone());
            }
        }

        let mut guard = self
            .topics
            .write()
            .expect("memory pubsub registry poisoned");
        Ok(guard
            .entry(topic.to_string())
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(cap);
                TopicEntry {
                    sender,
                    capacity: cap,
                }
            })
            .clone())
    }

    pub fn topic_capacity(&self, topic: &str) -> Option<usize> {
        let guard = self.topics.read().expect("memory pubsub registry poisoned");
        guard.get(topic).map(|entry| entry.capacity)
    }
}

impl Default for MemoryPubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Accessor for the process-wide memory pub/sub registry.
pub fn registry() -> &'static MemoryPubSubRegistry {
    static REGISTRY: Lazy<MemoryPubSubRegistry> = Lazy::new(MemoryPubSubRegistry::new);
    &REGISTRY
}
