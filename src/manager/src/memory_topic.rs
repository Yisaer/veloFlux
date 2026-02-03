use crate::pipeline::AppState;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use flow::connector::{DEFAULT_MEMORY_PUBSUB_CAPACITY, MemoryPubSubError, MemoryTopicKind};
use serde::{Deserialize, Serialize};
use storage::{StorageError, StoredMemoryTopic, StoredMemoryTopicKind};

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum MemoryTopicKindRequest {
    Bytes,
    Collection,
}

impl MemoryTopicKindRequest {
    fn as_flow_kind(&self) -> MemoryTopicKind {
        match self {
            MemoryTopicKindRequest::Bytes => MemoryTopicKind::Bytes,
            MemoryTopicKindRequest::Collection => MemoryTopicKind::Collection,
        }
    }

    fn as_storage_kind(&self) -> StoredMemoryTopicKind {
        match self {
            MemoryTopicKindRequest::Bytes => StoredMemoryTopicKind::Bytes,
            MemoryTopicKindRequest::Collection => StoredMemoryTopicKind::Collection,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CreateMemoryTopicRequest {
    pub topic: String,
    pub kind: MemoryTopicKindRequest,
    #[serde(default)]
    pub capacity: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct MemoryTopicInfo {
    pub topic: String,
    pub kind: StoredMemoryTopicKind,
    pub capacity: usize,
}

pub async fn create_memory_topic_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateMemoryTopicRequest>,
) -> impl IntoResponse {
    let topic = req.topic.trim().to_string();
    if topic.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "topic must not be empty".to_string(),
        )
            .into_response();
    }
    let capacity = req
        .capacity
        .unwrap_or(DEFAULT_MEMORY_PUBSUB_CAPACITY)
        .max(1);

    let stored = StoredMemoryTopic {
        topic: topic.clone(),
        kind: req.kind.as_storage_kind(),
        capacity,
    };

    match state.storage.create_memory_topic(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("memory topic {topic} already exists"),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist memory topic {topic}: {err}"),
            )
                .into_response();
        }
    }

    let registry = state.instance.memory_pubsub_registry();
    if let Err(err) = registry.declare_topic(&topic, req.kind.as_flow_kind(), capacity) {
        let _ = state.storage.delete_memory_topic(&topic);
        let (status, message) = match err {
            MemoryPubSubError::InvalidTopic => {
                (StatusCode::BAD_REQUEST, format!("invalid topic {topic}"))
            }
            MemoryPubSubError::NotFound { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unexpected memory pubsub error: {err}"),
            ),
            MemoryPubSubError::TimeoutWaitingForSubscribers { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unexpected memory pubsub error: {err}"),
            ),
            MemoryPubSubError::TopicKindMismatch { .. }
            | MemoryPubSubError::TopicCapacityMismatch { .. } => {
                (StatusCode::CONFLICT, err.to_string())
            }
        };
        return (status, message).into_response();
    }

    (
        StatusCode::CREATED,
        Json(MemoryTopicInfo {
            topic,
            kind: stored.kind,
            capacity,
        }),
    )
        .into_response()
}

pub async fn list_memory_topics_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_memory_topics() {
        Ok(topics) => {
            let mut items = topics
                .into_iter()
                .map(|topic| MemoryTopicInfo {
                    topic: topic.topic,
                    kind: topic.kind,
                    capacity: topic.capacity,
                })
                .collect::<Vec<_>>();
            items.sort_by(|a, b| a.topic.cmp(&b.topic));
            Json(items).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list memory topics: {err}"),
        )
            .into_response(),
    }
}
