//! Encoder abstractions for turning in-memory [`Collection`]s into outbound payloads.

mod json;
mod template_transform;

use crate::model::{Collection, Tuple};
use crate::planner::physical::ByIndexProjection;
use std::sync::Arc;

pub use json::JsonEncoder;

/// Errors that can occur during encoding.
#[derive(thiserror::Error, Debug)]
pub enum EncodeError {
    /// Failed to serialize into the requested format.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Custom error.
    #[error("{0}")]
    Other(String),
}

/// Trait implemented by every sink encoder.
pub trait CollectionEncoder: Send + Sync + 'static {
    /// Identifier for metrics/logging.
    fn id(&self) -> &str;
    /// Convert a collection into a single payload.
    fn encode(&self, collection: &dyn Collection) -> Result<Vec<u8>, EncodeError>;
    /// Whether this encoder supports streaming aggregation.
    fn supports_streaming(&self) -> bool {
        false
    }
    /// Whether this encoder supports index-based lazy materialization (`ByIndexProjection`).
    fn supports_index_lazy_materialization(&self) -> bool {
        false
    }
    /// Attach a by-index projection spec to enable index-based lazy materialization.
    fn with_by_index_projection(
        self: Arc<Self>,
        _spec: Arc<ByIndexProjection>,
    ) -> Result<Arc<dyn CollectionEncoder>, EncodeError> {
        Err(EncodeError::Other(
            "index lazy materialization is not supported for this encoder".to_string(),
        ))
    }
    /// Start a streaming session if supported.
    fn start_stream(&self) -> Option<Box<dyn CollectionEncoderStream>> {
        None
    }
}

/// Stateful encoder stream used for incremental encoding.
pub trait CollectionEncoderStream: Send {
    /// Append a tuple into the stream buffer.
    fn append(&mut self, tuple: &Tuple) -> Result<(), EncodeError>;
    /// Append an entire collection by iterating its rows.
    fn append_collection(&mut self, collection: &dyn Collection) -> Result<(), EncodeError> {
        for tuple in collection.rows() {
            self.append(tuple)?;
        }
        Ok(())
    }
    /// Finalize the stream and emit the payload.
    fn finish(self: Box<Self>) -> Result<Vec<u8>, EncodeError>;
}
