//! Sink connector abstractions for delivering results to external systems.

use async_trait::async_trait;

use crate::model::Collection;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeliveryResult {
    pub bytes_written: u64,
}

/// Trait implemented by all sink connectors.
#[async_trait]
pub trait SinkConnector: Send + Sync + 'static {
    /// Identifier for logging/metrics.
    fn id(&self) -> &str;

    /// Record that one logical message was accepted at the connector boundary.
    fn record_message_in(&self) {}

    /// Record that one logical message was successfully delivered externally.
    fn record_message_out(&self) {}

    /// Maximum encoded payload size accepted for one delivery.
    ///
    /// Connectors that do not have a fixed delivery-size limit return `None`.
    fn max_delivery_bytes(&self) -> Option<usize> {
        None
    }

    /// Start a new encoded delivery.
    ///
    /// Implementations must clean up connector-owned resources if this future is dropped before it
    /// completes. The sink processor may cancel it when a capture barrier interrupts delivery.
    async fn start_delivery(
        &mut self,
        _properties: &crate::PropertyContext,
    ) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Permanent(format!(
            "connector `{}` does not support encoded delivery",
            self.id()
        )))
    }

    /// Write a chunk into the active encoded delivery.
    async fn write_chunk(&mut self, _bytes: &[u8]) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Permanent(format!(
            "connector `{}` does not support encoded delivery",
            self.id()
        )))
    }

    /// Finish the active encoded delivery.
    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        Err(SinkConnectorError::Permanent(format!(
            "connector `{}` does not support encoded delivery",
            self.id()
        )))
    }

    /// Abort the active encoded delivery best-effort.
    async fn abort_delivery(&mut self) {}

    /// Send a `Collection` downstream without going through an encoder.
    ///
    /// By default connectors reject collection payloads. Connectors that operate on decoded
    /// data (e.g. Kuksa sink) should override this method.
    async fn send_collection(
        &mut self,
        _collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Permanent(format!(
            "connector `{}` does not support collection payloads",
            self.id()
        )))
    }

    /// Prepare the connector for sending (e.g. establish network connections).
    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        Ok(())
    }

    /// Signal that no more payloads will be sent.
    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        Ok(())
    }
}

/// Retry classes reported by sink connectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkConnectorErrorClass {
    Transient,
    Uncertain,
    Permanent,
}

/// Errors shared by sink connectors.
#[derive(thiserror::Error, Debug)]
pub enum SinkConnectorError {
    /// Connector availability is unknown (e.g. a channel closed).
    /// The sink treats this as uncertain unless the connector uses a more specific class.
    #[error("connector unavailable: {0}")]
    Unavailable(String),

    /// Failure with strong evidence of recovery after backoff.
    /// Examples: network timeout, connection refused, HTTP 408/429/502/503/504.
    #[error("transient failure: {0}")]
    Transient(String),

    /// Failure whose future outcome cannot be determined from this response.
    /// Examples: HTTP 500 and 404.
    #[error("uncertain failure: {0}")]
    Uncertain(String),

    /// Replaying the same delivery under the current configuration cannot succeed.
    /// Examples: invalid local URL, template, topic, or payload.
    #[error("permanent failure: {0}")]
    Permanent(String),

    /// Catch-all for errors that don't fall into the categories above.
    /// The sink treats this as uncertain until the connector provides a specific class.
    #[error("{0}")]
    Other(String),
}

impl SinkConnectorError {
    pub fn class(&self) -> SinkConnectorErrorClass {
        match self {
            Self::Transient(_) => SinkConnectorErrorClass::Transient,
            Self::Uncertain(_) | Self::Unavailable(_) | Self::Other(_) => {
                SinkConnectorErrorClass::Uncertain
            }
            Self::Permanent(_) => SinkConnectorErrorClass::Permanent,
        }
    }
}

pub mod file;
pub mod http;
pub mod kuksa;
pub mod kura;
pub mod memory;
pub mod mock;
pub mod mqtt;
#[cfg(feature = "nng_pubsub")]
pub mod nng_pubsub;
pub mod nop;
pub mod video;
