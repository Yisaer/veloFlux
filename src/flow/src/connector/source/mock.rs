//! Mock connector that emits configured payloads or accepts manually injected bytes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::checkpoint::CheckpointState;
use crate::connector::{
    ConnectorError, ConnectorEvent, ConnectorStream, SourceCheckpointMode, SourceCheckpointRequest,
    SourceConnector,
};

/// Connector exposing a handle for manually injecting payloads.
pub struct MockSourceConnector {
    id: String,
    payloads: Vec<Vec<u8>>,
    next_payload_index: Arc<AtomicUsize>,
    sender: mpsc::UnboundedSender<Result<ConnectorEvent, ConnectorError>>,
    payload_rx: Option<mpsc::UnboundedReceiver<Result<ConnectorEvent, ConnectorError>>>,
}

/// Handle used to push payloads into a [`MockSourceConnector`].
#[derive(Clone)]
pub struct MockSourceHandle {
    sender: mpsc::UnboundedSender<Result<ConnectorEvent, ConnectorError>>,
}

impl MockSourceConnector {
    /// Create a new mock connector along with the handle used for sending data.
    pub fn new(id: impl Into<String>) -> (Self, MockSourceHandle) {
        Self::new_with_channel_capacity(id, crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY)
    }

    pub fn new_with_channel_capacity(
        id: impl Into<String>,
        _channel_capacity: usize,
    ) -> (Self, MockSourceHandle) {
        Self::new_scripted(id, Vec::new())
    }

    /// Create a connector that emits the configured payloads once, in order.
    pub fn new_scripted(id: impl Into<String>, payloads: Vec<Vec<u8>>) -> (Self, MockSourceHandle) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                id: id.into(),
                payloads,
                next_payload_index: Arc::new(AtomicUsize::new(0)),
                sender: sender.clone(),
                payload_rx: Some(receiver),
            },
            MockSourceHandle { sender },
        )
    }
}

impl SourceConnector for MockSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        let receiver = self
            .payload_rx
            .take()
            .ok_or_else(|| ConnectorError::AlreadySubscribed(self.id.clone()))?;
        let start = self.next_payload_index.load(Ordering::Acquire);
        for (index, payload) in self.payloads.iter().enumerate().skip(start) {
            self.sender
                .send(Ok(ConnectorEvent::Payload(payload.clone())))
                .map_err(|_| ConnectorError::Other("mock source receiver closed".to_string()))?;
            self.next_payload_index
                .store(index.saturating_add(1), Ordering::Release);
        }
        tracing::info!(connector_id = %self.id, "mock source starting");

        Ok(Box::pin(UnboundedReceiverStream::new(receiver)))
    }

    fn request_checkpoint(
        &mut self,
        checkpoint_id: u64,
        _mode: SourceCheckpointMode,
    ) -> Result<SourceCheckpointRequest, ConnectorError> {
        self.sender
            .send(Ok(ConnectorEvent::CheckpointFence { checkpoint_id }))
            .map_err(|_| ConnectorError::Other("mock source receiver closed".to_string()))?;
        let next_payload_index = self.next_payload_index.load(Ordering::Acquire) as u64;
        Ok(Box::pin(async move {
            Ok(Some(CheckpointState::Unsigned(next_payload_index)))
        }))
    }

    fn validate_checkpoint(&self, state: &CheckpointState) -> Result<(), ConnectorError> {
        let CheckpointState::Unsigned(next_payload_index) = state else {
            return Err(ConnectorError::Other(
                "mock source checkpoint state must be an unsigned payload index".to_string(),
            ));
        };
        let next_payload_index = usize::try_from(*next_payload_index).map_err(|_| {
            ConnectorError::Other(
                "mock source checkpoint payload index exceeds platform limits".to_string(),
            )
        })?;
        if next_payload_index > self.payloads.len() {
            return Err(ConnectorError::Other(format!(
                "mock source checkpoint payload index {next_payload_index} exceeds configured payload count {}",
                self.payloads.len()
            )));
        }
        Ok(())
    }

    fn restore_checkpoint(&mut self, state: &CheckpointState) -> Result<(), ConnectorError> {
        self.validate_checkpoint(state)?;
        let CheckpointState::Unsigned(next_payload_index) = state else {
            return Err(ConnectorError::Other(
                "mock source checkpoint state must be an unsigned payload index".to_string(),
            ));
        };
        self.next_payload_index.store(
            usize::try_from(*next_payload_index).expect("validated mock source payload index"),
            Ordering::Release,
        );
        Ok(())
    }

    fn clear_checkpoint_restore(&mut self) {
        self.next_payload_index.store(0, Ordering::Release);
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.payload_rx = None;
        tracing::info!(connector_id = %self.id, "mock source closed");
        Ok(())
    }
}

impl MockSourceHandle {
    /// Send a payload to every subscriber of the mock connector.
    pub async fn send(&self, payload: impl Into<Vec<u8>>) -> Result<(), MockSourceError> {
        self.sender
            .send(Ok(ConnectorEvent::Payload(payload.into())))
            .map_err(|_| MockSourceError::Closed)
    }

    /// Signal that no further payloads will be sent.
    pub async fn close(&self) -> Result<(), MockSourceError> {
        self.sender
            .send(Ok(ConnectorEvent::EndOfStream))
            .map_err(|_| MockSourceError::Closed)
    }

    /// Borrow a clone of the underlying sender for advanced scenarios.
    pub fn sender(&self) -> mpsc::UnboundedSender<Result<ConnectorEvent, ConnectorError>> {
        self.sender.clone()
    }
}

/// Errors returned by [`MockSourceHandle`].
#[derive(thiserror::Error, Debug)]
pub enum MockSourceError {
    /// All subscribers dropped, no further payloads can be delivered.
    #[error("mock source closed")]
    Closed,
}
