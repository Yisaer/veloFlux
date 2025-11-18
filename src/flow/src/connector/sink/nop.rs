//! A sink connector that simply discards every payload.

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;

/// A no-op sink connector useful for benchmarks or tests.
pub struct NopSinkConnector {
    id: String,
}

impl NopSinkConnector {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

#[async_trait]
impl SinkConnector for NopSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&mut self, _payload: &[u8]) -> Result<(), SinkConnectorError> {
        Ok(())
    }
}
