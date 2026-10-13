//! Output boundary shared by channel-backed and in-task delivery stages.
use crate::processor::base::{send_with_backpressure, LinkOutput};
use crate::processor::{ProcessorError, ProcessorStats, StreamData};
use std::future::Future;

pub(crate) trait DeliveryOutput: Send {
    /// Encoder lifecycle notifications also cover deliveries that fail before
    /// emitting START. Channel outputs use the encoder's existing metrics.
    fn begin_delivery(&mut self) {}
    fn abort_delivery(&mut self) {}

    /// A due flush is a timer operation, not a failure of the new input.
    fn on_encoder_flush_error(&mut self, error: ProcessorError) -> Result<(), ProcessorError> {
        Err(error)
    }

    fn send(&mut self, data: StreamData)
        -> impl Future<Output = Result<(), ProcessorError>> + Send;
}

pub(crate) struct ChannelDeliveryOutput<'a> {
    pub(crate) output: &'a LinkOutput<StreamData>,
    pub(crate) capacity: usize,
    pub(crate) stats: &'a ProcessorStats,
}

impl DeliveryOutput for ChannelDeliveryOutput<'_> {
    fn on_encoder_flush_error(&mut self, error: ProcessorError) -> Result<(), ProcessorError> {
        self.stats
            .record_error_logged("encoder flush error", error.to_string());
        Ok(())
    }

    async fn send(&mut self, data: StreamData) -> Result<(), ProcessorError> {
        send_with_backpressure(self.output, self.capacity, data, Some(self.stats)).await
    }
}
