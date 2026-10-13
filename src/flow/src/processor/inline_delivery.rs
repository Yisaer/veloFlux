//! Direct stage connections for fused delivery chains. No payload assembly or queues.
use crate::processor::byte_stage::{CompressDriver, EncryptDriver};
use crate::processor::data_metrics::EncoderMetrics;
use crate::processor::delivery_output::{ChannelDeliveryOutput, DeliveryOutput};
use crate::processor::delivery_transform::{
    handle_compress_delivery, handle_encrypt_delivery, CompressDelivery, EncryptDelivery,
};
use crate::processor::{EncodedDeliveryFlags, ProcessorError, ProcessorStats, StreamData};
use std::sync::Arc;

pub(crate) struct ByteTransformChain {
    compress: Option<(CompressDriver, CompressDelivery)>,
    encrypt: Option<(EncryptDriver, EncryptDelivery)>,
    aborted: bool,
}

impl ByteTransformChain {
    pub(crate) fn new(compress: Option<CompressDriver>, encrypt: Option<EncryptDriver>) -> Self {
        Self {
            compress: compress.map(|driver| (driver, CompressDelivery::default())),
            encrypt: encrypt.map(|driver| (driver, EncryptDelivery::default())),
            aborted: false,
        }
    }
    pub(crate) fn has_transforms(&self) -> bool {
        self.compress.is_some() || self.encrypt.is_some()
    }
    pub(crate) fn output<'a>(
        &'a mut self,
        channel: ChannelDeliveryOutput<'a>,
        stats: &'a Arc<ProcessorStats>,
        metrics: &'a EncoderMetrics,
    ) -> InlineDeliveryOutput<'a> {
        InlineDeliveryOutput {
            chain: self,
            channel,
            stats,
            metrics,
        }
    }
}

pub(crate) struct InlineDeliveryOutput<'a> {
    chain: &'a mut ByteTransformChain,
    channel: ChannelDeliveryOutput<'a>,
    stats: &'a Arc<ProcessorStats>,
    metrics: &'a EncoderMetrics,
}

struct FinalDeliveryOutput<'a> {
    channel: ChannelDeliveryOutput<'a>,
    metrics: &'a EncoderMetrics,
}
impl DeliveryOutput for FinalDeliveryOutput<'_> {
    async fn send(&mut self, data: StreamData) -> Result<(), ProcessorError> {
        let counters = match &data {
            StreamData::EncodedDelivery { flags, bytes } => Some((*flags, bytes.len())),
            _ => None,
        };
        self.channel.send(data).await?;
        if let Some((flags, bytes)) = counters {
            if !flags.contains(EncodedDeliveryFlags::ABORT) {
                self.metrics.record_output_bytes(bytes);
                if flags.contains(EncodedDeliveryFlags::END) {
                    self.metrics.record_message_out();
                }
            }
        }
        Ok(())
    }
}

struct EncryptOutput<'a> {
    stage: &'a mut Option<(EncryptDriver, EncryptDelivery)>,
    downstream: FinalDeliveryOutput<'a>,
    stats: &'a Arc<ProcessorStats>,
    metrics: &'a EncoderMetrics,
    aborted: &'a mut bool,
}
impl DeliveryOutput for EncryptOutput<'_> {
    async fn send(&mut self, data: StreamData) -> Result<(), ProcessorError> {
        match (self.stage.as_mut(), data) {
            (Some((driver, delivery)), StreamData::EncodedDelivery { flags, bytes }) => {
                let aborted_delivery =
                    flags.contains(EncodedDeliveryFlags::START) || delivery.active;
                let result = handle_encrypt_delivery(
                    driver,
                    delivery,
                    flags,
                    bytes,
                    &mut self.downstream,
                    self.stats,
                    None,
                )
                .await;
                recover_transform(
                    result,
                    self.stats,
                    self.metrics,
                    self.aborted,
                    aborted_delivery,
                )
            }
            (_, data) => self.downstream.send(data).await,
        }
    }
}

fn recover_transform(
    result: Result<(), ProcessorError>,
    stats: &ProcessorStats,
    metrics: &EncoderMetrics,
    aborted: &mut bool,
    aborted_delivery: bool,
) -> Result<(), ProcessorError> {
    match result {
        Err(err @ ProcessorError::ChannelClosed) => Err(err),
        Err(err) => {
            stats.record_error_logged("fused byte stage error", err.to_string());
            if aborted_delivery {
                record_abort_once(aborted, metrics);
            }
            Ok(())
        }
        Ok(()) => Ok(()),
    }
}

fn record_abort_once(aborted: &mut bool, metrics: &EncoderMetrics) {
    if !*aborted {
        *aborted = true;
        metrics.record_message_aborted();
    }
}

impl DeliveryOutput for InlineDeliveryOutput<'_> {
    fn begin_delivery(&mut self) {
        self.chain.aborted = false;
    }
    fn abort_delivery(&mut self) {
        record_abort_once(&mut self.chain.aborted, self.metrics);
    }
    fn on_encoder_flush_error(&mut self, error: ProcessorError) -> Result<(), ProcessorError> {
        if self.chain.has_transforms() && matches!(error, ProcessorError::ChannelClosed) {
            return Err(error);
        }
        self.stats
            .record_error_logged("fused flush error", error.to_string());
        Ok(())
    }

    async fn send(&mut self, data: StreamData) -> Result<(), ProcessorError> {
        let mut downstream = EncryptOutput {
            stage: &mut self.chain.encrypt,
            downstream: FinalDeliveryOutput {
                channel: ChannelDeliveryOutput {
                    output: self.channel.output,
                    capacity: self.channel.capacity,
                    stats: self.channel.stats,
                },
                metrics: self.metrics,
            },
            stats: self.stats,
            metrics: self.metrics,
            aborted: &mut self.chain.aborted,
        };
        match (self.chain.compress.as_mut(), data) {
            (Some((driver, delivery)), StreamData::EncodedDelivery { flags, bytes }) => {
                let aborted_delivery =
                    flags.contains(EncodedDeliveryFlags::START) || delivery.active;
                let result = handle_compress_delivery(
                    driver,
                    delivery,
                    flags,
                    bytes,
                    &mut downstream,
                    self.stats,
                    None,
                )
                .await;
                recover_transform(
                    result,
                    self.stats,
                    self.metrics,
                    downstream.aborted,
                    aborted_delivery,
                )
            }
            (_, data) => downstream.send(data).await,
        }
    }
}
