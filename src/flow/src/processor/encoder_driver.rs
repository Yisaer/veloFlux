//! Channel transport driver for standalone sink encoders.
//! The shared EncoderStage remains synchronous.
use super::batch_policy::BatchPolicy;
use super::data_metrics::EncoderMetrics;
use super::delivery_output::DeliveryOutput;
use super::encoder_stage::{EncoderOutput, EncoderStage};
use super::{EncodedDeliveryFlags, ProcessorError, StreamData};
use crate::codec::SinkEncoder;
use crate::model::Collection;
use std::pin::Pin;
use tokio::time::{Instant, Sleep};

pub(crate) struct EncoderDriver {
    stage: EncoderStage,
}
impl EncoderDriver {
    pub(crate) fn new(encoder: Box<dyn SinkEncoder>, policy: BatchPolicy) -> Self {
        Self {
            stage: EncoderStage::new(encoder, policy),
        }
    }
    #[cfg(test)]
    pub(crate) fn has_data(&self) -> bool {
        self.stage.has_data()
    }
    pub(crate) fn schedule_timer(&self, timer: &mut Option<Pin<Box<Sleep>>>, origin: Instant) {
        if let Some(duration) = self.stage.batch_duration() {
            BatchPolicy::schedule_timer(timer, origin, duration, self.stage.has_data());
        }
    }
    async fn drive(
        &mut self,
        mut pending: EncoderOutput,
        output: &mut impl DeliveryOutput,
        metrics: Option<&EncoderMetrics>,
    ) -> Result<(), ProcessorError> {
        while let Some(step) = self.stage.next_step(&mut pending) {
            if step.began {
                output.begin_delivery();
            }
            if step.aborted {
                output.abort_delivery();
                if let Some(metrics) = metrics {
                    metrics.record_message_aborted();
                }
            }
            if let Some(frame) = step.frame {
                let measurement = match &frame {
                    StreamData::EncodedDelivery { flags, bytes } => (*flags, bytes.len()),
                    _ => {
                        return Err(ProcessorError::ProcessingError(
                            "encoder produced a non-delivery frame".into(),
                        ))
                    }
                };
                if let Err(error) = output.send(frame).await {
                    let (flags, _) = measurement;
                    if flags.contains(EncodedDeliveryFlags::START)
                        && !flags.contains(EncodedDeliveryFlags::END)
                    {
                        self.stage.output_failed(flags);
                    }
                    return Err(error);
                }
                if let Some(metrics) = metrics {
                    let (flags, bytes) = measurement;
                    if !flags.contains(EncodedDeliveryFlags::ABORT) {
                        metrics.record_output_bytes(bytes);
                        if flags.contains(EncodedDeliveryFlags::END) {
                            metrics.record_message_out();
                        }
                    }
                }
            }
            if let Some(error) = step.error {
                return Err(error);
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn feed(
        &mut self,
        id: &str,
        collection: Box<dyn Collection>,
        output: &mut impl DeliveryOutput,
        timer: &mut Option<Pin<Box<Sleep>>>,
        origin: Instant,
        metrics: Option<&EncoderMetrics>,
    ) -> Result<(), ProcessorError> {
        if BatchPolicy::take_due_timer(timer, Instant::now()) {
            if let Err(error) = self.flush(id, output, metrics).await {
                output.on_encoder_flush_error(error)?;
            }
        }
        let pending = EncoderStage::input(collection);
        let nonempty = !pending.is_empty();
        self.drive(pending, output, metrics).await?;
        if nonempty {
            self.schedule_timer(timer, origin);
        }
        Ok(())
    }
    pub(crate) async fn flush(
        &mut self,
        id: &str,
        output: &mut impl DeliveryOutput,
        metrics: Option<&EncoderMetrics>,
    ) -> Result<(), ProcessorError> {
        self.drive(EncoderStage::finish(), output, metrics).await?;
        tracing::debug!(processor_id = %id, "flushed encoded delivery");
        Ok(())
    }
}
