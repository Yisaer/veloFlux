//! Shared batching, framing, and error boundaries for sink encoding.
use crate::codec::SinkEncoder;
use crate::model::Collection;
use crate::processor::batch_policy::BatchPolicy;
use crate::processor::data_metrics::EncoderMetrics;
use crate::processor::delivery_output::DeliveryOutput;
use crate::processor::{ProcessorError, StreamData};
use std::pin::Pin;
use tokio::time::{Instant, Sleep};
#[derive(Default)]
struct DeliveryEncoding {
    row_count: usize,
    active: bool,
    emitted_chunk: bool,
    pending_start_chunk: Option<bytes::Bytes>,
}

pub(crate) struct EncoderStage {
    encoder: Box<dyn SinkEncoder>,
    delivery: DeliveryEncoding,
    policy: BatchPolicy,
}

impl EncoderStage {
    pub(crate) fn new(encoder: Box<dyn SinkEncoder>, policy: BatchPolicy) -> Self {
        Self {
            encoder,
            delivery: DeliveryEncoding::default(),
            policy,
        }
    }
    pub(crate) fn has_data(&self) -> bool {
        self.delivery.row_count > 0
    }
    pub(crate) fn schedule_timer(&self, timer: &mut Option<Pin<Box<Sleep>>>, origin: Instant) {
        if let Some(duration) = self.policy.duration() {
            BatchPolicy::schedule_timer(timer, origin, duration, self.has_data());
        }
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
        DeliveryEncoding::handle_collection(
            id,
            collection,
            self.encoder.as_mut(),
            &mut self.delivery,
            &self.policy,
            output,
            timer,
            origin,
            metrics,
        )
        .await
    }
    pub(crate) async fn flush(
        &mut self,
        id: &str,
        output: &mut impl DeliveryOutput,
        metrics: Option<&EncoderMetrics>,
    ) -> Result<(), ProcessorError> {
        DeliveryEncoding::finish_delivery(
            id,
            self.encoder.as_mut(),
            &mut self.delivery,
            None,
            output,
            metrics,
        )
        .await
    }
}

impl DeliveryEncoding {
    fn start_delivery(
        encoder: &mut dyn SinkEncoder,
        delivery: &mut DeliveryEncoding,
        output: &mut impl DeliveryOutput,
    ) -> Result<(), ProcessorError> {
        let start_chunk = encoder.begin_delivery().map_err(|err| {
            ProcessorError::ProcessingError(format!("encoder begin error: {err}"))
        })?;
        output.begin_delivery();
        delivery.row_count = 0;
        delivery.active = true;
        delivery.emitted_chunk = false;
        delivery.pending_start_chunk = start_chunk.filter(|chunk| !chunk.is_empty());
        Ok(())
    }

    fn combine_chunks(chunks: [Option<bytes::Bytes>; 3]) -> Option<bytes::Bytes> {
        let mut count = 0;
        let mut total_len = 0;
        for chunk in chunks.iter().flatten() {
            if !chunk.is_empty() {
                count += 1;
                total_len += chunk.len();
            }
        }

        match count {
            0 => None,
            1 => chunks.into_iter().flatten().find(|chunk| !chunk.is_empty()),
            _ => {
                let mut bytes = Vec::with_capacity(total_len);
                for chunk in chunks.into_iter().flatten() {
                    if !chunk.is_empty() {
                        bytes.extend_from_slice(&chunk);
                    }
                }
                Some(bytes.into())
            }
        }
    }

    fn take_pending_start_and_combine(
        delivery: &mut DeliveryEncoding,
        chunk: Option<bytes::Bytes>,
    ) -> Option<bytes::Bytes> {
        if delivery.emitted_chunk {
            chunk
        } else {
            Self::combine_chunks([delivery.pending_start_chunk.take(), chunk, None])
        }
    }

    async fn emit_intermediate_chunk(
        chunk: Option<bytes::Bytes>,
        delivery: &mut DeliveryEncoding,
        output: &mut impl DeliveryOutput,
        metrics: Option<&EncoderMetrics>,
    ) -> Result<(), ProcessorError> {
        let chunk = Self::take_pending_start_and_combine(delivery, chunk);
        let Some(chunk) = chunk else {
            return Ok(());
        };
        let output_bytes = chunk.len();
        let is_first_chunk = !delivery.emitted_chunk;
        let data = if is_first_chunk {
            StreamData::encoded_delivery_start(chunk)
        } else {
            StreamData::encoded_delivery_chunk(chunk)
        };
        output.send(data).await?;
        if let Some(metrics) = metrics {
            metrics.record_output_bytes(output_bytes);
        }
        if is_first_chunk {
            delivery.emitted_chunk = true;
        }
        Ok(())
    }

    async fn abort_delivery(
        encoder: &mut dyn SinkEncoder,
        delivery: &mut DeliveryEncoding,
        output: &mut impl DeliveryOutput,
        metrics: Option<&EncoderMetrics>,
    ) -> Result<(), ProcessorError> {
        if !delivery.active {
            return Ok(());
        }

        encoder.abort_delivery();
        output.abort_delivery();
        if let Some(metrics) = metrics {
            metrics.record_message_aborted();
        }
        let should_emit_abort = delivery.emitted_chunk;
        *delivery = DeliveryEncoding::default();

        if should_emit_abort {
            output.send(StreamData::encoded_delivery_abort()).await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_delivery(
        processor_id: &str,
        encoder: &mut dyn SinkEncoder,
        delivery: &mut DeliveryEncoding,
        prefix_chunk: Option<bytes::Bytes>,
        output: &mut impl DeliveryOutput,
        metrics: Option<&EncoderMetrics>,
    ) -> Result<(), ProcessorError> {
        if !delivery.active {
            return Ok(());
        }

        let end_chunk = match encoder.finish_delivery() {
            Ok(chunk) => chunk,
            Err(err) => {
                Self::abort_delivery(encoder, delivery, output, metrics).await?;
                return Err(ProcessorError::ProcessingError(format!(
                    "encoder finish error: {err}"
                )));
            }
        };
        let chunk = if delivery.emitted_chunk {
            Self::combine_chunks([prefix_chunk, end_chunk, None])
        } else {
            Self::combine_chunks([delivery.pending_start_chunk.take(), prefix_chunk, end_chunk])
        };
        let chunk = chunk.unwrap_or_default();
        let output_bytes = chunk.len();
        let data = if delivery.emitted_chunk {
            StreamData::encoded_delivery_end(chunk)
        } else {
            StreamData::encoded_delivery_single(chunk)
        };
        *delivery = DeliveryEncoding::default();
        output.send(data).await?;
        if let Some(metrics) = metrics {
            metrics.record_output_bytes(output_bytes);
        }
        if let Some(metrics) = metrics {
            metrics.record_message_out();
        }
        tracing::debug!(processor_id = %processor_id, "flushed encoded delivery");
        Ok(())
    }

    fn append_collection_to_delivery(
        encoder: &mut dyn SinkEncoder,
        delivery: &mut DeliveryEncoding,
        collection: &dyn Collection,
        output: &mut impl DeliveryOutput,
    ) -> Result<Option<bytes::Bytes>, ProcessorError> {
        if !delivery.active {
            Self::start_delivery(encoder, delivery, output)?;
        }

        let chunk = match encoder.append(collection) {
            Ok(chunk) => chunk,
            Err(err) => {
                return Err(ProcessorError::ProcessingError(format!(
                    "encoder append error: {err}"
                )));
            }
        };
        delivery.row_count += collection.num_rows();
        Ok(chunk)
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_collection(
        processor_id: &str,
        collection: Box<dyn Collection>,
        encoder: &mut dyn SinkEncoder,
        delivery: &mut DeliveryEncoding,
        mode: &BatchPolicy,
        output: &mut impl DeliveryOutput,
        timer: &mut Option<Pin<Box<Sleep>>>,
        grid_origin: Instant,
        metrics: Option<&EncoderMetrics>,
    ) -> Result<(), ProcessorError> {
        let row_total = collection.num_rows();

        if row_total == 0 {
            if matches!(mode, BatchPolicy::Immediate) {
                Self::start_delivery(encoder, delivery, output)?;
                Self::finish_delivery(processor_id, encoder, delivery, None, output, metrics)
                    .await?;
            }
            return Ok(());
        }

        if let Some(count) = mode.count_threshold() {
            let mut offset = 0;
            while offset < row_total {
                let remaining_capacity = count - delivery.row_count;
                let take_rows = remaining_capacity.min(row_total - offset);
                let end = offset + take_rows;
                let segment;
                let collection_segment: &dyn Collection = if offset == 0 && end == row_total {
                    collection.as_ref()
                } else {
                    segment = collection.slice(offset, end).map_err(|err| {
                        ProcessorError::ProcessingError(format!("collection slice error: {err}"))
                    })?;
                    segment.as_ref()
                };
                let chunk = match Self::append_collection_to_delivery(
                    encoder,
                    delivery,
                    collection_segment,
                    output,
                ) {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        Self::abort_delivery(encoder, delivery, output, metrics).await?;
                        return Err(err);
                    }
                };
                offset = end;

                if delivery.row_count >= count {
                    Self::finish_delivery(processor_id, encoder, delivery, chunk, output, metrics)
                        .await?;
                } else {
                    Self::emit_intermediate_chunk(chunk, delivery, output, metrics).await?;
                }
            }
        } else {
            let chunk = match Self::append_collection_to_delivery(
                encoder,
                delivery,
                collection.as_ref(),
                output,
            ) {
                Ok(chunk) => chunk,
                Err(err) => {
                    Self::abort_delivery(encoder, delivery, output, metrics).await?;
                    return Err(err);
                }
            };
            if matches!(mode, BatchPolicy::Immediate) {
                Self::finish_delivery(processor_id, encoder, delivery, chunk, output, metrics)
                    .await?;
                return Ok(());
            }
            Self::emit_intermediate_chunk(chunk, delivery, output, metrics).await?;
        }

        if let Some(duration) = mode.duration() {
            BatchPolicy::schedule_timer(timer, grid_origin, duration, delivery.row_count > 0);
        }
        Ok(())
    }
}
