//! Shared collection construction; historical batch transport exists only in tests.
use crate::model::{Collection, RecordBatch, Tuple};
#[cfg(test)]
use crate::processor::batch_policy::BatchPolicy;
#[cfg(test)]
use crate::processor::delivery_output::DeliveryOutput;
use crate::processor::ProcessorError;
#[cfg(test)]
use crate::processor::{ProcessorStats, StreamData};
#[cfg(test)]
use std::pin::Pin;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::time::{Instant, Sleep};
pub(crate) struct NativeBatchStage;
impl NativeBatchStage {
    pub(crate) fn collection(rows: Vec<Tuple>) -> Result<Box<dyn Collection>, ProcessorError> {
        RecordBatch::new(rows)
            .map(|batch| Box::new(batch) as Box<dyn Collection>)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub(crate) async fn feed<R: IntoIterator<Item = Tuple> + Send>(
        id: &str,
        rows: R,
        buffer: &mut Vec<Tuple>,
        output: &mut impl DeliveryOutput,
        timer: &mut Option<Pin<Box<Sleep>>>,
        origin: Instant,
        policy: &BatchPolicy,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        if BatchPolicy::take_due_timer(timer, Instant::now()) {
            Self::flush_all(id, buffer, output, stats).await?;
        }
        buffer.extend(rows);
        if let Some(count) = policy.count_threshold() {
            Self::drain_by_count(id, buffer, output, count, stats).await?;
        }
        if let Some(duration) = policy.duration() {
            BatchPolicy::schedule_timer(timer, origin, duration, !buffer.is_empty());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn emit_batch(
        processor_id: &str,
        rows: Vec<Tuple>,
        output: &mut impl DeliveryOutput,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        let row_count = rows.len() as u64;
        let collection = Self::collection(rows)?;
        output.send(StreamData::collection(collection)).await?;
        stats.record_collection_out(row_count);
        tracing::info!(processor_id = %processor_id, "flushed batch");
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn flush_all(
        processor_id: &str,
        buffer: &mut Vec<Tuple>,
        output: &mut impl DeliveryOutput,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        if buffer.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(buffer);
        Self::emit_batch(processor_id, rows, output, stats).await
    }

    #[cfg(test)]
    pub(crate) async fn flush_count(
        processor_id: &str,
        buffer: &mut Vec<Tuple>,
        output: &mut impl DeliveryOutput,
        count: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        if buffer.len() < count {
            return Ok(());
        }
        let rows: Vec<Tuple> = buffer.drain(..count).collect();
        Self::emit_batch(processor_id, rows, output, stats).await
    }

    #[cfg(test)]
    pub(crate) async fn drain_by_count(
        processor_id: &str,
        buffer: &mut Vec<Tuple>,
        output: &mut impl DeliveryOutput,
        count: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        while buffer.len() >= count {
            Self::flush_count(processor_id, buffer, output, count, stats).await?;
        }
        Ok(())
    }
}
