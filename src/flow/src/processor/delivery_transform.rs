//! Delivery framing and recovery shared by standalone and fused byte transforms.
use crate::processor::byte_stage::{CompressDriver, EncryptDriver};
use crate::processor::data_metrics::TransformMetrics;
use crate::processor::delivery_output::DeliveryOutput;
use crate::processor::{EncodedDeliveryFlags, ProcessorError, ProcessorStats, StreamData};
use bytes::Bytes;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct CompressDelivery {
    pub(crate) active: bool,
    /// Whether a START chunk has already been forwarded to downstream.
    /// Used to decide whether ABORT must be propagated.
    emitted_chunk: bool,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_compress_delivery(
    driver: &mut CompressDriver,
    delivery: &mut CompressDelivery,
    flags: EncodedDeliveryFlags,
    bytes: Bytes,
    output: &mut impl DeliveryOutput,
    stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
) -> Result<(), ProcessorError> {
    if let Some(metrics) = metrics {
        metrics.record_input_bytes(bytes.len());
    }

    let is_abort = flags.contains(EncodedDeliveryFlags::ABORT);
    let is_start = flags.contains(EncodedDeliveryFlags::START);
    let is_end = flags.contains(EncodedDeliveryFlags::END);

    if is_abort {
        if let Some(metrics) = metrics {
            metrics.record_aborted();
        }
        driver.abort();
        let was_started = delivery.emitted_chunk;
        *delivery = CompressDelivery::default();
        if was_started {
            // Forward ABORT only if downstream already received a START.
            // ABORT is not a completed delivery → not counted in record_out.
            output.send(StreamData::encoded_delivery_abort()).await?;
        }
        return Ok(());
    }

    if is_start {
        if delivery.active {
            return fail_compress_delivery(
                driver,
                delivery,
                output,
                stats,
                metrics,
                "START received while delivery already active",
            )
            .await;
        }
        if let Err(e) = driver.begin() {
            return fail_compress_delivery(
                driver,
                delivery,
                output,
                stats,
                metrics,
                &e.to_string(),
            )
            .await;
        }
        delivery.active = true;
    } else if !delivery.active {
        return Err(ProcessorError::ProcessingError(
            "chunk/END received without active delivery".into(),
        ));
    }

    if is_end {
        if let Some(metrics) = metrics {
            metrics.record_message_in();
        }
    }

    // Compress the input bytes (if any).
    let mut out = Vec::new();
    if !bytes.is_empty() {
        if let Err(e) = driver.write(&bytes, &mut out) {
            return fail_compress_delivery(
                driver,
                delivery,
                output,
                stats,
                metrics,
                &e.to_string(),
            )
            .await;
        }
    }

    if is_end {
        if let Err(e) = driver.finish(&mut out) {
            return fail_compress_delivery(
                driver,
                delivery,
                output,
                stats,
                metrics,
                &e.to_string(),
            )
            .await;
        }
        *delivery = CompressDelivery::default();
        // Always forward END (even if out is empty).
        let output_bytes = out.len();
        let data = if is_start {
            StreamData::encoded_delivery_single(out)
        } else {
            StreamData::encoded_delivery_end(out)
        };
        output.send(data).await?;
        if let Some(metrics) = metrics {
            metrics.record_output_bytes(output_bytes);
        }
        if let Some(metrics) = metrics {
            metrics.record_message_out();
        }
    } else if is_start {
        // Always forward START (even if out is empty). START is not a completed
        // delivery → not counted in record_out.
        let output_bytes = out.len();
        output.send(StreamData::encoded_delivery_start(out)).await?;
        if let Some(metrics) = metrics {
            metrics.record_output_bytes(output_bytes);
        }
        delivery.emitted_chunk = true;
    } else if !out.is_empty() {
        // Middle chunk: only forward when natural drain produced output.
        // Not a completed delivery → not counted in record_out.
        let output_bytes = out.len();
        output.send(StreamData::encoded_delivery_chunk(out)).await?;
        if let Some(metrics) = metrics {
            metrics.record_output_bytes(output_bytes);
        }
    }

    Ok(())
}

/// Abort an in-progress delivery: reset writer state, notify downstream if a
/// START was already emitted, record the error, and reset the delivery state.
///
/// This does **not** return an error — the caller records the error separately
/// and decides whether to propagate it.
pub(crate) async fn abort_compress_in_flight(
    driver: &mut CompressDriver,
    delivery: &mut CompressDelivery,
    output: &mut impl DeliveryOutput,
    stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
    processor_id: &str,
) {
    if let Some(metrics) = metrics {
        metrics.record_aborted();
    }
    driver.abort();
    let was_started = delivery.emitted_chunk;
    *delivery = CompressDelivery::default();
    if was_started {
        if let Err(err) = output.send(StreamData::encoded_delivery_abort()).await {
            tracing::error!(
                processor_id = %processor_id,
                error = %err,
                "failed to forward ABORT during in-flight abort"
            );
        }
    }
    tracing::error!(
        processor_id = %processor_id,
        "aborted in-flight delivery: terminal signal received mid-delivery"
    );
    stats.record_error("terminal signal received mid-delivery".to_string());
}

/// Reset writer + delivery state and notify downstream when START was already
/// emitted, then surface the error.
async fn fail_compress_delivery(
    driver: &mut CompressDriver,
    delivery: &mut CompressDelivery,
    output: &mut impl DeliveryOutput,
    _stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
    reason: &str,
) -> Result<(), ProcessorError> {
    if let Some(metrics) = metrics {
        metrics.record_aborted();
    }
    driver.abort();
    let was_started = delivery.emitted_chunk;
    *delivery = CompressDelivery::default();
    if was_started {
        // ABORT is not a completed delivery → not counted in record_out.
        output.send(StreamData::encoded_delivery_abort()).await?;
    }
    Err(ProcessorError::ProcessingError(reason.to_string()))
}

#[derive(Default)]
pub(crate) struct EncryptDelivery {
    pub(crate) active: bool,
    /// Whether a START chunk has already been forwarded downstream.
    emitted_chunk: bool,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_encrypt_delivery(
    driver: &mut EncryptDriver,
    delivery: &mut EncryptDelivery,
    flags: EncodedDeliveryFlags,
    bytes: Bytes,
    output: &mut impl DeliveryOutput,
    stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
) -> Result<(), ProcessorError> {
    if let Some(metrics) = metrics {
        metrics.record_input_bytes(bytes.len());
    }

    let is_abort = flags.contains(EncodedDeliveryFlags::ABORT);
    let is_start = flags.contains(EncodedDeliveryFlags::START);
    let is_end = flags.contains(EncodedDeliveryFlags::END);

    if is_abort {
        if let Some(metrics) = metrics {
            metrics.record_aborted();
        }
        driver.abort();
        let was_started = delivery.emitted_chunk;
        *delivery = EncryptDelivery::default();
        if was_started {
            output.send(StreamData::encoded_delivery_abort()).await?;
        }
        return Ok(());
    }

    if is_start {
        if delivery.active {
            return fail_encrypt_delivery(
                driver,
                delivery,
                output,
                stats,
                metrics,
                "START received while delivery already active",
            )
            .await;
        }

        let mut out = Vec::new();
        if let Err(e) = driver.begin(&mut out) {
            return fail_encrypt_delivery(driver, delivery, output, stats, metrics, &e.to_string())
                .await;
        }

        if is_end {
            if let Some(metrics) = metrics {
                metrics.record_message_in();
            }
            if let Err(e) = driver.finish(&bytes, &mut out) {
                return fail_encrypt_delivery(
                    driver,
                    delivery,
                    output,
                    stats,
                    metrics,
                    &e.to_string(),
                )
                .await;
            }
            let output_bytes = out.len();
            output
                .send(StreamData::encoded_delivery_single(out))
                .await?;
            if let Some(metrics) = metrics {
                metrics.record_output_bytes(output_bytes);
            }
            if let Some(metrics) = metrics {
                metrics.record_message_out();
            }
            *delivery = EncryptDelivery::default();
            return Ok(());
        }

        if !bytes.is_empty() {
            if let Err(e) = driver.write(&bytes, &mut out) {
                return fail_encrypt_delivery(
                    driver,
                    delivery,
                    output,
                    stats,
                    metrics,
                    &e.to_string(),
                )
                .await;
            }
        }

        let output_bytes = out.len();
        output.send(StreamData::encoded_delivery_start(out)).await?;
        if let Some(metrics) = metrics {
            metrics.record_output_bytes(output_bytes);
        }
        delivery.active = true;
        delivery.emitted_chunk = true;
        return Ok(());
    }

    if !delivery.active {
        return Err(ProcessorError::ProcessingError(
            "chunk/END received without active delivery".into(),
        ));
    }

    if is_end {
        if let Some(metrics) = metrics {
            metrics.record_message_in();
        }
        let mut out = Vec::new();
        if let Err(e) = driver.finish(&bytes, &mut out) {
            return fail_encrypt_delivery(driver, delivery, output, stats, metrics, &e.to_string())
                .await;
        }
        *delivery = EncryptDelivery::default();
        let output_bytes = out.len();
        output.send(StreamData::encoded_delivery_end(out)).await?;
        if let Some(metrics) = metrics {
            metrics.record_output_bytes(output_bytes);
        }
        if let Some(metrics) = metrics {
            metrics.record_message_out();
        }
        return Ok(());
    }

    if bytes.is_empty() {
        return Ok(());
    }

    let mut out = Vec::new();
    if let Err(e) = driver.write(&bytes, &mut out) {
        return fail_encrypt_delivery(driver, delivery, output, stats, metrics, &e.to_string())
            .await;
    }
    let output_bytes = out.len();
    output.send(StreamData::encoded_delivery_chunk(out)).await?;
    if let Some(metrics) = metrics {
        metrics.record_output_bytes(output_bytes);
    }

    Ok(())
}

pub(crate) async fn abort_encrypt_in_flight(
    driver: &mut EncryptDriver,
    delivery: &mut EncryptDelivery,
    output: &mut impl DeliveryOutput,
    stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
    processor_id: &str,
) {
    if let Some(metrics) = metrics {
        metrics.record_aborted();
    }
    driver.abort();
    let was_started = delivery.emitted_chunk;
    *delivery = EncryptDelivery::default();
    if was_started {
        if let Err(err) = output.send(StreamData::encoded_delivery_abort()).await {
            tracing::error!(
                processor_id = %processor_id,
                error = %err,
                "failed to forward ABORT during in-flight abort"
            );
        }
    }
    tracing::error!(
        processor_id = %processor_id,
        "aborted in-flight delivery: terminal signal received mid-delivery"
    );
    stats.record_error("terminal signal received mid-delivery".to_string());
}

async fn fail_encrypt_delivery(
    driver: &mut EncryptDriver,
    delivery: &mut EncryptDelivery,
    output: &mut impl DeliveryOutput,
    _stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
    reason: &str,
) -> Result<(), ProcessorError> {
    if let Some(metrics) = metrics {
        metrics.record_aborted();
    }
    driver.abort();
    let was_started = delivery.emitted_chunk;
    *delivery = EncryptDelivery::default();
    if was_started {
        output.send(StreamData::encoded_delivery_abort()).await?;
    }
    Err(ProcessorError::ProcessingError(reason.to_string()))
}
