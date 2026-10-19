//! Synchronous delivery framing shared by region and standalone byte transforms.
//! Compatibility transport adapters below await only the returned frame.
use crate::processor::byte_stage::{CompressDriver, EncryptDriver};
use crate::processor::data_metrics::TransformMetrics;
use crate::processor::delivery_output::DeliveryOutput;
use crate::processor::{EncodedDeliveryFlags, ProcessorError, ProcessorStats, StreamData};
use bytes::Bytes;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct CompressDelivery {
    pub(crate) active: bool,
    emitted_chunk: bool,
}

#[derive(Default)]
pub(crate) struct EncryptDelivery {
    pub(crate) active: bool,
    emitted_chunk: bool,
}

/// At most one frame precedes a recoverable error. Transport sends it first.
pub(crate) struct DeliveryStep {
    pub(crate) output: Option<StreamData>,
    pub(crate) error: Option<ProcessorError>,
    pub(crate) aborted: bool,
}
impl DeliveryStep {
    fn output(output: Option<StreamData>) -> Self {
        Self {
            output,
            error: None,
            aborted: false,
        }
    }
    fn error(output: Option<StreamData>, error: ProcessorError) -> Self {
        Self {
            output,
            error: Some(error),
            aborted: false,
        }
    }
    fn aborted(output: Option<StreamData>, error: Option<ProcessorError>) -> Self {
        Self {
            output,
            error,
            aborted: true,
        }
    }
}

pub(crate) fn abort_compress(
    driver: &mut CompressDriver,
    delivery: &mut CompressDelivery,
    metrics: Option<&TransformMetrics>,
) -> Option<StreamData> {
    if let Some(metrics) = metrics {
        metrics.record_aborted();
    }
    driver.abort();
    let emitted = delivery.emitted_chunk;
    *delivery = CompressDelivery::default();
    emitted.then(StreamData::encoded_delivery_abort)
}

pub(crate) fn abort_encrypt(
    driver: &mut EncryptDriver,
    delivery: &mut EncryptDelivery,
    metrics: Option<&TransformMetrics>,
) -> Option<StreamData> {
    if let Some(metrics) = metrics {
        metrics.record_aborted();
    }
    driver.abort();
    let emitted = delivery.emitted_chunk;
    *delivery = EncryptDelivery::default();
    emitted.then(StreamData::encoded_delivery_abort)
}

fn compress_error(
    driver: &mut CompressDriver,
    delivery: &mut CompressDelivery,
    metrics: Option<&TransformMetrics>,
    reason: String,
) -> DeliveryStep {
    DeliveryStep::aborted(
        abort_compress(driver, delivery, metrics),
        Some(ProcessorError::ProcessingError(reason)),
    )
}
fn encrypt_error(
    driver: &mut EncryptDriver,
    delivery: &mut EncryptDelivery,
    metrics: Option<&TransformMetrics>,
    reason: String,
) -> DeliveryStep {
    DeliveryStep::aborted(
        abort_encrypt(driver, delivery, metrics),
        Some(ProcessorError::ProcessingError(reason)),
    )
}

pub(crate) fn compress_delivery_step(
    driver: &mut CompressDriver,
    delivery: &mut CompressDelivery,
    flags: EncodedDeliveryFlags,
    bytes: Bytes,
    metrics: Option<&TransformMetrics>,
) -> DeliveryStep {
    if let Some(metrics) = metrics {
        metrics.record_input_bytes(bytes.len());
    }
    let start = flags.contains(EncodedDeliveryFlags::START);
    let end = flags.contains(EncodedDeliveryFlags::END);
    if flags.contains(EncodedDeliveryFlags::ABORT) {
        return DeliveryStep::aborted(abort_compress(driver, delivery, metrics), None);
    }
    if start {
        if delivery.active {
            return compress_error(
                driver,
                delivery,
                metrics,
                "START received while delivery already active".into(),
            );
        }
        if let Err(error) = driver.begin() {
            return compress_error(driver, delivery, metrics, error.to_string());
        }
        delivery.active = true;
    } else if !delivery.active {
        return DeliveryStep::error(
            None,
            ProcessorError::ProcessingError("chunk/END received without active delivery".into()),
        );
    }
    if end {
        if let Some(metrics) = metrics {
            metrics.record_message_in();
        }
    }
    let mut out = Vec::new();
    if !bytes.is_empty() {
        if let Err(error) = driver.write(&bytes, &mut out) {
            return compress_error(driver, delivery, metrics, error.to_string());
        }
    }
    if end {
        if let Err(error) = driver.finish(&mut out) {
            return compress_error(driver, delivery, metrics, error.to_string());
        }
        *delivery = CompressDelivery::default();
        DeliveryStep::output(Some(if start {
            StreamData::encoded_delivery_single(out)
        } else {
            StreamData::encoded_delivery_end(out)
        }))
    } else if start {
        delivery.emitted_chunk = true;
        DeliveryStep::output(Some(StreamData::encoded_delivery_start(out)))
    } else if !out.is_empty() {
        DeliveryStep::output(Some(StreamData::encoded_delivery_chunk(out)))
    } else {
        DeliveryStep::output(None)
    }
}

pub(crate) fn encrypt_delivery_step(
    driver: &mut EncryptDriver,
    delivery: &mut EncryptDelivery,
    flags: EncodedDeliveryFlags,
    bytes: Bytes,
    metrics: Option<&TransformMetrics>,
) -> DeliveryStep {
    if let Some(metrics) = metrics {
        metrics.record_input_bytes(bytes.len());
    }
    let start = flags.contains(EncodedDeliveryFlags::START);
    let end = flags.contains(EncodedDeliveryFlags::END);
    if flags.contains(EncodedDeliveryFlags::ABORT) {
        return DeliveryStep::aborted(abort_encrypt(driver, delivery, metrics), None);
    }
    let mut out = Vec::new();
    if start {
        if delivery.active {
            return encrypt_error(
                driver,
                delivery,
                metrics,
                "START received while delivery already active".into(),
            );
        }
        if let Err(error) = driver.begin(&mut out) {
            return encrypt_error(driver, delivery, metrics, error.to_string());
        }
    } else if !delivery.active {
        return DeliveryStep::error(
            None,
            ProcessorError::ProcessingError("chunk/END received without active delivery".into()),
        );
    }
    if end {
        if let Some(metrics) = metrics {
            metrics.record_message_in();
        }
        if let Err(error) = driver.finish(&bytes, &mut out) {
            return encrypt_error(driver, delivery, metrics, error.to_string());
        }
        *delivery = EncryptDelivery::default();
        return DeliveryStep::output(Some(if start {
            StreamData::encoded_delivery_single(out)
        } else {
            StreamData::encoded_delivery_end(out)
        }));
    }
    if !start && bytes.is_empty() {
        return DeliveryStep::output(None);
    }
    if !bytes.is_empty() {
        if let Err(error) = driver.write(&bytes, &mut out) {
            return encrypt_error(driver, delivery, metrics, error.to_string());
        }
    }
    if start {
        delivery.active = true;
        delivery.emitted_chunk = true;
    }
    DeliveryStep::output(Some(if start {
        StreamData::encoded_delivery_start(out)
    } else {
        StreamData::encoded_delivery_chunk(out)
    }))
}

fn record_transform_output(
    metrics: Option<&TransformMetrics>,
    flags: EncodedDeliveryFlags,
    bytes: usize,
) {
    if let Some(metrics) = metrics {
        if !flags.contains(EncodedDeliveryFlags::ABORT) {
            metrics.record_output_bytes(bytes);
            if flags.contains(EncodedDeliveryFlags::END) {
                metrics.record_message_out();
            }
        }
    }
}

async fn send_delivery_step(
    step: DeliveryStep,
    output: &mut impl DeliveryOutput,
    metrics: Option<&TransformMetrics>,
) -> Result<(), ProcessorError> {
    if let Some(data) = step.output {
        let measurement = match &data {
            StreamData::EncodedDelivery { flags, bytes } => Some((*flags, bytes.len())),
            _ => None,
        };
        output.send(data).await?;
        if let Some((flags, bytes)) = measurement {
            record_transform_output(metrics, flags, bytes);
        }
    }
    step.error.map_or(Ok(()), Err)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_compress_delivery(
    driver: &mut CompressDriver,
    delivery: &mut CompressDelivery,
    flags: EncodedDeliveryFlags,
    bytes: Bytes,
    output: &mut impl DeliveryOutput,
    _stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
) -> Result<(), ProcessorError> {
    let step = compress_delivery_step(driver, delivery, flags, bytes, metrics);
    let starts = matches!(&step.output, Some(StreamData::EncodedDelivery { flags, .. }) if flags.contains(EncodedDeliveryFlags::START) && !flags.contains(EncodedDeliveryFlags::END));
    let result = send_delivery_step(step, output, metrics).await;
    if starts && result.is_err() {
        delivery.emitted_chunk = false;
    }
    result
}
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_encrypt_delivery(
    driver: &mut EncryptDriver,
    delivery: &mut EncryptDelivery,
    flags: EncodedDeliveryFlags,
    bytes: Bytes,
    output: &mut impl DeliveryOutput,
    _stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
) -> Result<(), ProcessorError> {
    let step = encrypt_delivery_step(driver, delivery, flags, bytes, metrics);
    let starts = matches!(&step.output, Some(StreamData::EncodedDelivery { flags, .. }) if flags.contains(EncodedDeliveryFlags::START) && !flags.contains(EncodedDeliveryFlags::END));
    let result = send_delivery_step(step, output, metrics).await;
    if starts && result.is_err() {
        delivery.active = false;
        delivery.emitted_chunk = false;
    }
    result
}

async fn send_terminal_abort(
    frame: Option<StreamData>,
    output: &mut impl DeliveryOutput,
    stats: &ProcessorStats,
    processor_id: &str,
) {
    if let Some(frame) = frame {
        if let Err(error) = output.send(frame).await {
            tracing::error!(processor_id, %error, "failed to forward ABORT during in-flight abort");
        }
    }
    stats.record_error_logged(
        "terminal delivery abort",
        "terminal signal received mid-delivery",
    );
}
pub(crate) async fn abort_compress_in_flight(
    driver: &mut CompressDriver,
    delivery: &mut CompressDelivery,
    output: &mut impl DeliveryOutput,
    stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
    processor_id: &str,
) {
    send_terminal_abort(
        abort_compress(driver, delivery, metrics),
        output,
        stats,
        processor_id,
    )
    .await;
}
pub(crate) async fn abort_encrypt_in_flight(
    driver: &mut EncryptDriver,
    delivery: &mut EncryptDelivery,
    output: &mut impl DeliveryOutput,
    stats: &Arc<ProcessorStats>,
    metrics: Option<&TransformMetrics>,
    processor_id: &str,
) {
    send_terminal_abort(
        abort_encrypt(driver, delivery, metrics),
        output,
        stats,
        processor_id,
    )
    .await;
}
