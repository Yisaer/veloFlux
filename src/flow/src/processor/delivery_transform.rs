//! Synchronous delivery framing for region byte-transform stages.
use crate::processor::byte_stage::{CompressDriver, EncryptDriver};
use crate::processor::data_metrics::TransformMetrics;
use crate::processor::{EncodedDeliveryFlags, ProcessorError, StreamData};
use bytes::Bytes;

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
}
impl DeliveryStep {
    fn output(output: Option<StreamData>) -> Self {
        Self {
            output,
            error: None,
        }
    }
    fn error(output: Option<StreamData>, error: ProcessorError) -> Self {
        Self {
            output,
            error: Some(error),
        }
    }
    fn aborted(output: Option<StreamData>, error: Option<ProcessorError>) -> Self {
        Self { output, error }
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
