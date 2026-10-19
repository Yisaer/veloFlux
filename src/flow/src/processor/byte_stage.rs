//! Channel-free byte-transform drivers shared by the standalone sink
//! transform processors and general execution regions.
//!
//! The writer lifecycle (`begin`/`write`/`finish`/`abort`) and delivery
//! framing are identical in both topologies. Standalone stages exchange
//! delivery events through channels; fused stages exchange them directly.
//!
//! Codec drivers are used by the shared delivery handlers. Framing and
//! recovery live in `delivery_transform`, and execution connections live
//! in `delivery_output` and `region_processor`. Historical `inline_delivery`
//! is compiled only for the equivalence-test reference executor.

use crate::codec::compress::{CompressError, CompressWriter};
use crate::codec::encrypt::EncryptError;
use crate::codec::EncryptWriter;

/// Owned compress configuration: the writer plus nothing else.
pub(crate) struct CompressDriver {
    writer: Box<dyn CompressWriter>,
}

impl CompressDriver {
    pub(crate) fn new(writer: Box<dyn CompressWriter>) -> Self {
        Self { writer }
    }

    pub(crate) fn begin(&mut self) -> Result<(), CompressError> {
        self.writer.begin_delivery()
    }

    pub(crate) fn write(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError> {
        self.writer.write(input, out)
    }

    pub(crate) fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError> {
        self.writer.finish(out)
    }

    pub(crate) fn abort(&mut self) {
        self.writer.abort_delivery();
    }
}

/// Owned encrypt configuration: the writer plus nothing else.
pub(crate) struct EncryptDriver {
    writer: Box<dyn EncryptWriter>,
}

impl EncryptDriver {
    pub(crate) fn new(writer: Box<dyn EncryptWriter>) -> Self {
        Self { writer }
    }

    pub(crate) fn begin(&mut self, out: &mut Vec<u8>) -> Result<(), EncryptError> {
        self.writer.begin_delivery(out)
    }

    pub(crate) fn write(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), EncryptError> {
        self.writer.write(input, out)
    }

    pub(crate) fn finish(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), EncryptError> {
        self.writer.finish(input, out)
    }

    pub(crate) fn abort(&mut self) {
        self.writer.abort_delivery();
    }
}

/// A synchronous delivery transform; transport belongs to its region driver.
pub(crate) enum ByteTransformStage {
    Compress {
        driver: CompressDriver,
        delivery: super::delivery_transform::CompressDelivery,
    },
    Encrypt {
        driver: EncryptDriver,
        delivery: super::delivery_transform::EncryptDelivery,
    },
}

impl ByteTransformStage {
    pub(crate) fn active(&self) -> bool {
        match self {
            Self::Compress { delivery, .. } => delivery.active,
            Self::Encrypt { delivery, .. } => delivery.active,
        }
    }

    pub(crate) fn process(
        &mut self,
        flags: super::EncodedDeliveryFlags,
        bytes: bytes::Bytes,
        metrics: Option<&super::data_metrics::TransformMetrics>,
    ) -> super::delivery_transform::DeliveryStep {
        match self {
            Self::Compress { driver, delivery } => {
                super::delivery_transform::compress_delivery_step(
                    driver, delivery, flags, bytes, metrics,
                )
            }
            Self::Encrypt { driver, delivery } => super::delivery_transform::encrypt_delivery_step(
                driver, delivery, flags, bytes, metrics,
            ),
        }
    }

    pub(crate) fn finish(
        &mut self,
        stats: &super::ProcessorStats,
        metrics: Option<&super::data_metrics::TransformMetrics>,
    ) -> Option<super::StreamData> {
        if !self.active() {
            return None;
        }
        let frame = match self {
            Self::Compress { driver, delivery } => {
                super::delivery_transform::abort_compress(driver, delivery, metrics)
            }
            Self::Encrypt { driver, delivery } => {
                super::delivery_transform::abort_encrypt(driver, delivery, metrics)
            }
        };
        stats.record_error_logged(
            "terminal delivery abort",
            "terminal signal received mid-delivery",
        );
        frame
    }
}
