//! Channel-free byte-transform drivers shared by the standalone sink
//! transform processors and the fused transform task.
//!
//! The writer lifecycle (`begin`/`write`/`finish`/`abort`) and delivery
//! framing are identical in both topologies. Standalone stages exchange
//! delivery events through channels; fused stages exchange them directly.
//!
//! Codec drivers are used by the shared delivery handlers. Framing and
//! recovery live in `delivery_transform`, and execution connections live
//! in `delivery_output` and `inline_delivery`.

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
