//! Merger trait for accumulating raw bytes and producing batched binary output.
//!
//! The Merger is used by the Sampler processor's "Packer" strategy to
//! accumulate raw bytes (e.g., CAN frames in GBF packets) over an interval
//! and emit merged binary data. The merged output is then passed to a
//! Decoder to produce RecordBatches.

use crate::codec::CodecError;

/// Trait for merging raw byte data into accumulated state and triggering emission.
///
/// Implementations accumulate incoming byte payloads via `merge()` and produce
/// combined binary output via `trigger()` when the sampling interval elapses.
pub trait Merger: Send + Sync {
    /// Accumulate new byte data into the merger state.
    ///
    /// The bytes typically represent a raw payload (e.g., a GBF packet)
    /// before any decoding has occurred.
    fn merge(&mut self, data: &[u8]) -> Result<(), CodecError>;

    /// Trigger emission, returning the accumulated binary result.
    ///
    /// Returns `Ok(Some(bytes))` if there is data to emit,
    /// `Ok(None)` if no data accumulated, or `Err` on failure.
    fn trigger(&mut self) -> Result<Option<Vec<u8>>, CodecError>;
}
