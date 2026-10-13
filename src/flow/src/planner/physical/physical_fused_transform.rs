use crate::codec::{CompressionCodec, SinkEncryptionConfig};
use crate::planner::physical::output_layout::OutputLayout;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use crate::planner::sink::SinkEncoderConfig;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use super::physical_filter::PhysicalFilter;
use super::physical_order::PhysicalOrder;
use super::physical_project::PhysicalProject;

/// Physical node for a fused stateless transform chain on a sink branch.
///
/// Produced by the `fuse_stateless_transform` physical optimizer rule out of
/// `input -> [Order?] -> [Filter?] -> Project -> [EmptySuppress?]
/// -> [Batch?] -> [Encode?] -> [Compress?] -> [Encrypt?]`, sitting directly
/// below the (unfused) sink connector node.
///
/// The fused runtime task evaluates pure transforms inline: collections
/// stay collections upstream of encode (shared `apply_*` functions), bytes
/// stay bytes downstream, and encode is the only representation change.
/// External I/O (the connector itself, like lookup joins) is never fused:
/// delivery, retry, and checkpointing stay in the standalone sink task, so
/// no delivery machinery is transcribed.
///
/// Gates (anything else keeps the unfused plan):
/// - no `RowDiff`/`ColumnFilter` inside the span;
/// - single-child spine everywhere (a multi-child node bails instead of
///   dropping a leg);
/// - every consumed stage has one consumer (shared stages stay standalone);
/// - HTTP, MQTT, and custom connector branches retain streamed byte
///   processing so sink delivery-size limits apply before batch completion.
///
/// Batching (`Batch` standalone or already fused into `IncSinkEncoder`) is
/// supported through the shared encoder/native batch stages and fixed-grid policy.
///
/// Partitioned batching never appears in sink chains (it is only built for
/// count windows), so the fused task implements the simple buffer only.
#[derive(Clone)]
pub struct PhysicalFusedTransform {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub filter: Option<PhysicalFilter>,
    pub project: PhysicalProject,
    pub order: Option<PhysicalOrder>,
    /// Execution order of filter vs order when both are present
    /// (true = filter first, the canonical `WHERE ... ORDER BY ...`).
    pub filter_before_order: bool,
    pub empty_suppress_omit: bool,
    pub encoder: Option<SinkEncoderConfig>,
    pub encoder_output_layout: Option<Arc<OutputLayout>>,
    pub compress: Option<CompressionCodec>,
    pub encrypt: Option<SinkEncryptionConfig>,
    pub batch_count: Option<usize>,
    pub batch_duration: Option<Duration>,
}

impl PhysicalFusedTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        child: Arc<PhysicalPlan>,
        index: i64,
        sink_id: String,
        filter: Option<PhysicalFilter>,
        project: PhysicalProject,
        order: Option<PhysicalOrder>,
        filter_before_order: bool,
        empty_suppress_omit: bool,
        encoder: Option<SinkEncoderConfig>,
        encoder_output_layout: Option<Arc<OutputLayout>>,
        compress: Option<CompressionCodec>,
        encrypt: Option<SinkEncryptionConfig>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            sink_id,
            filter,
            project,
            order,
            filter_before_order,
            empty_suppress_omit,
            encoder,
            encoder_output_layout,
            compress,
            encrypt,
            batch_count,
            batch_duration,
        }
    }
}

impl fmt::Debug for PhysicalFusedTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalFusedTransform")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .finish()
    }
}
