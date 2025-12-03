use crate::planner::physical::BasePhysicalPlan;
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical node representing a streaming encoder stage that also handles batching.
#[derive(Clone)]
pub struct PhysicalStreamingEncoder {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub connector_id: String,
    pub encoder: SinkEncoderConfig,
    pub common: CommonSinkProps,
}

impl PhysicalStreamingEncoder {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        connector_id: String,
        encoder: SinkEncoderConfig,
        common: CommonSinkProps,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            connector_id,
            encoder,
            common,
        }
    }
}

impl fmt::Debug for PhysicalStreamingEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalStreamingEncoder")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .field("connector_id", &self.connector_id)
            .finish()
    }
}
