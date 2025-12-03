use super::BaseLogicalPlan;
use super::LogicalPlan;
use crate::planner::sink::PipelineSink;
use std::fmt;
use std::sync::Arc;

/// Logical plan node that represents a single sink definition.
#[derive(Clone)]
pub struct DataSinkPlan {
    pub base: BaseLogicalPlan,
    pub sink: PipelineSink,
}

impl DataSinkPlan {
    pub fn new(child: Arc<LogicalPlan>, index: i64, sink: PipelineSink) -> Self {
        Self {
            base: BaseLogicalPlan::new(vec![child], index),
            sink,
        }
    }
}

impl fmt::Debug for DataSinkPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataSinkPlan")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink.sink_id)
            .finish()
    }
}
