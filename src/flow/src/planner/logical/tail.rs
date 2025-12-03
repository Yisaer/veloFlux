use super::BaseLogicalPlan;
use std::fmt;
use std::sync::Arc;
use super::LogicalPlan;

/// Logical plan node that represents the tail/end of a plan with multiple sinks.
/// This node acts as a collection point for multiple DataSinkPlan children,
/// providing a clean abstraction for multi-sink scenarios.
#[derive(Clone)]
pub struct TailPlan {
    pub base: BaseLogicalPlan,
}

impl TailPlan {
    /// Create a new tail plan with multiple sink children
    pub fn new(sink_children: Vec<Arc<LogicalPlan>>, index: i64) -> Self {
        Self {
            base: BaseLogicalPlan::new(sink_children, index),
        }
    }
}

impl fmt::Debug for TailPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TailPlan")
            .field("index", &self.base.index())
            .field("sink_count", &self.base.children.len())
            .finish()
    }
}