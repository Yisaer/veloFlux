use crate::planner::physical::BasePhysicalPlan;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical plan node for tail stage with multiple sinks.
/// This node serves as a pass-through collection point for multiple sink children,
/// maintaining the logical structure while allowing physical plan builder to process each sink.
#[derive(Debug, Clone)]
pub struct PhysicalTail {
    pub base: BasePhysicalPlan,
}

impl PhysicalTail {
    pub fn new(children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
        }
    }
}