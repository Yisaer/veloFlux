use crate::planner::physical::BasePhysicalPlan;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical plan node that aligns barrier control signals across multiple upstream children.
///
/// Query input preparation uses this node for one or more inputs. Physical plan
/// optimization also inserts it for other fan-in boundaries. Only a real fan-in
/// requires a region boundary; a one-input node can execute inline.
#[derive(Debug, Clone)]
pub struct PhysicalBarrier {
    pub base: BasePhysicalPlan,
    /// Assign processing time at this query's input consumption boundary.
    pub assign_processing_time: bool,
}

impl PhysicalBarrier {
    pub fn new(children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            assign_processing_time: false,
        }
    }
}
