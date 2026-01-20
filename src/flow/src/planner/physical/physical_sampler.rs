use super::base_physical::BasePhysicalPlan;
use crate::planner::physical::PhysicalPlan;
use std::sync::Arc;
use std::time::Duration;

/// Physical plan node for throttling (rate limiting) a stream.
#[derive(Debug, Clone)]
pub struct PhysicalSampler {
    pub base: BasePhysicalPlan,
    pub interval: Duration,
}

impl PhysicalSampler {
    pub fn new(interval: Duration, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            interval,
        }
    }
}
