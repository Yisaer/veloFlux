use super::base_physical::BasePhysicalPlan;
use std::time::Duration;

/// Physical plan node for throttling (rate limiting) a stream.
#[derive(Debug, Clone)]
pub struct PhysicalSampler {
    pub base: BasePhysicalPlan,
    pub interval: Duration,
}

impl PhysicalSampler {
    pub fn new(index: i64, interval: Duration) -> Self {
        Self {
            base: BasePhysicalPlan::new(Vec::new(), index),
            interval,
        }
    }
}
