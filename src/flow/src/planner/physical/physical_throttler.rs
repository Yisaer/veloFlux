use super::base_physical::BasePhysicalPlan;
use std::time::Duration;

/// Physical plan node for throttling (rate limiting) a stream.
#[derive(Debug, Clone)]
pub struct PhysicalThrottler {
    pub base: BasePhysicalPlan,
    pub rate_limit: Duration,
}

impl PhysicalThrottler {
    pub fn new(index: i64, rate_limit: Duration) -> Self {
        Self {
            base: BasePhysicalPlan::new(Vec::new(), index),
            rate_limit,
        }
    }
}
