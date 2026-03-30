use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use std::fmt;
use std::sync::Arc;

/// Physical plan node describing sink-side empty-result suppression.
#[derive(Clone)]
pub struct PhysicalEmptySuppress {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub omit_if_empty: bool,
}

impl PhysicalEmptySuppress {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        omit_if_empty: bool,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            omit_if_empty,
        }
    }
}

impl fmt::Debug for PhysicalEmptySuppress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalEmptySuppress")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .field("omit_if_empty", &self.omit_if_empty)
            .finish()
    }
}
