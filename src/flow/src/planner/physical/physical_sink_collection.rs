use crate::planner::physical::BasePhysicalPlan;
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical plan node grouping multiple sink subtrees.
#[derive(Clone)]
pub struct PhysicalSinkCollection {
    pub base: BasePhysicalPlan,
}

impl PhysicalSinkCollection {
    pub fn new(children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
        }
    }
}

impl fmt::Debug for PhysicalSinkCollection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(PhysicalSinkCollection)
            .field(index, &self.base.index())
            .field(child_count, &self.base.children().len())
            .finish()
    }
}
