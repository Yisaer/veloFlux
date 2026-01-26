use crate::planner::physical::output_schema::OutputSchema;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use std::sync::Arc;

/// Materialize an arbitrary incoming `Collection` into a stable layout for memory collection sinks.
///
/// The node reshapes each tuple to `1 message + 0 affiliate` with keys ordered by the planned
/// output schema, filling missing columns with NULL.
#[derive(Debug, Clone)]
pub struct PhysicalMemoryCollectionMaterialize {
    pub base: BasePhysicalPlan,
    pub output_schema: OutputSchema,
}

impl PhysicalMemoryCollectionMaterialize {
    pub fn new(output_schema: OutputSchema, child: Arc<PhysicalPlan>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            output_schema,
        }
    }
}
