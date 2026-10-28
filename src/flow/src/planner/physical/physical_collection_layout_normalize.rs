use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use datatypes::Schema;
use std::sync::Arc;

/// Normalize incoming `Collection` tuples to match an expected full schema layout.
///
/// This is used for memory collection sources (decoder.type = "none") where runtime tuples
/// may have a different message source / column order than what downstream `ColumnRef::ByIndex`
/// expects. The node reshapes tuples to `1 message + 0 affiliate` with stable column ordering.
#[derive(Debug, Clone)]
pub struct PhysicalCollectionLayoutNormalize {
    pub base: BasePhysicalPlan,
    pub schema: Arc<Schema>,
    /// The message `source` to write in the normalized output tuple.
    pub output_source_name: Arc<str>,
    /// Source-preserving NULL padding for comma-separated FROM inputs.
    pub(crate) source_alignment: Option<SourceAlignment>,
}

impl PhysicalCollectionLayoutNormalize {
    pub fn new(
        schema: Arc<Schema>,
        output_source_name: Arc<str>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            schema,
            output_source_name,
            source_alignment: None,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn output_source_name(&self) -> &str {
        self.output_source_name.as_ref()
    }
}

/// One common tuple layout, with immutable NULL messages shared across all events.
#[derive(Debug, Clone)]
pub(crate) struct SourceAlignment {
    pub layout: crate::planner::physical::output_layout::PlanLayout,
    pub source_index: usize,
    pub null_messages: Arc<[Arc<crate::model::Message>]>,
}
