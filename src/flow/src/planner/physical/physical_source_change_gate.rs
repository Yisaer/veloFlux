use crate::pipeline::SourceInputConfig;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use std::fmt;
use std::sync::Arc;

/// Physical plan node describing source-side on-change gating.
#[derive(Clone)]
pub struct PhysicalSourceChangeGate {
    pub base: BasePhysicalPlan,
    pub source_name: String,
    pub input: SourceInputConfig,
    pub tracked_columns: Arc<[Arc<str>]>,
    pub tracked_column_indexes: Arc<[usize]>,
}

impl PhysicalSourceChangeGate {
    pub fn new(
        source_name: impl Into<String>,
        input: SourceInputConfig,
        tracked_columns: Vec<Arc<str>>,
        tracked_column_indexes: Vec<usize>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            source_name: source_name.into(),
            input,
            tracked_columns: Arc::from(tracked_columns),
            tracked_column_indexes: Arc::from(tracked_column_indexes),
        }
    }
}

impl fmt::Debug for PhysicalSourceChangeGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalSourceChangeGate")
            .field("index", &self.base.index())
            .field("source_name", &self.source_name)
            .field("input", &self.input)
            .field("tracked_columns", &self.tracked_columns)
            .finish()
    }
}
