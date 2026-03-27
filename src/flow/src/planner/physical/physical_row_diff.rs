use crate::planner::physical::output_schema::OutputSchema;
use crate::planner::physical::{BasePhysicalPlan, ByIndexProjection, PhysicalPlan};
use crate::planner::sink::SinkOutputConfig;
use std::fmt;
use std::sync::Arc;

/// Physical plan node describing sink-side row diff output preparation.
#[derive(Clone)]
pub struct PhysicalRowDiff {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub output: SinkOutputConfig,
    pub output_schema: Arc<OutputSchema>,
    pub tracked_columns: Arc<[Arc<str>]>,
    pub tracked_column_indexes: Arc<[usize]>,
    pub late_projection: Option<Arc<ByIndexProjection>>,
}

impl PhysicalRowDiff {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        output: SinkOutputConfig,
        output_schema: OutputSchema,
        tracked_columns: Vec<Arc<str>>,
        tracked_column_indexes: Vec<usize>,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            output,
            output_schema: Arc::new(output_schema),
            tracked_columns: Arc::from(tracked_columns),
            tracked_column_indexes: Arc::from(tracked_column_indexes),
            late_projection: None,
        }
    }
}

impl fmt::Debug for PhysicalRowDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalRowDiff")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .field("output", &self.output)
            .field("tracked_columns", &self.tracked_columns)
            .finish()
    }
}
