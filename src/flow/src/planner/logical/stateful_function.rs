use parser::StatefulCallSpec;
use std::sync::Arc;

use crate::planner::logical::{BaseLogicalPlan, LogicalPlan};

#[derive(Debug, Clone)]
pub struct LogicalStatefulCall {
    pub output_column: String,
    pub spec: StatefulCallSpec,
}

#[derive(Debug, Clone)]
pub struct StatefulFunctionPlan {
    pub base: BaseLogicalPlan,
    pub calls: Vec<LogicalStatefulCall>,
}

impl StatefulFunctionPlan {
    pub fn new(
        calls: Vec<LogicalStatefulCall>,
        children: Vec<Arc<LogicalPlan>>,
        index: i64,
    ) -> Self {
        Self {
            base: BaseLogicalPlan::new(children, index),
            calls,
        }
    }
}
