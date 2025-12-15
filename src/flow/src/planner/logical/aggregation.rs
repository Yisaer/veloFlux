use crate::planner::logical::{BaseLogicalPlan, LogicalPlan};
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Aggregation {
    pub base: BaseLogicalPlan,
    /// Mapping from replacement column name to original aggregate expression
    pub aggregate_mappings: HashMap<String, Expr>,
    /// GROUP BY expressions (non-window)
    pub group_by_exprs: Vec<Expr>,
}

impl Aggregation {
    pub fn new(
        aggregate_mappings: HashMap<String, Expr>,
        group_by_exprs: Vec<Expr>,
        children: Vec<Arc<LogicalPlan>>,
        index: i64,
    ) -> Self {
        Self {
            base: BaseLogicalPlan::new(children, index),
            aggregate_mappings,
            group_by_exprs,
        }
    }
}
