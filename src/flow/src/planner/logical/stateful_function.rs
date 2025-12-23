use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;

use crate::planner::logical::{BaseLogicalPlan, LogicalPlan};

#[derive(Debug, Clone)]
pub struct StatefulFunctionPlan {
    pub base: BaseLogicalPlan,
    pub stateful_mappings: HashMap<String, Expr>,
}

impl StatefulFunctionPlan {
    pub fn new(
        stateful_mappings: HashMap<String, Expr>,
        children: Vec<Arc<LogicalPlan>>,
        index: i64,
    ) -> Self {
        Self {
            base: BaseLogicalPlan::new(children, index),
            stateful_mappings,
        }
    }
}

