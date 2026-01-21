use crate::planner::logical::{BaseLogicalPlan, LogicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OrderItem {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub base: BaseLogicalPlan,
    pub items: Vec<OrderItem>,
}

impl Order {
    pub fn new(items: Vec<OrderItem>, children: Vec<Arc<LogicalPlan>>, index: i64) -> Self {
        let base = BaseLogicalPlan::new(children, index);
        Self { base, items }
    }
}
