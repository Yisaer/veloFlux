use crate::expr::ScalarExpr;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalOrderKey {
    pub original_expr: Expr,
    pub compiled_expr: ScalarExpr,
    pub asc: bool,
}

#[derive(Debug, Clone)]
pub struct PhysicalOrder {
    pub base: BasePhysicalPlan,
    pub keys: Vec<PhysicalOrderKey>,
}

impl PhysicalOrder {
    pub fn new(keys: Vec<PhysicalOrderKey>, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            keys,
        }
    }
}
