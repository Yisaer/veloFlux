use crate::expr::ScalarExpr;
use crate::planner::physical::BasePhysicalPlan;
use parser::StatefulCallSpec;
use sqlparser::ast::Expr;
use std::sync::Arc;

use super::PhysicalPlan;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartitionGroupKey {
    Global,
    Exprs(Vec<Expr>),
}

impl PartitionGroupKey {
    pub fn from_partition_by(partition_by: &[Expr]) -> Self {
        if partition_by.is_empty() {
            Self::Global
        } else {
            Self::Exprs(partition_by.to_vec())
        }
    }
}

#[derive(Debug, Clone)]
pub struct StatefulCall {
    pub output_column: String,
    pub func_name: String,
    pub arg_scalars: Vec<ScalarExpr>,
    pub when_scalar: Option<ScalarExpr>,
    pub partition_group_key: PartitionGroupKey,
    pub partition_by_scalars: Vec<ScalarExpr>,
    pub spec: StatefulCallSpec,
    pub original_expr: Expr,
}

#[derive(Debug, Clone)]
pub struct PhysicalStatefulFunction {
    pub base: BasePhysicalPlan,
    pub calls: Vec<StatefulCall>,
}

impl PhysicalStatefulFunction {
    pub fn new(calls: Vec<StatefulCall>, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            calls,
        }
    }
}
