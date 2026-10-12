use crate::expr::custom_func::CustomFunc;
use crate::expr::ScalarExpr;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

/// One collection-level batch-function evaluation lowered from a SQL expression.
#[derive(Clone)]
pub struct PhysicalBatchFunctionEvaluation {
    /// Internal affiliate column receiving one result per input row.
    pub output_column: String,
    /// Immutable registry-side batch-function definition.
    pub function: Arc<dyn CustomFunc>,
    /// Ordinary scalar expressions used to gather column-oriented arguments.
    pub args: Vec<ScalarExpr>,
    /// Original expression retained for explain and diagnostics.
    pub original_expr: Expr,
}

impl std::fmt::Debug for PhysicalBatchFunctionEvaluation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalBatchFunctionEvaluation")
            .field("output_column", &self.output_column)
            .field("function", &self.function)
            .field("args", &self.args)
            .field("original_expr", &self.original_expr)
            .finish()
    }
}

/// Physical collection operator for batch-function evaluation.
#[derive(Debug, Clone)]
pub struct PhysicalBatchFunctionCompute {
    pub base: BasePhysicalPlan,
    pub evaluations: Vec<PhysicalBatchFunctionEvaluation>,
}

impl PhysicalBatchFunctionCompute {
    pub fn new(
        evaluations: Vec<PhysicalBatchFunctionEvaluation>,
        child: Arc<PhysicalPlan>,
        index: i64,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            evaluations,
        }
    }
}
