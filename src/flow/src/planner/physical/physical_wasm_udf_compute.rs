use crate::expr::custom_func::CustomFunc;
use crate::expr::ScalarExpr;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

/// One collection-level WASM UDF evaluation lowered from a SQL expression.
#[derive(Clone)]
pub struct PhysicalWasmUdfEvaluation {
    /// Internal affiliate column receiving one result per input row.
    pub output_column: String,
    /// Immutable registry-side UDF definition.
    pub udf: Arc<dyn CustomFunc>,
    /// Ordinary scalar expressions used to gather column-oriented arguments.
    pub args: Vec<ScalarExpr>,
    /// Original expression retained for explain and diagnostics.
    pub original_expr: Expr,
}

impl std::fmt::Debug for PhysicalWasmUdfEvaluation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalWasmUdfEvaluation")
            .field("output_column", &self.output_column)
            .field("udf", &self.udf)
            .field("args", &self.args)
            .field("original_expr", &self.original_expr)
            .finish()
    }
}

/// Physical collection operator for batch-only WASM UDF evaluation.
#[derive(Debug, Clone)]
pub struct PhysicalWasmUdfCompute {
    pub base: BasePhysicalPlan,
    pub evaluations: Vec<PhysicalWasmUdfEvaluation>,
}

impl PhysicalWasmUdfCompute {
    pub fn new(
        evaluations: Vec<PhysicalWasmUdfEvaluation>,
        child: Arc<PhysicalPlan>,
        index: i64,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            evaluations,
        }
    }
}
