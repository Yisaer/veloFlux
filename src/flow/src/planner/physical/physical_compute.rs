use crate::expr::custom_func::CustomFuncRegistry;
use crate::expr::ScalarExpr;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

/// Field definition for physical compute.
///
/// A compute node materializes derived values into affiliate columns.
#[derive(Debug, Clone)]
pub struct PhysicalComputeField {
    /// Output (affiliate) field name.
    pub field_name: String,
    /// Original SQL expression from parser (for reference and debugging).
    pub original_expr: Expr,
    /// Compiled expression for execution.
    pub compiled_expr: ScalarExpr,
}

/// Physical operator that evaluates expressions and appends them as affiliate columns,
/// while preserving upstream messages unchanged.
#[derive(Debug, Clone)]
pub struct PhysicalCompute {
    pub base: BasePhysicalPlan,
    pub fields: Vec<PhysicalComputeField>,
}

impl PhysicalComputeField {
    pub fn new(field_name: String, original_expr: Expr, compiled_expr: ScalarExpr) -> Self {
        Self {
            field_name,
            original_expr,
            compiled_expr,
        }
    }

    pub fn from_logical(
        field_name: String,
        original_expr: Expr,
        bindings: &crate::expr::sql_conversion::SchemaBinding,
        custom_func_registry: &CustomFuncRegistry,
    ) -> Result<Self, String> {
        let compiled_expr =
            crate::expr::sql_conversion::convert_expr_to_scalar_with_bindings_and_custom_registry(
                &original_expr,
                bindings,
                custom_func_registry,
            )
            .map_err(|e| format!("Failed to compile expression: {}", e))?;

        Ok(Self {
            field_name,
            original_expr,
            compiled_expr,
        })
    }
}

impl PhysicalCompute {
    pub fn new(
        fields: Vec<PhysicalComputeField>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base, fields }
    }
}
