use std::any::Any;
use std::sync::Arc;
use crate::planner::physical::{PhysicalPlan, BasePhysicalPlan};
use crate::expr::ScalarExpr;
use sqlparser::ast::Expr;

/// Field definition for physical projection
/// 
/// Contains both the original SQL expression and the compiled ScalarExpr for execution
#[derive(Debug, Clone)]
pub struct PhysicalProjectField {
    /// Output field name
    pub field_name: String,
    /// Original SQL expression from parser (for reference and debugging)
    pub original_expr: Expr,
    /// Compiled expression for execution
    pub compiled_expr: ScalarExpr,
}

/// Physical operator for projection operations
/// 
/// This operator represents the physical execution of projection operations,
/// evaluating expressions and producing output with projected fields.
#[derive(Debug, Clone)]
pub struct PhysicalProject {
    pub base: BasePhysicalPlan,
    pub fields: Vec<PhysicalProjectField>,
}

impl PhysicalProjectField {
    /// Create a new PhysicalProjectField with both original and compiled expressions
    pub fn new(field_name: String, original_expr: Expr, compiled_expr: ScalarExpr) -> Self {
        Self {
            field_name,
            original_expr,
            compiled_expr,
        }
    }
    
    /// Create from a logical ProjectField by compiling the expression
    pub fn from_logical(field_name: String, original_expr: Expr) -> Result<Self, String> {
        // Compile the sqlparser expression to ScalarExpr
        let compiled_expr = crate::expr::sql_conversion::convert_expr_to_scalar(&original_expr)
            .map_err(|e| format!("Failed to compile expression: {}", e))?;
        
        Ok(Self {
            field_name,
            original_expr,
            compiled_expr,
        })
    }
}

impl PhysicalProject {
    /// Create a new PhysicalProject
    pub fn new(fields: Vec<PhysicalProjectField>, children: Vec<Arc<dyn PhysicalPlan>>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base, fields }
    }
    
    /// Create a new PhysicalProject with a single child
    pub fn with_single_child(fields: Vec<PhysicalProjectField>, child: Arc<dyn PhysicalPlan>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(vec![child], index);
        Self { base, fields }
    }
}

impl PhysicalPlan for PhysicalProject {
    fn children(&self) -> &[Arc<dyn PhysicalPlan>] {
        &self.base.children
    }
    
    fn get_plan_type(&self) -> &str {
        "PhysicalProject"
    }
    
    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}