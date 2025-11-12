use std::any::Any;
use std::sync::Arc;
use crate::planner::physical::{PhysicalPlan, BasePhysicalPlan};
use sqlparser::ast::Expr;

/// Physical operator for filter operations
/// 
/// This operator represents the physical execution of filter operations,
/// applying predicate expressions to filter records from input data.
#[derive(Debug, Clone)]
pub struct PhysicalFilter {
    pub base: BasePhysicalPlan,
    pub predicate: Expr,
}

impl PhysicalFilter {
    /// Create a new PhysicalFilter
    pub fn new(predicate: Expr, children: Vec<Arc<dyn PhysicalPlan>>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base, predicate }
    }
    
    /// Create a new PhysicalFilter with a single child
    pub fn with_single_child(predicate: Expr, child: Arc<dyn PhysicalPlan>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(vec![child], index);
        Self { base, predicate }
    }
}

impl PhysicalPlan for PhysicalFilter {
    fn children(&self) -> &[Arc<dyn PhysicalPlan>] {
        &self.base.children
    }
    
    fn get_plan_type(&self) -> &str {
        "PhysicalFilter"
    }
    
    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}