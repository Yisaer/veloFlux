//! Physical plan builder - converts logical plans to physical plans

use std::sync::Arc;
use crate::planner::logical::{LogicalPlan, DataSource as LogicalDataSource, Filter as LogicalFilter, Project as LogicalProject};
use crate::planner::physical::{PhysicalPlan, PhysicalDataSource, PhysicalFilter, PhysicalProject};
use crate::planner::physical::physical_project::PhysicalProjectField;

/// Create a physical plan from a logical plan
/// 
/// This function walks through the logical plan tree and creates corresponding physical plan nodes.
/// Uses downcast_ref for type-safe pattern matching and delegates to specific creation functions.
pub fn create_physical_plan(logical_plan: Arc<dyn LogicalPlan>) -> Result<Arc<dyn PhysicalPlan>, String> {
    // Try to downcast to specific logical plan types and delegate to corresponding creation functions
    if let Some(logical_ds) = logical_plan.as_any().downcast_ref::<LogicalDataSource>() {
        create_physical_data_source(logical_ds, *logical_plan.get_plan_index())
    } else if let Some(logical_filter) = logical_plan.as_any().downcast_ref::<LogicalFilter>() {
        create_physical_filter(logical_filter, &logical_plan, *logical_plan.get_plan_index())
    } else if let Some(logical_project) = logical_plan.as_any().downcast_ref::<LogicalProject>() {
        create_physical_project(logical_project, &logical_plan, *logical_plan.get_plan_index())
    } else {
        // Handle unsupported plan types
        Err(format!("Unsupported logical plan type: {}", logical_plan.get_plan_type()))
    }
}

/// Create a PhysicalDataSource from a LogicalDataSource
fn create_physical_data_source(
    logical_ds: &LogicalDataSource, 
    index: i64
) -> Result<Arc<dyn PhysicalPlan>, String> {
    let physical_ds = PhysicalDataSource::new(
        logical_ds.source_name.clone(),
        index,
    );
    Ok(Arc::new(physical_ds))
}

/// Create a PhysicalFilter from a LogicalFilter
fn create_physical_filter(
    logical_filter: &LogicalFilter,
    logical_plan: &Arc<dyn LogicalPlan>,
    index: i64
) -> Result<Arc<dyn PhysicalPlan>, String> {
    // Convert children first
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone())?;
        physical_children.push(physical_child);
    }
    
    let physical_filter = PhysicalFilter::new(
        logical_filter.predicate.clone(),
        physical_children,
        index,
    );
    Ok(Arc::new(physical_filter))
}

/// Create a PhysicalProject from a LogicalProject
fn create_physical_project(
    logical_project: &LogicalProject,
    logical_plan: &Arc<dyn LogicalPlan>,
    index: i64
) -> Result<Arc<dyn PhysicalPlan>, String> {
    // Convert children first
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone())?;
        physical_children.push(physical_child);
    }
    
    // Convert logical fields to physical fields
    let mut physical_fields = Vec::new();
    for logical_field in &logical_project.fields {
        let physical_field = PhysicalProjectField::from_logical(
            logical_field.field_name.clone(),
            logical_field.expr.clone(),
        )?;
        physical_fields.push(physical_field);
    }
    
    let physical_project = PhysicalProject::new(
        physical_fields,
        physical_children,
        index,
    );
    Ok(Arc::new(physical_project))
}