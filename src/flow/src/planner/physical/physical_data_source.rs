use std::any::Any;
use std::sync::Arc;
use crate::planner::physical::{PhysicalPlan, BasePhysicalPlan};

/// Physical operator for reading data from a data source
/// 
/// This is typically a leaf node in the physical plan tree that represents
/// the source of data for stream processing (e.g., a Kafka topic, file, etc.)
#[derive(Debug, Clone)]
pub struct PhysicalDataSource {
    pub base: BasePhysicalPlan,
    pub source_name: String,
}

impl PhysicalDataSource {
    /// Create a new PhysicalDataSource
    pub fn new(source_name: String, index: i64) -> Self {
        let base = BasePhysicalPlan::new_leaf(index);
        Self {
            base,
            source_name,
        }
    }
    
    /// Create a new PhysicalDataSource with source information
    pub fn with_source_info(source_name: String, _source_info: std::collections::HashMap<String, String>, index: i64) -> Self {
        let base = BasePhysicalPlan::new_leaf(index);
        Self {
            base,
            source_name,
        }
    }
}

impl PhysicalPlan for PhysicalDataSource {
    fn children(&self) -> &[Arc<dyn PhysicalPlan>] {
        &self.base.children
    }
    
    fn get_plan_type(&self) -> &str {
        "PhysicalDataSource"
    }
    
    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}