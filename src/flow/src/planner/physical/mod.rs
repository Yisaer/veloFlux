use std::sync::Arc;
use std::fmt::Debug;
use std::any::Any;

pub mod base_physical;
pub mod physical_data_source;
pub mod physical_project;
pub mod physical_filter;

pub use base_physical::BasePhysicalPlan;
pub use physical_data_source::PhysicalDataSource;
pub use physical_project::PhysicalProject;
pub use physical_filter::PhysicalFilter;

/// Core trait for all physical operators in the stream processing engine
/// 
/// Physical plans represent the physical execution structure for stream processing.
/// They define the execution hierarchy and data flow relationships between operators.
pub trait PhysicalPlan: Send + Sync + Debug {
    /// Get the children of this physical plan
    fn children(&self) -> &[Arc<dyn PhysicalPlan>];
    
    /// Get the type name of this physical plan
    fn get_plan_type(&self) -> &str;
    
    /// Get the unique index of this physical plan
    fn get_plan_index(&self) -> &i64;
    
    /// Allow downcasting to concrete types
    fn as_any(&self) -> &dyn Any;
}