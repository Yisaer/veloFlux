pub mod expr;
pub mod model;
pub mod planner;
pub mod processor;

pub use expr::{
    create_df_function_call, BinaryFunc, ConcatFunc, ConversionError,
    DataFusionEvaluator, EvalContext, ScalarExpr, StreamSqlConverter, UnaryFunc,
    convert_expr_to_scalar, convert_select_stmt_to_scalar,
    extract_select_expressions,
};
pub use expr::sql_conversion;
pub use model::{Collection, RecordBatch};
pub use datatypes::Schema;
pub use planner::logical::{LogicalPlan, BaseLogicalPlan, DataSource, Project, Filter};
pub use planner::create_physical_plan;
pub use processor::{
    ControlSourceProcessor, DataSourceProcessor, ResultSinkProcessor,
    StreamData, ControlSignal, Processor, ProcessorError
};

use processor::create_processor_pipeline;
use processor::ProcessorPipeline;
use planner::logical::create_logical_plan;

/// Create a processor pipeline from SQL query
///
/// This function provides a high-level interface to create a complete processing pipeline
/// from a SQL query string. It handles the entire process from SQL parsing to pipeline creation.
///
/// # Arguments
/// * `sql` - The SQL query string
///
/// # Returns
/// A Result containing the ProcessorPipeline if successful, or an error if any step fails
///
/// # Example
/// ```no_run
/// use flow::create_pipeline;
/// 
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let pipeline = create_pipeline("SELECT a + 1, b + 2 FROM stream")?;
/// // Use the pipeline...
/// # Ok(())
/// # }
/// ```
pub fn create_pipeline(sql: &str) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    // Parse SQL
    let select_stmt = parser::parse_sql(sql)?;
    
    // Create logical plan
    let logical_plan = create_logical_plan(select_stmt)?;
    
    // Create physical plan
    let physical_plan = create_physical_plan(logical_plan)?;
    
    // Create processor pipeline
    let pipeline = create_processor_pipeline(physical_plan)?;
    
    Ok(pipeline)
}