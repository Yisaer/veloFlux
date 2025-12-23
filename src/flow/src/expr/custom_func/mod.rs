pub mod registry;
pub mod string_func;

use crate::expr::func::EvalError;
use datatypes::Value;
pub use registry::{CustomFuncRegistry, CustomFuncRegistryError};
pub use string_func::ConcatFunc;

/// Custom function that can be implemented by users
/// This trait allows users to define their own functions for evaluation
pub trait CustomFunc: Send + Sync + std::fmt::Debug {
    /// Validate function arguments for a single row evaluation.
    ///
    /// # Arguments
    ///
    /// * `args` - Values for this row in call-order.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if arguments are valid for per-row processing, or an error otherwise.
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError>;

    /// Evaluate the function with row arguments (one row per call).
    ///
    /// # Arguments
    ///
    /// * `args` - Values for this row in call-order. Assumes they were validated by `validate_row`.
    ///
    /// # Returns
    ///
    /// Returns the evaluated result for the current row.
    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError>;

    /// Get the function name for debugging purposes
    fn name(&self) -> &str;
}
