pub mod helpers;
pub mod math_func;
pub mod registry;
pub mod string_func;
use crate::catalog::FunctionDef;
use crate::expr::func::EvalError;
use datatypes::Value;

pub use registry::CustomFuncRegistry;

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

    /// Additional SQL-visible aliases for this function.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }
}

pub fn builtin_custom_function_defs() -> Vec<FunctionDef> {
    let mut defs = Vec::new();

    defs.extend(math_func::builtin_function_defs());
    defs.extend(string_func::builtin_function_defs());

    defs
}
