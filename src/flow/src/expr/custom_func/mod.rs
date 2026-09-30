pub mod array_func;
pub mod helpers;
pub mod math_func;
pub mod misc_func;
pub mod null_func;
pub mod obj_func;
pub mod registry;
pub mod string_func;
pub mod time_func;
use crate::catalog::FunctionDef;
use crate::expr::func::EvalError;
use datatypes::Value;

pub use registry::{CustomFuncBindings, CustomFuncRegistry};

/// Processor-owned implementation for collection-level custom functions.
pub trait BatchFunc: Send + Sync + std::fmt::Debug {
    /// Evaluate one complete collection batch. Every argument vector and the returned vector
    /// must have the same row alignment.
    /// The outer error represents a batch-level failure. An inner error represents a
    /// row-level failure and must preserve its row position in the returned vector.
    fn eval_batch(
        &self,
        args: &[Vec<Value>],
        row_count: usize,
    ) -> Result<Vec<Result<Value, EvalError>>, EvalError>;
}

/// Custom function that can be implemented by users
/// This trait allows users to define their own functions for evaluation
pub trait CustomFunc: Send + Sync + std::fmt::Debug {
    /// Whether this definition must be lowered to the collection-level batch path.
    fn is_batch(&self) -> bool {
        false
    }
    /// Create a processor-bound implementation for this function.
    fn bind_runtime(&self) -> Result<Option<std::sync::Arc<dyn CustomFunc>>, String> {
        Ok(None)
    }

    /// Create a processor-bound collection-level implementation for this function.
    fn bind_batch_runtime(&self) -> Result<Option<std::sync::Arc<dyn BatchFunc>>, String> {
        Ok(None)
    }

    /// Validate function arguments for a single row evaluation.
    ///
    /// # Arguments
    ///
    /// * `args` - Values for this row in call-order.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if arguments are valid for per-row processing, or an error otherwise.
    fn validate_row(&self, _args: &[Value]) -> Result<(), EvalError> {
        Err(EvalError::NotImplemented {
            feature: "custom function does not support row evaluation".to_string(),
        })
    }

    /// Evaluate the function with row arguments (one row per call).
    ///
    /// # Arguments
    ///
    /// * `args` - Values for this row in call-order. Assumes they were validated by `validate_row`.
    ///
    /// # Returns
    ///
    /// Returns the evaluated result for the current row.
    fn eval_row(&self, _args: &[Value]) -> Result<Value, EvalError> {
        Err(EvalError::NotImplemented {
            feature: "custom function runtime is not initialized".to_string(),
        })
    }

    /// Get the function name for debugging purposes
    fn name(&self) -> &str;

    /// Additional SQL-visible aliases for this function.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }
}

pub fn builtin_custom_function_defs() -> Vec<FunctionDef> {
    let mut defs = Vec::new();

    defs.extend(array_func::builtin_function_defs());
    defs.extend(math_func::builtin_function_defs());
    defs.extend(null_func::builtin_function_defs());
    defs.extend(string_func::builtin_function_defs());
    defs.extend(time_func::builtin_function_defs());
    defs.extend(obj_func::builtin_function_defs());
    defs.extend(misc_func::builtin_function_defs());
    defs
}
