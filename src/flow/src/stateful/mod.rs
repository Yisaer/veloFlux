pub mod lag;
pub mod registry;

pub use lag::lag_function_def;
pub use lag::LagFunction;
pub use registry::{
    StatefulFunction, StatefulFunctionInstance, StatefulFunctionRegistry, StatefulRegistryError,
};

use crate::catalog::FunctionDef;

pub fn builtin_stateful_function_defs() -> Vec<FunctionDef> {
    vec![lag_function_def()]
}
