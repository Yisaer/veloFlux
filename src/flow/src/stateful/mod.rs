pub mod changed_col;
pub mod had_changed;
pub mod lag;
pub mod latest;
pub mod registry;

pub use changed_col::changed_col_function_def;
pub use changed_col::ChangedColFunction;
pub use had_changed::had_changed_function_def;
pub use had_changed::HadChangedFunction;
pub use lag::lag_function_def;
pub use lag::LagFunction;
pub use latest::latest_function_def;
pub use latest::LatestFunction;
pub use registry::{
    StatefulEvalInput, StatefulFunction, StatefulFunctionInstance, StatefulFunctionRegistry,
    StatefulRegistryError,
};

use crate::catalog::FunctionDef;

pub fn builtin_stateful_function_defs() -> Vec<FunctionDef> {
    vec![
        changed_col_function_def(),
        had_changed_function_def(),
        lag_function_def(),
        latest_function_def(),
    ]
}
