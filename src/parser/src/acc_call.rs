use serde::{Deserialize, Serialize};
use sqlparser::ast::Expr;

pub const BUILTIN_ACC_FUNCTIONS: [&str; 5] =
    ["acc_sum", "acc_max", "acc_min", "acc_count", "acc_avg"];

pub fn builtin_acc_function_names() -> &'static [&'static str] {
    &BUILTIN_ACC_FUNCTIONS
}

pub fn is_acc_function_name(name: &str) -> bool {
    BUILTIN_ACC_FUNCTIONS
        .iter()
        .any(|func_name| func_name.eq_ignore_ascii_case(name))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccCallSpec {
    pub func_name: String,
    pub args: Vec<Expr>,
    pub original_expr: Expr,
}

impl AccCallSpec {
    pub fn dedup_key(&self) -> String {
        let args = self
            .args
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}|args=[{}]", self.func_name, args)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccMappingEntry {
    pub output_column: String,
    pub spec: AccCallSpec,
}
