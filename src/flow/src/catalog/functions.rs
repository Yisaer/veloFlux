use crate::aggregation::sum_function_def;
use crate::catalog::FunctionDef;
use crate::expr::custom_func::string_func::concat_function_def;
use crate::stateful::lag_function_def;

fn builtin_function_defs_unsorted() -> Vec<FunctionDef> {
    vec![
        concat_function_def(),
        sum_function_def(),
        lag_function_def(),
    ]
}

/// List all known SQL-visible functions with their metadata.
pub fn list_function_defs() -> Vec<FunctionDef> {
    let mut defs = builtin_function_defs_unsorted();
    defs.sort_by(|a, b| a.name.cmp(&b.name));
    defs
}

/// Resolve a function definition by its canonical name or alias (case-insensitive).
pub fn describe_function_def(name_or_alias: &str) -> Option<FunctionDef> {
    let key = name_or_alias.to_ascii_lowercase();
    for def in builtin_function_defs_unsorted() {
        if def.name.eq_ignore_ascii_case(&key) {
            return Some(def);
        }
        if def
            .aliases
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(&key))
        {
            return Some(def);
        }
    }
    None
}
