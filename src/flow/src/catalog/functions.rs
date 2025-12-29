use crate::aggregation::builtin_aggregation_defs;
use crate::catalog::FunctionDef;
use crate::expr::custom_func::builtin_custom_function_defs;
use crate::stateful::builtin_stateful_function_defs;

fn builtin_function_defs_unsorted() -> Vec<FunctionDef> {
    let mut defs = Vec::new();
    defs.extend(builtin_custom_function_defs());
    defs.extend(builtin_aggregation_defs());
    defs.extend(builtin_stateful_function_defs());
    defs
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
