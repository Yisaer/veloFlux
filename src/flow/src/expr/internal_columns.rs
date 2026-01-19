//! Helpers for recognizing internal/derived column names produced by planning stages.
//!
//! Today we only have parser placeholders like `col_1`. In later stages (e.g. CSE),
//! we will introduce additional internal columns (e.g. `__vf_cse_1`). Centralizing
//! the recognition logic avoids subtle drift across modules.

pub const RESERVED_USER_PREFIX: &str = "__vf_";
pub const CSE_TEMP_PREFIX: &str = "__vf_cse_";

/// Returns true if `name` is a parser-produced placeholder column (e.g. `col_1`).
pub fn is_parser_placeholder(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("col_") else {
        return false;
    };
    !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit())
}

/// Returns true if `name` is a planner-produced CSE temporary column (e.g. `__vf_cse_1`).
///
/// Note: CSE is not implemented yet; this predicate is reserved for upcoming work.
pub fn is_cse_temp(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(CSE_TEMP_PREFIX) else {
        return false;
    };
    !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit())
}

/// Returns true if `name` is any internal derived column name.
pub fn is_internal_derived(name: &str) -> bool {
    is_parser_placeholder(name) || is_cse_temp(name)
}

/// Returns true if users must not define an alias with this name.
pub fn is_reserved_user_name(name: &str) -> bool {
    is_internal_derived(name) || name.starts_with(RESERVED_USER_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_placeholder_recognition() {
        assert!(is_parser_placeholder("col_1"));
        assert!(is_parser_placeholder("col_123"));
        assert!(!is_parser_placeholder("col_"));
        assert!(!is_parser_placeholder("col_a"));
        assert!(!is_parser_placeholder("xcol_1"));
    }

    #[test]
    fn cse_temp_recognition() {
        assert!(is_cse_temp("__vf_cse_1"));
        assert!(is_cse_temp("__vf_cse_999"));
        assert!(!is_cse_temp("__vf_cse_"));
        assert!(!is_cse_temp("__vf_cse_a"));
        assert!(!is_cse_temp("col_1"));
    }

    #[test]
    fn reserved_user_name_recognition() {
        assert!(is_reserved_user_name("col_1"));
        assert!(is_reserved_user_name("__vf_cse_1"));
        assert!(is_reserved_user_name("__vf_anything"));
        assert!(!is_reserved_user_name("a"));
        assert!(!is_reserved_user_name("b_1"));
    }
}
