use sqlparser::ast::{GroupByExpr, SetExpr, Statement};
use sqlparser::parser::ParserError;

use super::window;
pub use window::{Window, parse_window_expr, window_to_expr};

/// Stream processing dialect that supports window functions in GROUP BY clauses
/// Currently supports tumblingwindow (time-based) and countwindow (row-count based)
#[derive(Debug, Clone)]
pub struct StreamDialect {}

impl StreamDialect {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StreamDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl sqlparser::dialect::Dialect for StreamDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }
}

/// Collect window function present in a parsed statement
/// Enforces at most one window per statement
pub fn collect_windows_in_statement(statement: &Statement) -> Result<Option<Window>, ParserError> {
    if let Statement::Query(query) = statement {
        if let SetExpr::Select(select) = &*query.body {
            return collect_group_by_windows(&select.group_by);
        }
    }

    Ok(None)
}

/// Parse GROUP BY clause to extract supported window (if any)
/// Errors if multiple window functions are present
pub fn collect_group_by_windows(group_by: &GroupByExpr) -> Result<Option<Window>, ParserError> {
    let mut found: Option<Window> = None;

    match group_by {
        GroupByExpr::Expressions(exprs) => {
            for expr in exprs {
                if let Some(window) = parse_window_expr(expr)? {
                    if found.is_some() {
                        return Err(ParserError::ParserError(
                            "Only one window function is allowed in GROUP BY".to_string(),
                        ));
                    }
                    found = Some(window);
                }
            }
        }
        _ => {}
    };

    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::parser::Parser;

    #[test]
    fn parse_single_tumbling_window() {
        let sql = "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10)";
        let dialect = StreamDialect::new();

        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(statements.len(), 1);

        // Collect the windows from GROUP BY
        let windows = collect_windows_in_statement(&statements[0]).unwrap();
        assert!(windows.is_some());

        assert!(matches!(
            windows.unwrap(),
            window::Window::Tumbling {
                ref time_unit,
                length: 10
            } if *time_unit == window::TimeUnit::Seconds
        ));
    }
}
