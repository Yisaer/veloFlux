//! Extract all expressions from SQL statements
//! This module provides functionality to extract and analyze SQL expressions

use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value, Visit, Visitor};
use sqlparser::parser::Parser;

use crate::dialect::StreamDialect;

/// Collect functions from the resolved SELECT AST, including expressions that
/// were lifted into aggregate, stateful, or accumulate mappings.
pub fn referenced_functions(statement: &crate::SelectStmt) -> Vec<String> {
    struct Functions(Vec<String>);
    impl Visitor for Functions {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
            if let Expr::Function(function) = expr {
                self.0.push(function.name.to_string().to_ascii_lowercase());
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let mut visitor = Functions(Vec::new());
    for field in &statement.select_fields {
        let _ = field.expr.visit(&mut visitor);
    }
    for expr in statement
        .where_condition
        .iter()
        .chain(statement.having.iter())
        .chain(statement.group_by_exprs.iter())
        .chain(statement.aggregate_mappings.values())
    {
        let _ = expr.visit(&mut visitor);
    }
    for item in &statement.order_by {
        let _ = item.expr.visit(&mut visitor);
    }
    for item in &statement.stateful_mappings {
        let _ = item.spec.original_expr.visit(&mut visitor);
    }
    for item in &statement.acc_mappings {
        let _ = item.spec.original_expr.visit(&mut visitor);
    }
    for join in &statement.joins {
        if let crate::select_stmt::JoinConstraint::On(expr) = &join.constraint {
            let _ = expr.visit(&mut visitor);
        }
    }
    visitor.0.sort();
    visitor.0.dedup();
    visitor.0
}

/// Collect string literal arguments from calls to a named function in a
/// resolved SELECT statement.
pub fn referenced_function_string_arguments(
    statement: &crate::SelectStmt,
    function_name: &str,
) -> Vec<String> {
    struct FunctionArguments<'a> {
        function_name: &'a str,
        values: Vec<String>,
    }

    impl Visitor for FunctionArguments<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
            let Expr::Function(function) = expr else {
                return std::ops::ControlFlow::Continue(());
            };
            if !function
                .name
                .to_string()
                .eq_ignore_ascii_case(self.function_name)
            {
                return std::ops::ControlFlow::Continue(());
            }
            let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))) =
                function.args.first()
            else {
                return std::ops::ControlFlow::Continue(());
            };
            match value {
                Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => {
                    self.values.push(value.clone());
                }
                _ => {}
            }
            std::ops::ControlFlow::Continue(())
        }
    }

    let mut visitor = FunctionArguments {
        function_name,
        values: Vec::new(),
    };
    for expression in statement
        .select_fields
        .iter()
        .map(|field| &field.expr)
        .chain(statement.where_condition.iter())
        .chain(statement.having.iter())
        .chain(statement.group_by_exprs.iter())
        .chain(statement.order_by.iter().map(|item| &item.expr))
        .chain(statement.aggregate_mappings.values())
        .chain(
            statement
                .stateful_mappings
                .iter()
                .map(|mapping| &mapping.spec.original_expr),
        )
        .chain(
            statement
                .acc_mappings
                .iter()
                .map(|mapping| &mapping.spec.original_expr),
        )
        .chain(
            statement
                .joins
                .iter()
                .filter_map(|join| match &join.constraint {
                    crate::select_stmt::JoinConstraint::On(expr) => Some(expr),
                    crate::select_stmt::JoinConstraint::None => None,
                }),
        )
    {
        let _ = expression.visit(&mut visitor);
    }
    visitor.values.sort();
    visitor.values.dedup();
    visitor.values
}

/// Extract all expressions from SQL statements
pub fn extract_expressions_from_sql(sql: &str) -> Result<Vec<String>, String> {
    let dialect = StreamDialect::new();
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| format!("Parse error: {}", e))?;

    let mut extractor = ExpressionExtractor::new();

    for statement in &statements {
        let _ = statement.visit(&mut extractor);
    }

    Ok(extractor.expressions)
}

/// Extract SELECT expressions from SQL
pub fn extract_select_expressions_simple(sql: &str) -> Result<Vec<String>, String> {
    let dialect = StreamDialect::new();
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| format!("Parse error: {}", e))?;

    if statements.len() != 1 {
        return Err("Expected exactly one SQL statement".to_string());
    }

    let mut extractor = SelectExpressionExtractor::new();
    let _ = statements[0].visit(&mut extractor);

    Ok(extractor.expressions)
}

/// Simple visitor to extract all expressions
struct ExpressionExtractor {
    expressions: Vec<String>,
}

impl ExpressionExtractor {
    fn new() -> Self {
        Self {
            expressions: Vec::new(),
        }
    }
}

impl Visitor for ExpressionExtractor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
        self.expressions.push(format!("{:?}", expr));
        std::ops::ControlFlow::Continue(())
    }
}

/// Visitor to extract SELECT expressions specifically
struct SelectExpressionExtractor {
    expressions: Vec<String>,
}

impl SelectExpressionExtractor {
    fn new() -> Self {
        Self {
            expressions: Vec::new(),
        }
    }
}

impl Visitor for SelectExpressionExtractor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
        // Only extract expressions that are part of SELECT projection
        self.expressions.push(format!("{:?}", expr));
        std::ops::ControlFlow::Continue(())
    }
}

/// Helper function to parse and analyze SQL expressions
pub fn analyze_sql_expressions(sql: &str) -> Result<ExpressionAnalysis, String> {
    let dialect = StreamDialect::new();
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| format!("Parse error: {}", e))?;

    let mut analysis = ExpressionAnalysis {
        sql: sql.to_string(),
        expression_count: 0,
        binary_operations: Vec::new(),
        literals: Vec::new(),
        functions: Vec::new(),
    };

    for statement in &statements {
        let mut analyzer = ExpressionAnalyzer::new(&mut analysis);
        let _ = statement.visit(&mut analyzer);
    }

    Ok(analysis)
}

/// Analysis results for SQL expressions
#[derive(Debug, Clone)]
pub struct ExpressionAnalysis {
    pub sql: String,
    pub expression_count: usize,
    pub binary_operations: Vec<String>,
    pub literals: Vec<String>,
    pub functions: Vec<String>,
}

/// Visitor to analyze expression types
struct ExpressionAnalyzer<'a> {
    analysis: &'a mut ExpressionAnalysis,
}

impl<'a> ExpressionAnalyzer<'a> {
    fn new(analysis: &'a mut ExpressionAnalysis) -> Self {
        Self { analysis }
    }
}

impl Visitor for ExpressionAnalyzer<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
        self.analysis.expression_count += 1;

        match expr {
            Expr::BinaryOp { op, .. } => {
                self.analysis.binary_operations.push(format!("{:?}", op));
            }
            Expr::Value(value) => {
                self.analysis.literals.push(format!("{:?}", value));
            }
            Expr::Function(func) => {
                self.analysis.functions.push(func.name.to_string());
            }
            // Note: Removed flow-specific expression types (JsonAccess, MapAccess)
            // Parser module should only focus on basic SQL parsing analysis
            _ => {}
        }

        std::ops::ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_expression() {
        let sql = "SELECT a + b";
        let result = extract_expressions_from_sql(sql);

        assert!(result.is_ok());
        let expressions = result.unwrap();
        assert!(!expressions.is_empty());

        // Should contain the binary operation
        let has_binary_op = expressions.iter().any(|expr| expr.contains("BinaryOp"));
        assert!(has_binary_op);
    }

    #[test]
    fn test_analyze_expressions() {
        let sql = "SELECT a + b, CONCAT(name, 'test'), 42";
        let analysis = analyze_sql_expressions(sql).unwrap();

        assert!(analysis.expression_count > 0);
        assert!(!analysis.binary_operations.is_empty());
        assert!(!analysis.functions.is_empty());
        assert!(!analysis.literals.is_empty());
    }
}
