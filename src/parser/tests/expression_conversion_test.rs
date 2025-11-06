//! Expression conversion tests using StreamDialect Parser
//! This tests the conversion from StreamDialect parsed expressions to custom formats

use sqlparser::ast::{Expr, BinaryOperator};

use parser::{parse_sql, SelectStmt};

// Custom expression format that we convert StreamDialect parsed expressions to
#[derive(Debug, Clone, PartialEq)]
pub enum CustomExpr {
    Column(String),
    Literal(String),
    BinaryOp {
        left: Box<CustomExpr>,
        op: CustomBinaryOp,
        right: Box<CustomExpr>,
    },
    Function {
        name: String,
        args: Vec<CustomExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CustomBinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    And,
    Or,
}

// Convert from sqlparser BinaryOperator to our custom operator
impl From<&BinaryOperator> for CustomBinaryOp {
    fn from(op: &BinaryOperator) -> Self {
        match op {
            BinaryOperator::Plus => CustomBinaryOp::Add,
            BinaryOperator::Minus => CustomBinaryOp::Subtract,
            BinaryOperator::Multiply => CustomBinaryOp::Multiply,
            BinaryOperator::Divide => CustomBinaryOp::Divide,
            BinaryOperator::Eq => CustomBinaryOp::Equal,
            BinaryOperator::NotEq => CustomBinaryOp::NotEqual,
            BinaryOperator::Gt => CustomBinaryOp::GreaterThan,
            BinaryOperator::Lt => CustomBinaryOp::LessThan,
            BinaryOperator::GtEq => CustomBinaryOp::GreaterThanOrEqual,
            BinaryOperator::LtEq => CustomBinaryOp::LessThanOrEqual,
            BinaryOperator::And => CustomBinaryOp::And,
            BinaryOperator::Or => CustomBinaryOp::Or,
            _ => CustomBinaryOp::Equal, // Default fallback
        }
    }
}

/// Convert StreamDialect parsed SelectStmt to custom expressions recursively
pub fn convert_select_stmt_to_custom(select_stmt: &SelectStmt) -> Vec<CustomExpr> {
    let mut results = Vec::new();
    
    for field in &select_stmt.select_fields {
        let converted = convert_expr_recursive(&field.expr);
        results.push(converted);
    }
    
    results
}

/// Convert parsed SQL directly to custom expressions using StreamDialect
pub fn parse_sql_to_custom_expressions(sql: &str) -> Result<Vec<CustomExpr>, String> {
    let select_stmt = parse_sql(sql)?;
    Ok(convert_select_stmt_to_custom(&select_stmt))
}

/// Helper function to extract Expr from FunctionArgExpr
fn func_arg_expr_as_expr(func_arg_expr: &sqlparser::ast::FunctionArgExpr) -> Option<&Expr> {
    match func_arg_expr {
        sqlparser::ast::FunctionArgExpr::Expr(expr) => Some(expr),
        _ => None,
    }
}

/// Recursively convert sqlparser Expr to custom format
fn convert_expr_recursive(expr: &Expr) -> CustomExpr {
    match expr {
        Expr::Identifier(ident) => {
            CustomExpr::Column(ident.value.clone())
        }
        Expr::Value(value) => {
            CustomExpr::Literal(format!("{:?}", value))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_expr = convert_expr_recursive(left);
            let right_expr = convert_expr_recursive(right);
            CustomExpr::BinaryOp {
                left: Box::new(left_expr),
                op: CustomBinaryOp::from(op),
                right: Box::new(right_expr),
            }
        }
        Expr::Function(func) => {
            let mut args = Vec::new();
            for arg in &func.args {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(func_arg_expr) => {
                        if let Some(expr) = func_arg_expr_as_expr(func_arg_expr) {
                            args.push(convert_expr_recursive(expr));
                        }
                    }
                    sqlparser::ast::FunctionArg::Named { arg: func_arg_expr, .. } => {
                        if let Some(expr) = func_arg_expr_as_expr(func_arg_expr) {
                            args.push(convert_expr_recursive(expr));
                        }
                    }
                }
            }
            CustomExpr::Function {
                name: func.name.to_string(),
                args,
            }
        }
        Expr::Nested(inner) => {
            // Handle nested expressions by converting the inner expression
            convert_expr_recursive(inner)
        }
        _ => CustomExpr::Literal("unsupported".to_string()),
    }
}

#[test]
fn test_stream_dialect_simple_expression_conversion() {
    let sql = "SELECT a + b";
    
    // Step 1: Parse using StreamDialect
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Step 2: Convert to custom expressions
    let custom_exprs = convert_select_stmt_to_custom(&select_stmt);
    
    // Step 3: Verify conversion
    assert_eq!(custom_exprs.len(), 1);
    match &custom_exprs[0] {
        CustomExpr::BinaryOp { op, .. } => {
            assert_eq!(*op, CustomBinaryOp::Add);
            println!("Simple binary operation converted: {:?}", custom_exprs[0]);
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_stream_dialect_literal_conversion() {
    let sql = "SELECT 42, 'hello', true";
    
    // Parse using StreamDialect
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    assert_eq!(select_stmt.select_fields.len(), 3);
    
    // Convert to custom expressions
    let custom_exprs = convert_select_stmt_to_custom(&select_stmt);
    
    // Verify literals
    let literals: Vec<&CustomExpr> = custom_exprs.iter()
        .filter(|expr| matches!(expr, CustomExpr::Literal(_)))
        .collect();
    
    assert_eq!(literals.len(), 3);
    println!("Found {} literal expressions", literals.len());
    
    // Test direct conversion function
    let direct_result = parse_sql_to_custom_expressions(sql);
    assert!(direct_result.is_ok());
    assert_eq!(direct_result.unwrap().len(), 3);
}

#[test]
fn test_stream_dialect_function_conversion() {
    let sql = "SELECT CONCAT(a, b), UPPER(name)";
    
    // Parse using StreamDialect
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    assert_eq!(select_stmt.select_fields.len(), 2);
    
    // Convert to custom expressions
    let custom_exprs = convert_select_stmt_to_custom(&select_stmt);
    
    // Verify functions
    let functions: Vec<&CustomExpr> = custom_exprs.iter()
        .filter(|expr| matches!(expr, CustomExpr::Function { .. }))
        .collect();
    
    assert_eq!(functions.len(), 2);
    
    // Check function names
    for func in functions {
        if let CustomExpr::Function { name, args } = func {
            assert!(!name.is_empty());
            println!("Function: {} with {} args", name, args.len());
        }
    }
}

#[test]
fn test_stream_dialect_complex_expression_conversion() {
    let sql = "SELECT (a + b) * c";
    
    // Parse using StreamDialect
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Convert to custom expressions
    let custom_exprs = convert_select_stmt_to_custom(&select_stmt);
    
    // Should handle complex nested expressions
    assert_eq!(custom_exprs.len(), 1);
    match &custom_exprs[0] {
        CustomExpr::BinaryOp { op, left, .. } => {
            assert_eq!(*op, CustomBinaryOp::Multiply);
            println!("Complex expression converted: multiplication with nested operands");
            
            // Verify nested structure
            match left.as_ref() {
                CustomExpr::BinaryOp { op: inner_op, .. } => {
                    assert_eq!(*inner_op, CustomBinaryOp::Add);
                    println!("Left operand is addition: (a + b)");
                }
                _ => panic!("Expected nested binary operation"),
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_stream_dialect_nested_expression_conversion() {
    let sql = "SELECT a + (b * c)";
    
    // Parse using StreamDialect
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Convert to custom expressions
    let custom_exprs = convert_select_stmt_to_custom(&select_stmt);
    
    // Should handle nested structure
    assert_eq!(custom_exprs.len(), 1);
    match &custom_exprs[0] {
        CustomExpr::BinaryOp { op, left: _, right } => {
            assert_eq!(*op, CustomBinaryOp::Add);
            
            // Right operand should be multiplication
            match right.as_ref() {
                CustomExpr::BinaryOp { op: inner_op, .. } => {
                    assert_eq!(*inner_op, CustomBinaryOp::Multiply);
                    println!("Nested expression converted: right operand is (b * c)");
                }
                _ => panic!("Expected nested binary operation"),
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_stream_dialect_alias_preservation() {
    let sql = "SELECT a + b AS total, c * 2 AS doubled";
    
    // Parse using StreamDialect
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    assert_eq!(select_stmt.select_fields.len(), 2);
    
    // Verify aliases are preserved in SelectStmt
    assert_eq!(select_stmt.select_fields[0].alias, Some("total".to_string()));
    assert_eq!(select_stmt.select_fields[1].alias, Some("doubled".to_string()));
    
    println!("Aliases preserved in StreamDialect parsing: total, doubled");
    
    // Convert to custom expressions
    let custom_exprs = convert_select_stmt_to_custom(&select_stmt);
    assert_eq!(custom_exprs.len(), 2);
    
    println!("Expression conversion completed for aliased fields");
}

#[test]
fn test_stream_dialect_error_handling() {
    // Test invalid SQL
    let invalid_sql = "INVALID SQL EXPRESSION";
    let result = parse_sql_to_custom_expressions(invalid_sql);
    
    assert!(result.is_err());
    println!("Invalid SQL properly rejected: {:?}", result.unwrap_err());
    
    // Test non-SELECT SQL
    let insert_sql = "INSERT INTO table VALUES (1)";
    let result = parse_sql_to_custom_expressions(insert_sql);
    
    assert!(result.is_err());
    println!("Non-SELECT SQL properly rejected: {:?}", result.unwrap_err());
}