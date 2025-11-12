//! Aggregate function visitor for parser module
//! Uses sqlparser's visitor pattern to efficiently detect and extract aggregate functions
//! Now with duplicate detection: same aggregate function gets same replacement name

use sqlparser::ast::{Expr, Visitor, Visit};
use std::collections::HashMap;

/// Visitor that collects aggregate functions from SQL expressions
/// Enhanced with duplicate detection - same aggregate function gets same replacement name
pub struct AggregateVisitor {
    /// Found aggregate functions: replacement_name -> (function_name, expr)
    /// Maintains insertion order for consistent naming
    pub aggregates: HashMap<String, (String, Expr)>,
    /// Counter for generating replacement names (local to this visitor instance)
    replacement_counter: usize,
    /// Reverse lookup: expr_string -> replacement_name (for duplicate detection)
    expr_to_replacement: HashMap<String, String>,
}

impl AggregateVisitor {
    pub fn new() -> Self {
        Self {
            aggregates: HashMap::new(),
            replacement_counter: 0,
            expr_to_replacement: HashMap::new(),
        }
    }

    /// Check if a function is an aggregate function
    fn is_aggregate_function(&self, function_name: &str) -> bool {
        let aggregates = ["sum", "count", "avg", "min", "max", "stddev", "variance"];
        aggregates.contains(&function_name.to_lowercase().as_str())
    }

    /// Generate a replacement column name using local counter
    fn generate_replacement_name(&mut self) -> String {
        self.replacement_counter += 1;
        format!("col_{}", self.replacement_counter)
    }

    /// Check if we've already seen this exact aggregate expression
    fn find_existing_replacement(&self, expr: &Expr) -> Option<String> {
        let expr_str = format!("{:?}", expr);
        self.expr_to_replacement.get(&expr_str).cloned()
    }

    /// Get aggregate mappings from the visitor (replacement_name -> original_expr)
    pub fn get_aggregate_mappings(&self) -> HashMap<String, Expr> {
        let mut result = HashMap::new();
        for (replacement_name, (_func_name, original_expr)) in &self.aggregates {
            result.insert(replacement_name.clone(), original_expr.clone());
        }
        result
    }
}

impl Default for AggregateVisitor {
    fn default() -> Self {
        Self::new()
    }
}

impl Visitor for AggregateVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        if let Expr::Function(func) = expr {
            let func_name = func.name.to_string();
            if self.is_aggregate_function(&func_name) {
                // Check if we've already seen this exact aggregate expression
                if let Some(_existing_replacement) = self.find_existing_replacement(expr) {
                    // We've seen this exact aggregate before, skip it
                    return std::ops::ControlFlow::Continue(());
                }

                // This is a new aggregate expression, generate replacement name
                let replacement_name = self.generate_replacement_name();
                
                // Store the mapping: replacement_name -> (function_name, original_expr)
                self.aggregates.insert(replacement_name.clone(), (func_name, expr.clone()));
                
                // Also store reverse mapping for duplicate detection
                let expr_str = format!("{:?}", expr);
                self.expr_to_replacement.insert(expr_str, replacement_name);
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// Simple function to extract aggregate functions using visitor pattern
/// Returns: HashMap<replacement_name, (function_name, original_expr)>
pub fn extract_aggregates_with_visitor(expr: &Expr) -> HashMap<String, (String, Expr)> {
    let mut visitor = AggregateVisitor::new();
    let _ = expr.visit(&mut visitor);
    visitor.aggregates
}

/// Check if an expression contains aggregate functions using visitor
pub fn contains_aggregates_with_visitor(expr: &Expr) -> bool {
    let mut visitor = AggregateVisitor::new();
    let _ = expr.visit(&mut visitor);
    !visitor.aggregates.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Function, Ident};
    use sqlparser::ast::ObjectName;
    use sqlparser::ast::FunctionArg;
    use sqlparser::ast::FunctionArgExpr;

    #[test]
    fn test_simple_aggregate_extraction() {
        println!("\n=== Testing Simple Aggregate Extraction ===");
        
        // Create a simple aggregate function
        let expr = Expr::Function(Function {
            name: ObjectName(vec![Ident::new("sum")]),
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                Expr::Identifier(Ident::new("a"))
            ))],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        });

        let mappings = extract_aggregates_with_visitor(&expr);
        
        assert_eq!(mappings.len(), 1);
        assert!(mappings.contains_key("col_1"));
        
        let (_func_name, expr) = mappings.get("col_1").unwrap();
        match expr {
            Expr::Function(func) => {
                assert_eq!(func.name.to_string(), "sum");
            }
            _ => panic!("Expected function expression"),
        }
    }

    #[test]
    fn test_duplicate_aggregate_detection() {
        println!("\n=== Testing Duplicate Aggregate Detection ===");
        
        // Create a binary operation with two identical aggregates: sum(a) + sum(a)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("sum")]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new("a"))
                ))],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            })),
            op: sqlparser::ast::BinaryOperator::Plus,
            right: Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("sum")]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new("a"))
                ))],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            })),
        };

        let mappings = extract_aggregates_with_visitor(&expr);
        
        // Should only find ONE aggregate, not two!
        assert_eq!(mappings.len(), 1, "Should detect duplicate aggregates and only create one mapping");
        assert!(mappings.contains_key("col_1"));
        
        // Verify the mapping
        let (func_name, _) = mappings.get("col_1").unwrap();
        assert_eq!(func_name, "sum");
        
        println!("✓ Successfully detected duplicate aggregates: only 1 mapping created for sum(a) + sum(a)");
    }

    #[test]
    fn test_different_aggregates() {
        println!("\n=== Testing Different Aggregates ===");
        
        // Create a binary operation with two different aggregates: sum(a) + count(b)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("sum")]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new("a"))
                ))],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            })),
            op: sqlparser::ast::BinaryOperator::Plus,
            right: Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("count")]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new("b"))
                ))],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            })),
        };

        let mappings = extract_aggregates_with_visitor(&expr);
        
        // Should find TWO different aggregates
        assert_eq!(mappings.len(), 2, "Should find two different aggregates");
        assert!(mappings.contains_key("col_1"));
        assert!(mappings.contains_key("col_2"));
        
        // Verify the mappings
        let (func1_name, _) = mappings.get("col_1").unwrap();
        let (func2_name, _) = mappings.get("col_2").unwrap();
        assert_eq!(func1_name, "sum");
        assert_eq!(func2_name, "count");
        
        println!("✓ Successfully extracted different aggregates: sum(a) + count(b) -> col_1 + col_2");
    }

    #[test]
    fn test_same_function_different_args() {
        println!("\n=== Testing Same Function Different Args ===");
        
        // Create a binary operation with same function but different args: sum(a) + sum(b)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("sum")]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new("a"))
                ))],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            })),
            op: sqlparser::ast::BinaryOperator::Plus,
            right: Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("sum")]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new("b"))
                ))],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            })),
        };

        let mappings = extract_aggregates_with_visitor(&expr);
        
        // Should find TWO different aggregates (different args)
        assert_eq!(mappings.len(), 2, "Should find two aggregates with different arguments");
        assert!(mappings.contains_key("col_1"));
        assert!(mappings.contains_key("col_2"));
        
        // Verify both are sum functions but with different expressions
        let (func1_name, expr1) = mappings.get("col_1").unwrap();
        let (func2_name, expr2) = mappings.get("col_2").unwrap();
        assert_eq!(func1_name, "sum");
        assert_eq!(func2_name, "sum");
        
        // The expressions should be different (one with "a", one with "b")
        assert_ne!(format!("{:?}", expr1), format!("{:?}", expr2));
        
        println!("✓ Successfully distinguished same function with different args: sum(a) + sum(b) -> col_1 + col_2");
    }
}