//! Integration test for StreamDialect parser with flow conversion

use flow::{StreamSqlConverter, parse_sql_to_scalar_expr, DataFusionEvaluator, ScalarExpr};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type, Value, StringType};

fn create_test_schema() -> Schema {
    Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
    ])
}

#[test]
fn test_stream_parser_to_scalar_conversion() {
    println!("\n=== Stream Parser to ScalarExpr Integration Test ===");
    
    let schema = create_test_schema();
    let converter = StreamSqlConverter::new();
    
    // Test simple expression
    let sql = "SELECT a + b";
    println!("Testing SQL: {}", sql);
    
    let result = converter.parse_sql_to_scalar(sql, &schema);
    assert!(result.is_ok());
    
    let expressions = result.unwrap();
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            println!("✓ Converted to binary operation: {:?}", func);
            // Verify both operands are column references
            match (expr1.as_ref(), expr2.as_ref()) {
                (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) => {
                    assert_eq!(*idx1, 0); // a is at index 0
                    assert_eq!(*idx2, 1); // b is at index 1
                    println!("✓ Both operands correctly mapped to columns {} + {}", idx1, idx2);
                }
                _ => panic!("Expected column references for both operands"),
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_stream_parser_multiple_expressions() {
    println!("\n=== Multiple Expressions Test ===");
    
    let schema = create_test_schema();
    
    let sql = "SELECT a, b + c, 42, CONCAT(name, 'test')";
    println!("Testing SQL: {}", sql);
    
    let expressions = parse_sql_to_scalar_expr(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 4);
    
    // First expression: a
    match &expressions[0] {
        ScalarExpr::Column(idx) => {
            assert_eq!(*idx, 0);
            println!("✓ First expression: column {} (a)", idx);
        }
        _ => panic!("Expected column reference"),
    }
    
    // Second expression: b + c
    match &expressions[1] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, flow::expr::BinaryFunc::Add);
            match (expr1.as_ref(), expr2.as_ref()) {
                (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) => {
                    assert_eq!(*idx1, 1); // b
                    assert_eq!(*idx2, 2); // c
                    println!("✓ Second expression: columns {} + {} (b + c)", idx1, idx2);
                }
                _ => panic!("Expected column references"),
            }
        }
        _ => panic!("Expected binary operation"),
    }
    
    // Third expression: 42 (literal)
    match &expressions[2] {
        ScalarExpr::Literal(val, _) => {
            assert_eq!(*val, Value::Int64(42));
            println!("✓ Third expression: literal {:?}", val);
        }
        _ => panic!("Expected literal"),
    }
    
    // Fourth expression: CONCAT function
    match &expressions[3] {
        ScalarExpr::CallDf { function_name, args } => {
            assert_eq!(function_name, "CONCAT");
            assert_eq!(args.len(), 2);
            println!("✓ Fourth expression: {} function with {} args", function_name, args.len());
        }
        _ => panic!("Expected function call"),
    }
}

#[test]
fn test_stream_parser_with_alias() {
    println!("\n=== Parse with Alias Test ===");
    
    let schema = create_test_schema();
    let converter = StreamSqlConverter::new();
    
    let sql = "SELECT a + b AS total, c * 2 AS doubled";
    println!("Testing SQL: {}", sql);
    
    let result = converter.parse_and_convert(sql, &schema);
    assert!(result.is_ok());
    
    let expressions = result.unwrap();
    assert_eq!(expressions.len(), 2);
    
    // First expression with alias
    assert_eq!(expressions[0].1, Some("total".to_string()));
    println!("✓ First expression alias: {:?}", expressions[0].1);
    
    // Second expression with alias
    assert_eq!(expressions[1].1, Some("doubled".to_string()));
    println!("✓ Second expression alias: {:?}", expressions[1].1);
}

#[test]
fn test_stream_parser_evaluation() {
    println!("\n=== End-to-End Evaluation Test ===");
    
    let schema = create_test_schema();
    let evaluator = DataFusionEvaluator::new();
    
    // Create test data
    let row = Row::from(vec![
        Value::Int64(5),  // a
        Value::Int64(3),  // b
        Value::Int64(2),  // c
        Value::String("test".to_string()), // name
    ]);
    let tuple = Tuple::new(schema.clone(), row);
    
    // Parse and convert SQL
    let sql = "SELECT a + b, c * 2";
    let expressions = parse_sql_to_scalar_expr(sql, &schema).unwrap();
    
    // Evaluate expressions
    for (i, expr) in expressions.iter().enumerate() {
        let result = expr.eval(&evaluator, &tuple);
        assert!(result.is_ok());
        
        match i {
            0 => {
                // a + b = 5 + 3 = 8
                assert_eq!(result.unwrap(), Value::Int64(8));
                println!("✓ Expression {}: a + b = 8", i);
            }
            1 => {
                // c * 2 = 2 * 2 = 4
                assert_eq!(result.unwrap(), Value::Int64(4));
                println!("✓ Expression {}: c * 2 = 4", i);
            }
            _ => {}
        }
    }
}

#[test]
fn test_complex_expression_parsing() {
    println!("\n=== Complex Expression Parsing Test ===");
    
    let schema = create_test_schema();
    
    let sql = "SELECT (a + b) * c, a + (b * c)";
    println!("Testing SQL: {}", sql);
    
    let expressions = parse_sql_to_scalar_expr(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 2);
    
    // Both expressions should be complex nested operations
    match (&expressions[0], &expressions[1]) {
        (ScalarExpr::CallBinary { func: func1, .. }, ScalarExpr::CallBinary { func: func2, .. }) => {
            assert_eq!(*func1, flow::expr::BinaryFunc::Mul); // (a + b) * c
            assert_eq!(*func2, flow::expr::BinaryFunc::Add); // a + (b * c)
            println!("✓ Complex expressions parsed correctly");
        }
        _ => panic!("Expected binary operations"),
    }
}