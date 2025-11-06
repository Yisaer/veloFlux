//! Core conversion test: verify the complete flow of parser → SelectStmt → ScalarExpr → evaluation results
//! Core demonstration: directly receive SelectStmt, convert to ScalarExpr, verify calculation results
//! Test SQL: SELECT a+b, 42
//! Core flow: directly receive SelectStmt, convert to ScalarExpr, verify calculation results

use flow::{StreamSqlConverter, DataFusionEvaluator, ScalarExpr};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type, Value};
use parser::parse_sql;

#[test]
fn test_core_conversion_flow() {
    println!("\n=== Core Conversion Flow Test ===");
    println!("Core demonstration: SelectStmt → ScalarExpr → Calculation Results");
    println!("Test SQL: SELECT a+b, 42");
    
    // 1. Create test schema
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // 2. Use parser module to parse SQL and get SelectStmt
    println!("\n🔍 Step 1: Use parser module to parse SQL");
    let sql = "SELECT a+b, 42";
    println!("Input SQL: {}", sql);
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parsing should succeed");
    println!("✓ Successfully got SelectStmt with {} fields", select_stmt.select_fields.len());
    
    // 3. View SelectStmt structure (verify input is correct)
    println!("\n🔍 Step 2: SelectStmt structure validation");
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        println!("  Field {}: {:?}", i + 1, field.expr);
        println!("         Alias: {:?}", field.alias);
    }
    
    // 4. Core conversion: use StreamSqlConverter to convert SelectStmt to ScalarExpr
    println!("\n🔍 Step 3: Core conversion - SelectStmt → ScalarExpr");
    let converter = StreamSqlConverter::new();
    let expressions = converter.convert_select_stmt_to_scalar(&select_stmt, &schema)
        .expect("SelectStmt conversion should succeed");
    
    // 5. Verify conversion results
    println!("✓ Successfully got {} ScalarExpr", expressions.len());
    assert_eq!(expressions.len(), 2, "Should get 2 expressions");
    
    // 6. Detailed validation of each expression
    println!("\n🔍 Step 4: Expression detailed validation");
    
    // First expression: a + b
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            println!("✓ First expression is binary operation: {:?}", func);
            assert_eq!(*func, flow::expr::BinaryFunc::Add, "Should be addition operation");
            
            // Verify operand mapping
            match (expr1.as_ref(), expr2.as_ref()) {
                (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) => {
                    println!("✓ Operands correctly mapped to columns {} + {}", idx1, idx2);
                    assert_eq!(*idx1, 0, "First operand should be column 0 (a)");
                    assert_eq!(*idx2, 1, "Second operand should be column 1 (b)");
                }
                _ => panic!("Operands should be column references"),
            }
        }
        _ => panic!("First expression should be binary operation"),
    }
    
    // Second expression: 42 (literal)
    match &expressions[1] {
        ScalarExpr::Literal(val, _) => {
            println!("✓ Second expression is literal: {:?}", val);
            assert_eq!(*val, Value::Int64(42), "Should be integer 42");
        }
        _ => panic!("Second expression should be literal"),
    }
    
    // 7. Create test data for calculation verification
    println!("\n🔍 Step 5: Calculation results verification");
    let evaluator = DataFusionEvaluator::new();
    let test_data = Row::from(vec![
        Value::Int64(5),  // a = 5
        Value::Int64(3),  // b = 3
    ]);
    let tuple = Tuple::new(schema, test_data);
    
    // Calculate first expression: a + b = 5 + 3 = 8
    let result1 = expressions[0].eval(&evaluator, &tuple).expect("Calculation should succeed");
    println!("✓ Expression 1 (a+b) calculation result: {:?}", result1);
    assert_eq!(result1, Value::Int64(8), "a+b should equal 8");
    
    // Calculate second expression: 42 (literal)
    let result2 = expressions[1].eval(&evaluator, &tuple).expect("Calculation should succeed");
    println!("✓ Expression 2 (42) calculation result: {:?}", result2);
    assert_eq!(result2, Value::Int64(42), "Literal 42 should equal 42");
    
    println!("\n✅ Core conversion flow test completed!");
    println!("🎯 Verification result: parser → SelectStmt → StreamSqlConverter → ScalarExpr → Calculation Results");
    println!("   The entire flow is completely correct!");
}