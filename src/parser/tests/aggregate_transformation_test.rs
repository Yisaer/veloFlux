//! Tests for aggregate function transformation
//! Tests the complete transformation: SQL → SelectStmt with aggregate mappings

use parser::parse_sql;
use sqlparser::ast::Expr;

// coverage-covers: parser.function.aggregate_rewrite
#[test]
fn test_basic_aggregate_transformation() {
    println!("\n=== Testing Basic Aggregate Transformation ===");
    println!("Goal: Verify parse_sql() returns SelectStmt with aggregate mappings");

    // Test: SELECT sum(a) FROM t
    let sql = "SELECT sum(a) FROM t";
    println!("Input SQL: {}", sql);

    // Step 1: Parse SQL - aggregate transformation happens automatically in parse()
    let select_stmt = parse_sql(sql).expect("Should parse successfully");

    // Step 2: Verify basic structure
    assert_eq!(
        select_stmt.select_fields.len(),
        1,
        "Should have one select field"
    );

    // Step 3: Verify aggregate mappings are present in SelectStmt
    println!("\nVerifying aggregate mappings in SelectStmt:");
    assert_eq!(
        select_stmt.aggregate_mappings.len(),
        1,
        "Should have one aggregate mapping"
    );
    assert!(
        select_stmt.aggregate_mappings.contains_key("col_1"),
        "Should have col_1 mapping"
    );

    // Step 4: Verify the mapping content: col_1 -> sum(a)
    let col_1_expr = select_stmt
        .aggregate_mappings
        .get("col_1")
        .expect("Should have col_1 mapping");
    match col_1_expr {
        Expr::Function(func) => {
            assert_eq!(func.name.to_string(), "sum", "Should be sum function");
            assert_eq!(func.args.len(), 1, "Should have one argument");
            println!("  [OK] col_1 maps to sum(a) expression");
        }
        _ => panic!("Expected function expression for sum(a)"),
    }

    // Step 5: Verify the transformed expression in select fields
    println!("\nVerifying transformed expression in select fields:");
    match &select_stmt.select_fields[0].expr {
        Expr::Identifier(ident) => {
            assert_eq!(ident.value, "col_1", "Should be replaced with col_1");
            println!("  [OK] Transformed expression is col_1");
        }
        _ => panic!("Expected identifier expression after transformation"),
    }

    println!("\n✓ Basic aggregate transformation successful");
    println!("  ✓ parse_sql() automatically transformed sum(a) to col_1");
    println!("  ✓ SelectStmt contains aggregate mappings");
    println!("  ✓ Mapping: col_1 -> sum(a)");
}
