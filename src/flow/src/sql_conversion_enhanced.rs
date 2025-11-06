use datatypes::Schema;

use crate::expr::ScalarExpr;
use crate::sql_conversion::{convert_expr_to_scalar, ConversionError};

/// Enhanced SQL conversion using StreamDialect parser
/// This provides a direct interface to parse SQL and convert to ScalarExpr
pub struct StreamSqlConverter;

impl StreamSqlConverter {
    /// Create a new StreamSqlConverter
    pub fn new() -> Self {
        Self
    }

    /// Parse SQL using StreamDialect and convert expressions to ScalarExpr
    /// Returns a vector of converted expressions and their optional aliases
    pub fn parse_and_convert(
        &self, 
        sql: &str, 
        schema: &Schema
    ) -> Result<Vec<(ScalarExpr, Option<String>)>, ConversionError> {
        // Use the StreamDialect parser to parse SQL
        let select_stmt = parser::parse_sql(sql)
            .map_err(|e| ConversionError::UnsupportedExpression(format!("Parse error: {}", e)))?;

        let mut results = Vec::new();

        // Convert each select field to ScalarExpr
        for field in select_stmt.select_fields {
            let scalar_expr = convert_expr_to_scalar(&field.expr, schema)?;
            results.push((scalar_expr, field.alias));
        }

        Ok(results)
    }

    /// Parse SQL and convert to ScalarExpr without aliases
    /// This is a convenience method that matches the existing API
    pub fn parse_sql_to_scalar(
        &self,
        sql: &str,
        schema: &Schema
    ) -> Result<Vec<ScalarExpr>, ConversionError> {
        let results = self.parse_and_convert(sql, schema)?;
        Ok(results.into_iter().map(|(expr, _)| expr).collect())
    }
}

impl Default for StreamSqlConverter {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to parse SQL using StreamDialect and convert to ScalarExpr
pub fn parse_sql_to_scalar_expr(
    sql: &str,
    schema: &Schema
) -> Result<Vec<ScalarExpr>, ConversionError> {
    let converter = StreamSqlConverter::new();
    converter.parse_sql_to_scalar(sql, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ])
    }

    #[test]
    fn test_parse_simple_expression() {
        let schema = create_test_schema();
        let result = parse_sql_to_scalar_expr("SELECT a + b", &schema);
        
        assert!(result.is_ok());
        let expressions = result.unwrap();
        assert_eq!(expressions.len(), 1);
    }

    #[test]
    fn test_parse_multiple_expressions() {
        let schema = create_test_schema();
        let result = parse_sql_to_scalar_expr("SELECT a, b + c, 42", &schema);
        
        assert!(result.is_ok());
        let expressions = result.unwrap();
        assert_eq!(expressions.len(), 3);
    }

    #[test]
    fn test_parse_with_alias() {
        let schema = create_test_schema();
        let converter = StreamSqlConverter::new();
        let result = converter.parse_and_convert("SELECT a + b AS total", &schema);
        
        assert!(result.is_ok());
        let expressions = result.unwrap();
        assert_eq!(expressions.len(), 1);
        assert_eq!(expressions[0].1, Some("total".to_string()));
    }

    #[test]
    fn test_parse_invalid_sql() {
        let schema = create_test_schema();
        let result = parse_sql_to_scalar_expr("INVALID SQL", &schema);
        
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_non_select() {
        let schema = create_test_schema();
        let result = parse_sql_to_scalar_expr("INSERT INTO table VALUES (1)", &schema);
        
        assert!(result.is_err());
    }
}