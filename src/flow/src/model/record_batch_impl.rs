use crate::model::{Collection, CollectionError, Column};
use super::RecordBatch;
use datatypes::Value;
use crate::planner::physical::PhysicalProjectField;
use crate::expr::datafusion_func::DataFusionEvaluator;
use crate::expr::ScalarExpr;

impl Collection for RecordBatch {
    fn num_rows(&self) -> usize {
        self.num_rows()
    }
    
    fn num_columns(&self) -> usize {
        self.num_columns()
    }
    
    fn column(&self, index: usize) -> Option<&Column> {
        self.column(index)
    }
    
    fn column_by_name(&self, source_name: &str, name: &str) -> Option<&Column> {
        self.column_by_name(source_name, name)
    }
    
    fn slice(&self, start: usize, end: usize) -> Result<Box<dyn Collection>, CollectionError> {
        if start > end || end > self.num_rows() {
            return Err(CollectionError::InvalidSliceRange {
                start,
                end,
                len: self.num_rows(),
            });
        }
        
        let mut new_columns = Vec::with_capacity(self.columns().len());
        
        for column in self.columns() {
            let new_data = column.values()[start..end].to_vec();
            new_columns.push(Column::new(
                column.name.clone(),
                column.source_name.clone(),
                new_data
            ));
        }
        
        let new_batch = RecordBatch::new(new_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn take(&self, indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError> {
        if indices.is_empty() {
            return Ok(Box::new(RecordBatch::empty()));
        }
        
        // Validate all indices
        for &idx in indices {
            if idx >= self.num_rows() {
                return Err(CollectionError::IndexOutOfBounds {
                    index: idx,
                    len: self.num_rows(),
                });
            }
        }
        
        let mut new_columns = Vec::with_capacity(self.columns().len());
        
        for column in self.columns() {
            let mut new_data = Vec::with_capacity(indices.len());
            for &idx in indices {
                if let Some(value) = column.get(idx) {
                    new_data.push(value.clone());
                } else {
                    new_data.push(Value::Null);
                }
            }
            new_columns.push(Column::new(
                column.name.clone(),
                column.source_name.clone(),
                new_data
            ));
        }
        
        let new_batch = RecordBatch::new(new_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn columns(&self) -> &[Column] {
        self.columns()
    }
    
    fn apply_projection(&self, fields: &[PhysicalProjectField]) -> Result<Box<dyn Collection>, CollectionError> {
        let num_rows = self.num_rows();
        let mut projected_columns = Vec::with_capacity(fields.len());
        
        // Create DataFusion evaluator for expression evaluation
        let evaluator = DataFusionEvaluator::new();
        
        for field in fields {
            // Evaluate the compiled expression using vectorized evaluation
            // If evaluation fails, return the error immediately
            let evaluated_values = field.compiled_expr.eval_vectorized(&evaluator, self)
                .map_err(|eval_error| {
                    CollectionError::Other(format!(
                        "Failed to evaluate expression for field '{}': {}",
                        field.field_name, eval_error
                    ))
                })?;
            
            // Ensure we have the right number of values
            if evaluated_values.len() != num_rows {
                return Err(CollectionError::Other(format!(
                    "Expression evaluation for field '{}' returned {} values, expected {}",
                    field.field_name, evaluated_values.len(), num_rows
                )));
            }
            
            // Create new column with evaluated values
            let column = Column::new(
                field.field_name.clone(),
                "".to_string(),
                evaluated_values,
            );
            projected_columns.push(column);
        }
        
        // Create new RecordBatch with projected columns
        // This leverages the existing columnar structure efficiently
        let new_batch = RecordBatch::new(projected_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn clone_box(&self) -> Box<dyn Collection> {
        Box::new(self.clone())
    }
    
    fn apply_filter(&self, filter_expr: &ScalarExpr) -> Result<Box<dyn Collection>, CollectionError> {
        // Create DataFusion evaluator for expression evaluation
        let evaluator = DataFusionEvaluator::new();
        
        // Evaluate the filter expression to get boolean results for each row
        let filter_results = filter_expr.eval_vectorized(&evaluator, self)
            .map_err(|eval_error| {
                CollectionError::FilterError { 
                    message: format!(
                        "Failed to evaluate filter expression: {}",
                        eval_error
                    )
                }
            })?;
        
        // Collect indices of rows that satisfy the filter condition (true values)
        let mut selected_indices = Vec::new();
        for (i, result) in filter_results.iter().enumerate() {
            if let Value::Bool(true) = result {
                selected_indices.push(i);
            } else if let Value::Bool(false) = result {
                // This is a valid false value, continue
                continue;
            } else {
                // Non-boolean result from filter expression
                return Err(CollectionError::FilterError { 
                    message: format!(
                        "Filter expression must return boolean values, got {:?} at row {}",
                        result, i
                    )
                });
            }
        }
        
        // If no rows satisfy the filter, return batch with same columns but no rows
        if selected_indices.is_empty() {
            let mut empty_columns = Vec::with_capacity(self.columns().len());
            for column in self.columns() {
                empty_columns.push(Column::new(
                    column.name.clone(),
                    column.source_name.clone(),
                    Vec::new()
                ));
            }
            let empty_batch = RecordBatch::new(empty_columns)?;
            return Ok(Box::new(empty_batch));
        }
        
        // Create new columns with only the selected rows
        let mut new_columns = Vec::with_capacity(self.columns().len());
        
        for column in self.columns() {
            let mut new_data = Vec::with_capacity(selected_indices.len());
            for &idx in &selected_indices {
                if let Some(value) = column.get(idx) {
                    new_data.push(value.clone());
                } else {
                    new_data.push(Value::Null);
                }
            }
            new_columns.push(Column::new(
                column.name.clone(),
                column.source_name.clone(),
                new_data
            ));
        }
        
        // Create new RecordBatch with filtered columns
        let new_batch = RecordBatch::new(new_columns)?;
        Ok(Box::new(new_batch))
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{Collection, Column, RecordBatch};
    use crate::planner::physical::PhysicalProjectField;
    use sqlparser::ast::{Expr, Value as SqlValue};
    use datatypes::Value;
    #[test]
    fn test_recordbatch_apply_projection_expression_calculation() {
        let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
        let col_b_values = vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)];
        let col_c_values = vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)];
        
        // 使用空字符串作为 source_name，与 convert_identifier_to_column 中的用法一致
        let column_a = Column::new("a".to_string(), "".to_string(), col_a_values);
        let column_b = Column::new("b".to_string(), "".to_string(), col_b_values);
        let column_c = Column::new("c".to_string(), "".to_string(), col_c_values);
        
        let batch = RecordBatch::new(vec![column_a, column_b, column_c]).expect("Failed to create RecordBatch");

        let project_field_a_plus_1 = PhysicalProjectField::from_logical(
            "a_plus_1".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("a"))),
                op: sqlparser::ast::BinaryOperator::Plus,
                right: Box::new(Expr::Value(SqlValue::Number("1".to_string(), false))),
            },
        ).expect("Failed to compile a+1 expression");
        
        let project_field_b_plus_2 = PhysicalProjectField::from_logical(
            "b_plus_2".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("b"))),
                op: sqlparser::ast::BinaryOperator::Plus,
                right: Box::new(Expr::Value(SqlValue::Number("2".to_string(), false))),
            },
        ).expect("Failed to compile b+2 expression");
        

        
        let fields = vec![project_field_a_plus_1, project_field_b_plus_2];

        let result = batch.apply_projection(&fields);
        if let Err(ref e) = result {
            println!("apply_projection failed: {:?}", e);
        }
        assert!(result.is_ok(), "apply_projection should succeed");
        
        let projected_collection = result.unwrap();

        assert_eq!(projected_collection.num_rows(), 3, "Should maintain row count");
        assert_eq!(projected_collection.num_columns(), 2, "Should have 2 projected columns");

        let col1 = projected_collection.column(0).expect("Should have first column");
        let col2 = projected_collection.column(1).expect("Should have second column");

        assert_eq!(col1.name, "a_plus_1");
        assert_eq!(col2.name, "b_plus_2");

        assert_eq!(col1.values(),vec![Value::Int64(11), Value::Int64(21), Value::Int64(31)],"" );
        assert_eq!(col2.values(),vec![Value::Int64(102), Value::Int64(202), Value::Int64(302)],"" );
    }
    
    #[test]
    fn test_recordbatch_apply_filter_basic() {
        // Create test data: a=10,20,30, b=100,200,300
        let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
        let col_b_values = vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)];
        
        let column_a = Column::new("a".to_string(), "".to_string(), col_a_values);
        let column_b = Column::new("b".to_string(), "".to_string(), col_b_values);
        
        let batch = RecordBatch::new(vec![column_a, column_b]).expect("Failed to create RecordBatch");
        
        // Create filter expression: a > 15
        use crate::expr::{ScalarExpr, func::BinaryFunc};
        let filter_expr = ScalarExpr::CallBinary {
            func: BinaryFunc::Gt,
            expr1: Box::new(ScalarExpr::column("", "a")),
            expr2: Box::new(ScalarExpr::literal(Value::Int64(15), datatypes::ConcreteDatatype::Int64(datatypes::Int64Type))),
        };
        
        // Apply filter
        let result = batch.apply_filter(&filter_expr);
        assert!(result.is_ok(), "apply_filter should succeed");
        
        let filtered_collection = result.unwrap();
        
        // Should have 2 rows (a=20, a=30) and 2 columns
        assert_eq!(filtered_collection.num_rows(), 2, "Should have 2 filtered rows");
        assert_eq!(filtered_collection.num_columns(), 2, "Should have 2 columns");
        
        // Check filtered values
        let col_a = filtered_collection.column(0).expect("Should have column a");
        let col_b = filtered_collection.column(1).expect("Should have column b");
        
        assert_eq!(col_a.values(), &vec![Value::Int64(20), Value::Int64(30)]);
        assert_eq!(col_b.values(), &vec![Value::Int64(200), Value::Int64(300)]);
    }
    
    #[test]
    fn test_recordbatch_apply_filter_no_match() {
        // Create test data
        let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
        let column_a = Column::new("a".to_string(), "".to_string(), col_a_values);
        let batch = RecordBatch::new(vec![column_a]).expect("Failed to create RecordBatch");
        
        // Create filter expression: a > 100 (no matches)
        use crate::expr::{ScalarExpr, func::BinaryFunc};
        let filter_expr = ScalarExpr::CallBinary {
            func: BinaryFunc::Gt,
            expr1: Box::new(ScalarExpr::column("", "a")),
            expr2: Box::new(ScalarExpr::literal(Value::Int64(100), datatypes::ConcreteDatatype::Int64(datatypes::Int64Type))),
        };
        
        // Apply filter
        let result = batch.apply_filter(&filter_expr);
        assert!(result.is_ok(), "apply_filter should succeed even with no matches");
        
        let filtered_collection = result.unwrap();
        assert_eq!(filtered_collection.num_rows(), 0, "Should have 0 rows when no matches");
        assert_eq!(filtered_collection.num_columns(), 1, "Should still have 1 column");
    }
    
    #[test]
    fn test_recordbatch_apply_filter_all_match() {
        // Create test data
        let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
        let column_a = Column::new("a".to_string(), "".to_string(), col_a_values);
        let batch = RecordBatch::new(vec![column_a]).expect("Failed to create RecordBatch");
        
        // Create filter expression: a > 5 (all matches)
        use crate::expr::{ScalarExpr, func::BinaryFunc};
        let filter_expr = ScalarExpr::CallBinary {
            func: BinaryFunc::Gt,
            expr1: Box::new(ScalarExpr::column("", "a")),
            expr2: Box::new(ScalarExpr::literal(Value::Int64(5), datatypes::ConcreteDatatype::Int64(datatypes::Int64Type))),
        };
        
        // Apply filter
        let result = batch.apply_filter(&filter_expr);
        assert!(result.is_ok(), "apply_filter should succeed");
        
        let filtered_collection = result.unwrap();
        assert_eq!(filtered_collection.num_rows(), 3, "Should have all 3 rows");
        assert_eq!(filtered_collection.num_columns(), 1, "Should have 1 column");
        
        // Check that values are unchanged
        let col_a = filtered_collection.column(0).expect("Should have column a");
        assert_eq!(col_a.values(), &vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)]);
    }
}