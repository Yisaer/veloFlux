use crate::expr::custom_func::CustomFunc;
use crate::expr::func::{BinaryFunc, EvalError, UnaryFunc};
use crate::model::{Collection, Tuple};
use datatypes::{ConcreteDatatype, StructField, StructType, StructValue, Value};
use std::sync::Arc;

/// A scalar expression, which can be evaluated to a value.
#[derive(Clone)]
pub enum ScalarExpr {
    /// A column reference
    Column(ColumnRef),
    /// Wildcard reference (`*` or `source.*`)
    Wildcard {
        /// Optional source/table qualifier
        source_name: Option<String>,
    },
    /// A literal value with its type
    Literal(Value, ConcreteDatatype),
    /// A unary function call
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    /// A binary function call
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    /// A field access expression (e.g., a.b where a is a struct)
    FieldAccess {
        /// The expression that evaluates to a struct value
        expr: Box<ScalarExpr>,
        /// The name of the field to access
        field_name: String,
    },
    /// A list indexing expression (e.g., a[0] where a is a list)
    ListIndex {
        /// The expression that evaluates to a list value
        expr: Box<ScalarExpr>,
        /// The index expression (can be any scalar expression)
        index_expr: Box<ScalarExpr>,
    },
    /// A call to a custom user-implemented function
    CallFunc {
        /// The custom function implementation
        func: Arc<dyn CustomFunc>,
        /// The arguments to the function
        args: Vec<ScalarExpr>,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub enum ColumnRef {
    ByName {
        column_name: String,
    },
    ByIndex {
        source_name: String,
        column_index: usize,
    },
}

impl ScalarExpr {
    /// Evaluate this expression against a collection row by row.
    pub fn eval_with_collection(
        &self,
        collection: &dyn Collection,
    ) -> Result<Vec<Value>, EvalError> {
        let mut results = Vec::with_capacity(collection.num_rows());
        for row in collection.rows() {
            results.push(self.eval_with_tuple(row)?);
        }
        Ok(results)
    }

    /// Evaluate this expression against a single tuple (row).
    pub fn eval_with_tuple(&self, tuple: &Tuple) -> Result<Value, EvalError> {
        match self {
            ScalarExpr::Column(column_ref) => match column_ref {
                ColumnRef::ByIndex {
                    source_name,
                    column_index,
                } => tuple
                    .value_by_index(source_name, *column_index)
                    .cloned()
                    .ok_or_else(|| EvalError::ColumnNotFound {
                        source: source_name.clone(),
                        column: format!("#{}", column_index),
                    }),
                ColumnRef::ByName { column_name } => tuple
                    .value_by_name("", column_name)
                    .cloned()
                    .ok_or_else(|| EvalError::ColumnNotFound {
                        source: "".to_string(),
                        column: column_name.clone(),
                    }),
            },
            ScalarExpr::Wildcard { source_name } => {
                let selected: Vec<_> = tuple
                    .entries()
                    .into_iter()
                    .filter(|((src, _), _)| match source_name.as_ref() {
                        Some(prefix) => src == prefix,
                        None => true,
                    })
                    .collect();

                if selected.is_empty() && source_name.is_some() {
                    return Err(EvalError::ColumnNotFound {
                        source: source_name.clone().unwrap_or_default(),
                        column: "*".to_string(),
                    });
                }

                let mut fields = Vec::with_capacity(selected.len());
                let mut values = Vec::with_capacity(selected.len());
                for ((_, column_name), value) in selected {
                    fields.push(StructField::new(
                        column_name.to_string(),
                        value.datatype(),
                        true,
                    ));
                    values.push(value.clone());
                }

                Ok(Value::Struct(StructValue::new(
                    values,
                    StructType::new(Arc::new(fields)),
                )))
            }
            ScalarExpr::Literal(val, _) => Ok(val.clone()),
            ScalarExpr::CallUnary { func, expr } => {
                let arg = expr.eval_with_tuple(tuple)?;
                func.eval_unary(arg)
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                let left = expr1.eval_with_tuple(tuple)?;
                let right = expr2.eval_with_tuple(tuple)?;
                func.eval_binary(left, right)
            }
            ScalarExpr::FieldAccess { expr, field_name } => {
                let struct_val = expr.eval_with_tuple(tuple)?;
                if let Value::Struct(struct_val) = struct_val {
                    struct_val.get_field(field_name).cloned().ok_or_else(|| {
                        EvalError::FieldNotFound {
                            field_name: field_name.clone(),
                            struct_type: format!("{:?}", struct_val.fields()),
                        }
                    })
                } else {
                    Err(EvalError::TypeMismatch {
                        expected: "Struct".to_string(),
                        actual: format!("{:?}", struct_val),
                    })
                }
            }
            ScalarExpr::ListIndex { expr, index_expr } => {
                let list_val = expr.eval_with_tuple(tuple)?;
                let index_val = index_expr.eval_with_tuple(tuple)?;

                match (list_val, index_val) {
                    (Value::List(list), Value::Int64(index)) => {
                        if index < 0 || (index as usize) >= list.len() {
                            return Err(EvalError::ListIndexOutOfBounds {
                                index: index as usize,
                                list_length: list.len(),
                            });
                        }
                        list.get(index as usize)
                            .cloned()
                            .ok_or(EvalError::ListIndexOutOfBounds {
                                index: index as usize,
                                list_length: list.len(),
                            })
                    }
                    (Value::List(_), other) => Err(EvalError::InvalidIndexType {
                        expected: "Int64".to_string(),
                        actual: format!("{:?}", other),
                    }),
                    (other, _) => Err(EvalError::TypeMismatch {
                        expected: "List".to_string(),
                        actual: format!("{:?}", other),
                    }),
                }
            }
            ScalarExpr::CallFunc { func, args } => {
                let mut row_args = Vec::with_capacity(args.len());
                for arg_expr in args {
                    row_args.push(arg_expr.eval_with_tuple(tuple)?);
                }
                func.validate_row(&row_args)?;
                func.eval_row(&row_args)
            }
        }
    }

    /// Create a column reference expression by column name only (no source qualifier)
    pub fn column_with_column_name(column_name: impl Into<String>) -> Self {
        ScalarExpr::Column(ColumnRef::ByName {
            column_name: column_name.into(),
        })
    }

    pub fn column_with_index(
        source_name: impl Into<String>,
        column_name: impl Into<String>,
        column_index: Option<usize>,
    ) -> Result<Self, String> {
        match column_index {
            Some(idx) => Ok(ScalarExpr::Column(ColumnRef::ByIndex {
                source_name: source_name.into(),
                column_index: idx,
            })),
            None => {
                Err(format!(
                    "column_with_index called with None index for column '{}' - use column_with_column_name for name-based references",
                    column_name.into()
                ))
            }
        }
    }

    /// Create a literal expression
    pub fn literal(value: Value, typ: ConcreteDatatype) -> Self {
        ScalarExpr::Literal(value, typ)
    }

    /// Create an unqualified wildcard expression (`*`)
    pub fn wildcard_all() -> Self {
        ScalarExpr::Wildcard { source_name: None }
    }

    /// Create a qualified wildcard expression (`source.*`)
    pub fn wildcard_for(source_name: impl Into<String>) -> Self {
        ScalarExpr::Wildcard {
            source_name: Some(source_name.into()),
        }
    }

    /// Create a unary function call expression
    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    /// Create a binary function call expression
    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    /// Create a custom function call expression
    pub fn call_func(func: Arc<dyn CustomFunc>, args: Vec<ScalarExpr>) -> Self {
        ScalarExpr::CallFunc { func, args }
    }

    /// Create a field access expression (e.g., a.b where a is a struct)
    pub fn field_access(expr: ScalarExpr, field_name: impl Into<String>) -> Self {
        ScalarExpr::FieldAccess {
            expr: Box::new(expr),
            field_name: field_name.into(),
        }
    }

    /// Create a list indexing expression (e.g., a[0] where a is a list)
    pub fn list_index(expr: ScalarExpr, index_expr: ScalarExpr) -> Self {
        ScalarExpr::ListIndex {
            expr: Box::new(expr),
            index_expr: Box::new(index_expr),
        }
    }

    /// Check if this expression is a column reference
    pub fn is_column(&self) -> bool {
        matches!(self, ScalarExpr::Column { .. })
    }

    /// Get the source and column names if this is a column reference
    pub fn as_column(&self) -> Option<(&str, &str)> {
        match self {
            ScalarExpr::Column(ColumnRef::ByName { column_name }) => Some(("", column_name)),
            ScalarExpr::Column(ColumnRef::ByIndex { source_name, .. }) => Some((source_name, "")),
            _ => None,
        }
    }

    /// Check if this expression is a literal
    pub fn is_literal(&self) -> bool {
        matches!(self, ScalarExpr::Literal(..))
    }

    /// Get the literal value if this is a literal expression
    pub fn as_literal(&self) -> Option<&Value> {
        if let ScalarExpr::Literal(val, _) = self {
            Some(val)
        } else {
            None
        }
    }
}

impl std::fmt::Debug for ScalarExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarExpr::Column(ColumnRef::ByName { column_name }) => {
                write!(f, "Column({})", column_name)
            }
            ScalarExpr::Column(ColumnRef::ByIndex {
                source_name,
                column_index,
            }) => write!(f, "Column({}@{})", source_name, column_index),
            ScalarExpr::Wildcard { source_name } => {
                write!(f, "Wildcard({:?})", source_name)
            }
            ScalarExpr::Literal(val, typ) => write!(f, "Literal({:?}, {:?})", val, typ),
            ScalarExpr::CallUnary { func, expr } => write!(f, "CallUnary({:?}, {:?})", func, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                write!(f, "CallBinary({:?}, {:?}, {:?})", func, expr1, expr2)
            }
            ScalarExpr::FieldAccess { expr, field_name } => {
                write!(f, "FieldAccess({:?}, {})", expr, field_name)
            }
            ScalarExpr::ListIndex { expr, index_expr } => {
                write!(f, "ListIndex({:?}, {:?})", expr, index_expr)
            }
            ScalarExpr::CallFunc { func, args } => {
                write!(f, "CallFunc({}, {:?})", func.name(), args)
            }
        }
    }
}

impl PartialEq for ScalarExpr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                ScalarExpr::Column(ColumnRef::ByName { column_name: ca }),
                ScalarExpr::Column(ColumnRef::ByName { column_name: cb }),
            ) => ca == cb,
            (
                ScalarExpr::Column(ColumnRef::ByIndex {
                    source_name: sa,
                    column_index: ia,
                }),
                ScalarExpr::Column(ColumnRef::ByIndex {
                    source_name: sb,
                    column_index: ib,
                }),
            ) => sa == sb && ia == ib,
            (
                ScalarExpr::Wildcard { source_name: sa },
                ScalarExpr::Wildcard { source_name: sb },
            ) => sa == sb,
            (ScalarExpr::Literal(va, ta), ScalarExpr::Literal(vb, tb)) => va == vb && ta == tb,
            (
                ScalarExpr::CallUnary { func: fa, expr: ea },
                ScalarExpr::CallUnary { func: fb, expr: eb },
            ) => fa == fb && ea == eb,
            (
                ScalarExpr::CallBinary {
                    func: fa,
                    expr1: e1a,
                    expr2: e2a,
                },
                ScalarExpr::CallBinary {
                    func: fb,
                    expr1: e1b,
                    expr2: e2b,
                },
            ) => fa == fb && e1a == e1b && e2a == e2b,
            (
                ScalarExpr::FieldAccess {
                    expr: ea,
                    field_name: na,
                },
                ScalarExpr::FieldAccess {
                    expr: eb,
                    field_name: nb,
                },
            ) => ea == eb && na == nb,
            (
                ScalarExpr::ListIndex {
                    expr: ea,
                    index_expr: ia,
                },
                ScalarExpr::ListIndex {
                    expr: eb,
                    index_expr: ib,
                },
            ) => ea == eb && ia == ib,
            (
                ScalarExpr::CallFunc { func: fa, args: aa },
                ScalarExpr::CallFunc { func: fb, args: ab },
            ) => fa.name() == fb.name() && aa == ab,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::Int64Type;

    #[test]
    fn scalar_literal_creation() {
        let lit = ScalarExpr::literal(Value::Int64(42), ConcreteDatatype::Int64(Int64Type));
        assert!(lit.is_literal());
        assert_eq!(lit.as_literal(), Some(&Value::Int64(42)));
    }

    #[test]
    fn scalar_column_by_name() {
        let col = ScalarExpr::column_with_column_name("my_col");
        assert!(col.is_column());
        assert_eq!(col.as_column(), Some(("", "my_col")));
    }

    #[test]
    fn scalar_column_by_index() {
        let col = ScalarExpr::column_with_index("src", "col", Some(0)).unwrap();
        assert!(col.is_column());
        assert_eq!(col.as_column(), Some(("src", "")));
    }

    #[test]
    fn scalar_column_by_index_error() {
        let result = ScalarExpr::column_with_index("src", "col", None);
        assert!(result.is_err());
    }

    #[test]
    fn scalar_wildcard_all() {
        let wc = ScalarExpr::wildcard_all();
        assert!(!wc.is_column());
        assert!(!wc.is_literal());
    }

    #[test]
    fn scalar_wildcard_for() {
        let wc = ScalarExpr::wildcard_for("source");
        match wc {
            ScalarExpr::Wildcard { source_name } => {
                assert_eq!(source_name, Some("source".to_string()))
            }
            _ => panic!("expected wildcard"),
        }
    }

    #[test]
    fn scalar_call_unary() {
        let lit = ScalarExpr::literal(Value::Int64(42), ConcreteDatatype::Int64(Int64Type));
        let neg = lit.call_unary(UnaryFunc::Neg);
        assert!(!neg.is_literal());
        match neg {
            ScalarExpr::CallUnary { func, .. } => assert_eq!(func, UnaryFunc::Neg),
            _ => panic!("expected CallUnary"),
        }
    }

    #[test]
    fn scalar_call_binary() {
        let lit1 = ScalarExpr::literal(Value::Int64(10), ConcreteDatatype::Int64(Int64Type));
        let lit2 = ScalarExpr::literal(Value::Int64(20), ConcreteDatatype::Int64(Int64Type));
        let add = lit1.call_binary(lit2, BinaryFunc::Add);
        match add {
            ScalarExpr::CallBinary { func, .. } => assert_eq!(func, BinaryFunc::Add),
            _ => panic!("expected CallBinary"),
        }
    }

    #[test]
    fn scalar_field_access() {
        let lit = ScalarExpr::literal(Value::Null, ConcreteDatatype::Int64(Int64Type));
        let fa = ScalarExpr::field_access(lit, "field_name");
        match fa {
            ScalarExpr::FieldAccess { field_name, .. } => assert_eq!(field_name, "field_name"),
            _ => panic!("expected FieldAccess"),
        }
    }

    #[test]
    fn scalar_list_index() {
        let list_lit = ScalarExpr::literal(Value::Null, ConcreteDatatype::Int64(Int64Type));
        let idx_lit = ScalarExpr::literal(Value::Int64(0), ConcreteDatatype::Int64(Int64Type));
        let li = ScalarExpr::list_index(list_lit, idx_lit);
        match li {
            ScalarExpr::ListIndex { .. } => {}
            _ => panic!("expected ListIndex"),
        }
    }

    #[test]
    fn scalar_debug_format() {
        let lit = ScalarExpr::literal(Value::Int64(42), ConcreteDatatype::Int64(Int64Type));
        let debug_str = format!("{:?}", lit);
        assert!(debug_str.contains("Literal"));

        let col = ScalarExpr::column_with_column_name("test");
        let debug_str = format!("{:?}", col);
        assert!(debug_str.contains("Column"));

        let wc = ScalarExpr::wildcard_all();
        let debug_str = format!("{:?}", wc);
        assert!(debug_str.contains("Wildcard"));
    }

    #[test]
    fn scalar_partial_eq_literals() {
        let lit1 = ScalarExpr::literal(Value::Int64(42), ConcreteDatatype::Int64(Int64Type));
        let lit2 = ScalarExpr::literal(Value::Int64(42), ConcreteDatatype::Int64(Int64Type));
        let lit3 = ScalarExpr::literal(Value::Int64(43), ConcreteDatatype::Int64(Int64Type));
        assert_eq!(lit1, lit2);
        assert_ne!(lit1, lit3);
    }

    #[test]
    fn scalar_partial_eq_columns() {
        let col1 = ScalarExpr::column_with_column_name("a");
        let col2 = ScalarExpr::column_with_column_name("a");
        let col3 = ScalarExpr::column_with_column_name("b");
        assert_eq!(col1, col2);
        assert_ne!(col1, col3);
    }

    #[test]
    fn scalar_partial_eq_wildcards() {
        let wc1 = ScalarExpr::wildcard_all();
        let wc2 = ScalarExpr::wildcard_all();
        let wc3 = ScalarExpr::wildcard_for("src");
        assert_eq!(wc1, wc2);
        assert_ne!(wc1, wc3);
    }

    #[test]
    fn scalar_partial_eq_call_unary() {
        let lit = ScalarExpr::literal(Value::Int64(1), ConcreteDatatype::Int64(Int64Type));
        let neg1 = lit.clone().call_unary(UnaryFunc::Neg);
        let neg2 = lit.clone().call_unary(UnaryFunc::Neg);
        let not = lit.call_unary(UnaryFunc::Not);
        assert_eq!(neg1, neg2);
        assert_ne!(neg1, not);
    }

    #[test]
    fn scalar_partial_eq_call_binary() {
        let lit1 = ScalarExpr::literal(Value::Int64(1), ConcreteDatatype::Int64(Int64Type));
        let lit2 = ScalarExpr::literal(Value::Int64(2), ConcreteDatatype::Int64(Int64Type));
        let add1 = lit1.clone().call_binary(lit2.clone(), BinaryFunc::Add);
        let add2 = lit1.clone().call_binary(lit2.clone(), BinaryFunc::Add);
        let sub = lit1.call_binary(lit2, BinaryFunc::Sub);
        assert_eq!(add1, add2);
        assert_ne!(add1, sub);
    }

    #[test]
    fn scalar_partial_eq_field_access() {
        let lit = ScalarExpr::literal(Value::Null, ConcreteDatatype::Int64(Int64Type));
        let fa1 = ScalarExpr::field_access(lit.clone(), "a");
        let fa2 = ScalarExpr::field_access(lit.clone(), "a");
        let fa3 = ScalarExpr::field_access(lit, "b");
        assert_eq!(fa1, fa2);
        assert_ne!(fa1, fa3);
    }

    #[test]
    fn scalar_partial_eq_list_index() {
        let list = ScalarExpr::literal(Value::Null, ConcreteDatatype::Int64(Int64Type));
        let idx1 = ScalarExpr::literal(Value::Int64(0), ConcreteDatatype::Int64(Int64Type));
        let idx2 = ScalarExpr::literal(Value::Int64(1), ConcreteDatatype::Int64(Int64Type));
        let li1 = ScalarExpr::list_index(list.clone(), idx1.clone());
        let li2 = ScalarExpr::list_index(list.clone(), idx1);
        let li3 = ScalarExpr::list_index(list, idx2);
        assert_eq!(li1, li2);
        assert_ne!(li1, li3);
    }

    #[test]
    fn scalar_partial_eq_different_types() {
        let lit = ScalarExpr::literal(Value::Int64(42), ConcreteDatatype::Int64(Int64Type));
        let col = ScalarExpr::column_with_column_name("a");
        assert_ne!(lit, col);
    }

    #[test]
    fn scalar_is_not_column_or_literal() {
        let wc = ScalarExpr::wildcard_all();
        assert!(!wc.is_column());
        assert!(!wc.is_literal());
        assert!(wc.as_column().is_none());
        assert!(wc.as_literal().is_none());
    }
}
