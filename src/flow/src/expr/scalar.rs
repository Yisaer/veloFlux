use datatypes::{ConcreteDatatype, Value};

use crate::expr::func::{BinaryFunc, EvalError, UnaryFunc};
use crate::expr::evaluator::DataFusionEvaluator;
use crate::tuple::Tuple;
use std::sync::Arc;

/// Custom function that can be implemented by users
/// This trait allows users to define their own functions for evaluation
pub trait CustomFunc: Send + Sync + std::fmt::Debug {
    /// Validate the function arguments before evaluation
    /// 
    /// # Arguments
    /// 
    /// * `args` - A slice of evaluated argument values to validate
    /// 
    /// # Returns
    /// 
    /// Returns Ok(()) if arguments are valid, or an error if validation fails.
    /// This method should check argument count, types, and other constraints.
    fn validate(&self, args: &[Value]) -> Result<(), EvalError>;
    
    /// Evaluate the function with the given arguments
    /// 
    /// # Arguments
    /// 
    /// * `args` - A slice of evaluated argument values
    /// 
    /// # Returns
    /// 
    /// Returns the evaluated result, or an error if evaluation fails.
    /// Note: This method assumes arguments have been validated by validate().
    fn eval(&self, args: &[Value]) -> Result<Value, EvalError>;
    
    /// Get the function name for debugging purposes
    fn name(&self) -> &str;
}

/// A scalar expression, which can be evaluated to a value.
#[derive(Clone)]
pub enum ScalarExpr {
    /// A column reference by index
    Column(usize),
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
    /// A call to a DataFusion scalar function
    CallDf {
        /// The name of the DataFusion function (e.g., "concat", "upper", "lower")
        function_name: String,
        /// The arguments to the function
        args: Vec<ScalarExpr>,
    },
    /// A call to a custom user-implemented function
    CallFunc {
        /// The custom function implementation
        func: Arc<dyn CustomFunc>,
        /// The arguments to the function
        args: Vec<ScalarExpr>,
    },
}

impl ScalarExpr {

    /// Evaluate this expression using DataFusion evaluator when needed.
    /// This method can handle all expression types including CallDf.
    ///
    /// # Arguments
    ///
    /// * `evaluator` - The DataFusion evaluator for handling CallDf expressions
    /// * `tuple` - The tuple containing the row data
    ///
    /// # Returns
    ///
    /// Returns the evaluated value, or an error if evaluation fails.
    pub fn eval(&self, evaluator: &DataFusionEvaluator, tuple: &Tuple) -> Result<Value, EvalError> {
        match self {
            ScalarExpr::Column(index) => {
                tuple.row()
                    .get(*index)
                    .cloned()
                    .ok_or(EvalError::IndexOutOfBounds {
                        index: *index,
                        length: tuple.row().len(),
                    })
            }
            ScalarExpr::Literal(val, _) => Ok(val.clone()),
            ScalarExpr::CallUnary { func, expr } => {
                // Recursively evaluate the argument expression
                let arg = expr.eval(evaluator, tuple)?;
                // Apply the unary function to the evaluated argument
                func.eval_unary(arg)
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                // Recursively evaluate both argument expressions
                let left = expr1.eval(evaluator, tuple)?;
                let right = expr2.eval(evaluator, tuple)?;
                // Apply the binary function to the evaluated arguments
                func.eval_binary(left, right)
            }
            ScalarExpr::CallDf { .. } => {
                // Use DataFusion evaluator for CallDf expressions
                match evaluator.evaluate_expr(self, tuple) {
                    Ok(value) => Ok(value),
                    Err(df_error) => Err(EvalError::DataFusionError { 
                        message: df_error.to_string() 
                    }),
                }
            }
            ScalarExpr::CallFunc { func, args } => {
                // Recursively evaluate all argument expressions
                let mut arg_values = Vec::new();
                for arg in args {
                    arg_values.push(arg.eval(evaluator, tuple)?);
                }
                // Validate arguments before evaluation
                CustomFunc::validate(func.as_ref(), &arg_values)?;
                // Call the custom function with evaluated arguments
                func.eval(&arg_values)
            }
        }
    }

    /// Create a column reference expression
    pub fn column(index: usize) -> Self {
        ScalarExpr::Column(index)
    }

    /// Create a literal expression
    pub fn literal(value: Value, typ: ConcreteDatatype) -> Self {
        ScalarExpr::Literal(value, typ)
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

    /// Create a DataFusion function call expression
    pub fn call_df(function_name: impl Into<String>, args: Vec<ScalarExpr>) -> Self {
        ScalarExpr::CallDf {
            function_name: function_name.into(),
            args,
        }
    }

    /// Create a custom function call expression
    pub fn call_func(func: Arc<dyn CustomFunc>, args: Vec<ScalarExpr>) -> Self {
        ScalarExpr::CallFunc {
            func,
            args,
        }
    }

    /// Check if this expression is a column reference
    pub fn is_column(&self) -> bool {
        matches!(self, ScalarExpr::Column(_))
    }

    /// Get the column index if this is a column reference
    pub fn as_column(&self) -> Option<usize> {
        if let ScalarExpr::Column(index) = self {
            Some(*index)
        } else {
            None
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
            ScalarExpr::Column(index) => write!(f, "Column({})", index),
            ScalarExpr::Literal(val, typ) => write!(f, "Literal({:?}, {:?})", val, typ),
            ScalarExpr::CallUnary { func, expr } => write!(f, "CallUnary({:?}, {:?})", func, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                write!(f, "CallBinary({:?}, {:?}, {:?})", func, expr1, expr2)
            }
            ScalarExpr::CallDf { function_name, args } => {
                write!(f, "CallDf({}, {:?})", function_name, args)
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
            (ScalarExpr::Column(a), ScalarExpr::Column(b)) => a == b,
            (ScalarExpr::Literal(va, ta), ScalarExpr::Literal(vb, tb)) => va == vb && ta == tb,
            (ScalarExpr::CallUnary { func: fa, expr: ea }, ScalarExpr::CallUnary { func: fb, expr: eb }) => {
                fa == fb && ea == eb
            }
            (
                ScalarExpr::CallBinary { func: fa, expr1: e1a, expr2: e2a },
                ScalarExpr::CallBinary { func: fb, expr1: e1b, expr2: e2b },
            ) => fa == fb && e1a == e1b && e2a == e2b,
            (ScalarExpr::CallDf { function_name: na, args: aa }, ScalarExpr::CallDf { function_name: nb, args: ab }) => {
                na == nb && aa == ab
            }
            (ScalarExpr::CallFunc { func: fa, args: aa }, ScalarExpr::CallFunc { func: fb, args: ab }) => {
                // Compare custom functions by name and arguments
                fa.name() == fb.name() && aa == ab
            }
            _ => false,
        }
    }
}
