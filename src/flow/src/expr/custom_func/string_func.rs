use crate::expr::scalar::CustomFunc;
use crate::expr::func::EvalError;
use datatypes::Value;

/// Custom implementation of the concat function
/// This function concatenates exactly 2 String arguments
#[derive(Debug, Clone)]
pub struct ConcatFunc;

impl CustomFunc for ConcatFunc {
    /// Validate the concat function arguments
    /// 
    /// # Arguments
    /// 
    /// * `args` - A slice of argument values to validate
    /// 
    /// # Returns
    /// 
    /// Returns Ok(()) if exactly 2 String arguments are provided, otherwise returns an error.
    fn validate(&self, args: &[Value]) -> Result<(), EvalError> {
        // Check that exactly 2 arguments are provided
        if args.len() != 2 {
            return Err(EvalError::TypeMismatch {
                expected: "2 arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }

        // Check that both arguments are String type
        for (i, arg) in args.iter().enumerate() {
            if !matches!(arg, Value::String(_)) {
                return Err(EvalError::TypeMismatch {
                    expected: "String".to_string(),
                    actual: format!("{:?} at argument {}", arg, i),
                });
            }
        }

        Ok(())
    }

    /// Evaluate the concat function
    /// 
    /// # Arguments
    /// 
    /// * `args` - A slice of exactly 2 String values to concatenate.
    ///   This method assumes arguments have been validated by validate().
    /// 
    /// # Returns
    /// 
    /// Returns a String value containing the concatenation of the two string arguments.
    /// Returns an error if any argument is not a String.
    fn eval(&self, args: &[Value]) -> Result<Value, EvalError> {
        // Extract both arguments as strings, return error if not String
        let first = match &args[0] {
            Value::String(s) => s,
            _ => {
                return Err(EvalError::TypeMismatch {
                    expected: "String".to_string(),
                    actual: format!("{:?}", args[0]),
                });
            }
        };

        let second = match &args[1] {
            Value::String(s) => s,
            _ => {
                return Err(EvalError::TypeMismatch {
                    expected: "String".to_string(),
                    actual: format!("{:?}", args[1]),
                });
            }
        };

        // Concatenate the two strings
        Ok(Value::String(format!("{}{}", first, second)))
    }

    fn name(&self) -> &str {
        "concat"
    }
}
