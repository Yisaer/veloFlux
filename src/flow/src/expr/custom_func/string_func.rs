use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionSignatureSpec, TypeSpec,
};
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use datatypes::Value;

/// Custom implementation of the concat function
/// This function concatenates exactly 2 String arguments
#[derive(Debug, Clone)]
pub struct ConcatFunc;

pub fn concat_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Scalar,
        name: "concat".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![
                FunctionArgSpec {
                    name: "a".to_string(),
                    r#type: TypeSpec::Named {
                        name: "string".to_string(),
                    },
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "b".to_string(),
                    r#type: TypeSpec::Named {
                        name: "string".to_string(),
                    },
                    optional: false,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Named {
                name: "string".to_string(),
            },
        },
        description: "Concatenate two strings.".to_string(),
        allowed_contexts: vec![
            FunctionContext::Select,
            FunctionContext::Where,
            FunctionContext::GroupBy,
        ],
        requirements: vec![],
        constraints: vec![
            "Requires exactly 2 arguments.".to_string(),
            "Both arguments must be strings.".to_string(),
            "NULL is not accepted as a string argument in the current implementation.".to_string(),
        ],
        examples: vec![
            "SELECT concat('hello', 'world') AS s".to_string(),
            "SELECT concat(a, b)".to_string(),
        ],
        aggregate: None,
        stateful: None,
    }
}

impl CustomFunc for ConcatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        if args.len() != 2 {
            return Err(EvalError::TypeMismatch {
                expected: "2 arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }
        for (idx, arg) in args.iter().enumerate() {
            if !matches!(arg, Value::String(_)) {
                return Err(EvalError::TypeMismatch {
                    expected: "String".to_string(),
                    actual: format!("{:?} at argument {}", arg, idx),
                });
            }
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        self.validate_row(args)?;
        let first = match &args[0] {
            Value::String(s) => s,
            _ => unreachable!("validated as string"),
        };
        let second = match &args[1] {
            Value::String(s) => s,
            _ => unreachable!("validated as string"),
        };
        Ok(Value::String(format!("{}{}", first, second)))
    }

    fn name(&self) -> &str {
        "concat"
    }
}
