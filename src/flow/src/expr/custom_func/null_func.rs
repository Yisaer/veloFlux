use super::helpers::{any_type, bool_type, req_arg, scalar_function_def, validate_arity};
use crate::catalog::FunctionDef;
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use datatypes::Value;

#[derive(Debug, Clone)]
pub struct IsNullFunc;

pub fn builtin_function_defs() -> Vec<FunctionDef> {
    vec![isnull_function_def()]
}

pub fn isnull_function_def() -> FunctionDef {
    scalar_function_def(
        "isnull",
        vec![req_arg("value", any_type())],
        bool_type(),
        "Return whether the argument is NULL.",
        vec![
            "Requires exactly 1 argument.",
            "Accepts any argument type.",
            "Returns true when the argument is NULL, otherwise false.",
        ],
        vec!["SELECT isnull(a)", "SELECT isnull(payload->device_id)"],
    )
}

impl CustomFunc for IsNullFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        Ok(Value::Bool(args[0].is_null()))
    }

    fn name(&self) -> &str {
        "isnull"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::custom_func::helpers::{assert_bool, i, n};

    #[test]
    fn isnull_returns_true_only_for_null() {
        let func = IsNullFunc;

        func.validate_row(&[n()]).expect("null arg should validate");
        func.validate_row(&[i(1)])
            .expect("non-null arg should validate");

        assert_bool(func.eval_row(&[n()]).expect("eval null"), true);
        assert_bool(func.eval_row(&[i(1)]).expect("eval int"), false);
    }
}
