use super::{StatefulEvalInput, StatefulFunction, StatefulFunctionInstance};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Value};
use std::collections::VecDeque;

pub struct LagFunction;

pub fn lag_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "lag".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![
                FunctionArgSpec {
                    name: "x".to_string(),
                    r#type: TypeSpec::Any,
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "offset".to_string(),
                    r#type: TypeSpec::Named {
                        name: "int64".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "default".to_string(),
                    r#type: TypeSpec::Any,
                    optional: true,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "ignore_null".to_string(),
                    r#type: TypeSpec::Named {
                        name: "bool".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Any,
        },
        description: "Return the previous row's value of the argument.".to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires 1 to 4 arguments.".to_string(),
            "Return type matches the argument type.".to_string(),
            "First rows return the optional default (or NULL) until enough history exists."
                .to_string(),
            "The optional offset must be a positive integer.".to_string(),
            "When ignore_null is true, NULL input values do not advance the lag state."
                .to_string(),
            "Row order is the pipeline's processing order (no explicit ORDER BY support yet)."
                .to_string(),
        ],
        examples: vec![
            "SELECT lag(x) AS prev_x, x".to_string(),
            "SELECT lag(x, 2, 0, true) AS prev_x2 FROM stream".to_string(),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics:
                "Maintains a bounded lag buffer per partition and returns the current visible lag value."
                    .to_string(),
        }),
    }
}

impl LagFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LagFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct LagInstance {
    queue: Option<VecDeque<Value>>,
}

impl StatefulFunctionInstance for LagInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        if input.args.is_empty() || input.args.len() > 4 {
            return Err(format!(
                "lag() expects 1 to 4 arguments, got {}",
                input.args.len()
            ));
        }

        let offset = match input.args.get(1) {
            Some(value) => lag_offset(value)?,
            None => 1,
        };
        let default_value = input.args.get(2).cloned().unwrap_or(Value::Null);
        let ignore_null = match input.args.get(3) {
            Some(value) => bool_arg("lag() fourth argument", value)?,
            None => true,
        };

        let queue = self.queue.get_or_insert_with(|| {
            let mut values = VecDeque::with_capacity(offset);
            for _ in 0..offset {
                values.push_back(default_value.clone());
            }
            values
        });

        let visible = queue.front().cloned().unwrap_or(Value::Null);
        if !input.should_apply {
            return Ok(visible);
        }

        let current = &input.args[0];
        if ignore_null && current.is_null() {
            return Ok(visible);
        }

        let out = queue.pop_front().unwrap_or(Value::Null);
        queue.push_back(current.clone());
        Ok(out)
    }
}

impl StatefulFunction for LagFunction {
    fn name(&self) -> &str {
        "lag"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.is_empty() || input_types.len() > 4 {
            return Err(format!(
                "lag() expects 1 to 4 argument types, got {}",
                input_types.len()
            ));
        }
        Ok(input_types[0].clone())
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(LagInstance::default())
    }
}

fn lag_offset(value: &Value) -> Result<usize, String> {
    let offset = match value {
        Value::Int8(v) => i64::from(*v),
        Value::Int16(v) => i64::from(*v),
        Value::Int32(v) => i64::from(*v),
        Value::Int64(v) => *v,
        Value::Uint8(v) => i64::from(*v),
        Value::Uint16(v) => i64::from(*v),
        Value::Uint32(v) => i64::from(*v),
        Value::Uint64(v) => i64::try_from(*v)
            .map_err(|_| format!("lag() offset is too large for i64: {value:?}"))?,
        other => {
            return Err(format!(
                "lag() second argument must be an integer, got {other:?}"
            ))
        }
    };

    usize::try_from(offset)
        .ok()
        .filter(|offset| *offset >= 1)
        .ok_or_else(|| format!("lag() offset must be >= 1, got {offset}"))
}

fn bool_arg(name: &str, value: &Value) -> Result<bool, String> {
    match value {
        Value::Bool(v) => Ok(*v),
        other => Err(format!("{name} must be bool, got {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::types;

    #[test]
    fn lag_emits_previous_value() {
        let function = LagFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(1)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(2)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(1)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(3)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(2)
        );
    }

    #[test]
    fn lag_skip_returns_current_visible_lag_value() {
        let function = LagFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(1)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(999)],
                    should_apply: false,
                })
                .unwrap(),
            Value::Int64(1)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(2)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(1)
        );
    }

    #[test]
    fn lag_supports_offset_default_and_ignore_null() {
        let function = LagFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[
                        Value::Int64(10),
                        Value::Int64(2),
                        Value::Int64(0),
                        Value::Bool(true)
                    ],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(0)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[
                        Value::Null,
                        Value::Int64(2),
                        Value::Int64(999),
                        Value::Bool(true)
                    ],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(0)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[
                        Value::Int64(30),
                        Value::Int64(2),
                        Value::Int64(999),
                        Value::Bool(true)
                    ],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(0)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[
                        Value::Int64(40),
                        Value::Int64(2),
                        Value::Int64(999),
                        Value::Bool(true)
                    ],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(10)
        );
    }

    #[test]
    fn lag_type_matches_input() {
        let function = LagFunction::new();
        let ty = function
            .return_type(&[
                ConcreteDatatype::Int64(types::Int64Type),
                ConcreteDatatype::Int64(types::Int64Type),
            ])
            .unwrap();
        assert!(matches!(ty, ConcreteDatatype::Int64(_)));
    }
}
