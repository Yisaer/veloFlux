use super::{StatefulEvalInput, StatefulFunction, StatefulFunctionInstance};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, DataType, Float64Type, Int64Type, Value};

pub struct AccSumFunction;
pub struct AccMaxFunction;
pub struct AccMinFunction;
pub struct AccCountFunction;
pub struct AccAvgFunction;

pub fn acc_sum_function_def() -> FunctionDef {
    acc_numeric_function_def(
        "acc_sum",
        "Return the cumulative sum of non-NULL numeric input values.",
        "Maintains a cumulative numeric sum per partition.",
    )
}

pub fn acc_max_function_def() -> FunctionDef {
    acc_numeric_function_def(
        "acc_max",
        "Return the cumulative maximum of non-NULL numeric input values.",
        "Maintains the cumulative numeric maximum per partition.",
    )
}

pub fn acc_min_function_def() -> FunctionDef {
    acc_numeric_function_def(
        "acc_min",
        "Return the cumulative minimum of non-NULL numeric input values.",
        "Maintains the cumulative numeric minimum per partition.",
    )
}

pub fn acc_avg_function_def() -> FunctionDef {
    acc_numeric_function_def(
        "acc_avg",
        "Return the cumulative average of non-NULL numeric input values.",
        "Maintains a cumulative numeric sum and count per partition.",
    )
}

pub fn acc_count_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "acc_count".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "x".to_string(),
                r#type: TypeSpec::Any,
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::Named {
                name: "int64".to_string(),
            },
        },
        description: "Return the cumulative count of non-NULL input values.".to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires exactly 1 argument in the current implementation.".to_string(),
            "Ignores NULL input values.".to_string(),
            "Returns 0 before any non-NULL value has been accepted.".to_string(),
        ],
        examples: vec!["SELECT acc_count(a) AS n FROM stream".to_string()],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: "Maintains a cumulative non-NULL count per partition.".to_string(),
        }),
    }
}

fn acc_numeric_function_def(name: &str, description: &str, state_semantics: &str) -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: name.to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "x".to_string(),
                r#type: TypeSpec::Category {
                    name: "numeric".to_string(),
                },
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::Named {
                name: "float64".to_string(),
            },
        },
        description: description.to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires exactly 1 argument in the current implementation.".to_string(),
            "Argument type must be numeric.".to_string(),
            "Ignores NULL input values.".to_string(),
            "Returns 0.0 before any non-NULL value has been accepted.".to_string(),
        ],
        examples: vec![format!("SELECT {name}(a) FROM stream")],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: state_semantics.to_string(),
        }),
    }
}

impl AccSumFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccSumFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AccMaxFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccMaxFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AccMinFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccMinFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AccCountFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccCountFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AccAvgFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccAvgFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct AccSumInstance {
    sum: f64,
}

#[derive(Default)]
struct AccMaxInstance {
    value: Option<f64>,
}

#[derive(Default)]
struct AccMinInstance {
    value: Option<f64>,
}

#[derive(Default)]
struct AccCountInstance {
    count: i64,
}

#[derive(Default)]
struct AccAvgInstance {
    sum: f64,
    count: u64,
}

fn validate_arg_count(name: &str, args: &[Value]) -> Result<(), String> {
    if args.len() != 1 {
        return Err(format!(
            "{name} expects exactly 1 argument in the current implementation, got {}",
            args.len()
        ));
    }
    Ok(())
}

fn numeric_arg(name: &str, value: &Value) -> Result<Option<f64>, String> {
    if value.is_null() {
        return Ok(None);
    }

    let float64 = Float64Type;
    match float64.try_cast(value.clone()) {
        Some(Value::Float64(v)) => Ok(Some(v)),
        _ => Err(format!("{name} expects a numeric value, got {value:?}")),
    }
}

fn validate_numeric_return_type(
    name: &str,
    input_types: &[ConcreteDatatype],
) -> Result<ConcreteDatatype, String> {
    if input_types.len() != 1 {
        return Err(format!(
            "{name} expects exactly 1 argument type, got {}",
            input_types.len()
        ));
    }

    match &input_types[0] {
        ConcreteDatatype::Int8(_)
        | ConcreteDatatype::Int16(_)
        | ConcreteDatatype::Int32(_)
        | ConcreteDatatype::Int64(_)
        | ConcreteDatatype::Uint8(_)
        | ConcreteDatatype::Uint16(_)
        | ConcreteDatatype::Uint32(_)
        | ConcreteDatatype::Uint64(_)
        | ConcreteDatatype::Float32(_)
        | ConcreteDatatype::Float64(_) => Ok(ConcreteDatatype::Float64(Float64Type)),
        other => Err(format!("{name} does not support type {other:?}")),
    }
}

impl StatefulFunctionInstance for AccSumInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        validate_arg_count("acc_sum", input.args)?;
        if input.should_apply {
            if let Some(value) = numeric_arg("acc_sum", &input.args[0])? {
                self.sum += value;
            }
        }
        Ok(Value::Float64(self.sum))
    }
}

impl StatefulFunctionInstance for AccMaxInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        validate_arg_count("acc_max", input.args)?;
        if input.should_apply {
            if let Some(value) = numeric_arg("acc_max", &input.args[0])? {
                self.value = Some(match self.value {
                    Some(current) => current.max(value),
                    None => value,
                });
            }
        }
        Ok(Value::Float64(self.value.unwrap_or(0.0)))
    }
}

impl StatefulFunctionInstance for AccMinInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        validate_arg_count("acc_min", input.args)?;
        if input.should_apply {
            if let Some(value) = numeric_arg("acc_min", &input.args[0])? {
                self.value = Some(match self.value {
                    Some(current) => current.min(value),
                    None => value,
                });
            }
        }
        Ok(Value::Float64(self.value.unwrap_or(0.0)))
    }
}

impl StatefulFunctionInstance for AccCountInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        validate_arg_count("acc_count", input.args)?;
        if input.should_apply && !input.args[0].is_null() {
            self.count = self.count.saturating_add(1);
        }
        Ok(Value::Int64(self.count))
    }
}

impl StatefulFunctionInstance for AccAvgInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        validate_arg_count("acc_avg", input.args)?;
        if input.should_apply {
            if let Some(value) = numeric_arg("acc_avg", &input.args[0])? {
                self.sum += value;
                self.count = self.count.saturating_add(1);
            }
        }
        if self.count == 0 {
            Ok(Value::Float64(0.0))
        } else {
            Ok(Value::Float64(self.sum / self.count as f64))
        }
    }
}

impl StatefulFunction for AccSumFunction {
    fn name(&self) -> &str {
        "acc_sum"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_return_type("acc_sum", input_types)
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccSumInstance::default())
    }
}

impl StatefulFunction for AccMaxFunction {
    fn name(&self) -> &str {
        "acc_max"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_return_type("acc_max", input_types)
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccMaxInstance::default())
    }
}

impl StatefulFunction for AccMinFunction {
    fn name(&self) -> &str {
        "acc_min"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_return_type("acc_min", input_types)
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccMinInstance::default())
    }
}

impl StatefulFunction for AccCountFunction {
    fn name(&self) -> &str {
        "acc_count"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "acc_count expects exactly 1 argument type, got {}",
                input_types.len()
            ));
        }
        Ok(ConcreteDatatype::Int64(Int64Type))
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccCountInstance::default())
    }
}

impl StatefulFunction for AccAvgFunction {
    fn name(&self) -> &str {
        "acc_avg"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_return_type("acc_avg", input_types)
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccAvgInstance::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval_all(function: &dyn StatefulFunction, values: &[Value]) -> Vec<Value> {
        let mut instance = function.create_instance();
        values
            .iter()
            .map(|value| {
                instance
                    .eval(StatefulEvalInput {
                        args: std::slice::from_ref(value),
                        should_apply: true,
                    })
                    .expect("acc eval")
            })
            .collect()
    }

    #[test]
    fn acc_sum_returns_running_sum() {
        let function = AccSumFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::Float64(1.0),
                    Value::Float64(2.5),
                    Value::Null,
                    Value::Int64(3),
                ],
            ),
            vec![
                Value::Float64(1.0),
                Value::Float64(3.5),
                Value::Float64(3.5),
                Value::Float64(6.5),
            ]
        );
    }

    #[test]
    fn acc_max_returns_running_max() {
        let function = AccMaxFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::Float64(2.0),
                    Value::Float64(1.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            ),
            vec![
                Value::Float64(2.0),
                Value::Float64(2.0),
                Value::Float64(2.0),
                Value::Float64(3.0),
            ]
        );
    }

    #[test]
    fn acc_min_returns_running_min() {
        let function = AccMinFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::Float64(2.0),
                    Value::Float64(1.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            ),
            vec![
                Value::Float64(2.0),
                Value::Float64(1.0),
                Value::Float64(1.0),
                Value::Float64(1.0),
            ]
        );
    }

    #[test]
    fn acc_count_returns_running_non_null_count() {
        let function = AccCountFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::String("a".to_string()),
                    Value::Null,
                    Value::Bool(false),
                    Value::Int64(3),
                ],
            ),
            vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(3),
            ]
        );
    }

    #[test]
    fn acc_avg_returns_running_average() {
        let function = AccAvgFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::Float64(1.0),
                    Value::Float64(2.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            ),
            vec![
                Value::Float64(1.0),
                Value::Float64(1.5),
                Value::Float64(1.5),
                Value::Float64(2.0),
            ]
        );
    }

    #[test]
    fn acc_numeric_functions_return_zero_before_valid_input() {
        for function in [
            Box::new(AccSumFunction::new()) as Box<dyn StatefulFunction>,
            Box::new(AccMaxFunction::new()) as Box<dyn StatefulFunction>,
            Box::new(AccMinFunction::new()) as Box<dyn StatefulFunction>,
            Box::new(AccAvgFunction::new()) as Box<dyn StatefulFunction>,
        ] {
            let mut instance = function.create_instance();
            let out = instance
                .eval(StatefulEvalInput {
                    args: &[Value::Null],
                    should_apply: true,
                })
                .expect("acc eval");
            assert_eq!(out, Value::Float64(0.0));
        }
    }

    #[test]
    fn acc_numeric_functions_reject_non_numeric_values() {
        let function = AccSumFunction::new();
        let mut instance = function.create_instance();
        let err = instance
            .eval(StatefulEvalInput {
                args: &[Value::String("x".to_string())],
                should_apply: true,
            })
            .expect_err("non-numeric value should fail");
        assert!(err.contains("numeric"));
    }
}
