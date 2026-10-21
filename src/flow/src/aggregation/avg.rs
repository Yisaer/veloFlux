use crate::aggregation::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, DataType, Float64Type, Value};
use std::any::Any;

#[derive(Debug)]
pub struct AvgFunction;

pub fn avg_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "avg".to_string(),
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
        description: "Average of numeric values.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Argument type must be numeric (int/uint/float).".to_string(),
            "Ignores NULL inputs; returns NULL if all inputs are NULL.".to_string(),
            "Returns float64.".to_string(),
        ],
        examples: vec![
            "SELECT avg(x) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT avg(amount) FROM orders GROUP BY user_id".to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: true,
        }),
        stateful: None,
    }
}

impl AvgFunction {
    pub fn new() -> Self {
        Self
    }

    fn validate_numeric_type(
        &self,
        input_types: &[ConcreteDatatype],
    ) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "AVG expects exactly one argument, got {}",
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
            other => Err(format!("AVG does not support type {:?}", other)),
        }
    }
}

impl Default for AvgFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateFunction for AvgFunction {
    fn supports_inline_execution(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "avg"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        self.validate_numeric_type(input_types)
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(AvgAccumulator::default())
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

#[derive(Debug, Default, Clone)]
pub(super) struct AvgAccumulator {
    sum: f64,
    count: u64,
}

impl AvgAccumulator {
    #[inline]
    fn cast_to_f64(value: &Value) -> Result<Option<f64>, String> {
        if value.is_null() {
            return Ok(None);
        }

        match value {
            Value::Float64(value) => return Ok(Some(*value)),
            Value::Int64(value) => return Ok(Some(*value as f64)),
            _ => {}
        }
        let float64 = Float64Type;
        match float64.try_cast(value.clone()) {
            Some(Value::Float64(v)) => Ok(Some(v)),
            _ => Err(format!("AVG does not support value {:?}", value)),
        }
    }
}

impl AvgAccumulator {
    #[inline(always)]
    pub(super) fn prepare(&self, args: &[Value]) -> Result<AvgUpdate, String> {
        let Some(value) = args.first() else {
            return Err("AVG expects one argument".to_string());
        };

        Ok(AvgUpdate {
            value: Self::cast_to_f64(value)?,
        })
    }

    #[inline]
    pub(super) fn commit(&mut self, update: AvgUpdate) {
        if let Some(v) = update.value {
            self.sum += v;
            self.count = self.count.saturating_add(1);
        }
    }
}

impl AggregateAccumulator for AvgAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        Ok(Box::new(self.prepare(args)?))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        self.commit(
            *update
                .into_any()
                .downcast::<AvgUpdate>()
                .expect("avg accumulator received incompatible update"),
        );
    }

    fn finalize(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            Value::Float64(self.sum / self.count as f64)
        }
    }
}

pub(super) struct AvgUpdate {
    value: Option<f64>,
}

impl AggregateUpdate for AvgUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avg_accumulator_returns_average_of_non_null_values() {
        let func = AvgFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(10)]).unwrap();
        acc.update(&[Value::Null]).unwrap();
        acc.update(&[Value::Int64(20)]).unwrap();
        acc.update(&[Value::Float64(30.0)]).unwrap();

        assert_eq!(acc.finalize(), Value::Float64(20.0));
    }
}
