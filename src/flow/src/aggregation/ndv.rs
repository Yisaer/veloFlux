use crate::aggregation::{AggregateAccumulator, AggregateFunction};
use datatypes::{ConcreteDatatype, Int64Type, Value};
use std::collections::HashSet;

#[derive(Debug, Default)]
pub struct NdvFunction;

impl NdvFunction {
    pub fn new() -> Self {
        Self
    }
}

impl AggregateFunction for NdvFunction {
    fn name(&self) -> &str {
        "ndv"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "NDV expects exactly one argument, got {}",
                input_types.len()
            ));
        }
        Ok(ConcreteDatatype::Int64(Int64Type))
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(NdvAccumulator::default())
    }
}

#[derive(Debug, Default, Clone)]
struct NdvAccumulator {
    distinct_values: HashSet<Value>,
}

impl AggregateAccumulator for NdvAccumulator {
    fn update(&mut self, args: &[Value]) -> Result<(), String> {
        let Some(value) = args.first() else {
            return Err("NDV expects one argument".to_string());
        };
        if value.is_null() {
            return Ok(());
        }
        self.distinct_values.insert(value.clone());
        Ok(())
    }

    fn finalize(&self) -> Value {
        Value::Int64(i64::try_from(self.distinct_values.len()).unwrap_or(i64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndv_function_name() {
        let func = NdvFunction::new();
        assert_eq!(func.name(), "ndv");
    }

    #[test]
    fn ndv_function_default() {
        let func = NdvFunction::default();
        assert_eq!(func.name(), "ndv");
    }

    #[test]
    fn ndv_function_return_type_valid() {
        let func = NdvFunction::new();
        let result = func.return_type(&[ConcreteDatatype::Int64(Int64Type)]);
        assert!(result.is_ok());
        // NDV always returns Int64
        assert!(matches!(result.unwrap(), ConcreteDatatype::Int64(_)));
    }

    #[test]
    fn ndv_function_return_type_no_args() {
        let func = NdvFunction::new();
        let result = func.return_type(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly one argument"));
    }

    #[test]
    fn ndv_accumulator_counts_distinct() {
        let func = NdvFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(1)]).unwrap();
        acc.update(&[Value::Int64(2)]).unwrap();
        acc.update(&[Value::Int64(1)]).unwrap(); // duplicate
        acc.update(&[Value::Int64(3)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(3)); // 3 distinct values
    }

    #[test]
    fn ndv_accumulator_ignores_nulls() {
        let func = NdvFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(1)]).unwrap();
        acc.update(&[Value::Null]).unwrap();
        acc.update(&[Value::Int64(2)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(2)); // nulls not counted
    }

    #[test]
    fn ndv_accumulator_empty_returns_zero() {
        let func = NdvFunction::new();
        let acc = func.create_accumulator();
        assert_eq!(acc.finalize(), Value::Int64(0));
    }

    #[test]
    fn ndv_accumulator_no_args_error() {
        let func = NdvFunction::new();
        let mut acc = func.create_accumulator();
        let result = acc.update(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn ndv_accumulator_strings() {
        let func = NdvFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::String("a".to_string())]).unwrap();
        acc.update(&[Value::String("b".to_string())]).unwrap();
        acc.update(&[Value::String("a".to_string())]).unwrap(); // duplicate

        assert_eq!(acc.finalize(), Value::Int64(2)); // 2 distinct values
    }
}
