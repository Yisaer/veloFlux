use crate::datatypes::DataType;
use crate::value::Value;

/// Boolean type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BooleanType;

impl DataType for BooleanType {
    fn name(&self) -> String {
        "Boolean".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Bool(false)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
            Value::Bool(v) => Some(Value::Bool(v)),
            _ => None,
        }
    }
}
