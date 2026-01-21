use crate::datatypes::DataType;
use crate::value::Value;

/// 64-bit floating point number type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Float64Type;

impl DataType for Float64Type {
    fn name(&self) -> String {
        "Float64".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Float64(0.0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
            Value::Float32(v) => Some(Value::Float64(v as f64)),
            Value::Float64(v) => Some(Value::Float64(v)),
            Value::Int8(v) => Some(Value::Float64(v as f64)),
            Value::Int16(v) => Some(Value::Float64(v as f64)),
            Value::Int32(v) => Some(Value::Float64(v as f64)),
            Value::Int64(v) => Some(Value::Float64(v as f64)),
            Value::Uint8(v) => Some(Value::Float64(v as f64)),
            Value::Uint16(v) => Some(Value::Float64(v as f64)),
            Value::Uint32(v) => Some(Value::Float64(v as f64)),
            Value::Uint64(v) => Some(Value::Float64(v as f64)),
            _ => None,
        }
    }
}
