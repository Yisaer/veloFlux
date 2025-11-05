use crate::datatypes::DataType;
use crate::value::Value;

/// 32-bit floating point number type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Float32Type;

impl DataType for Float32Type {
    fn name(&self) -> String {
        "Float32".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Float32(0.0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Float32(v) => Some(Value::Float32(v)),
            Value::Float64(v) => Some(Value::Float32(v as f32)),
            Value::Int8(v) => Some(Value::Float32(v as f32)),
            Value::Int16(v) => Some(Value::Float32(v as f32)),
            Value::Int32(v) => Some(Value::Float32(v as f32)),
            Value::Int64(v) => Some(Value::Float32(v as f32)),
            Value::Uint8(v) => Some(Value::Float32(v as f32)),
            Value::Uint16(v) => Some(Value::Float32(v as f32)),
            Value::Uint32(v) => Some(Value::Float32(v as f32)),
            Value::Uint64(v) => Some(Value::Float32(v as f32)),
            Value::Bool(v) => Some(Value::Float32(if v { 1.0 } else { 0.0 })),
            Value::String(s) => s.parse::<f32>().ok().map(Value::Float32),
            _ => None,
        }
    }
}