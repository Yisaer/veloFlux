use crate::datatypes::DataType;
use crate::value::Value;

/// 8-bit unsigned integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Uint8Type;

impl DataType for Uint8Type {
    fn name(&self) -> String {
        "Uint8".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Uint8(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Uint8(v) => Some(Value::Uint8(v)),
            Value::Int64(v) => {
                if v >= 0 && v <= u8::MAX as i64 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Float64(v) => {
                if v >= 0.0 && v <= u8::MAX as f64 && v.fract() == 0.0 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Bool(v) => Some(Value::Uint8(if v { 1 } else { 0 })),
            Value::String(s) => s.parse::<u8>().ok().map(Value::Uint8),
            _ => None,
        }
    }
}