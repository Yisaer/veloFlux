use crate::datatypes::DataType;
use crate::value::Value;

/// 32-bit signed integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Int32Type;

impl DataType for Int32Type {
    fn name(&self) -> String {
        "Int32".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Int32(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Int8(v) => Some(Value::Int32(v as i32)),
            Value::Int16(v) => Some(Value::Int32(v as i32)),
            Value::Int32(v) => Some(Value::Int32(v)),
            Value::Int64(v) => {
                if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
                    Some(Value::Int32(v as i32))
                } else {
                    None
                }
            }
            Value::Uint8(v) => Some(Value::Int32(v as i32)),
            Value::Uint16(v) => Some(Value::Int32(v as i32)),
            Value::Uint32(v) => {
                if v <= i32::MAX as u32 {
                    Some(Value::Int32(v as i32))
                } else {
                    None
                }
            }
            Value::Uint64(v) => {
                if v <= i32::MAX as u64 {
                    Some(Value::Int32(v as i32))
                } else {
                    None
                }
            }
            Value::Float64(v) => {
                if v >= i32::MIN as f64 && v <= i32::MAX as f64 && v.fract() == 0.0 {
                    Some(Value::Int32(v as i32))
                } else {
                    None
                }
            }
            Value::Bool(v) => Some(Value::Int32(if v { 1 } else { 0 })),
            Value::String(s) => s.parse::<i32>().ok().map(Value::Int32),
            _ => None,
        }
    }
}