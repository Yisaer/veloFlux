use crate::datatypes::DataType;
use crate::value::Value;

/// 32-bit unsigned integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Uint32Type;

impl DataType for Uint32Type {
    fn name(&self) -> String {
        "Uint32".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Uint32(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Int8(v) => {
                if v >= 0 {
                    Some(Value::Uint32(v as u32))
                } else {
                    None
                }
            }
            Value::Int16(v) => {
                if v >= 0 {
                    Some(Value::Uint32(v as u32))
                } else {
                    None
                }
            }
            Value::Int32(v) => {
                if v >= 0 {
                    Some(Value::Uint32(v as u32))
                } else {
                    None
                }
            }
            Value::Int64(v) => {
                if v >= 0 && v <= u32::MAX as i64 {
                    Some(Value::Uint32(v as u32))
                } else {
                    None
                }
            }
            Value::Uint8(v) => Some(Value::Uint32(v as u32)),
            Value::Uint16(v) => Some(Value::Uint32(v as u32)),
            Value::Uint32(v) => Some(Value::Uint32(v)),
            Value::Uint64(v) => {
                if v <= u32::MAX as u64 {
                    Some(Value::Uint32(v as u32))
                } else {
                    None
                }
            }
            Value::Float64(v) => {
                if v >= 0.0 && v <= u32::MAX as f64 && v.fract() == 0.0 {
                    Some(Value::Uint32(v as u32))
                } else {
                    None
                }
            }
            Value::Bool(v) => Some(Value::Uint32(if v { 1 } else { 0 })),
            Value::String(s) => s.parse::<u32>().ok().map(Value::Uint32),
            _ => None,
        }
    }
}