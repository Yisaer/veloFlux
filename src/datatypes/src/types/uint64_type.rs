use crate::datatypes::DataType;
use crate::value::Value;

/// 64-bit unsigned integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Uint64Type;

impl DataType for Uint64Type {
    fn name(&self) -> String {
        "Uint64".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Uint64(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Int8(v) => {
                if v >= 0 {
                    Some(Value::Uint64(v as u64))
                } else {
                    None
                }
            }
            Value::Int16(v) => {
                if v >= 0 {
                    Some(Value::Uint64(v as u64))
                } else {
                    None
                }
            }
            Value::Int32(v) => {
                if v >= 0 {
                    Some(Value::Uint64(v as u64))
                } else {
                    None
                }
            }
            Value::Int64(v) => {
                if v >= 0 {
                    Some(Value::Uint64(v as u64))
                } else {
                    None
                }
            }
            Value::Uint8(v) => Some(Value::Uint64(v as u64)),
            Value::Uint16(v) => Some(Value::Uint64(v as u64)),
            Value::Uint32(v) => Some(Value::Uint64(v as u64)),
            Value::Uint64(v) => Some(Value::Uint64(v)),
            Value::Float64(v) => {
                if v >= 0.0 && v <= u64::MAX as f64 && v.fract() == 0.0 {
                    Some(Value::Uint64(v as u64))
                } else {
                    None
                }
            }
            Value::Bool(v) => Some(Value::Uint64(if v { 1 } else { 0 })),
            Value::String(s) => s.parse::<u64>().ok().map(Value::Uint64),
            _ => None,
        }
    }
}