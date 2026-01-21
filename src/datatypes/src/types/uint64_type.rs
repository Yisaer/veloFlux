use crate::datatypes::DataType;
use crate::value::Value;

/// 64-bit unsigned integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Uint64Type;

fn try_cast_f64_to_u64(v: f64) -> Option<u64> {
    if !v.is_finite() || v.fract() != 0.0 || v < 0.0 || v > u64::MAX as f64 {
        return None;
    }
    Some(v as u64)
}

impl DataType for Uint64Type {
    fn name(&self) -> String {
        "Uint64".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Uint64(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
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
            Value::Float32(v) => try_cast_f64_to_u64(v as f64).map(Value::Uint64),
            Value::Float64(v) => try_cast_f64_to_u64(v).map(Value::Uint64),
            _ => None,
        }
    }
}
