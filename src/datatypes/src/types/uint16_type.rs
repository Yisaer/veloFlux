use crate::datatypes::DataType;
use crate::value::Value;

/// 16-bit unsigned integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Uint16Type;

fn try_cast_f64_to_u16(v: f64) -> Option<u16> {
    if !v.is_finite() || v.fract() != 0.0 || v < 0.0 || v > u16::MAX as f64 {
        return None;
    }
    Some(v as u16)
}

impl DataType for Uint16Type {
    fn name(&self) -> String {
        "Uint16".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Uint16(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
            Value::Int8(v) => {
                if v >= 0 {
                    Some(Value::Uint16(v as u16))
                } else {
                    None
                }
            }
            Value::Int16(v) => {
                if v >= 0 {
                    Some(Value::Uint16(v as u16))
                } else {
                    None
                }
            }
            Value::Int32(v) => {
                if v >= 0 && v <= u16::MAX as i32 {
                    Some(Value::Uint16(v as u16))
                } else {
                    None
                }
            }
            Value::Int64(v) => {
                if v >= 0 && v <= u16::MAX as i64 {
                    Some(Value::Uint16(v as u16))
                } else {
                    None
                }
            }
            Value::Uint8(v) => Some(Value::Uint16(v as u16)),
            Value::Uint16(v) => Some(Value::Uint16(v)),
            Value::Uint32(v) => {
                if v <= u16::MAX as u32 {
                    Some(Value::Uint16(v as u16))
                } else {
                    None
                }
            }
            Value::Uint64(v) => {
                if v <= u16::MAX as u64 {
                    Some(Value::Uint16(v as u16))
                } else {
                    None
                }
            }
            Value::Float32(v) => try_cast_f64_to_u16(v as f64).map(Value::Uint16),
            Value::Float64(v) => try_cast_f64_to_u16(v).map(Value::Uint16),
            _ => None,
        }
    }
}
