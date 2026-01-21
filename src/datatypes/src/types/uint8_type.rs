use crate::datatypes::DataType;
use crate::value::Value;

/// 8-bit unsigned integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Uint8Type;

fn try_cast_f64_to_u8(v: f64) -> Option<u8> {
    if !v.is_finite() || v.fract() != 0.0 || v < 0.0 || v > u8::MAX as f64 {
        return None;
    }
    Some(v as u8)
}

impl DataType for Uint8Type {
    fn name(&self) -> String {
        "Uint8".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Uint8(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
            Value::Int8(v) => {
                if v >= 0 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Int16(v) => {
                if v >= 0 && v <= u8::MAX as i16 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Int32(v) => {
                if v >= 0 && v <= u8::MAX as i32 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Uint8(v) => Some(Value::Uint8(v)),
            Value::Int64(v) => {
                if v >= 0 && v <= u8::MAX as i64 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Uint16(v) => {
                if v <= u8::MAX as u16 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Uint32(v) => {
                if v <= u8::MAX as u32 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Uint64(v) => {
                if v <= u8::MAX as u64 {
                    Some(Value::Uint8(v as u8))
                } else {
                    None
                }
            }
            Value::Float32(v) => try_cast_f64_to_u8(v as f64).map(Value::Uint8),
            Value::Float64(v) => try_cast_f64_to_u8(v).map(Value::Uint8),
            _ => None,
        }
    }
}
