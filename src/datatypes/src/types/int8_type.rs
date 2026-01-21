use crate::datatypes::DataType;
use crate::value::Value;

/// 8-bit signed integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Int8Type;

fn try_cast_f64_to_i8(v: f64) -> Option<i8> {
    if !v.is_finite() || v.fract() != 0.0 || v < i8::MIN as f64 || v > i8::MAX as f64 {
        return None;
    }
    Some(v as i8)
}

impl DataType for Int8Type {
    fn name(&self) -> String {
        "Int8".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Int8(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
            Value::Int8(v) => Some(Value::Int8(v)),
            Value::Int16(v) => {
                if v >= i8::MIN as i16 && v <= i8::MAX as i16 {
                    Some(Value::Int8(v as i8))
                } else {
                    None
                }
            }
            Value::Int32(v) => {
                if v >= i8::MIN as i32 && v <= i8::MAX as i32 {
                    Some(Value::Int8(v as i8))
                } else {
                    None
                }
            }
            Value::Int64(v) => {
                if v >= i8::MIN as i64 && v <= i8::MAX as i64 {
                    Some(Value::Int8(v as i8))
                } else {
                    None
                }
            }
            Value::Uint8(v) => {
                if v <= i8::MAX as u8 {
                    Some(Value::Int8(v as i8))
                } else {
                    None
                }
            }
            Value::Uint16(v) => {
                if v <= i8::MAX as u16 {
                    Some(Value::Int8(v as i8))
                } else {
                    None
                }
            }
            Value::Uint32(v) => {
                if v <= i8::MAX as u32 {
                    Some(Value::Int8(v as i8))
                } else {
                    None
                }
            }
            Value::Uint64(v) => {
                if v <= i8::MAX as u64 {
                    Some(Value::Int8(v as i8))
                } else {
                    None
                }
            }
            Value::Float32(v) => try_cast_f64_to_i8(v as f64).map(Value::Int8),
            Value::Float64(v) => try_cast_f64_to_i8(v).map(Value::Int8),
            _ => None,
        }
    }
}
