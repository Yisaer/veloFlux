use crate::datatypes::DataType;
use crate::value::Value;

/// 32-bit signed integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Int32Type;

fn try_cast_f64_to_i32(v: f64) -> Option<i32> {
    if !v.is_finite() || v.fract() != 0.0 || v < i32::MIN as f64 || v > i32::MAX as f64 {
        return None;
    }
    Some(v as i32)
}

impl DataType for Int32Type {
    fn name(&self) -> String {
        "Int32".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Int32(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
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
            Value::Float32(v) => try_cast_f64_to_i32(v as f64).map(Value::Int32),
            Value::Float64(v) => try_cast_f64_to_i32(v).map(Value::Int32),
            _ => None,
        }
    }
}
