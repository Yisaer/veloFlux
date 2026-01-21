use crate::datatypes::DataType;
use crate::value::Value;

/// 64-bit signed integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Int64Type;

fn try_cast_f64_to_i64(v: f64) -> Option<i64> {
    if !v.is_finite() || v.fract() != 0.0 || v < i64::MIN as f64 || v > i64::MAX as f64 {
        return None;
    }
    Some(v as i64)
}

impl DataType for Int64Type {
    fn name(&self) -> String {
        "Int64".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Int64(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
            Value::Int8(v) => Some(Value::Int64(v as i64)),
            Value::Int16(v) => Some(Value::Int64(v as i64)),
            Value::Int32(v) => Some(Value::Int64(v as i64)),
            Value::Int64(v) => Some(Value::Int64(v)),
            Value::Uint8(v) => Some(Value::Int64(v as i64)),
            Value::Uint16(v) => Some(Value::Int64(v as i64)),
            Value::Uint32(v) => Some(Value::Int64(v as i64)),
            Value::Uint64(v) => {
                if v <= i64::MAX as u64 {
                    Some(Value::Int64(v as i64))
                } else {
                    None
                }
            }
            Value::Float32(v) => try_cast_f64_to_i64(v as f64).map(Value::Int64),
            Value::Float64(v) => try_cast_f64_to_i64(v).map(Value::Int64),
            _ => None,
        }
    }
}
