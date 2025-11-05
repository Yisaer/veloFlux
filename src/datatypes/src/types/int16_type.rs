use crate::datatypes::DataType;
use crate::value::Value;

/// 16-bit signed integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Int16Type;

impl DataType for Int16Type {
    fn name(&self) -> String {
        "Int16".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Int16(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Int8(v) => Some(Value::Int16(v as i16)),
            Value::Int16(v) => Some(Value::Int16(v)),
            Value::Int32(v) => {
                if v >= i16::MIN as i32 && v <= i16::MAX as i32 {
                    Some(Value::Int16(v as i16))
                } else {
                    None
                }
            }
            Value::Int64(v) => {
                if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
                    Some(Value::Int16(v as i16))
                } else {
                    None
                }
            }
            Value::Uint8(v) => Some(Value::Int16(v as i16)),
            Value::Uint16(v) => {
                if v <= i16::MAX as u16 {
                    Some(Value::Int16(v as i16))
                } else {
                    None
                }
            }
            Value::Uint32(v) => {
                if v <= i16::MAX as u32 {
                    Some(Value::Int16(v as i16))
                } else {
                    None
                }
            }
            Value::Uint64(v) => {
                if v <= i16::MAX as u64 {
                    Some(Value::Int16(v as i16))
                } else {
                    None
                }
            }
            Value::Float64(v) => {
                if v >= i16::MIN as f64 && v <= i16::MAX as f64 && v.fract() == 0.0 {
                    Some(Value::Int16(v as i16))
                } else {
                    None
                }
            }
            Value::Bool(v) => Some(Value::Int16(if v { 1 } else { 0 })),
            Value::String(s) => s.parse::<i16>().ok().map(Value::Int16),
            _ => None,
        }
    }
}