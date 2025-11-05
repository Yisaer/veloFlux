use crate::datatypes::DataType;
use crate::value::Value;

/// 8-bit signed integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Int8Type;

impl DataType for Int8Type {
    fn name(&self) -> String {
        "Int8".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Int8(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
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
            Value::Float64(v) => {
                if v >= i8::MIN as f64 && v <= i8::MAX as f64 && v.fract() == 0.0 {
                    Some(Value::Int8(v as i8))
                } else {
                    None
                }
            }
            Value::Bool(v) => Some(Value::Int8(if v { 1 } else { 0 })),
            Value::String(s) => s.parse::<i8>().ok().map(Value::Int8),
            _ => None,
        }
    }
}