use bytes::Bytes;

use crate::datatypes::DataType;
use crate::value::Value;

/// Bytes type for opaque binary payloads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BytesType;

impl DataType for BytesType {
    fn name(&self) -> String {
        "Bytes".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Bytes(Bytes::new())
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
            Value::Bytes(bytes) => Some(Value::Bytes(bytes)),
            _ => None,
        }
    }
}
