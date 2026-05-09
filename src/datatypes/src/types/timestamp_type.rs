use crate::datatypes::DataType;
use crate::value::{TimestampValue, Value};

/// UTC timestamp type stored as Unix epoch microseconds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimestampType;

impl DataType for TimestampType {
    fn name(&self) -> String {
        "Timestamp".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Timestamp(TimestampValue::from_epoch_micros(0))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Null => Some(Value::Null),
            Value::Timestamp(v) => Some(Value::Timestamp(v)),
            Value::String(s) => TimestampValue::parse_rfc3339(&s).map(Value::Timestamp),
            _ => None,
        }
    }
}
