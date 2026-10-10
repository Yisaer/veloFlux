use crate::datatypes::ConcreteDatatype;
use crate::types::StructType;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// UTC timestamp value stored as signed Unix epoch microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimestampValue {
    epoch_micros: i64,
}

impl TimestampValue {
    pub fn from_epoch_micros(epoch_micros: i64) -> Self {
        Self { epoch_micros }
    }

    pub fn epoch_micros(&self) -> i64 {
        self.epoch_micros
    }

    pub fn parse_rfc3339(value: &str) -> Option<Self> {
        DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|dt| Self::from_epoch_micros(dt.with_timezone(&Utc).timestamp_micros()))
    }

    pub fn to_rfc3339_utc(&self) -> Option<String> {
        let secs = self.epoch_micros.div_euclid(1_000_000);
        let micros = self.epoch_micros.rem_euclid(1_000_000) as u32;
        DateTime::<Utc>::from_timestamp(secs, micros * 1_000)
            .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Micros, true))
    }
}

/// List value sharing immutable items and their datatype.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ListValue {
    inner: Arc<ListValueInner>,
}

impl std::fmt::Debug for ListValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListValue")
            .field("items", &self.inner.items)
            .field("datatype", &self.inner.datatype)
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListValueInner {
    items: Vec<Value>,
    /// Inner values datatype, to distinguish empty lists of different datatypes
    datatype: Arc<ConcreteDatatype>,
}

impl ListValue {
    pub fn new(items: Vec<Value>, datatype: Arc<ConcreteDatatype>) -> Self {
        Self {
            inner: Arc::new(ListValueInner { items, datatype }),
        }
    }

    pub fn items(&self) -> &[Value] {
        &self.inner.items
    }

    pub fn datatype(&self) -> &ConcreteDatatype {
        &self.inner.datatype
    }

    /// Get element at the given index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.inner.items.get(index)
    }

    /// Get the length of the list
    pub fn len(&self) -> usize {
        self.inner.items.len()
    }

    /// Check if the list is empty
    pub fn is_empty(&self) -> bool {
        self.inner.items.is_empty()
    }
}

/// Struct value sharing immutable items and field definitions.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StructValue {
    inner: Arc<StructValueInner>,
}

impl std::fmt::Debug for StructValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructValue")
            .field("items", &self.inner.items)
            .field("fields", &self.inner.fields)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StructValueInner {
    items: Vec<Value>,
    fields: StructType,
}

impl StructValue {
    pub fn new(items: Vec<Value>, fields: StructType) -> Self {
        Self {
            inner: Arc::new(StructValueInner { items, fields }),
        }
    }

    pub fn items(&self) -> &[Value] {
        &self.inner.items
    }

    pub fn fields(&self) -> &StructType {
        &self.inner.fields
    }

    /// Move uniquely owned items, or shallow-clone the shared item container.
    pub fn into_parts(self) -> (Vec<Value>, StructType) {
        let inner = Arc::try_unwrap(self.inner).unwrap_or_else(|inner| inner.as_ref().clone());
        (inner.items, inner.fields)
    }

    /// Get field value by field name
    pub fn get_field(&self, field_name: &str) -> Option<&Value> {
        self.inner
            .fields
            .fields()
            .iter()
            .position(|field| field.name() == field_name)
            .and_then(|index| self.inner.items.get(index))
    }
}

/// Value type for type casting
/// Should be synchronized with ConcreteDatatype variants
#[derive(Debug, Clone)]
pub enum Value {
    /// Null value representing missing/unknown data
    Null,
    /// 32-bit floating point number
    Float32(f32),
    /// 64-bit floating point number
    Float64(f64),
    /// 8-bit signed integer
    Int8(i8),
    /// 16-bit signed integer
    Int16(i16),
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 8-bit unsigned integer
    Uint8(u8),
    /// 16-bit unsigned integer
    Uint16(u16),
    /// 32-bit unsigned integer
    Uint32(u32),
    /// 64-bit unsigned integer
    Uint64(u64),
    /// Shared UTF-8 text; moving a String into Arc preserves its buffer.
    String(Arc<String>),
    /// Shared opaque bytes payload; moving Bytes preserves its backing storage.
    Bytes(Arc<Bytes>),
    /// Boolean type
    Bool(bool),
    /// UTC timestamp type
    Timestamp(TimestampValue),
    /// Struct value
    Struct(StructValue),
    /// List value
    List(ListValue),
}

impl Value {
    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get the datatype of this value
    pub fn datatype(&self) -> ConcreteDatatype {
        match self {
            Value::Null => ConcreteDatatype::Null,
            Value::Float32(_) => ConcreteDatatype::Float32(crate::types::Float32Type),
            Value::Float64(_) => ConcreteDatatype::Float64(crate::types::Float64Type),
            Value::Int8(_) => ConcreteDatatype::Int8(crate::types::Int8Type),
            Value::Int16(_) => ConcreteDatatype::Int16(crate::types::Int16Type),
            Value::Int32(_) => ConcreteDatatype::Int32(crate::types::Int32Type),
            Value::Int64(_) => ConcreteDatatype::Int64(crate::types::Int64Type),
            Value::Uint8(_) => ConcreteDatatype::Uint8(crate::types::Uint8Type),
            Value::Uint16(_) => ConcreteDatatype::Uint16(crate::types::Uint16Type),
            Value::Uint32(_) => ConcreteDatatype::Uint32(crate::types::Uint32Type),
            Value::Uint64(_) => ConcreteDatatype::Uint64(crate::types::Uint64Type),
            Value::String(_) => ConcreteDatatype::String(crate::types::StringType),
            Value::Bytes(_) => ConcreteDatatype::Bytes(crate::types::BytesType),
            Value::Bool(_) => ConcreteDatatype::Bool(crate::types::BooleanType),
            Value::Timestamp(_) => ConcreteDatatype::Timestamp(crate::types::TimestampType),
            Value::Struct(s) => ConcreteDatatype::Struct(s.fields().clone()),
            Value::List(l) => {
                ConcreteDatatype::List(crate::types::ListType::new(Arc::new(l.datatype().clone())))
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Float32(a), Value::Float32(b)) => (a.is_nan() && b.is_nan()) || a == b,
            (Value::Float64(a), Value::Float64(b)) => (a.is_nan() && b.is_nan()) || a == b,
            (Value::Int8(a), Value::Int8(b)) => a == b,
            (Value::Int16(a), Value::Int16(b)) => a == b,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Uint8(a), Value::Uint8(b)) => a == b,
            (Value::Uint16(a), Value::Uint16(b)) => a == b,
            (Value::Uint32(a), Value::Uint32(b)) => a == b,
            (Value::Uint64(a), Value::Uint64(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Struct(a), Value::Struct(b)) => a == b,
            (Value::List(a), Value::List(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => {
                0_u8.hash(state);
            }
            Value::Float32(v) => {
                1_u8.hash(state);
                if v.is_nan() {
                    u32::MAX.hash(state);
                } else if *v == 0.0 {
                    0_u32.hash(state);
                } else {
                    v.to_bits().hash(state);
                }
            }
            Value::Float64(v) => {
                2_u8.hash(state);
                if v.is_nan() {
                    u64::MAX.hash(state);
                } else if *v == 0.0 {
                    0_u64.hash(state);
                } else {
                    v.to_bits().hash(state);
                }
            }
            Value::Int8(v) => {
                3_u8.hash(state);
                v.hash(state);
            }
            Value::Int16(v) => {
                4_u8.hash(state);
                v.hash(state);
            }
            Value::Int32(v) => {
                5_u8.hash(state);
                v.hash(state);
            }
            Value::Int64(v) => {
                6_u8.hash(state);
                v.hash(state);
            }
            Value::Uint8(v) => {
                7_u8.hash(state);
                v.hash(state);
            }
            Value::Uint16(v) => {
                8_u8.hash(state);
                v.hash(state);
            }
            Value::Uint32(v) => {
                9_u8.hash(state);
                v.hash(state);
            }
            Value::Uint64(v) => {
                10_u8.hash(state);
                v.hash(state);
            }
            Value::String(v) => {
                11_u8.hash(state);
                v.hash(state);
            }
            Value::Bytes(v) => {
                12_u8.hash(state);
                v.hash(state);
            }
            Value::Bool(v) => {
                13_u8.hash(state);
                v.hash(state);
            }
            Value::Timestamp(v) => {
                14_u8.hash(state);
                v.hash(state);
            }
            Value::Struct(v) => {
                15_u8.hash(state);
                v.hash(state);
            }
            Value::List(v) => {
                16_u8.hash(state);
                v.hash(state);
            }
        }
    }
}
