#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

pub mod datatypes;
pub mod schema;
pub mod types;
pub mod value;

pub use datatypes::{ConcreteDatatype, DataType};
pub use schema::{ColumnSchema, Schema};
pub use types::{
    BooleanType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, ListType,
    StringType, StructField, StructType, Uint16Type, Uint32Type, Uint64Type, Uint8Type,
};
pub use value::{ListValue, StructValue, Value};
