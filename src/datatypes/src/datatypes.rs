use crate::types::{
    BooleanType, BytesType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    ListType, StringType, StructType, TimestampType, Uint16Type, Uint32Type, Uint64Type, Uint8Type,
};
use crate::value::Value;

/// Data type abstraction trait
pub trait DataType: std::fmt::Debug + Send + Sync {
    /// Name of this data type
    fn name(&self) -> String;

    /// Returns the default value of this type
    fn default_value(&self) -> Value;

    /// Casts the value to this DataType
    /// Returns None if cast failed
    fn try_cast(&self, from: Value) -> Option<Value>;
}

/// Concrete data type definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConcreteDatatype {
    /// Null type
    Null,
    /// 32-bit floating point number
    Float32(Float32Type),
    /// 64-bit floating point number
    Float64(Float64Type),
    /// 8-bit signed integer
    Int8(Int8Type),
    /// 16-bit signed integer
    Int16(Int16Type),
    /// 32-bit signed integer
    Int32(Int32Type),
    /// 64-bit signed integer
    Int64(Int64Type),
    /// 8-bit unsigned integer
    Uint8(Uint8Type),
    /// 16-bit unsigned integer
    Uint16(Uint16Type),
    /// 32-bit unsigned integer
    Uint32(Uint32Type),
    /// 64-bit unsigned integer
    Uint64(Uint64Type),
    /// String type
    String(StringType),
    /// Opaque bytes type
    Bytes(BytesType),
    /// Struct type, containing field definitions
    Struct(StructType),
    /// List type, containing element type
    List(ListType),
    /// Boolean type
    Bool(BooleanType),
    /// UTC timestamp type stored as Unix epoch microseconds
    Timestamp(TimestampType),
}
