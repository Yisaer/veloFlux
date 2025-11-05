use crate::types::{BooleanType, Float64Type, Int64Type, ListType, StringType, StructType, Uint8Type};
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
    /// 64-bit floating point number
    Float64(Float64Type),
    /// 64-bit signed integer
    Int64(Int64Type),
    /// 8-bit unsigned integer
    Uint8(Uint8Type),
    /// String type
    String(StringType),
    /// Struct type, containing field definitions
    Struct(StructType),
    /// List type, containing element type
    List(ListType),
    /// Boolean type
    Bool(BooleanType),
}
