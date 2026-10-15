//! Shared model metadata and prepared-model cache for veloFlux.

#![deny(unsafe_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use datatypes::{
    BooleanType, ConcreteDatatype, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, ListType, StringType, StructField, StructType, Uint16Type, Uint32Type, Uint64Type,
    Uint8Type,
};
use storage::{ModelSchemaColumn, StorageManager, StoredModel};

/// Immutable model metadata used by planning and runtime binding.
#[derive(Debug, Clone)]
pub struct ModelMetadata {
    pub model: Arc<StoredModel>,
    pub model_path: PathBuf,
}

impl ModelMetadata {
    pub fn input_types(&self) -> Result<Vec<ConcreteDatatype>, String> {
        self.model
            .inputs
            .iter()
            .map(model_schema_column_type)
            .collect()
    }

    pub fn output_type(&self) -> Result<ConcreteDatatype, String> {
        let fields = self
            .model
            .outputs
            .iter()
            .map(|column| {
                Ok(StructField::new(
                    column.name.clone(),
                    model_schema_column_type(column)?,
                    true,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ConcreteDatatype::Struct(StructType::new(Arc::new(fields))))
    }
}

fn model_schema_column_type(column: &ModelSchemaColumn) -> Result<ConcreteDatatype, String> {
    match column.data_type.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Ok(ConcreteDatatype::Bool(BooleanType)),
        "float32" => Ok(ConcreteDatatype::Float32(Float32Type)),
        "float64" => Ok(ConcreteDatatype::Float64(Float64Type)),
        "int8" => Ok(ConcreteDatatype::Int8(Int8Type)),
        "int16" => Ok(ConcreteDatatype::Int16(Int16Type)),
        "int32" => Ok(ConcreteDatatype::Int32(Int32Type)),
        "int64" => Ok(ConcreteDatatype::Int64(Int64Type)),
        "uint8" => Ok(ConcreteDatatype::Uint8(Uint8Type)),
        "uint16" => Ok(ConcreteDatatype::Uint16(Uint16Type)),
        "uint32" => Ok(ConcreteDatatype::Uint32(Uint32Type)),
        "uint64" => Ok(ConcreteDatatype::Uint64(Uint64Type)),
        "string" => Ok(ConcreteDatatype::String(StringType)),
        "list" => {
            let element = column
                .element
                .as_deref()
                .ok_or_else(|| format!("list column '{}' is missing element", column.name))?;
            Ok(ConcreteDatatype::List(ListType::new(Arc::new(
                model_schema_column_type(element)?,
            ))))
        }
        "struct" => {
            let fields = column
                .fields
                .as_ref()
                .ok_or_else(|| format!("struct column '{}' is missing fields", column.name))?
                .iter()
                .map(|field| {
                    Ok(StructField::new(
                        field.name.clone(),
                        model_schema_column_type(field)?,
                        false,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(ConcreteDatatype::Struct(StructType::new(Arc::new(fields))))
        }
        data_type => Err(format!(
            "model column '{}' uses unsupported data_type '{data_type}'",
            column.name
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ModelCacheKey {
    name: String,
    revision: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRuntimeStatus {
    pub revision: u64,
    pub model_sha256: String,
    pub prepared: bool,
    pub prepare_count: u64,
}

/// A parsed and optimized model shared by active pipeline runtimes.
#[derive(Debug)]
pub struct PreparedOnnxModel {
    metadata: Arc<ModelMetadata>,
    #[cfg(feature = "onnx")]
    runnable: Arc<tract_onnx::prelude::TypedSimplePlan>,
}

impl PreparedOnnxModel {
    pub fn metadata(&self) -> &ModelMetadata {
        self.metadata.as_ref()
    }

    #[cfg(feature = "onnx")]
    pub fn runnable(&self) -> &Arc<tract_onnx::prelude::TypedSimplePlan> {
        &self.runnable
    }

    #[cfg(feature = "onnx")]
    pub fn run_values(&self, inputs: &[datatypes::Value]) -> Result<datatypes::Value, String> {
        use tract_onnx::prelude::{IntoTValue, Runnable};

        if inputs.len() != self.runnable.input_count() {
            return Err(format!(
                "model '{}' expects {} tensor inputs, got {}",
                self.metadata.model.name,
                self.runnable.input_count(),
                inputs.len()
            ));
        }

        let tensors = inputs
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let schema = self
                    .metadata
                    .model
                    .inputs
                    .get(index)
                    .ok_or_else(|| format!("model input {index} has no metadata schema"))?;
                validate_input_value(value, schema, &schema.name)?;
                let input_fact = self
                    .runnable
                    .input_fact(index)
                    .map_err(|error| format!("read input {index} fact: {error}"))?;
                let shape = match input_fact.shape.as_concrete() {
                    Some(shape) => shape.to_vec(),
                    None => vec![flatten_value(value)?.len()],
                };
                build_input_tensor(value, input_fact.datum_type, &shape)
                    .map(IntoTValue::into_tvalue)
            })
            .collect::<Result<Vec<_>, String>>()?;

        let outputs = self
            .runnable
            .run(tensors.into())
            .map_err(|error| format!("run model '{}': {error}", self.metadata.model.name))?;
        if outputs.len() != self.metadata.model.outputs.len() {
            return Err(format!(
                "model '{}' returned {} outputs, metadata declares {}",
                self.metadata.model.name,
                outputs.len(),
                self.metadata.model.outputs.len()
            ));
        }

        let values = outputs
            .iter()
            .zip(self.metadata.model.outputs.iter())
            .map(|(tensor, schema)| tensor_to_value(tensor, schema))
            .collect::<Result<Vec<_>, String>>()?;
        let output_type = self.metadata.output_type()?;
        let datatypes::ConcreteDatatype::Struct(output_type) = output_type else {
            return Err("model output type is not a struct".to_string());
        };
        Ok(datatypes::Value::Struct(datatypes::StructValue::new(
            values,
            output_type,
        )))
    }
}

#[cfg(feature = "onnx")]
#[derive(Debug, Clone, PartialEq)]
enum ScalarValue {
    Bool(bool),
    Int(i128),
    Uint(u128),
    Float(f64),
    String(String),
}

#[cfg(feature = "onnx")]
fn validate_input_value(
    value: &datatypes::Value,
    schema: &ModelSchemaColumn,
    path: &str,
) -> Result<(), String> {
    match schema.data_type.to_ascii_lowercase().as_str() {
        "list" => {
            let datatypes::Value::List(values) = value else {
                return Err(format!("input '{path}' must be a list"));
            };
            if let Some(cardinality) = schema.cardinality {
                if values.len() != cardinality {
                    return Err(format!(
                        "input '{path}' expects list cardinality {cardinality}, got {}",
                        values.len()
                    ));
                }
            }
            let element = schema
                .element
                .as_deref()
                .ok_or_else(|| format!("input '{path}' list is missing element schema"))?;
            for (index, item) in values.items().iter().enumerate() {
                validate_input_value(item, element, &format!("{path}[{index}]"))?;
            }
            Ok(())
        }
        "struct" => {
            let datatypes::Value::Struct(values) = value else {
                return Err(format!("input '{path}' must be a struct"));
            };
            let fields = schema
                .fields
                .as_ref()
                .ok_or_else(|| format!("input '{path}' struct is missing fields schema"))?;
            if values.items().len() != fields.len() {
                return Err(format!(
                    "input '{path}' expects {} struct fields, got {}",
                    fields.len(),
                    values.items().len()
                ));
            }
            for (field, item) in fields.iter().zip(values.items()) {
                validate_input_value(item, field, &format!("{path}.{}", field.name))?;
            }
            Ok(())
        }
        _ if matches!(
            value,
            datatypes::Value::List(_) | datatypes::Value::Struct(_)
        ) =>
        {
            Err(format!("input '{path}' must be a scalar value"))
        }
        _ => Ok(()),
    }
}

#[cfg(feature = "onnx")]
fn flatten_value(value: &datatypes::Value) -> Result<Vec<ScalarValue>, String> {
    match value {
        datatypes::Value::Bool(value) => Ok(vec![ScalarValue::Bool(*value)]),
        datatypes::Value::Int8(value) => Ok(vec![ScalarValue::Int(*value as i128)]),
        datatypes::Value::Int16(value) => Ok(vec![ScalarValue::Int(*value as i128)]),
        datatypes::Value::Int32(value) => Ok(vec![ScalarValue::Int(*value as i128)]),
        datatypes::Value::Int64(value) => Ok(vec![ScalarValue::Int(*value as i128)]),
        datatypes::Value::Uint8(value) => Ok(vec![ScalarValue::Uint(*value as u128)]),
        datatypes::Value::Uint16(value) => Ok(vec![ScalarValue::Uint(*value as u128)]),
        datatypes::Value::Uint32(value) => Ok(vec![ScalarValue::Uint(*value as u128)]),
        datatypes::Value::Uint64(value) => Ok(vec![ScalarValue::Uint(*value as u128)]),
        datatypes::Value::Float32(value) => Ok(vec![ScalarValue::Float(*value as f64)]),
        datatypes::Value::Float64(value) => Ok(vec![ScalarValue::Float(*value)]),
        datatypes::Value::String(value) => Ok(vec![ScalarValue::String(value.as_ref().clone())]),
        datatypes::Value::List(values) => {
            values
                .items()
                .iter()
                .try_fold(Vec::new(), |mut flattened, value| {
                    flattened.extend(flatten_value(value)?);
                    Ok(flattened)
                })
        }
        datatypes::Value::Struct(value) => {
            value
                .items()
                .iter()
                .try_fold(Vec::new(), |mut flattened, value| {
                    flattened.extend(flatten_value(value)?);
                    Ok(flattened)
                })
        }
        datatypes::Value::Null => Err("null cannot be converted to an ONNX tensor".to_string()),
        other => Err(format!("unsupported ONNX tensor value: {other:?}")),
    }
}

#[cfg(feature = "onnx")]
fn build_input_tensor(
    value: &datatypes::Value,
    datum_type: tract_onnx::prelude::DatumType,
    shape: &[usize],
) -> Result<tract_onnx::prelude::Tensor, String> {
    use tract_onnx::prelude::{DatumType, IntoTensor, Tensor};

    let values = flatten_value(value)?;
    let expected = shape.iter().product::<usize>();
    if expected != values.len() {
        return Err(format!(
            "model input expects {expected} values with shape {shape:?}, got {}",
            values.len()
        ));
    }

    macro_rules! build_numeric {
        ($type:ty, $convert:expr) => {{
            let converted = values
                .iter()
                .map($convert)
                .collect::<Result<Vec<$type>, String>>()?;
            Tensor::from_shape(shape, &converted)
                .map_err(|error| format!("build ONNX input tensor: {error}"))
        }};
    }

    match datum_type {
        DatumType::Bool => build_numeric!(bool, scalar_to_bool),
        DatumType::I8 => build_numeric!(i8, scalar_to_i8),
        DatumType::I16 => build_numeric!(i16, scalar_to_i16),
        DatumType::I32 => build_numeric!(i32, scalar_to_i32),
        DatumType::I64 => build_numeric!(i64, scalar_to_i64),
        DatumType::U8 => build_numeric!(u8, scalar_to_u8),
        DatumType::U16 => build_numeric!(u16, scalar_to_u16),
        DatumType::U32 => build_numeric!(u32, scalar_to_u32),
        DatumType::U64 => build_numeric!(u64, scalar_to_u64),
        DatumType::F32 => build_numeric!(f32, scalar_to_f32),
        DatumType::F64 => build_numeric!(f64, scalar_to_f64),
        DatumType::String => {
            let converted = values
                .iter()
                .map(scalar_to_string)
                .collect::<Result<Vec<_>, String>>()?;
            tract_onnx::prelude::tract_ndarray::ArrayD::from_shape_vec(
                tract_onnx::prelude::tract_ndarray::IxDyn(shape),
                converted,
            )
            .map(|array| array.into_tensor())
            .map_err(|error| format!("build ONNX string input tensor: {error}"))
        }
        other => Err(format!("unsupported ONNX input tensor type {other:?}")),
    }
}

#[cfg(feature = "onnx")]
fn scalar_to_bool(value: &ScalarValue) -> Result<bool, String> {
    match value {
        ScalarValue::Bool(value) => Ok(*value),
        other => Err(format!("expected bool ONNX value, got {other:?}")),
    }
}

#[cfg(feature = "onnx")]
macro_rules! scalar_to_signed {
    ($name:ident, $type:ty, $min:expr, $max_exclusive:expr) => {
        fn $name(value: &ScalarValue) -> Result<$type, String> {
            match value {
                ScalarValue::Int(value) => (*value).try_into().map_err(|_| {
                    format!("integer value {value} does not fit {}", stringify!($type))
                }),
                ScalarValue::Uint(value) => (*value).try_into().map_err(|_| {
                    format!("unsigned value {value} does not fit {}", stringify!($type))
                }),
                ScalarValue::Float(value)
                    if value.is_finite()
                        && value.fract() == 0.0
                        && *value >= $min
                        && *value < $max_exclusive =>
                {
                    Ok(*value as $type)
                }
                other => Err(format!("expected numeric ONNX value, got {other:?}")),
            }
        }
    };
}

#[cfg(feature = "onnx")]
macro_rules! scalar_to_unsigned {
    ($name:ident, $type:ty, $max_exclusive:expr) => {
        fn $name(value: &ScalarValue) -> Result<$type, String> {
            match value {
                ScalarValue::Int(value) => (*value).try_into().map_err(|_| {
                    format!("integer value {value} does not fit {}", stringify!($type))
                }),
                ScalarValue::Uint(value) => (*value).try_into().map_err(|_| {
                    format!("unsigned value {value} does not fit {}", stringify!($type))
                }),
                ScalarValue::Float(value)
                    if value.is_finite()
                        && value.fract() == 0.0
                        && *value >= 0.0
                        && *value < $max_exclusive =>
                {
                    Ok(*value as $type)
                }
                other => Err(format!("expected numeric ONNX value, got {other:?}")),
            }
        }
    };
}

#[cfg(feature = "onnx")]
scalar_to_signed!(scalar_to_i8, i8, -128.0, 128.0);
#[cfg(feature = "onnx")]
scalar_to_signed!(scalar_to_i16, i16, -32768.0, 32768.0);
#[cfg(feature = "onnx")]
scalar_to_signed!(scalar_to_i32, i32, -2147483648.0, 2147483648.0);
#[cfg(feature = "onnx")]
scalar_to_signed!(
    scalar_to_i64,
    i64,
    -9223372036854775808.0,
    9223372036854775808.0
);
#[cfg(feature = "onnx")]
scalar_to_unsigned!(scalar_to_u8, u8, 256.0);
#[cfg(feature = "onnx")]
scalar_to_unsigned!(scalar_to_u16, u16, 65536.0);
#[cfg(feature = "onnx")]
scalar_to_unsigned!(scalar_to_u32, u32, 4294967296.0);
#[cfg(feature = "onnx")]
scalar_to_unsigned!(scalar_to_u64, u64, 18446744073709551616.0);

#[cfg(feature = "onnx")]
macro_rules! exact_signed_to_float {
    ($name:ident, $float:ty) => {
        fn $name(value: i128) -> Result<$float, String> {
            let converted = value as $float;
            if converted.is_finite() && (converted as i128) == value {
                Ok(converted)
            } else {
                Err(format!(
                    "integer value {value} cannot be represented exactly as {}",
                    stringify!($float)
                ))
            }
        }
    };
}

#[cfg(feature = "onnx")]
macro_rules! exact_unsigned_to_float {
    ($name:ident, $float:ty) => {
        fn $name(value: u128) -> Result<$float, String> {
            let converted = value as $float;
            if converted.is_finite() && (converted as u128) == value {
                Ok(converted)
            } else {
                Err(format!(
                    "unsigned value {value} cannot be represented exactly as {}",
                    stringify!($float)
                ))
            }
        }
    };
}

#[cfg(feature = "onnx")]
exact_signed_to_float!(signed_to_f32, f32);
#[cfg(feature = "onnx")]
exact_signed_to_float!(signed_to_f64, f64);
#[cfg(feature = "onnx")]
exact_unsigned_to_float!(unsigned_to_f32, f32);
#[cfg(feature = "onnx")]
exact_unsigned_to_float!(unsigned_to_f64, f64);

#[cfg(feature = "onnx")]
fn scalar_to_f32(value: &ScalarValue) -> Result<f32, String> {
    match value {
        ScalarValue::Int(value) => signed_to_f32(*value),
        ScalarValue::Uint(value) => unsigned_to_f32(*value),
        ScalarValue::Float(value) if value.is_finite() => {
            let converted = *value as f32;
            if converted.is_finite() && converted as f64 == *value {
                Ok(converted)
            } else {
                Err(format!(
                    "float value {value} cannot be represented exactly as f32"
                ))
            }
        }
        other => Err(format!("expected numeric ONNX value, got {other:?}")),
    }
}

#[cfg(feature = "onnx")]
fn scalar_to_f64(value: &ScalarValue) -> Result<f64, String> {
    match value {
        ScalarValue::Int(value) => signed_to_f64(*value),
        ScalarValue::Uint(value) => unsigned_to_f64(*value),
        ScalarValue::Float(value) if value.is_finite() => Ok(*value),
        other => Err(format!("expected numeric ONNX value, got {other:?}")),
    }
}

#[cfg(feature = "onnx")]
fn scalar_to_string(value: &ScalarValue) -> Result<String, String> {
    match value {
        ScalarValue::String(value) => Ok(value.clone()),
        other => Err(format!("expected string ONNX value, got {other:?}")),
    }
}

#[cfg(feature = "onnx")]
fn scalar_to_value(
    value: &ScalarValue,
    schema: &ModelSchemaColumn,
) -> Result<datatypes::Value, String> {
    macro_rules! scalar_value {
        ($converter:ident, $variant:ident) => {
            $converter(value).map(datatypes::Value::$variant)
        };
    }
    match schema.data_type.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => scalar_value!(scalar_to_bool, Bool),
        "int8" => scalar_value!(scalar_to_i8, Int8),
        "int16" => scalar_value!(scalar_to_i16, Int16),
        "int32" => scalar_value!(scalar_to_i32, Int32),
        "int64" => scalar_value!(scalar_to_i64, Int64),
        "uint8" => scalar_value!(scalar_to_u8, Uint8),
        "uint16" => scalar_value!(scalar_to_u16, Uint16),
        "uint32" => scalar_value!(scalar_to_u32, Uint32),
        "uint64" => scalar_value!(scalar_to_u64, Uint64),
        "float32" => scalar_value!(scalar_to_f32, Float32),
        "float64" => scalar_value!(scalar_to_f64, Float64),
        "string" => scalar_to_string(value).map(|value| datatypes::Value::String(Arc::new(value))),
        data_type => Err(format!("unsupported ONNX output data_type '{data_type}'")),
    }
}

#[cfg(feature = "onnx")]
fn tensor_to_value(
    tensor: &tract_onnx::prelude::TValue,
    schema: &ModelSchemaColumn,
) -> Result<datatypes::Value, String> {
    let scalar_values = read_tensor_scalars(tensor)?;
    let mut offset = 0;
    let value = decode_schema_value(schema, &scalar_values, &mut offset, tensor.shape())?;
    if offset != scalar_values.len() {
        return Err(format!(
            "output schema '{}' consumed {offset} of {} tensor values",
            schema.name,
            scalar_values.len()
        ));
    }
    Ok(value)
}

#[cfg(feature = "onnx")]
fn read_tensor_scalars(tensor: &tract_onnx::prelude::TValue) -> Result<Vec<ScalarValue>, String> {
    use tract_onnx::prelude::DatumType;
    let plain = tensor
        .try_as_plain_ram()
        .map_err(|error| format!("read output tensor: {error}"))?;
    macro_rules! read_values {
        ($type:ty, $map:expr) => {
            plain
                .as_slice::<$type>()
                .map_err(|error| format!("read output tensor: {error}"))?
                .iter()
                .map($map)
                .collect()
        };
    }
    Ok(match tensor.datum_type() {
        DatumType::Bool => read_values!(bool, |value| ScalarValue::Bool(*value)),
        DatumType::I8 => read_values!(i8, |value| ScalarValue::Int(*value as i128)),
        DatumType::I16 => read_values!(i16, |value| ScalarValue::Int(*value as i128)),
        DatumType::I32 => read_values!(i32, |value| ScalarValue::Int(*value as i128)),
        DatumType::I64 => read_values!(i64, |value| ScalarValue::Int(*value as i128)),
        DatumType::U8 => read_values!(u8, |value| ScalarValue::Uint(*value as u128)),
        DatumType::U16 => read_values!(u16, |value| ScalarValue::Uint(*value as u128)),
        DatumType::U32 => read_values!(u32, |value| ScalarValue::Uint(*value as u128)),
        DatumType::U64 => read_values!(u64, |value| ScalarValue::Uint(*value as u128)),
        DatumType::F32 => read_values!(f32, |value| ScalarValue::Float(*value as f64)),
        DatumType::F64 => read_values!(f64, |value| ScalarValue::Float(*value)),
        DatumType::String => plain
            .as_slice::<String>()
            .map_err(|error| format!("read output tensor: {error}"))?
            .iter()
            .cloned()
            .map(ScalarValue::String)
            .collect(),
        other => return Err(format!("unsupported ONNX output tensor type {other:?}")),
    })
}

#[cfg(feature = "onnx")]
fn decode_schema_value(
    schema: &ModelSchemaColumn,
    values: &[ScalarValue],
    offset: &mut usize,
    shape: &[usize],
) -> Result<datatypes::Value, String> {
    let list_depth = list_nesting_depth(schema);
    decode_schema_value_with_depth(schema, values, offset, shape, list_depth, list_depth)
}

#[cfg(feature = "onnx")]
fn decode_schema_value_with_depth(
    schema: &ModelSchemaColumn,
    values: &[ScalarValue],
    offset: &mut usize,
    shape: &[usize],
    list_depth: usize,
    root_list_depth: usize,
) -> Result<datatypes::Value, String> {
    match schema.data_type.to_ascii_lowercase().as_str() {
        "list" => {
            let element = schema
                .element
                .as_deref()
                .ok_or_else(|| format!("list output '{}' is missing element", schema.name))?;
            let length = if let Some(cardinality) = schema.cardinality {
                cardinality
            } else if root_list_depth <= 1 {
                values.len().saturating_sub(*offset)
            } else {
                let dimension = shape
                    .len()
                    .checked_sub(list_depth)
                    .and_then(|index| shape.get(index).copied())
                    .ok_or_else(|| {
                        format!(
                            "output '{}' shape {shape:?} cannot describe nested list depth {list_depth}",
                            schema.name
                        )
                    })?;
                dimension
            };
            let mut items = Vec::with_capacity(length);
            for _ in 0..length {
                items.push(decode_schema_value_with_depth(
                    element,
                    values,
                    offset,
                    shape,
                    list_depth.saturating_sub(1),
                    root_list_depth,
                )?);
            }
            let item_type = model_schema_column_type(element)?;
            Ok(datatypes::Value::List(datatypes::ListValue::new(
                items,
                Arc::new(item_type),
            )))
        }
        "struct" => {
            let fields = schema
                .fields
                .as_ref()
                .ok_or_else(|| format!("struct output '{}' is missing fields", schema.name))?;
            let mut items = Vec::with_capacity(fields.len());
            for field in fields {
                items.push(decode_schema_value_with_depth(
                    field,
                    values,
                    offset,
                    shape,
                    list_nesting_depth(field),
                    root_list_depth,
                )?);
            }
            let struct_type = model_schema_column_type(schema)?;
            let ConcreteDatatype::Struct(struct_type) = struct_type else {
                return Err(format!("output '{}' is not a struct", schema.name));
            };
            Ok(datatypes::Value::Struct(datatypes::StructValue::new(
                items,
                struct_type,
            )))
        }
        _ => {
            let value = values
                .get(*offset)
                .ok_or_else(|| format!("output '{}' tensor is missing a scalar", schema.name))?;
            *offset += 1;
            scalar_to_value(value, schema)
        }
    }
}

#[cfg(feature = "onnx")]
fn list_nesting_depth(schema: &ModelSchemaColumn) -> usize {
    if schema.data_type.eq_ignore_ascii_case("list") {
        schema
            .element
            .as_deref()
            .map(list_nesting_depth)
            .unwrap_or(0)
            .saturating_add(1)
    } else {
        schema
            .fields
            .as_deref()
            .map(|fields| fields.iter().map(list_nesting_depth).max().unwrap_or(0))
            .unwrap_or(0)
    }
}

/// Process-wide model metadata registry and shared prepared-model cache.
pub struct ModelRegistry {
    models_dir: PathBuf,
    metadata: RwLock<HashMap<String, Arc<ModelMetadata>>>,
    prepared: Mutex<HashMap<ModelCacheKey, Weak<PreparedOnnxModel>>>,
    in_flight: Mutex<HashMap<ModelCacheKey, Arc<OnceLock<Result<Arc<PreparedOnnxModel>, String>>>>>,
    prepare_counts: Mutex<HashMap<ModelCacheKey, u64>>,
}

impl ModelRegistry {
    pub fn new(models_dir: impl Into<PathBuf>) -> Self {
        Self {
            models_dir: models_dir.into(),
            metadata: RwLock::new(HashMap::new()),
            prepared: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            prepare_counts: Mutex::new(HashMap::new()),
        }
    }

    pub fn sync_from_storage(&self, storage: &StorageManager) -> Result<(), String> {
        let models = storage
            .list_models()
            .map_err(|error| format!("list models for runtime registry: {error}"))?;
        let mut metadata = self
            .metadata
            .write()
            .map_err(|_| "model metadata registry lock poisoned".to_string())?;
        metadata.clear();
        for model in models {
            let name = model.name.clone();
            metadata.insert(name, Arc::new(self.metadata_for(model)));
        }
        self.prepare_counts
            .lock()
            .map_err(|_| "model prepare counter lock poisoned".to_string())?
            .clear();
        Ok(())
    }

    pub fn upsert(&self, model: StoredModel) -> Result<(), String> {
        let name = model.name.clone();
        let mut metadata = self
            .metadata
            .write()
            .map_err(|_| "model metadata registry lock poisoned".to_string())?;
        metadata.insert(name.clone(), Arc::new(self.metadata_for(model)));
        self.prepare_counts
            .lock()
            .map_err(|_| "model prepare counter lock poisoned".to_string())?
            .retain(|key, _| key.name != name);
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<(), String> {
        let mut metadata = self
            .metadata
            .write()
            .map_err(|_| "model metadata registry lock poisoned".to_string())?;
        metadata.remove(name);
        self.prepare_counts
            .lock()
            .map_err(|_| "model prepare counter lock poisoned".to_string())?
            .retain(|key, _| key.name != name);
        Ok(())
    }

    pub fn metadata(&self, name: &str) -> Result<Option<Arc<ModelMetadata>>, String> {
        let metadata = self
            .metadata
            .read()
            .map_err(|_| "model metadata registry lock poisoned".to_string())?;
        Ok(metadata.get(name).cloned())
    }

    /// Acquire a prepared model for a pipeline runtime.
    ///
    /// Prepared models are weakly cached while active pipeline runtimes own the strong references.
    /// Preparation is deduplicated per model key without holding the cache lock while parsing and
    /// optimizing the ONNX graph.
    pub fn acquire(&self, name: &str) -> Result<Arc<PreparedOnnxModel>, String> {
        let metadata = self
            .metadata(name)?
            .ok_or_else(|| format!("model '{name}' is not registered"))?;
        let key = ModelCacheKey {
            name: metadata.model.name.clone(),
            revision: metadata.model.revision,
            sha256: metadata.model.model_sha256.clone(),
        };

        if let Some(model) = self
            .prepared
            .lock()
            .map_err(|_| "prepared model cache lock poisoned".to_string())?
            .get(&key)
            .and_then(Weak::upgrade)
        {
            return Ok(model);
        }

        let loading = {
            let mut in_flight = self
                .in_flight
                .lock()
                .map_err(|_| "in-flight model cache lock poisoned".to_string())?;
            in_flight
                .entry(key.clone())
                .or_insert_with(|| Arc::new(OnceLock::new()))
                .clone()
        };

        let result = loading
            .get_or_init(|| {
                let model = Arc::new(self.prepare(metadata)?);
                self.prepared
                    .lock()
                    .map_err(|_| "prepared model cache lock poisoned".to_string())?
                    .insert(key.clone(), Arc::downgrade(&model));
                let mut prepare_counts = self
                    .prepare_counts
                    .lock()
                    .map_err(|_| "model prepare counter lock poisoned".to_string())?;
                *prepare_counts.entry(key.clone()).or_default() += 1;
                Ok(model)
            })
            .clone();

        let mut in_flight = self
            .in_flight
            .lock()
            .map_err(|_| "in-flight model cache lock poisoned".to_string())?;
        if in_flight
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &loading))
        {
            in_flight.remove(&key);
        }
        result
    }

    pub fn status(&self, name: &str) -> Result<Option<ModelRuntimeStatus>, String> {
        let Some(metadata) = self.metadata(name)? else {
            return Ok(None);
        };
        let key = ModelCacheKey {
            name: metadata.model.name.clone(),
            revision: metadata.model.revision,
            sha256: metadata.model.model_sha256.clone(),
        };
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared model cache lock poisoned".to_string())?;
        let is_prepared = prepared.get(&key).and_then(Weak::upgrade).is_some();
        prepared.retain(|_, cached| cached.strong_count() > 0);
        let prepare_count = self
            .prepare_counts
            .lock()
            .map_err(|_| "model prepare counter lock poisoned".to_string())?
            .get(&key)
            .copied()
            .unwrap_or(0);
        Ok(Some(ModelRuntimeStatus {
            revision: key.revision,
            model_sha256: key.sha256,
            prepared: is_prepared,
            prepare_count,
        }))
    }

    /// Backward-compatible alias for callers that still use the old name.
    pub fn load(&self, name: &str) -> Result<Arc<PreparedOnnxModel>, String> {
        self.acquire(name)
    }

    fn metadata_for(&self, model: StoredModel) -> ModelMetadata {
        ModelMetadata {
            model: Arc::new(model.clone()),
            model_path: self.models_dir.join(model.name).join("model.onnx"),
        }
    }

    fn prepare(&self, metadata: Arc<ModelMetadata>) -> Result<PreparedOnnxModel, String> {
        #[cfg(feature = "onnx")]
        {
            use tract_onnx::prelude::{Framework, InferenceModelExt, IntoRunnable};
            let runnable = tract_onnx::onnx()
                .model_for_path(&metadata.model_path)
                .map_err(|error| {
                    format!(
                        "load model '{}' from {}: {error}",
                        metadata.model.name,
                        metadata.model_path.display()
                    )
                })?
                .into_optimized()
                .map_err(|error| format!("optimize model '{}': {error}", metadata.model.name))?
                .into_runnable()
                .map_err(|error| format!("prepare model '{}': {error}", metadata.model.name))?;
            return Ok(PreparedOnnxModel { metadata, runnable });
        }

        #[cfg(not(feature = "onnx"))]
        {
            let _ = metadata;
            Err("ONNX model runtime is disabled".to_string())
        }
    }
}

#[cfg(all(test, feature = "onnx"))]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tract_onnx::prelude::{DatumType, IntoTValue, Tensor};

    fn column(name: &str, data_type: &str) -> ModelSchemaColumn {
        ModelSchemaColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            cardinality: None,
            fields: None,
            element: None,
        }
    }

    fn identity_pairs_column(name: &str) -> ModelSchemaColumn {
        let mut pair = column("pair", "struct");
        pair.fields = Some(vec![column("left", "float32"), column("right", "float32")]);
        let mut pairs = column(name, "list");
        pairs.cardinality = Some(2);
        pairs.element = Some(Box::new(pair));
        pairs
    }

    fn identity_input(seed: f32) -> datatypes::Value {
        let pair_type = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![])));
        let pair = |offset| {
            datatypes::Value::Struct(datatypes::StructValue::new(
                vec![
                    datatypes::Value::Float32(seed + offset),
                    datatypes::Value::Float32(seed + offset + 0.5),
                ],
                StructType::new(Arc::new(vec![])),
            ))
        };
        datatypes::Value::List(datatypes::ListValue::new(
            vec![pair(0.0), pair(1.0)],
            Arc::new(pair_type),
        ))
    }

    fn test_registry() -> (Arc<ModelRegistry>, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "veloflux-model-runtime-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let model_dir = root.join("identity");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.onnx"),
            include_bytes!("../../manager/src/model_testdata/identity-4.onnx"),
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new(&root));
        registry
            .upsert(StoredModel {
                name: "identity".to_string(),
                revision: 1,
                model_sha256: "identity-test".to_string(),
                model_size_bytes: 0,
                model_card_yaml: String::new(),
                inputs: vec![identity_pairs_column("pairs")],
                outputs: vec![identity_pairs_column("pairs")],
                test_vector_count: 0,
            })
            .unwrap();
        (registry, root)
    }

    #[test]
    fn prepared_model_is_reused_across_acquires() {
        let (registry, root) = test_registry();
        let first = registry.acquire("identity").unwrap();
        let second = registry.acquire("identity").unwrap();
        assert_eq!(Arc::strong_count(&second), 2);
        assert_eq!(
            registry.status("identity").unwrap().unwrap().prepare_count,
            1
        );
        assert!(registry.status("identity").unwrap().unwrap().prepared);
        drop(first);
        drop(second);
        assert!(!registry.status("identity").unwrap().unwrap().prepared);
        let third = registry.acquire("identity").unwrap();
        assert_eq!(
            registry.status("identity").unwrap().unwrap().prepare_count,
            2
        );
        drop(third);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_acquires_prepare_a_model_once() {
        let (registry, root) = test_registry();
        let barrier = Arc::new(Barrier::new(8));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                registry.acquire("identity").unwrap()
            }));
        }
        let models = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        for model in &models[1..] {
            assert!(Arc::ptr_eq(&models[0], model));
        }
        assert_eq!(
            registry.status("identity").unwrap().unwrap().prepare_count,
            1
        );
        drop(models);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_runs_share_one_prepared_identity_model() {
        const THREAD_COUNT: usize = 8;

        let (registry, root) = test_registry();
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));
        let mut threads = Vec::with_capacity(THREAD_COUNT);
        for index in 0..THREAD_COUNT {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                let model = registry.acquire("identity").unwrap();
                let input = identity_input(index as f32);
                let output = model.run_values(std::slice::from_ref(&input)).unwrap();
                (input, output)
            }));
        }

        let results = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        for (input, output) in results {
            let datatypes::Value::List(input_pairs) = input else {
                panic!("test input must be a list");
            };
            let datatypes::Value::Struct(output_record) = output else {
                panic!("identity output must be a struct");
            };
            let datatypes::Value::List(output_pairs) = &output_record.items()[0] else {
                panic!("identity output field must be a list");
            };
            assert_eq!(output_pairs.len(), input_pairs.len());
            for (input_pair, output_pair) in input_pairs.items().iter().zip(output_pairs.items()) {
                let datatypes::Value::Struct(input_pair) = input_pair else {
                    panic!("test input pair must be a struct");
                };
                let datatypes::Value::Struct(output_pair) = output_pair else {
                    panic!("identity output pair must be a struct");
                };
                assert_eq!(output_pair.items(), input_pair.items());
            }
        }
        assert_eq!(
            registry.status("identity").unwrap().unwrap().prepare_count,
            1
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn input_flatten_supports_bool_integer_string_and_nested_values() {
        let value = datatypes::Value::Struct(datatypes::StructValue::new(
            vec![
                datatypes::Value::Bool(true),
                datatypes::Value::Int32(7),
                datatypes::Value::String(Arc::new("ok".to_string())),
            ],
            datatypes::StructType::new(Arc::new(vec![])),
        ));
        let values = flatten_value(&value).unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], ScalarValue::Bool(true));
        assert_eq!(values[1], ScalarValue::Int(7));
        assert_eq!(values[2], ScalarValue::String("ok".to_string()));

        let tensor = build_input_tensor(
            &datatypes::Value::List(datatypes::ListValue::new(
                vec![datatypes::Value::Int32(1), datatypes::Value::Int32(2)],
                Arc::new(ConcreteDatatype::Int32(Int32Type)),
            )),
            DatumType::I32,
            &[2],
        )
        .unwrap();
        assert_eq!(
            read_tensor_scalars(&tensor.into_tvalue()).unwrap(),
            vec![ScalarValue::Int(1), ScalarValue::Int(2)]
        );
    }

    #[test]
    fn input_validation_rejects_mismatched_composite_cardinality() {
        let mut item = column("item", "struct");
        item.fields = Some(vec![column("a", "int32"), column("b", "int32")]);
        let mut pairs = column("pairs", "list");
        pairs.cardinality = Some(2);
        pairs.element = Some(Box::new(item));

        let struct_value = || {
            datatypes::Value::Struct(datatypes::StructValue::new(
                vec![datatypes::Value::Int32(1), datatypes::Value::Int32(2)],
                datatypes::StructType::new(Arc::new(vec![])),
            ))
        };
        let value = datatypes::Value::List(datatypes::ListValue::new(
            vec![struct_value(), struct_value(), struct_value()],
            Arc::new(ConcreteDatatype::Struct(StructType::new(Arc::new(vec![])))),
        ));
        let error = validate_input_value(&value, &pairs, "pairs").unwrap_err();
        assert!(
            error.contains("expects list cardinality 2, got 3"),
            "{error}"
        );

        let mut values = column("values", "list");
        values.cardinality = Some(2);
        values.element = Some(Box::new(column("value", "int32")));
        let mut record = column("record", "struct");
        record.fields = Some(vec![values]);
        let value = datatypes::Value::Struct(datatypes::StructValue::new(
            vec![datatypes::Value::List(datatypes::ListValue::new(
                vec![datatypes::Value::Int32(1)],
                Arc::new(ConcreteDatatype::Int32(Int32Type)),
            ))],
            StructType::new(Arc::new(vec![])),
        ));
        let error = validate_input_value(&value, &record, "record").unwrap_err();
        assert!(error.contains("record.values"), "{error}");
    }

    #[test]
    fn output_decode_supports_bool_integer_string_and_nested_lists() {
        let bool_schema = column("enabled", "bool");
        let bool_tensor = Tensor::from_shape(&[1], &[true]).unwrap().into_tvalue();
        assert_eq!(
            tensor_to_value(&bool_tensor, &bool_schema).unwrap(),
            datatypes::Value::Bool(true)
        );

        let mut nested_schema = column("values", "list");
        nested_schema.cardinality = Some(2);
        let mut inner = column("inner", "list");
        inner.cardinality = Some(2);
        inner.element = Some(Box::new(column("item", "int32")));
        nested_schema.element = Some(Box::new(inner));
        let nested_tensor = Tensor::from_shape(&[2, 2], &[1i32, 2, 3, 4])
            .unwrap()
            .into_tvalue();
        let decoded = tensor_to_value(&nested_tensor, &nested_schema).unwrap();
        let datatypes::Value::List(outer) = decoded else {
            panic!("expected outer list");
        };
        assert_eq!(outer.len(), 2);
        assert_eq!(
            outer.get(0).unwrap().datatype(),
            ConcreteDatatype::List(ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type))))
        );
    }

    #[test]
    fn output_decode_preserves_composite_boundaries_with_cardinality() {
        let mut item = column("item", "struct");
        item.fields = Some(vec![
            column("class_id", "int32"),
            column("score", "float32"),
        ]);
        let mut items = column("items", "list");
        items.cardinality = Some(2);
        items.element = Some(Box::new(item));
        let tensor = Tensor::from_shape(&[4], &[1i32, 10, 2, 20])
            .unwrap()
            .into_tvalue();
        let decoded = tensor_to_value(&tensor, &items).unwrap();
        let datatypes::Value::List(decoded) = decoded else {
            panic!("expected list output");
        };
        assert_eq!(decoded.len(), 2);
        let datatypes::Value::Struct(first) = decoded.get(0).unwrap() else {
            panic!("expected struct list element");
        };
        assert_eq!(first.items()[0], datatypes::Value::Int32(1));
        assert_eq!(first.items()[1], datatypes::Value::Float32(10.0));

        let mut values = column("values", "list");
        values.cardinality = Some(2);
        values.element = Some(Box::new(column("value", "int32")));
        let mut record = column("record", "struct");
        record.fields = Some(vec![column("id", "int32"), values]);
        let tensor = Tensor::from_shape(&[3], &[7i32, 8, 9])
            .unwrap()
            .into_tvalue();
        let decoded = tensor_to_value(&tensor, &record).unwrap();
        let datatypes::Value::Struct(decoded) = decoded else {
            panic!("expected struct output");
        };
        let datatypes::Value::List(values) = &decoded.items()[1] else {
            panic!("expected list struct field");
        };
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn numeric_conversions_reject_lossy_values() {
        assert!(scalar_to_i32(&ScalarValue::Float(1.9)).is_err());
        assert!(scalar_to_u8(&ScalarValue::Float(300.0)).is_err());
        assert!(scalar_to_i64(&ScalarValue::Float(f64::NAN)).is_err());
        assert!(scalar_to_f32(&ScalarValue::Int(16_777_217)).is_err());
        assert!(scalar_to_f64(&ScalarValue::Int(9_007_199_254_740_993)).is_err());
        assert_eq!(scalar_to_i32(&ScalarValue::Float(42.0)).unwrap(), 42);
        assert_eq!(
            scalar_to_f32(&ScalarValue::Int(16_777_216)).unwrap(),
            16_777_216.0
        );
    }
}
