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

/// ONNX tensor metadata used by planning and row-based execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TensorSignature {
    pub name: String,
    pub element_type: ConcreteDatatype,
    /// `None` means that no tensor shape metadata is available. `Some` preserves the graph rank;
    /// an inner `None` represents a symbolic dimension.
    pub shape: Option<Vec<Option<usize>>>,
}

/// Input/output signatures extracted from an ONNX graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelSignatures {
    pub inputs: Vec<TensorSignature>,
    pub outputs: Vec<TensorSignature>,
}

/// Convert tensor signatures to the flat compatibility representation used by storage and API
/// responses. This representation intentionally does not encode tensor rank or dimensions.
pub fn flat_schema_columns(
    signatures: &[TensorSignature],
) -> Result<Vec<ModelSchemaColumn>, String> {
    signatures
        .iter()
        .map(|signature| {
            let data_type = scalar_data_type_name(&signature.element_type)?;
            let column = if signature.shape.as_ref().is_some_and(Vec::is_empty) {
                ModelSchemaColumn {
                    name: signature.name.clone(),
                    data_type: data_type.to_string(),
                    cardinality: None,
                    fields: None,
                    element: None,
                }
            } else {
                ModelSchemaColumn {
                    name: signature.name.clone(),
                    data_type: "list".to_string(),
                    cardinality: None,
                    fields: None,
                    element: Some(Box::new(ModelSchemaColumn {
                        name: "element".to_string(),
                        data_type: data_type.to_string(),
                        cardinality: None,
                        fields: None,
                        element: None,
                    })),
                }
            };
            Ok(column)
        })
        .collect()
}

fn scalar_data_type_name(data_type: &ConcreteDatatype) -> Result<&'static str, String> {
    match data_type {
        ConcreteDatatype::Bool(_) => Ok("bool"),
        ConcreteDatatype::Float32(_) => Ok("float32"),
        ConcreteDatatype::Float64(_) => Ok("float64"),
        ConcreteDatatype::Int8(_) => Ok("int8"),
        ConcreteDatatype::Int16(_) => Ok("int16"),
        ConcreteDatatype::Int32(_) => Ok("int32"),
        ConcreteDatatype::Int64(_) => Ok("int64"),
        ConcreteDatatype::Uint8(_) => Ok("uint8"),
        ConcreteDatatype::Uint16(_) => Ok("uint16"),
        ConcreteDatatype::Uint32(_) => Ok("uint32"),
        ConcreteDatatype::Uint64(_) => Ok("uint64"),
        ConcreteDatatype::String(_) => Ok("string"),
        other => Err(format!("non-scalar ONNX signature type: {other:?}")),
    }
}

/// Immutable model metadata used by planning and runtime binding.
#[derive(Debug, Clone)]
pub struct ModelMetadata {
    pub model: Arc<StoredModel>,
    pub model_path: PathBuf,
    pub inputs: Vec<TensorSignature>,
    pub outputs: Vec<TensorSignature>,
}

impl ModelMetadata {
    pub fn input_types(&self) -> Result<Vec<ConcreteDatatype>, String> {
        self.inputs.iter().map(signature_value_type).collect()
    }

    pub fn output_type(&self) -> Result<ConcreteDatatype, String> {
        let fields = self
            .outputs
            .iter()
            .map(|signature| {
                Ok(StructField::new(
                    signature.name.clone(),
                    signature_value_type(signature)?,
                    true,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ConcreteDatatype::Struct(StructType::new(Arc::new(fields))))
    }
}

fn signature_value_type(signature: &TensorSignature) -> Result<ConcreteDatatype, String> {
    if signature.shape.as_ref().is_some_and(Vec::is_empty) {
        Ok(signature.element_type.clone())
    } else {
        Ok(ConcreteDatatype::List(ListType::new(Arc::new(
            signature.element_type.clone(),
        ))))
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

type InFlightPreparation = Arc<OnceLock<Result<Arc<PreparedOnnxModel>, String>>>;

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
                let signature = self
                    .metadata
                    .inputs
                    .get(index)
                    .ok_or_else(|| format!("model input {index} has no metadata signature"))?;
                validate_flat_input_value(value, signature, &signature.name)?;
                let input_fact = self
                    .runnable
                    .input_fact(index)
                    .map_err(|error| format!("read input {index} fact: {error}"))?;
                let shape = resolve_input_shape(value, signature.shape.as_ref())?;
                build_input_tensor(value, input_fact.datum_type, &shape)
                    .map(IntoTValue::into_tvalue)
            })
            .collect::<Result<Vec<_>, String>>()?;

        let outputs = self
            .runnable
            .run(tensors.into())
            .map_err(|error| format!("run model '{}': {error}", self.metadata.model.name))?;
        if outputs.len() != self.metadata.outputs.len() {
            return Err(format!(
                "model '{}' returned {} outputs, metadata declares {}",
                self.metadata.model.name,
                outputs.len(),
                self.metadata.outputs.len()
            ));
        }

        let values = outputs
            .iter()
            .zip(self.metadata.outputs.iter())
            .map(|(tensor, signature)| tensor_to_flat_value(tensor, signature))
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
fn validate_flat_input_value(
    value: &datatypes::Value,
    signature: &TensorSignature,
    path: &str,
) -> Result<(), String> {
    let is_scalar = signature.shape.as_ref().is_some_and(Vec::is_empty);
    if is_scalar {
        if matches!(
            value,
            datatypes::Value::List(_) | datatypes::Value::Struct(_)
        ) {
            return Err(format!("input '{path}' must be a scalar value"));
        }
    } else {
        let datatypes::Value::List(values) = value else {
            return Err(format!("input '{path}' must be a flat list"));
        };
        if values.items().iter().any(|item| {
            matches!(
                item,
                datatypes::Value::List(_) | datatypes::Value::Struct(_)
            )
        }) {
            return Err(format!("input '{path}' must be a flat list"));
        }
    }
    Ok(())
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
fn resolve_input_shape(
    value: &datatypes::Value,
    shape: Option<&Vec<Option<usize>>>,
) -> Result<Vec<usize>, String> {
    let value_len = flatten_value(value)?.len();
    let Some(shape) = shape else {
        return Ok(vec![value_len]);
    };
    if shape.is_empty() {
        if value_len != 1 {
            return Err(format!(
                "model input expects one scalar value, got {value_len} values"
            ));
        }
        return Ok(Vec::new());
    }

    let symbolic = shape
        .iter()
        .enumerate()
        .filter_map(|(index, dimension)| dimension.is_none().then_some(index))
        .collect::<Vec<_>>();
    if symbolic.len() > 1 {
        return Err(format!(
            "model input shape {shape:?} has multiple symbolic dimensions and cannot be inferred from a flat value"
        ));
    }

    let fixed_product = shape
        .iter()
        .flatten()
        .try_fold(1usize, |product, dimension| product.checked_mul(*dimension))
        .ok_or_else(|| format!("model input shape {shape:?} is too large"))?;
    let mut resolved = shape.iter().flatten().copied().collect::<Vec<_>>();
    if let Some(&symbolic_index) = symbolic.first() {
        if fixed_product == 0 || value_len % fixed_product != 0 {
            return Err(format!(
                "model input shape {shape:?} cannot resolve from {} values",
                value_len
            ));
        }
        resolved.insert(symbolic_index, value_len / fixed_product);
    } else if fixed_product != value_len {
        return Err(format!(
            "model input expects {fixed_product} values with shape {shape:?}, got {value_len}"
        ));
    }
    Ok(resolved)
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
fn tensor_to_flat_value(
    tensor: &tract_onnx::prelude::TValue,
    signature: &TensorSignature,
) -> Result<datatypes::Value, String> {
    let values = read_tensor_scalars(tensor)?;
    let scalar_schema = scalar_schema_column(signature)?;
    if signature.shape.as_ref().is_some_and(Vec::is_empty) {
        let value = values
            .first()
            .ok_or_else(|| format!("output '{}' tensor is empty", signature.name))?;
        return scalar_to_value(value, &scalar_schema);
    }

    let element_type = signature.element_type.clone();
    let items = values
        .iter()
        .map(|value| scalar_to_value(value, &scalar_schema))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(datatypes::Value::List(datatypes::ListValue::new(
        items,
        Arc::new(element_type),
    )))
}

#[cfg(feature = "onnx")]
fn scalar_schema_column(signature: &TensorSignature) -> Result<ModelSchemaColumn, String> {
    let data_type = match &signature.element_type {
        ConcreteDatatype::Bool(_) => "bool",
        ConcreteDatatype::Float32(_) => "float32",
        ConcreteDatatype::Float64(_) => "float64",
        ConcreteDatatype::Int8(_) => "int8",
        ConcreteDatatype::Int16(_) => "int16",
        ConcreteDatatype::Int32(_) => "int32",
        ConcreteDatatype::Int64(_) => "int64",
        ConcreteDatatype::Uint8(_) => "uint8",
        ConcreteDatatype::Uint16(_) => "uint16",
        ConcreteDatatype::Uint32(_) => "uint32",
        ConcreteDatatype::Uint64(_) => "uint64",
        ConcreteDatatype::String(_) => "string",
        other => return Err(format!("unsupported ONNX scalar type {other:?}")),
    };
    Ok(ModelSchemaColumn {
        name: signature.name.clone(),
        data_type: data_type.to_string(),
        cardinality: None,
        fields: None,
        element: None,
    })
}

#[cfg(feature = "onnx")]
pub fn inspect_model(path: &std::path::Path) -> Result<ModelSignatures, String> {
    use tract_onnx::prelude::{Framework, InferenceModelExt};

    let model = tract_onnx::onnx()
        .model_for_path(path)
        .map_err(|error| format!("load ONNX model {}: {error}", path.display()))?
        .into_optimized()
        .map_err(|error| format!("inspect ONNX model {}: {error}", path.display()))?;
    let inputs = model
        .input_outlets()
        .map_err(|error| format!("read ONNX input outlets: {error}"))?
        .iter()
        .map(|outlet| tensor_signature(&model, *outlet))
        .collect::<Result<Vec<_>, _>>()?;
    for signature in &inputs {
        if let Some(shape) = &signature.shape {
            if shape.iter().filter(|dimension| dimension.is_none()).count() > 1 {
                return Err(format!(
                    "ONNX input '{}' has multiple symbolic dimensions in shape {shape:?}; flat input shape inference is ambiguous",
                    signature.name
                ));
            }
        }
    }
    let outputs = model
        .output_outlets()
        .map_err(|error| format!("read ONNX output outlets: {error}"))?
        .iter()
        .map(|outlet| tensor_signature(&model, *outlet))
        .collect::<Result<Vec<_>, _>>()?;
    if inputs.is_empty() {
        return Err("ONNX model must declare at least one input".to_string());
    }
    if outputs.is_empty() {
        return Err("ONNX model must declare at least one output".to_string());
    }
    Ok(ModelSignatures { inputs, outputs })
}

#[cfg(feature = "onnx")]
fn tensor_signature(
    model: &tract_onnx::prelude::TypedModel,
    outlet: tract_onnx::prelude::OutletId,
) -> Result<TensorSignature, String> {
    let fact = model
        .outlet_fact(outlet)
        .map_err(|error| format!("read ONNX tensor fact: {error}"))?;
    let element_type = datum_type_to_datatype(fact.datum_type)?;
    let name = model
        .outlet_label(outlet)
        .map(str::to_string)
        .unwrap_or_else(|| model.node(outlet.node).name.to_string());
    Ok(TensorSignature {
        name,
        element_type,
        shape: Some(
            fact.shape
                .dims()
                .iter()
                .map(|dimension| {
                    dimension
                        .as_i64()
                        .map(|value| usize::try_from(value).map_err(|_| value))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|value| format!("invalid ONNX dimension {value}"))?,
        ),
    })
}

#[cfg(feature = "onnx")]
fn datum_type_to_datatype(
    datum_type: tract_onnx::prelude::DatumType,
) -> Result<ConcreteDatatype, String> {
    use tract_onnx::prelude::DatumType;

    match datum_type {
        DatumType::Bool => Ok(ConcreteDatatype::Bool(BooleanType)),
        DatumType::F32 => Ok(ConcreteDatatype::Float32(Float32Type)),
        DatumType::F64 => Ok(ConcreteDatatype::Float64(Float64Type)),
        DatumType::I8 => Ok(ConcreteDatatype::Int8(Int8Type)),
        DatumType::I16 => Ok(ConcreteDatatype::Int16(Int16Type)),
        DatumType::I32 => Ok(ConcreteDatatype::Int32(Int32Type)),
        DatumType::I64 => Ok(ConcreteDatatype::Int64(Int64Type)),
        DatumType::U8 => Ok(ConcreteDatatype::Uint8(Uint8Type)),
        DatumType::U16 => Ok(ConcreteDatatype::Uint16(Uint16Type)),
        DatumType::U32 => Ok(ConcreteDatatype::Uint32(Uint32Type)),
        DatumType::U64 => Ok(ConcreteDatatype::Uint64(Uint64Type)),
        DatumType::String => Ok(ConcreteDatatype::String(StringType)),
        other => Err(format!("unsupported ONNX tensor type {other:?}")),
    }
}

fn legacy_signature(column: &ModelSchemaColumn) -> Result<TensorSignature, String> {
    let data_type = model_schema_column_type(column)?;
    let (element_type, shape) = match data_type {
        ConcreteDatatype::List(list) => (list.item_type().clone(), None),
        scalar => (scalar, Some(Vec::new())),
    };
    Ok(TensorSignature {
        name: column.name.clone(),
        element_type,
        shape,
    })
}

/// Process-wide model metadata registry and shared prepared-model cache.
pub struct ModelRegistry {
    models_dir: PathBuf,
    metadata: RwLock<HashMap<String, Arc<ModelMetadata>>>,
    prepared: Mutex<HashMap<ModelCacheKey, Weak<PreparedOnnxModel>>>,
    in_flight: Mutex<HashMap<ModelCacheKey, InFlightPreparation>>,
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
            metadata.insert(name, Arc::new(self.metadata_for(model)?));
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
        metadata.insert(name.clone(), Arc::new(self.metadata_for(model)?));
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

    fn metadata_for(&self, model: StoredModel) -> Result<ModelMetadata, String> {
        let model_path = self.models_dir.join(&model.name).join("model.onnx");
        let signatures = if model_path.exists() {
            #[cfg(feature = "onnx")]
            {
                inspect_model(&model_path)?
            }
            #[cfg(not(feature = "onnx"))]
            {
                return Err("ONNX model runtime is disabled".to_string());
            }
        } else {
            ModelSignatures {
                inputs: model
                    .inputs
                    .iter()
                    .map(legacy_signature)
                    .collect::<Result<Vec<_>, _>>()?,
                outputs: model
                    .outputs
                    .iter()
                    .map(legacy_signature)
                    .collect::<Result<Vec<_>, _>>()?,
            }
        };
        Ok(ModelMetadata {
            model: Arc::new(model.clone()),
            model_path,
            inputs: signatures.inputs,
            outputs: signatures.outputs,
        })
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
            Ok(PreparedOnnxModel { metadata, runnable })
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

    fn identity_input(seed: f32) -> datatypes::Value {
        datatypes::Value::List(datatypes::ListValue::new(
            vec![
                datatypes::Value::Float32(seed),
                datatypes::Value::Float32(seed + 0.5),
                datatypes::Value::Float32(seed + 1.0),
                datatypes::Value::Float32(seed + 1.5),
            ],
            Arc::new(ConcreteDatatype::Float32(Float32Type)),
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
                inputs: vec![column("input", "list")],
                outputs: vec![column("output", "list")],
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
            let datatypes::Value::List(input_values) = input else {
                panic!("test input must be a list");
            };
            let datatypes::Value::Struct(output_record) = output else {
                panic!("identity output must be a struct");
            };
            let datatypes::Value::List(output_values) = &output_record.items()[0] else {
                panic!("identity output field must be a list");
            };
            assert_eq!(output_values.items(), input_values.items());
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
    fn flat_input_and_output_signatures_preserve_tensor_values() {
        let input_signature = TensorSignature {
            name: "input".to_string(),
            element_type: ConcreteDatatype::Float32(Float32Type),
            shape: Some(vec![Some(2), Some(2)]),
        };
        let input = identity_input(1.0);
        assert!(validate_flat_input_value(&input, &input_signature, "input").is_ok());
        let struct_input = datatypes::Value::Struct(datatypes::StructValue::new(
            vec![],
            StructType::new(Arc::new(vec![])),
        ));
        assert!(validate_flat_input_value(&struct_input, &input_signature, "input").is_err());

        let output_signature = TensorSignature {
            name: "output".to_string(),
            element_type: ConcreteDatatype::Int32(Int32Type),
            shape: Some(vec![Some(2), Some(2)]),
        };
        let tensor = Tensor::from_shape(&[2, 2], &[1i32, 2, 3, 4])
            .unwrap()
            .into_tvalue();
        let datatypes::Value::List(values) =
            tensor_to_flat_value(&tensor, &output_signature).unwrap()
        else {
            panic!("expected flat list output");
        };
        assert_eq!(values.len(), 4);
        assert_eq!(values.items()[0], datatypes::Value::Int32(1));
        assert_eq!(values.items()[3], datatypes::Value::Int32(4));
    }

    #[test]
    fn dynamic_input_shape_preserves_rank_and_infers_batch_dimension() {
        let input = datatypes::Value::List(datatypes::ListValue::new(
            (1..=6)
                .map(|value| datatypes::Value::Float32(value as f32))
                .collect(),
            Arc::new(ConcreteDatatype::Float32(Float32Type)),
        ));
        assert_eq!(
            resolve_input_shape(&input, Some(&vec![None, Some(3)])).unwrap(),
            vec![2, 3]
        );
        assert!(resolve_input_shape(&input, Some(&vec![None, Some(4)])).is_err());
        assert!(resolve_input_shape(&input, Some(&vec![None, None])).is_err());
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
