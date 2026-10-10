use datatypes::Value;
use thiserror::Error;

/// Errors that can occur during WASM ↔ Value serialization.
#[derive(Debug, Error)]
pub enum AbiError {
    #[error("JSON serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("unsupported JSON value type for conversion to veloFlux Value")]
    UnsupportedJsonType,
    #[error("WASM UDF returned an error: {0}")]
    UdfError(String),
}

/// Serialize a slice of `Value` args into a JSON byte vector.
pub fn serialize_args(args: &[Value]) -> Result<Vec<u8>, AbiError> {
    let json_args: Vec<serde_json::Value> = args
        .iter()
        .map(value_to_json)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(serde_json::to_vec(&json_args)?)
}

/// Serialize column-oriented batch arguments into the version 1 batch envelope.
pub fn serialize_batch_args(args: &[Vec<Value>], row_count: usize) -> Result<Vec<u8>, AbiError> {
    if args.iter().any(|column| column.len() != row_count) {
        return Err(AbiError::UnsupportedJsonType);
    }
    let json_args: Vec<Vec<serde_json::Value>> = args
        .iter()
        .map(|column| {
            column
                .iter()
                .map(value_to_json)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(
        serde_json::json!({ "row_count": row_count, "args": json_args })
            .to_string()
            .into_bytes(),
    )
}

/// Deserialize JSON bytes into a veloFlux `Value`.
///
/// Handles both single values and single-element arrays (the UDF may return
/// its result wrapped in an array).
pub fn deserialize_result(json_bytes: &[u8]) -> Result<Value, AbiError> {
    let v: serde_json::Value = serde_json::from_slice(json_bytes)?;
    // If the result is an array with one element, unwrap it
    let v = match &v {
        serde_json::Value::Array(arr) if arr.len() == 1 => &arr[0],
        _ => &v,
    };
    json_to_value(v)
}

/// Deserialize a batch result envelope and preserve result row order.
pub fn deserialize_batch_result(
    json_bytes: &[u8],
) -> Result<Vec<Result<Value, AbiError>>, AbiError> {
    let envelope: serde_json::Value = serde_json::from_slice(json_bytes)?;
    if let Some(message) = envelope
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(serde_json::Value::as_str)
    {
        return Err(AbiError::UdfError(message.to_string()));
    }
    if let Some(values) = envelope.get("values").and_then(serde_json::Value::as_array) {
        return values.iter().map(json_to_value).map(Ok).collect();
    }
    let results = envelope
        .get("results")
        .and_then(serde_json::Value::as_array)
        .ok_or(AbiError::UnsupportedJsonType)?;
    results
        .iter()
        .map(|result| {
            if let Some(message) = result
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(serde_json::Value::as_str)
            {
                return Ok(Err(AbiError::UdfError(message.to_string())));
            }
            result
                .get("value")
                .ok_or(AbiError::UnsupportedJsonType)
                .and_then(json_to_value)
                .map(Ok)
        })
        .collect()
}

fn value_to_json(v: &Value) -> Result<serde_json::Value, AbiError> {
    match v {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int8(i) => Ok(serde_json::Value::Number(serde_json::Number::from(
            *i as i64,
        ))),
        Value::Int16(i) => Ok(serde_json::Value::Number(serde_json::Number::from(
            *i as i64,
        ))),
        Value::Int32(i) => Ok(serde_json::Value::Number(serde_json::Number::from(
            *i as i64,
        ))),
        Value::Int64(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
        Value::Uint8(i) => Ok(serde_json::Value::Number(serde_json::Number::from(
            *i as i64,
        ))),
        Value::Uint16(i) => Ok(serde_json::Value::Number(serde_json::Number::from(
            *i as i64,
        ))),
        Value::Uint32(i) => Ok(serde_json::Value::Number(serde_json::Number::from(
            *i as i64,
        ))),
        Value::Uint64(i) => Ok(serde_json::Value::Number(serde_json::Number::from(
            *i as i64,
        ))),
        Value::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .ok_or(AbiError::UnsupportedJsonType),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or(AbiError::UnsupportedJsonType),
        Value::String(s) => Ok(serde_json::Value::String(s.as_ref().clone())),
        Value::Bytes(_) => Err(AbiError::UnsupportedJsonType),
        Value::Timestamp(_) => Err(AbiError::UnsupportedJsonType),
        Value::List(_) | Value::Struct(_) => Err(AbiError::UnsupportedJsonType),
    }
}

fn json_to_value(v: &serde_json::Value) -> Result<Value, AbiError> {
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int64(i))
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Ok(Value::Int64(u as i64))
                } else {
                    Ok(Value::Uint64(u))
                }
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float64(f))
            } else {
                Err(AbiError::UnsupportedJsonType)
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s.clone().into())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(AbiError::UnsupportedJsonType)
        }
    }
}
