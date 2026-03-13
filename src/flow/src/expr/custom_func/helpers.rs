use crate::expr::func::EvalError;
use datatypes::Value;

pub fn validate_arity(args: &[Value], allowed: &[usize]) -> Result<(), EvalError> {
    if allowed.contains(&args.len()) {
        Ok(())
    } else {
        let mut a: Vec<usize> = allowed.to_vec();
        a.sort_unstable();
        let expected = a
            .into_iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(" or ");
        Err(EvalError::TypeMismatch {
            expected: format!("{} arguments", expected),
            actual: format!("{} arguments", args.len()),
        })
    }
}

pub fn any_null(args: &[Value]) -> bool {
    args.iter().any(Value::is_null)
}

pub fn value_to_string(value: &Value) -> Result<String, EvalError> {
    match value {
        Value::String(s) => Ok(s.clone()),
        other => Err(EvalError::TypeMismatch {
            expected: "string".to_string(),
            actual: format!("{:?}", other),
        }),
    }
}

pub fn nth_string_or_null(args: &[Value], idx: usize) -> Result<Option<String>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) => Ok(None),
        Some(v) => value_to_string(v).map(Some),
        None => Err(EvalError::TypeMismatch {
            expected: format!("argument {}", idx),
            actual: format!("missing argument {}", idx),
        }),
    }
}

pub fn map_numeric_value(
    v: &Value,
    map_i: impl FnOnce(i64) -> Result<i64, EvalError>,
    map_u: impl FnOnce(u64) -> Result<u64, EvalError>,
    map_f: impl FnOnce(f64) -> Result<f64, EvalError>,
) -> Result<Value, EvalError> {
    match v {
        Value::Null => Ok(Value::Null),

        Value::Int8(x) => Ok(Value::Int8(map_i(*x as i64)? as i8)),
        Value::Int16(x) => Ok(Value::Int16(map_i(*x as i64)? as i16)),
        Value::Int32(x) => Ok(Value::Int32(map_i(*x as i64)? as i32)),
        Value::Int64(x) => Ok(Value::Int64(map_i(*x)?)),

        Value::Uint8(x) => Ok(Value::Uint8(map_u(*x as u64)? as u8)),
        Value::Uint16(x) => Ok(Value::Uint16(map_u(*x as u64)? as u16)),
        Value::Uint32(x) => Ok(Value::Uint32(map_u(*x as u64)? as u32)),
        Value::Uint64(x) => Ok(Value::Uint64(map_u(*x)?)),

        Value::Float32(x) => Ok(Value::Float32(map_f(*x as f64)? as f32)),
        Value::Float64(x) => Ok(Value::Float64(map_f(*x)?)),

        other => Err(EvalError::TypeMismatch {
            expected: "numeric".to_string(),
            actual: format!("{:?}", other),
        }),
    }
}

pub fn value_to_f64(value: &Value) -> Result<f64, EvalError> {
    match value {
        Value::Null => Err(EvalError::TypeMismatch {
            expected: "numeric".to_string(),
            actual: "Null".to_string(),
        }),
        _ => match map_numeric_value(value, Ok, Ok, Ok)? {
            Value::Int8(v) => Ok(v as f64),
            Value::Int16(v) => Ok(v as f64),
            Value::Int32(v) => Ok(v as f64),
            Value::Int64(v) => Ok(v as f64),
            Value::Uint8(v) => Ok(v as f64),
            Value::Uint16(v) => Ok(v as f64),
            Value::Uint32(v) => Ok(v as f64),
            Value::Uint64(v) => Ok(v as f64),
            Value::Float32(v) => Ok(v as f64),
            Value::Float64(v) => Ok(v),
            _ => unreachable!(),
        },
    }
}

pub fn value_to_i64(value: &Value) -> Result<i64, EvalError> {
    match value {
        Value::Int8(v) => Ok(*v as i64),
        Value::Int16(v) => Ok(*v as i64),
        Value::Int32(v) => Ok(*v as i64),
        Value::Int64(v) => Ok(*v),
        Value::Uint8(v) => Ok(*v as i64),
        Value::Uint16(v) => Ok(*v as i64),
        Value::Uint32(v) => Ok(*v as i64),
        Value::Uint64(v) => i64::try_from(*v).map_err(|_| EvalError::TypeMismatch {
            expected: "int64-compatible integer".to_string(),
            actual: format!("{:?}", value),
        }),
        other => Err(EvalError::TypeMismatch {
            expected: "integer".to_string(),
            actual: format!("{:?}", other),
        }),
    }
}

pub fn nth_f64_or_null(args: &[Value], idx: usize) -> Result<Option<f64>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) => Ok(None),
        Some(v) => value_to_f64(v).map(Some),
        None => Err(EvalError::TypeMismatch {
            expected: format!("argument {}", idx),
            actual: format!("missing argument {}", idx),
        }),
    }
}

pub fn nth_i64_or_null(args: &[Value], idx: usize) -> Result<Option<i64>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) => Ok(None),
        Some(v) => value_to_i64(v).map(Some),
        None => Err(EvalError::TypeMismatch {
            expected: format!("argument {}", idx),
            actual: format!("missing argument {}", idx),
        }),
    }
}

pub fn f64_to_value_nan_null(v: f64) -> Value {
    if v.is_nan() {
        Value::Null
    } else {
        Value::Float64(v)
    }
}

pub fn f64_to_value_nan_null_inf_err(v: f64) -> Result<Value, EvalError> {
    if v.is_nan() {
        Ok(Value::Null)
    } else if v.is_infinite() {
        Err(EvalError::TypeMismatch {
            expected: "finite numeric result".to_string(),
            actual: format!("{:?}", v),
        })
    } else {
        Ok(Value::Float64(v))
    }
}

pub fn unary_numeric_to_f64(
    args: &[Value],
    op: impl FnOnce(f64) -> f64,
) -> Result<Value, EvalError> {
    validate_arity(args, &[1])?;
    let Some(x) = nth_f64_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(f64_to_value_nan_null(op(x)))
}

pub fn unary_numeric_to_f64_inf_err(
    args: &[Value],
    op: impl FnOnce(f64) -> f64,
) -> Result<Value, EvalError> {
    validate_arity(args, &[1])?;
    let Some(x) = nth_f64_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    f64_to_value_nan_null_inf_err(op(x))
}

pub fn binary_numeric_to_f64(
    args: &[Value],
    op: impl FnOnce(f64, f64) -> f64,
) -> Result<Value, EvalError> {
    validate_arity(args, &[2])?;
    let Some(a) = nth_f64_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(b) = nth_f64_or_null(args, 1)? else {
        return Ok(Value::Null);
    };
    Ok(f64_to_value_nan_null(op(a, b)))
}

pub fn unary_i64(args: &[Value], op: impl FnOnce(i64) -> i64) -> Result<Value, EvalError> {
    validate_arity(args, &[1])?;
    let Some(x) = nth_i64_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int64(op(x)))
}

pub fn binary_i64(args: &[Value], op: impl FnOnce(i64, i64) -> i64) -> Result<Value, EvalError> {
    validate_arity(args, &[2])?;
    let Some(a) = nth_i64_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(b) = nth_i64_or_null(args, 1)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int64(op(a, b)))
}

pub fn nullary_f64(args: &[Value], op: impl FnOnce() -> f64) -> Result<Value, EvalError> {
    validate_arity(args, &[0])?;
    Ok(Value::Float64(op()))
}

pub fn precision_arg_or_default(
    args: &[Value],
    idx: usize,
    default: i32,
) -> Result<i32, EvalError> {
    match args.get(idx) {
        None => Ok(default),
        Some(Value::Null) => Ok(default),
        Some(v) => {
            let raw = value_to_i64(v)?;
            i32::try_from(raw).map_err(|_| EvalError::TypeMismatch {
                expected: "i32-compatible integer".to_string(),
                actual: format!("{:?}", v),
            })
        }
    }
}

pub fn binary_numeric_to_f64_inf_err(
    args: &[Value],
    op: impl FnOnce(f64, f64) -> f64,
) -> Result<Value, EvalError> {
    validate_arity(args, &[2])?;
    let Some(a) = nth_f64_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(b) = nth_f64_or_null(args, 1)? else {
        return Ok(Value::Null);
    };
    f64_to_value_nan_null_inf_err(op(a, b))
}

pub fn sign_numeric(args: &[Value]) -> Result<Value, EvalError> {
    validate_arity(args, &[1])?;
    let Some(x) = nth_f64_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let v = if x > 0.0 {
        1
    } else if x < 0.0 {
        -1
    } else {
        0
    };
    Ok(Value::Int64(v))
}
