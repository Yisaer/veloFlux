use crate::expr::func::EvalError;
use datatypes::Value;
use regex::Regex;
use std::collections::BTreeMap;

// helpers for testing
pub fn i(v: i64) -> Value {
    Value::Int64(v)
}

pub fn f(v: f64) -> Value {
    Value::Float64(v)
}

pub fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

pub fn b(v: bool) -> Value {
    Value::Bool(v)
}

pub fn a(v: Vec<Value>) -> Value {
    Value::List(v)
}

pub fn n() -> Value {
    Value::Null
}

pub fn m(entries: Vec<(&str, Value)>) -> Value {
    let mut map = std::collections::BTreeMap::new();
    for (k, v) in entries {
        map.insert(k.to_string(), v);
    }
    Value::Struct(map)
}

pub fn assert_int(actual: Value, expected: i64) {
    match actual {
        Value::Int64(v) => {
            assert_eq!(v, expected, "expected Int64({expected}), got Int64({v})");
        }
        other => panic!("expected Int64({expected}), got {:?}", other),
    }
}

pub fn assert_bool(actual: Value, expected: bool) {
    match actual {
        Value::Bool(v) => {
            assert_eq!(v, expected, "expected Bool({expected}), got Bool({v})");
        }
        other => panic!("expected Bool({expected}), got {:?}", other),
    }
}

pub fn assert_string(actual: Value, expected: &str) {
    match actual {
        Value::String(v) => {
            assert_eq!(
                v, expected,
                "expected String({expected:?}), got String({v:?})"
            );
        }
        other => panic!("expected String({expected:?}), got {:?}", other),
    }
}

pub fn assert_array(actual: Value, expected: Vec<Value>) {
    match actual {
        Value::List(v) => {
            assert_eq!(
                v, expected,
                "expected Array({expected:?}), got Array({v:?})"
            );
        }
        other => panic!("expected Array({expected:?}), got {:?}", other),
    }
}

pub fn assert_null(actual: Value) {
    assert!(
        matches!(actual, Value::Null),
        "expected Null, got {:?}",
        actual
    );
}

pub fn assert_map(actual: Value, expected: Vec<(&str, Value)>) {
    let mut expected_map = std::collections::BTreeMap::new();
    for (k, v) in expected {
        expected_map.insert(k.to_string(), v);
    }

    match actual {
        Value::Struct(v) => {
            assert_eq!(
                v, expected_map,
                "expected Map({:?}), got Map({:?})",
                expected_map, v
            );
        }
        other => panic!("expected Map({:?}), got {:?}", expected_map, other),
    }
}

// general helpers
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

// math funcs helpers
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

pub fn nth_converted_or_null<T>(
    args: &[Value],
    idx: usize,
    convert: impl FnOnce(&Value) -> Result<T, EvalError>,
) -> Result<Option<T>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) => Ok(None),
        Some(v) => convert(v).map(Some),
        None => Err(EvalError::TypeMismatch {
            expected: format!("argument {}", idx),
            actual: format!("missing argument {}", idx),
        }),
    }
}

pub fn nth_string_or_null(args: &[Value], idx: usize) -> Result<Option<String>, EvalError> {
    nth_converted_or_null(args, idx, value_to_string)
}

pub fn nth_f64_or_null(args: &[Value], idx: usize) -> Result<Option<f64>, EvalError> {
    nth_converted_or_null(args, idx, value_to_f64)
}

pub fn nth_i64_or_null(args: &[Value], idx: usize) -> Result<Option<i64>, EvalError> {
    nth_converted_or_null(args, idx, value_to_i64)
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

pub fn validate_at_least_arity(args: &[Value], min: usize) -> Result<(), EvalError> {
    if args.len() >= min {
        Ok(())
    } else {
        Err(EvalError::TypeMismatch {
            expected: format!("at least {} arguments", min),
            actual: format!("{} arguments", args.len()),
        })
    }
}

// string helpers
pub fn nth_string_strict(args: &[Value], idx: usize) -> Result<String, EvalError> {
    match args.get(idx) {
        Some(v) => value_to_string(v),
        None => Err(EvalError::TypeMismatch {
            expected: format!("argument {}", idx),
            actual: format!("missing argument {}", idx),
        }),
    }
}

pub fn unary_string_to_string(
    args: &[Value],
    op: impl FnOnce(&str) -> String,
) -> Result<Value, EvalError> {
    validate_arity(args, &[1])?;
    let Some(s) = nth_string_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::String(op(&s)))
}

pub fn unary_string_to_i64(
    args: &[Value],
    op: impl FnOnce(&str) -> i64,
) -> Result<Value, EvalError> {
    validate_arity(args, &[1])?;
    let Some(s) = nth_string_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int64(op(&s)))
}

fn binary_string_map<T>(
    args: &[Value],
    op: impl FnOnce(&str, &str) -> T,
    wrap: impl FnOnce(T) -> Value,
) -> Result<Value, EvalError> {
    validate_arity(args, &[2])?;
    let Some(a) = nth_string_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(b) = nth_string_or_null(args, 1)? else {
        return Ok(Value::Null);
    };
    Ok(wrap(op(&a, &b)))
}

pub fn binary_string_to_string(
    args: &[Value],
    op: impl FnOnce(&str, &str) -> String,
) -> Result<Value, EvalError> {
    binary_string_map(args, op, Value::String)
}

pub fn binary_string_to_bool(
    args: &[Value],
    op: impl FnOnce(&str, &str) -> bool,
) -> Result<Value, EvalError> {
    binary_string_map(args, op, Value::Bool)
}

pub fn binary_string_to_i64(
    args: &[Value],
    op: impl FnOnce(&str, &str) -> i64,
) -> Result<Value, EvalError> {
    binary_string_map(args, op, Value::Int64)
}

pub fn binary_string_to_string_result(
    args: &[Value],
    op: impl FnOnce(&str, &str) -> Result<String, EvalError>,
) -> Result<Value, EvalError> {
    validate_arity(args, &[2])?;
    let Some(a) = nth_string_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(b) = nth_string_or_null(args, 1)? else {
        return Ok(Value::Null);
    };
    Ok(Value::String(op(&a, &b)?))
}

pub fn binary_string_to_bool_result(
    args: &[Value],
    op: impl FnOnce(&str, &str) -> Result<bool, EvalError>,
) -> Result<Value, EvalError> {
    validate_arity(args, &[2])?;
    let Some(a) = nth_string_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(b) = nth_string_or_null(args, 1)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Bool(op(&a, &b)?))
}

pub fn binary_string_to_i64_result(
    args: &[Value],
    op: impl FnOnce(&str, &str) -> Result<i64, EvalError>,
) -> Result<Value, EvalError> {
    validate_arity(args, &[2])?;
    let Some(a) = nth_string_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(b) = nth_string_or_null(args, 1)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int64(op(&a, &b)?))
}

pub fn ternary_string_to_string(
    args: &[Value],
    op: impl FnOnce(&str, &str, &str) -> Result<String, EvalError>,
) -> Result<Value, EvalError> {
    validate_arity(args, &[3])?;
    let Some(a) = nth_string_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(b) = nth_string_or_null(args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(c) = nth_string_or_null(args, 2)? else {
        return Ok(Value::Null);
    };
    Ok(Value::String(op(&a, &b, &c)?))
}

pub fn compile_regex(pattern: &str) -> Result<Regex, EvalError> {
    Regex::new(pattern).map_err(|e| EvalError::TypeMismatch {
        expected: "valid regex".to_string(),
        actual: e.to_string(),
    })
}

pub fn validate_one_string_or_null(args: &[Value]) -> Result<(), EvalError> {
    validate_arity(args, &[1])?;
    nth_string_or_null(args, 0)?;
    Ok(())
}

pub fn validate_two_strings_or_null(args: &[Value]) -> Result<(), EvalError> {
    validate_arity(args, &[2])?;
    nth_string_or_null(args, 0)?;
    nth_string_or_null(args, 1)?;
    Ok(())
}

pub fn validate_two_strict_strings(args: &[Value]) -> Result<(), EvalError> {
    validate_arity(args, &[2])?;
    nth_string_strict(args, 0)?;
    nth_string_strict(args, 1)?;
    Ok(())
}

pub fn validate_string_i64_or_null(args: &[Value]) -> Result<(), EvalError> {
    validate_arity(args, &[2])?;
    nth_string_or_null(args, 0)?;
    nth_i64_or_null(args, 1)?;
    Ok(())
}

pub fn string_nonnegative_i64_to_string(
    args: &[Value],
    op: impl FnOnce(&str, usize) -> String,
) -> Result<Value, EvalError> {
    validate_arity(args, &[2])?;

    let Some(s) = nth_string_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(n) = nth_i64_or_null(args, 1)? else {
        return Ok(Value::Null);
    };

    if n < 0 {
        return Err(EvalError::TypeMismatch {
            expected: "non-negative integer".to_string(),
            actual: n.to_string(),
        });
    }

    Ok(Value::String(op(&s, n as usize)))
}

pub fn substring_by_char_start_length(
    s: &str,
    start: i64,
    length: Option<i64>,
) -> Result<String, EvalError> {
    if start < 0 {
        return Err(EvalError::TypeMismatch {
            expected: "non-negative start index".to_string(),
            actual: start.to_string(),
        });
    }

    if let Some(l) = length {
        if l < 0 {
            return Err(EvalError::TypeMismatch {
                expected: "non-negative length".to_string(),
                actual: l.to_string(),
            });
        }
    }

    let chars: Vec<char> = s.chars().collect();
    let n = chars.len() as i64;

    if start >= n {
        return Ok(String::new());
    }

    let start_usize = start as usize;

    let end_usize = match length {
        Some(l) => {
            let end = start + l;
            if end > n {
                n as usize
            } else {
                end as usize
            }
        }
        None => n as usize,
    };

    Ok(chars[start_usize..end_usize].iter().collect())
}

// array helpers
pub fn value_to_array(value: &Value) -> Result<Vec<Value>, EvalError> {
    match value {
        Value::List(v) => Ok(v.clone()),
        other => Err(EvalError::TypeMismatch {
            expected: "array".to_string(),
            actual: format!("{:?}", other),
        }),
    }
}

pub fn value_to_map(value: &Value) -> Result<BTreeMap<String, Value>, EvalError> {
    match value {
        Value::Struct(m) => Ok(m.clone()),
        other => Err(EvalError::TypeMismatch {
            expected: "object".to_string(),
            actual: format!("{:?}", other),
        }),
    }
}

pub fn nth_array_or_null(args: &[Value], idx: usize) -> Result<Option<Vec<Value>>, EvalError> {
    nth_converted_or_null(args, idx, value_to_array)
}

pub fn nth_map_or_null(
    args: &[Value],
    idx: usize,
) -> Result<Option<BTreeMap<String, Value>>, EvalError> {
    nth_converted_or_null(args, idx, value_to_map)
}

pub fn validate_one_array_or_null(args: &[Value]) -> Result<(), EvalError> {
    validate_arity(args, &[1])?;
    nth_array_or_null(args, 0)?;
    Ok(())
}

pub fn validate_two_arrays_or_null(args: &[Value]) -> Result<(), EvalError> {
    validate_arity(args, &[2])?;
    nth_array_or_null(args, 0)?;
    nth_array_or_null(args, 1)?;
    Ok(())
}

pub fn value_to_string_lossy(value: &Value) -> Result<String, EvalError> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Bool(v) => Ok(v.to_string()),
        Value::Int8(v) => Ok(v.to_string()),
        Value::Int16(v) => Ok(v.to_string()),
        Value::Int32(v) => Ok(v.to_string()),
        Value::Int64(v) => Ok(v.to_string()),
        Value::Uint8(v) => Ok(v.to_string()),
        Value::Uint16(v) => Ok(v.to_string()),
        Value::Uint32(v) => Ok(v.to_string()),
        Value::Uint64(v) => Ok(v.to_string()),
        Value::Float32(v) => Ok(v.to_string()),
        Value::Float64(v) => Ok(v.to_string()),
        other => Err(EvalError::TypeMismatch {
            expected: "string-convertible scalar".to_string(),
            actual: format!("{:?}", other),
        }),
    }
}

pub fn value_sort_key(value: &Value) -> Result<String, EvalError> {
    value_to_string_lossy(value)
}

pub fn array_distinct_values(values: Vec<Value>) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    for v in values {
        if !out.iter().any(|x| x == &v) {
            out.push(v);
        }
    }
    out
}
