use super::helpers::*;
use crate::catalog::FunctionDef;
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use datatypes::Value;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use crc32fast::hash as crc32_hash;
use md5;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512};

#[derive(Debug, Clone)]
pub struct BypassFunc;

#[derive(Debug, Clone)]
pub struct ChrFunc;

#[derive(Debug, Clone)]
pub struct EncodeFunc;

#[derive(Debug, Clone)]
pub struct DecodeFunc;

#[derive(Debug, Clone)]
pub struct TruncFunc;

#[derive(Debug, Clone)]
pub struct Md5Func;

#[derive(Debug, Clone)]
pub struct Sha1Func;

#[derive(Debug, Clone)]
pub struct Sha256Func;

#[derive(Debug, Clone)]
pub struct Sha384Func;

#[derive(Debug, Clone)]
pub struct Sha512Func;

#[derive(Debug, Clone)]
pub struct Crc32Func;

#[derive(Debug, Clone)]
pub struct CoalesceFunc;

#[derive(Debug, Clone)]
pub struct Hex2DecFunc;

#[derive(Debug, Clone)]
pub struct Dec2HexFunc;

#[derive(Debug, Clone)]
pub struct ToJsonFunc;

#[derive(Debug, Clone)]
pub struct ParseJsonFunc;

#[derive(Debug, Clone)]
pub struct CardinalityFunc;

#[derive(Debug, Clone)]
pub struct NewUuidFunc;

#[derive(Debug, Clone)]
pub struct CastFunc;

#[derive(Debug, Clone)]
pub struct TstampFunc;

#[derive(Debug, Clone)]
pub struct DelayFunc;

pub fn builtin_function_defs() -> Vec<FunctionDef> {
    vec![
        bypass_function_def(),
        chr_function_def(),
        cast_function_def(),
        encode_function_def(),
        decode_function_def(),
        trunc_function_def(),
        md5_function_def(),
        sha1_function_def(),
        sha256_function_def(),
        sha384_function_def(),
        sha512_function_def(),
        crc32_function_def(),
        isnull_function_def(),
        coalesce_function_def(),
        hex2dec_function_def(),
        dec2hex_function_def(),
        to_json_function_def(),
        parse_json_function_def(),
        cardinality_function_def(),
        newuuid_function_def(),
        tstamp_function_def(),
        delay_function_def(),
    ]
}

pub fn bypass_function_def() -> FunctionDef {
    scalar_function_def(
        "bypass",
        vec![req_arg("value", any_type())],
        any_type(),
        "Return the input value unchanged.",
        vec!["Requires exactly 1 argument."],
        vec!["SELECT bypass(a)"],
    )
}

pub fn chr_function_def() -> FunctionDef {
    scalar_function_def(
        "chr",
        vec![req_arg("value", any_type())],
        string_type(),
        "Convert an integer or single-character string to a one-character string.",
        vec![
            "Requires exactly 1 argument.",
            "Accepts integer-compatible numeric values or a string of length 1.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT chr(65)", "SELECT chr('A')"],
    )
}

pub fn encode_function_def() -> FunctionDef {
    scalar_function_def(
        "encode",
        vec![
            req_arg("value", string_type()),
            req_arg("format", string_type()),
        ],
        string_type(),
        "Encode a string using the requested format.",
        vec![
            "Requires exactly 2 arguments.",
            "Only base64 is currently supported.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT encode('abc', 'base64')"],
    )
}

pub fn decode_function_def() -> FunctionDef {
    scalar_function_def(
        "decode",
        vec![
            req_arg("value", string_type()),
            req_arg("format", string_type()),
        ],
        string_type(),
        "Decode a string using the requested format.",
        vec![
            "Requires exactly 2 arguments.",
            "Only base64 is currently supported.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT decode('YWJj', 'base64')"],
    )
}

pub fn trunc_function_def() -> FunctionDef {
    scalar_function_def(
        "trunc",
        vec![
            req_arg("value", float_type()),
            req_arg("precision", int_type()),
        ],
        float_type(),
        "Truncate a numeric value to the specified precision.",
        vec![
            "Requires exactly 2 arguments.",
            "The first argument must be numeric.",
            "The second argument must be an integer.",
            "Positive precision truncates fractional digits.",
            "Zero precision truncates to an integer.",
            "Negative precision truncates digits to the left of the decimal point.",
            "If the second argument is less than -34, it is set to -34.",
            "If the second argument is greater than 34, it is set to 34.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT trunc(12.3456, 2)", "SELECT trunc(1234.56, -2)"],
    )
}

pub fn md5_function_def() -> FunctionDef {
    unary_string_fn_def(
        "md5",
        string_type(),
        "Return the MD5 hex digest of a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT md5('abc')"],
    )
}

pub fn sha1_function_def() -> FunctionDef {
    unary_string_fn_def(
        "sha1",
        string_type(),
        "Return the SHA-1 hex digest of a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT sha1('abc')"],
    )
}

pub fn sha256_function_def() -> FunctionDef {
    unary_string_fn_def(
        "sha256",
        string_type(),
        "Return the SHA-256 hex digest of a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT sha256('abc')"],
    )
}

pub fn sha384_function_def() -> FunctionDef {
    unary_string_fn_def(
        "sha384",
        string_type(),
        "Return the SHA-384 hex digest of a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT sha384('abc')"],
    )
}

pub fn sha512_function_def() -> FunctionDef {
    unary_string_fn_def(
        "sha512",
        string_type(),
        "Return the SHA-512 hex digest of a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT sha512('abc')"],
    )
}

pub fn crc32_function_def() -> FunctionDef {
    unary_string_fn_def(
        "crc32",
        string_type(),
        "Return the CRC32 hex digest of a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT crc32('abc')"],
    )
}

pub fn isnull_function_def() -> FunctionDef {
    scalar_function_def(
        "isnull",
        vec![req_arg("value", any_type())],
        bool_type(),
        "Return whether the input value is NULL.",
        vec!["Requires exactly 1 argument."],
        vec!["SELECT isnull(a)"],
    )
}

pub fn coalesce_function_def() -> FunctionDef {
    scalar_function_def(
        "coalesce",
        vec![variadic_arg("values", any_type())],
        any_type(),
        "Return the first non-NULL argument, or NULL if all are NULL.",
        vec!["Requires at least 1 argument."],
        vec!["SELECT coalesce(a, b, c)"],
    )
}

pub fn hex2dec_function_def() -> FunctionDef {
    unary_string_fn_def(
        "hex2dec",
        int_type(),
        "Convert a hexadecimal string to a decimal integer.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "The string may optionally start with 0x.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT hex2dec('0xff')"],
    )
}

pub fn dec2hex_function_def() -> FunctionDef {
    scalar_function_def(
        "dec2hex",
        vec![req_arg("value", int_type())],
        string_type(),
        "Convert an integer to a hexadecimal string prefixed with 0x.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be an integer or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT dec2hex(255)"],
    )
}

pub fn to_json_function_def() -> FunctionDef {
    scalar_function_def(
        "to_json",
        vec![req_arg("value", any_type())],
        string_type(),
        "Convert a value to its JSON string representation.",
        vec![
            "Requires exactly 1 argument.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT to_json(meta)"],
    )
}

pub fn parse_json_function_def() -> FunctionDef {
    scalar_function_def(
        "parse_json",
        vec![req_arg("value", string_type())],
        any_type(),
        "Parse a JSON string into a value.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL or the literal string 'null'.",
        ],
        vec!["SELECT parse_json('{\"a\":1}')"],
    )
}

pub fn cardinality_function_def() -> FunctionDef {
    scalar_function_def(
        "cardinality",
        vec![req_arg("value", any_type())],
        int_type(),
        "Return the number of elements in an array.",
        vec![
            "Requires exactly 1 argument.",
            "Returns 0 for NULL.",
            "Returns 0 for non-array values.",
        ],
        vec!["SELECT cardinality(arr)"],
    )
}

pub fn newuuid_function_def() -> FunctionDef {
    scalar_function_def(
        "newuuid",
        vec![],
        string_type(),
        "Generate a new UUID string.",
        vec!["Requires exactly 0 arguments."],
        vec!["SELECT newuuid()"],
    )
}

pub fn cast_function_def() -> FunctionDef {
    scalar_function_def(
        "cast",
        vec![req_arg("value", any_type()), req_arg("type", string_type())],
        any_type(),
        "Cast a value to the requested type.",
        vec![
            "Requires exactly 2 arguments.",
            "The second argument must be a string or NULL.",
            "Currently supports bigint, float, string, and boolean.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT cast('123', 'bigint')", "SELECT cast(1, 'string')"],
    )
}

pub fn tstamp_function_def() -> FunctionDef {
    scalar_function_def(
        "tstamp",
        vec![],
        int_type(),
        "Return the current Unix timestamp in milliseconds.",
        vec!["Requires exactly 0 arguments."],
        vec!["SELECT tstamp()"],
    )
}

pub fn delay_function_def() -> FunctionDef {
    scalar_function_def(
        "delay",
        vec![req_arg("millis", int_type()), req_arg("value", any_type())],
        any_type(),
        "Sleep for the given number of milliseconds and then return the second argument.",
        vec![
            "Requires exactly 2 arguments.",
            "The first argument must be an integer or NULL.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT delay(100, value)"],
    )
}

impl CustomFunc for BypassFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        Ok(args[0].clone())
    }

    fn name(&self) -> &str {
        "bypass"
    }
}

impl CustomFunc for ChrFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                if s.chars().count() != 1 {
                    return Err(EvalError::TypeMismatch {
                        expected: "single-character string".to_string(),
                        actual: s.clone(),
                    });
                }
                Ok(Value::String(s.clone()))
            }
            v => {
                let n = value_to_i64(v)?;
                let ch = char::from_u32(n as u32).ok_or_else(|| EvalError::TypeMismatch {
                    expected: "valid Unicode scalar value".to_string(),
                    actual: n.to_string(),
                })?;
                Ok(Value::String(ch.to_string()))
            }
        }
    }

    fn name(&self) -> &str {
        "chr"
    }
}

impl CustomFunc for EncodeFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_string_or_null(args, 0)?;
        nth_string_or_null(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(input) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(format) = nth_string_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        if !format.eq_ignore_ascii_case("base64") {
            return Err(EvalError::TypeMismatch {
                expected: "base64".to_string(),
                actual: format,
            });
        }

        Ok(Value::String(BASE64_STANDARD.encode(input.as_bytes())))
    }

    fn name(&self) -> &str {
        "encode"
    }
}

impl CustomFunc for DecodeFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_string_or_null(args, 0)?;
        nth_string_or_null(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(input) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(format) = nth_string_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        if !format.eq_ignore_ascii_case("base64") {
            return Err(EvalError::TypeMismatch {
                expected: "base64".to_string(),
                actual: format,
            });
        }

        let bytes =
            BASE64_STANDARD
                .decode(input.as_bytes())
                .map_err(|e| EvalError::TypeMismatch {
                    expected: "valid base64 string".to_string(),
                    actual: e.to_string(),
                })?;

        let s = String::from_utf8(bytes).map_err(|e| EvalError::TypeMismatch {
            expected: "base64-decoded UTF-8 string".to_string(),
            actual: e.to_string(),
        })?;

        Ok(Value::String(s))
    }

    fn name(&self) -> &str {
        "decode"
    }
}

impl CustomFunc for TruncFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_f64_or_null(args, 0)?;
        nth_i64_or_null(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        let Some(v) = nth_f64_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(raw_precision) = nth_i64_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        let precision = raw_precision.clamp(-34, 34) as i32;

        let result = if precision >= 0 {
            let factor = 10_f64.powi(precision);
            (v * factor).trunc() / factor
        } else {
            let factor = 10_f64.powi(-precision);
            (v / factor).trunc() * factor
        };

        Ok(Value::Float64(result))
    }

    fn name(&self) -> &str {
        "trunc"
    }
}

impl CustomFunc for Md5Func {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_string_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        Ok(Value::String(format!("{:x}", md5::compute(s.as_bytes()))))
    }

    fn name(&self) -> &str {
        "md5"
    }
}

impl CustomFunc for Sha1Func {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_string_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let mut h = Sha1::new();
        h.update(s.as_bytes());
        Ok(Value::String(format!("{:x}", h.finalize())))
    }

    fn name(&self) -> &str {
        "sha1"
    }
}

impl CustomFunc for Sha256Func {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_string_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let mut h = Sha256::new();
        h.update(s.as_bytes());
        Ok(Value::String(format!("{:x}", h.finalize())))
    }

    fn name(&self) -> &str {
        "sha256"
    }
}

impl CustomFunc for Sha384Func {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_string_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let mut h = Sha384::new();
        h.update(s.as_bytes());
        Ok(Value::String(format!("{:x}", h.finalize())))
    }

    fn name(&self) -> &str {
        "sha384"
    }
}

impl CustomFunc for Sha512Func {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_string_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let mut h = Sha512::new();
        h.update(s.as_bytes());
        Ok(Value::String(format!("{:x}", h.finalize())))
    }

    fn name(&self) -> &str {
        "sha512"
    }
}

impl CustomFunc for Crc32Func {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_string_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        Ok(Value::String(format!("{:x}", crc32_hash(s.as_bytes()))))
    }

    fn name(&self) -> &str {
        "crc32"
    }
}

impl CustomFunc for CoalesceFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        if args.is_empty() {
            return Err(EvalError::TypeMismatch {
                expected: "at least 1 argument".to_string(),
                actual: "0 arguments".to_string(),
            });
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        if args.is_empty() {
            return Err(EvalError::TypeMismatch {
                expected: "at least 1 argument".to_string(),
                actual: "0 arguments".to_string(),
            });
        }

        for arg in args {
            if !arg.is_null() {
                return Ok(arg.clone());
            }
        }
        Ok(Value::Null)
    }

    fn name(&self) -> &str {
        "coalesce"
    }
}

impl CustomFunc for Hex2DecFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_string_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(mut s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
            s = rest.to_string();
        }

        let value = i64::from_str_radix(&s, 16).map_err(|_| EvalError::TypeMismatch {
            expected: "valid hexadecimal string".to_string(),
            actual: s,
        })?;

        Ok(Value::Int64(value))
    }

    fn name(&self) -> &str {
        "hex2dec"
    }
}

impl CustomFunc for Dec2HexFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_i64_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(n) = nth_i64_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        Ok(Value::String(format!("0x{:x}", n)))
    }

    fn name(&self) -> &str {
        "dec2hex"
    }
}

impl CustomFunc for ToJsonFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;

        if args[0].is_null() {
            return Ok(Value::Null);
        }

        let json = value_to_json_value(&args[0])?;
        let s = serde_json::to_string(&json).map_err(|e| EvalError::TypeMismatch {
            expected: "JSON-serializable value".to_string(),
            actual: e.to_string(),
        })?;

        Ok(Value::String(s))
    }

    fn name(&self) -> &str {
        "to_json"
    }
}

impl CustomFunc for ParseJsonFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_string_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;

        let Some(text) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        if text == "null" {
            return Ok(Value::Null);
        }

        let parsed: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| EvalError::TypeMismatch {
                expected: "valid JSON string".to_string(),
                actual: e.to_string(),
            })?;

        json_value_to_value(&parsed)
    }

    fn name(&self) -> &str {
        "parse_json"
    }
}

impl CustomFunc for CardinalityFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;

        match &args[0] {
            Value::Null => Ok(Value::Int64(0)),
            Value::List(list) => Ok(Value::Int64(list.items().len() as i64)),
            _ => Ok(Value::Int64(0)),
        }
    }

    fn name(&self) -> &str {
        "cardinality"
    }
}

impl CustomFunc for NewUuidFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[0])?;
        Ok(Value::String(uuid::Uuid::new_v4().to_string()))
    }

    fn name(&self) -> &str {
        "newuuid"
    }
}

impl CustomFunc for CastFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_string_or_null(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        if args[0].is_null() || args[1].is_null() {
            return Ok(Value::Null);
        }

        let target = match nth_string_or_null(args, 1)? {
            Some(s) => s.to_ascii_lowercase(),
            // args[1].is_null() was checked above; this branch is unreachable
            None => return Ok(Value::Null),
        };

        match target.as_str() {
            "bigint" => cast_to_bigint(&args[0]),
            "float" => cast_to_float(&args[0]),
            "string" => cast_to_string(&args[0]),
            "boolean" => cast_to_boolean(&args[0]),
            other => Err(EvalError::TypeMismatch {
                expected: "one of bigint, float, string, boolean".to_string(),
                actual: other.to_string(),
            }),
        }
    }

    fn name(&self) -> &str {
        "cast"
    }
}

impl CustomFunc for TstampFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[0])?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        Ok(Value::Int64(now_ms))
    }

    fn name(&self) -> &str {
        "tstamp"
    }
}

impl CustomFunc for DelayFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_i64_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        let Some(ms) = nth_i64_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        if args[1].is_null() {
            return Ok(Value::Null);
        }

        if ms < 0 {
            return Err(EvalError::TypeMismatch {
                expected: "non-negative integer milliseconds".to_string(),
                actual: ms.to_string(),
            });
        }

        std::thread::sleep(std::time::Duration::from_millis(ms as u64));
        Ok(args[1].clone())
    }

    fn name(&self) -> &str {
        "delay"
    }
}

#[cfg(test)]
mod tests {
    use super::super::helpers::*;
    use super::*;
    use crate::expr::func::EvalError;

    fn type_mismatch(expected: &str, actual: &str) -> EvalError {
        EvalError::TypeMismatch {
            expected: expected.to_string(),
            actual: actual.to_string(),
        }
    }

    #[test]
    fn test_bypass_basic() {
        let func = BypassFunc;

        assert_int(func.eval_row(&[i(42)]).unwrap(), 42);
        assert_string(func.eval_row(&[s("abc")]).unwrap(), "abc");
        assert_bool(func.eval_row(&[b(true)]).unwrap(), true);
        assert_null(func.eval_row(&[n()]).unwrap());
    }

    #[test]
    fn test_bypass_wrong_arity() {
        let func = BypassFunc;
        let err = func.eval_row(&[]).unwrap_err();
        assert_eq!(err, type_mismatch("1 arguments", "0 arguments"));
    }

    #[test]
    fn test_chr_from_int() {
        let func = ChrFunc;

        assert_string(func.eval_row(&[i(65)]).unwrap(), "A");
        assert_string(func.eval_row(&[i(97)]).unwrap(), "a");
        assert_string(func.eval_row(&[Value::Uint8(66)]).unwrap(), "B");
    }

    #[test]
    fn test_chr_from_string() {
        let func = ChrFunc;

        assert_string(func.eval_row(&[s("A")]).unwrap(), "A");
        assert_string(func.eval_row(&[s("b")]).unwrap(), "b");
    }

    #[test]
    fn test_chr_null() {
        let func = ChrFunc;
        assert_null(func.eval_row(&[n()]).unwrap());
    }

    #[test]
    fn test_chr_string_too_long() {
        let func = ChrFunc;

        let err = func.eval_row(&[s("ab")]).unwrap_err();
        assert_eq!(err, type_mismatch("single-character string", "ab"));
    }

    #[test]
    fn test_chr_invalid_codepoint() {
        let func = ChrFunc;

        let err = func.eval_row(&[i(-1)]).unwrap_err();
        assert_eq!(err, type_mismatch("valid Unicode scalar value", "-1"));
    }

    #[test]
    fn test_chr_wrong_type() {
        let func = ChrFunc;

        let err = func.eval_row(&[b(true)]).unwrap_err();
        assert_eq!(err, type_mismatch("integer", "Bool(true)"));
    }

    #[test]
    fn test_encode_base64() {
        let func = EncodeFunc;

        assert_string(func.eval_row(&[s("abc"), s("base64")]).unwrap(), "YWJj");
        assert_string(func.eval_row(&[s("abc"), s("BASE64")]).unwrap(), "YWJj");
        assert_string(
            func.eval_row(&[s("hello world"), s("base64")]).unwrap(),
            "aGVsbG8gd29ybGQ=",
        );
    }

    #[test]
    fn test_encode_null() {
        let func = EncodeFunc;

        assert_null(func.eval_row(&[n(), s("base64")]).unwrap());
        assert_null(func.eval_row(&[s("abc"), n()]).unwrap());
    }

    #[test]
    fn test_encode_unsupported_format() {
        let func = EncodeFunc;

        let err = func.eval_row(&[s("abc"), s("hex")]).unwrap_err();
        assert_eq!(err, type_mismatch("base64", "hex"));
    }

    #[test]
    fn test_encode_wrong_type() {
        let func = EncodeFunc;

        let err = func.eval_row(&[i(1), s("base64")]).unwrap_err();
        assert_eq!(err, type_mismatch("string", "Int64(1)"));
    }

    #[test]
    fn test_decode_base64() {
        let func = DecodeFunc;

        assert_string(func.eval_row(&[s("YWJj"), s("base64")]).unwrap(), "abc");
        assert_string(func.eval_row(&[s("YWJj"), s("BASE64")]).unwrap(), "abc");
        assert_string(
            func.eval_row(&[s("aGVsbG8gd29ybGQ="), s("base64")])
                .unwrap(),
            "hello world",
        );
    }

    #[test]
    fn test_decode_null() {
        let func = DecodeFunc;

        assert_null(func.eval_row(&[n(), s("base64")]).unwrap());
        assert_null(func.eval_row(&[s("YWJj"), n()]).unwrap());
    }

    #[test]
    fn test_decode_unsupported_format() {
        let func = DecodeFunc;

        let err = func.eval_row(&[s("YWJj"), s("hex")]).unwrap_err();
        assert_eq!(err, type_mismatch("base64", "hex"));
    }

    #[test]
    fn test_decode_invalid_base64() {
        let func = DecodeFunc;

        let err = func.eval_row(&[s("%%%"), s("base64")]).unwrap_err();
        match err {
            EvalError::TypeMismatch { expected, .. } => {
                assert_eq!(expected, "valid base64 string");
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_decode_non_utf8_bytes() {
        let func = DecodeFunc;

        // /w== is [255], invalid UTF-8
        let err = func.eval_row(&[s("/w=="), s("base64")]).unwrap_err();
        match err {
            EvalError::TypeMismatch { expected, .. } => {
                assert_eq!(expected, "base64-decoded UTF-8 string");
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_trunc_basic() {
        let func = TruncFunc;

        assert_float(func.eval_row(&[f(12.3456), i(2)]).unwrap(), 12.34);
        assert_float(func.eval_row(&[f(12.344), i(2)]).unwrap(), 12.34);
        assert_float(func.eval_row(&[f(-1.25), i(1)]).unwrap(), -1.2);
        assert_float(func.eval_row(&[f(100.0), i(0)]).unwrap(), 100.0);
    }

    #[test]
    fn test_trunc_negative_precision() {
        let func = TruncFunc;

        assert_float(func.eval_row(&[f(1234.56), i(-2)]).unwrap(), 1200.0);
        assert_float(func.eval_row(&[f(1299.99), i(-2)]).unwrap(), 1200.0);
        assert_float(func.eval_row(&[f(-1234.56), i(-2)]).unwrap(), -1200.0);
        assert_float(func.eval_row(&[f(19.99), i(-1)]).unwrap(), 10.0);
        assert_float(func.eval_row(&[f(-19.99), i(-1)]).unwrap(), -10.0);
    }

    #[test]
    fn test_trunc_null() {
        let func = TruncFunc;

        assert_null(func.eval_row(&[n(), i(2)]).unwrap());
        assert_null(func.eval_row(&[f(12.3), n()]).unwrap());
    }

    #[test]
    fn test_trunc_wrong_type() {
        let func = TruncFunc;

        let err = func.eval_row(&[s("12.3"), i(2)]).unwrap_err();
        assert_eq!(err, type_mismatch("numeric", r#"String("12.3")"#));

        let err = func.eval_row(&[f(12.3), s("2")]).unwrap_err();
        assert_eq!(err, type_mismatch("integer", r#"String("2")"#));
    }

    #[test]
    fn test_trunc_precision_zero() {
        let func = TruncFunc;

        assert_float(func.eval_row(&[f(12.999), i(0)]).unwrap(), 12.0);
        assert_float(func.eval_row(&[f(-12.999), i(0)]).unwrap(), -12.0);
    }

    #[test]
    fn test_trunc_precision_clamped_high() {
        let func = TruncFunc;

        let actual_34 = func.eval_row(&[f(1.2345678901234567), i(34)]).unwrap();
        let actual_100 = func.eval_row(&[f(1.2345678901234567), i(100)]).unwrap();

        assert_eq!(actual_34, actual_100);
    }

    #[test]
    fn test_trunc_precision_clamped_low() {
        let func = TruncFunc;

        let actual_neg_34 = func.eval_row(&[f(1234.5678), i(-34)]).unwrap();
        let actual_neg_100 = func.eval_row(&[f(1234.5678), i(-100)]).unwrap();

        assert_eq!(actual_neg_34, actual_neg_100);
    }

    #[test]
    fn test_md5() {
        let func = Md5Func;
        assert_string(
            func.eval_row(&[s("abc")]).unwrap(),
            "900150983cd24fb0d6963f7d28e17f72",
        );
    }

    #[test]
    fn test_sha1() {
        let func = Sha1Func;
        assert_string(
            func.eval_row(&[s("abc")]).unwrap(),
            "a9993e364706816aba3e25717850c26c9cd0d89d",
        );
    }

    #[test]
    fn test_sha256() {
        let func = Sha256Func;
        assert_string(
            func.eval_row(&[s("abc")]).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        );
    }

    #[test]
    fn test_sha384() {
        let func = Sha384Func;
        assert_string(
            func.eval_row(&[s("abc")]).unwrap(),
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
        );
    }

    #[test]
    fn test_sha512() {
        let func = Sha512Func;
        assert_string(
            func.eval_row(&[s("abc")]).unwrap(),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea2\
0a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd\
454d4423643ce80e2a9ac94fa54ca49f",
        );
    }

    #[test]
    fn test_crc32() {
        let func = Crc32Func;
        assert_string(
            func.eval_row(&[s("abc")]).unwrap(),
            "352441c2".to_lowercase().as_str(),
        );
    }

    #[test]
    fn test_hash_funcs_null() {
        assert_null(Md5Func.eval_row(&[n()]).unwrap());
        assert_null(Sha1Func.eval_row(&[n()]).unwrap());
        assert_null(Sha256Func.eval_row(&[n()]).unwrap());
        assert_null(Sha384Func.eval_row(&[n()]).unwrap());
        assert_null(Sha512Func.eval_row(&[n()]).unwrap());
        assert_null(Crc32Func.eval_row(&[n()]).unwrap());
    }

    #[test]
    fn test_hash_funcs_wrong_type() {
        let err = Md5Func.eval_row(&[i(1)]).unwrap_err();
        assert_eq!(err, type_mismatch("string", "Int64(1)"));
    }

    #[test]
    fn test_coalesce_basic() {
        let func = CoalesceFunc;

        assert_int(func.eval_row(&[n(), i(2), i(3)]).unwrap(), 2);
        assert_string(func.eval_row(&[n(), s("x"), s("y")]).unwrap(), "x");
        assert_bool(func.eval_row(&[n(), b(false), b(true)]).unwrap(), false);
    }

    #[test]
    fn test_coalesce_all_null() {
        let func = CoalesceFunc;
        assert_null(func.eval_row(&[n(), n(), n()]).unwrap());
    }

    #[test]
    fn test_coalesce_single_arg() {
        let func = CoalesceFunc;
        assert_int(func.eval_row(&[i(7)]).unwrap(), 7);
        assert_null(func.eval_row(&[n()]).unwrap());
    }

    #[test]
    fn test_coalesce_zero_args() {
        let func = CoalesceFunc;
        let err = func.eval_row(&[]).unwrap_err();
        assert_eq!(err, type_mismatch("at least 1 argument", "0 arguments"));
    }

    #[test]
    fn test_hex2dec_basic() {
        let func = Hex2DecFunc;

        assert_int(func.eval_row(&[s("ff")]).unwrap(), 255);
        assert_int(func.eval_row(&[s("0xff")]).unwrap(), 255);
        assert_int(func.eval_row(&[s("0Xff")]).unwrap(), 255);
        assert_int(func.eval_row(&[s("7f")]).unwrap(), 127);
    }

    #[test]
    fn test_hex2dec_null() {
        let func = Hex2DecFunc;
        assert_null(func.eval_row(&[n()]).unwrap());
    }

    #[test]
    fn test_hex2dec_invalid() {
        let func = Hex2DecFunc;

        let err = func.eval_row(&[s("xyz")]).unwrap_err();
        assert_eq!(err, type_mismatch("valid hexadecimal string", "xyz"));
    }

    #[test]
    fn test_hex2dec_wrong_type() {
        let func = Hex2DecFunc;

        let err = func.eval_row(&[i(255)]).unwrap_err();
        assert_eq!(err, type_mismatch("string", "Int64(255)"));
    }

    #[test]
    fn test_dec2hex_basic() {
        let func = Dec2HexFunc;

        assert_string(func.eval_row(&[i(255)]).unwrap(), "0xff");
        assert_string(func.eval_row(&[i(0)]).unwrap(), "0x0");
        assert_string(func.eval_row(&[i(-1)]).unwrap(), "0xffffffffffffffff");
    }

    #[test]
    fn test_dec2hex_null() {
        let func = Dec2HexFunc;
        assert_null(func.eval_row(&[n()]).unwrap());
    }

    #[test]
    fn test_dec2hex_wrong_type() {
        let func = Dec2HexFunc;

        let err = func.eval_row(&[s("255")]).unwrap_err();
        assert_eq!(err, type_mismatch("integer", r#"String("255")"#));
    }

    #[test]
    fn test_to_json_basic() {
        let func = ToJsonFunc;

        assert_string(func.eval_row(&[i(1)]).unwrap(), "1");
        assert_string(func.eval_row(&[b(true)]).unwrap(), "true");
        assert_string(func.eval_row(&[s("abc")]).unwrap(), r#""abc""#);

        assert_string(func.eval_row(&[a(vec![i(1), i(2)])]).unwrap(), "[1,2]");

        assert_string(
            func.eval_row(&[m(vec![("a", i(1)), ("b", i(2))])]).unwrap(),
            r#"{"a":1,"b":2}"#,
        );
    }

    #[test]
    fn test_to_json_null() {
        let func = ToJsonFunc;
        assert_null(func.eval_row(&[n()]).unwrap());
    }

    #[test]
    fn test_parse_json_basic() {
        let func = ParseJsonFunc;

        assert_int(func.eval_row(&[s("1")]).unwrap(), 1);
        assert_bool(func.eval_row(&[s("true")]).unwrap(), true);

        assert_array(func.eval_row(&[s("[1,2]")]).unwrap(), vec![i(1), i(2)]);

        assert_map(
            func.eval_row(&[s(r#"{"a":1}"#)]).unwrap(),
            vec![("a", i(1))],
        );
    }

    #[test]
    fn test_parse_json_null_cases() {
        let func = ParseJsonFunc;

        assert_null(func.eval_row(&[n()]).unwrap());
        assert_null(func.eval_row(&[s("null")]).unwrap());
    }

    #[test]
    fn test_parse_json_invalid() {
        let func = ParseJsonFunc;

        let err = func.eval_row(&[s("{invalid}")]).unwrap_err();
        match err {
            EvalError::TypeMismatch { expected, .. } => {
                assert_eq!(expected, "valid JSON string");
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_cardinality_basic() {
        let func = CardinalityFunc;

        assert_int(func.eval_row(&[a(vec![i(1), i(2), i(3)])]).unwrap(), 3);
        assert_int(func.eval_row(&[a(vec![])]).unwrap(), 0);
    }

    #[test]
    fn test_cardinality_null() {
        let func = CardinalityFunc;

        assert_int(func.eval_row(&[n()]).unwrap(), 0);
    }

    #[test]
    fn test_cardinality_non_array() {
        let func = CardinalityFunc;

        assert_int(func.eval_row(&[i(1)]).unwrap(), 0);
    }

    #[test]
    fn test_newuuid_basic() {
        let func = NewUuidFunc;

        let v1 = func.eval_row(&[]).unwrap();
        let v2 = func.eval_row(&[]).unwrap();

        let s1 = match v1 {
            Value::String(s) => s,
            other => panic!("unexpected value: {:?}", other),
        };

        let s2 = match v2 {
            Value::String(s) => s,
            other => panic!("unexpected value: {:?}", other),
        };

        assert_eq!(s1.len(), 36);
        assert_eq!(s2.len(), 36);

        assert_ne!(s1, s2);
    }

    #[test]
    fn test_newuuid_wrong_arity() {
        let func = NewUuidFunc;

        let err = func.eval_row(&[i(1)]).unwrap_err();
        assert_eq!(err, type_mismatch("0 arguments", "1 arguments"));
    }

    #[test]
    fn test_cast_basic() {
        let func = CastFunc;

        assert_int(func.eval_row(&[s("123"), s("bigint")]).unwrap(), 123);
        assert_float(func.eval_row(&[s("12.5"), s("float")]).unwrap(), 12.5);
        assert_string(func.eval_row(&[i(1), s("string")]).unwrap(), "1");
        assert_bool(func.eval_row(&[s("true"), s("boolean")]).unwrap(), true);
    }

    #[test]
    fn test_cast_null() {
        let func = CastFunc;

        assert_null(func.eval_row(&[n(), s("bigint")]).unwrap());
        assert_null(func.eval_row(&[i(1), n()]).unwrap());
    }

    #[test]
    fn test_tstamp_basic() {
        let func = TstampFunc;

        let v1 = func.eval_row(&[]).unwrap();
        let v2 = func.eval_row(&[]).unwrap();

        match (v1, v2) {
            (Value::Int64(a), Value::Int64(b)) => {
                assert!(b >= a);
            }
            other => panic!("unexpected values: {:?}", other),
        }
    }

    #[test]
    fn test_delay_basic() {
        let func = DelayFunc;

        assert_string(func.eval_row(&[i(0), s("abc")]).unwrap(), "abc");
    }

    #[test]
    fn test_delay_null() {
        let func = DelayFunc;

        assert_null(func.eval_row(&[n(), s("abc")]).unwrap());
        assert_null(func.eval_row(&[i(0), n()]).unwrap());
    }
}
