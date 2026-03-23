use crate::expr::custom_func::helpers::{
    binary_string_to_bool, binary_string_to_bool_result, binary_string_to_i64_result,
    binary_string_to_string_result, compile_regex, nth_f64_or_null, nth_i64_or_null,
    nth_string_or_null, nth_string_strict, string_nonnegative_i64_to_string,
    substring_by_char_start_length, ternary_string_to_string, unary_string_to_i64,
    unary_string_to_string, validate_arity, validate_one_string_or_null,
    validate_string_i64_or_null, validate_two_strict_strings, validate_two_strings_or_null,
};
use crate::expr::custom_func::CustomFunc;
use icu::decimal::input::Decimal as IcuDecimal;
use icu::decimal::DecimalFormatter;
use icu::locale::LanguageIdentifier;
use std::str::FromStr;

// format_time function is moved to "cast" function in Transform Functions

use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionSignatureSpec, TypeSpec,
};
use crate::expr::func::EvalError;
use datatypes::Value;

#[derive(Debug, Clone)]
pub struct FormatFunc;

#[derive(Debug, Clone)]
pub struct ConcatFunc;

#[derive(Debug, Clone)]
pub struct EndsWithFunc;

#[derive(Debug, Clone)]
pub struct IndexOfFunc;

#[derive(Debug, Clone)]
pub struct LengthFunc;

#[derive(Debug, Clone)]
pub struct LowerFunc;

#[derive(Debug, Clone)]
pub struct LPadFunc;

#[derive(Debug, Clone)]
pub struct LTrimFunc;

#[derive(Debug, Clone)]
pub struct NumBytesFunc;

#[derive(Debug, Clone)]
pub struct RegexpMatchesFunc;

#[derive(Debug, Clone)]
pub struct RegexpReplaceFunc;

#[derive(Debug, Clone)]
pub struct RegexpSubstrFunc;

#[derive(Debug, Clone)]
pub struct ReverseFunc;

#[derive(Debug, Clone)]
pub struct RPadFunc;

#[derive(Debug, Clone)]
pub struct RTrimFunc;

#[derive(Debug, Clone)]
pub struct SubstringFunc;

#[derive(Debug, Clone)]
pub struct StartsWithFunc;

#[derive(Debug, Clone)]
pub struct SplitValueFunc;

#[derive(Debug, Clone)]
pub struct TrimFunc;

#[derive(Debug, Clone)]
pub struct UpperFunc;

fn reverse_by_chars(s: &str) -> String {
    s.chars().rev().collect()
}

/// Custom implementation of the concat function
/// This function concatenates exactly 2 String arguments
pub fn concat_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Scalar,
        name: "concat".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![
                FunctionArgSpec {
                    name: "a".to_string(),
                    r#type: TypeSpec::Named {
                        name: "string".to_string(),
                    },
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "b".to_string(),
                    r#type: TypeSpec::Named {
                        name: "string".to_string(),
                    },
                    optional: false,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Named {
                name: "string".to_string(),
            },
        },
        description: "Concatenate two strings.".to_string(),
        allowed_contexts: vec![
            FunctionContext::Select,
            FunctionContext::Where,
            FunctionContext::GroupBy,
        ],
        requirements: vec![],
        constraints: vec![
            "Requires exactly 2 arguments.".to_string(),
            "Both arguments must be strings.".to_string(),
            "NULL is not accepted as a string argument in the current implementation.".to_string(),
        ],
        examples: vec![
            "SELECT concat('hello', 'world') AS s".to_string(),
            "SELECT concat(a, b)".to_string(),
        ],
        aggregate: None,
        stateful: None,
    }
}

impl CustomFunc for ConcatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_strict_strings(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let a = nth_string_strict(args, 0)?;
        let b = nth_string_strict(args, 1)?;
        Ok(Value::String(format!("{}{}", a, b)))
    }

    fn name(&self) -> &str {
        "concat"
    }
}

impl CustomFunc for EndsWithFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_strings_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_string_to_bool(args, |a, b| a.ends_with(b))
    }

    fn name(&self) -> &str {
        "endswith"
    }
}

impl CustomFunc for IndexOfFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_strings_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_string_to_i64_result(args, |a, b| {
            Ok(match a.find(b) {
                Some(byte_idx) => a[..byte_idx].chars().count() as i64,
                None => -1,
            })
        })
    }

    fn name(&self) -> &str {
        "indexof"
    }
}

impl CustomFunc for LengthFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_string_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_string_to_i64(args, |s| s.chars().count() as i64)
    }

    fn name(&self) -> &str {
        "length"
    }
}

impl CustomFunc for LowerFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_string_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_string_to_string(args, |s| s.to_lowercase())
    }

    fn name(&self) -> &str {
        "lower"
    }
}

impl CustomFunc for LPadFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_string_i64_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        string_nonnegative_i64_to_string(args, |s, n| format!("{}{}", " ".repeat(n), s))
    }

    fn name(&self) -> &str {
        "lpad"
    }
}

impl CustomFunc for LTrimFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_string_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_string_to_string(args, |s| s.trim_start().to_string())
    }

    fn name(&self) -> &str {
        "ltrim"
    }
}

impl CustomFunc for NumBytesFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_string_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_string_to_i64(args, |s| s.len() as i64)
    }

    fn name(&self) -> &str {
        "numbytes"
    }
}

impl CustomFunc for RegexpMatchesFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_strings_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_string_to_bool_result(args, |s, pat| {
            let re = compile_regex(pat)?;
            Ok(re.is_match(s))
        })
    }

    fn name(&self) -> &str {
        "regexp_matches"
    }
}

impl CustomFunc for RegexpReplaceFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[3])?;
        nth_string_or_null(args, 0)?;
        nth_string_or_null(args, 1)?;
        nth_string_or_null(args, 2)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        ternary_string_to_string(args, |s, pat, repl| {
            let re = compile_regex(pat)?;
            Ok(re.replace_all(s, repl).to_string())
        })
    }

    fn name(&self) -> &str {
        "regexp_replace"
    }
}

impl CustomFunc for RegexpSubstrFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_strings_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_string_to_string_result(args, |s, pat| {
            let re = compile_regex(pat)?;
            Ok(re
                .find(s)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default())
        })
    }

    fn name(&self) -> &str {
        "regexp_substring"
    }
}

impl CustomFunc for ReverseFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_string_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_string_to_string(args, reverse_by_chars)
    }

    fn name(&self) -> &str {
        "reverse"
    }
}

impl CustomFunc for RPadFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_string_i64_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        string_nonnegative_i64_to_string(args, |s, n| format!("{}{}", s, " ".repeat(n)))
    }

    fn name(&self) -> &str {
        "rpad"
    }
}

impl CustomFunc for RTrimFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_string_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_string_to_string(args, |s| s.trim_end().to_string())
    }

    fn name(&self) -> &str {
        "rtrim"
    }
}

impl CustomFunc for SubstringFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2, 3])?;
        nth_string_or_null(args, 0)?;
        nth_i64_or_null(args, 1)?;
        if args.len() == 3 {
            nth_i64_or_null(args, 2)?;
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2, 3])?;

        let Some(s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(start) = nth_i64_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        let length = if args.len() == 3 {
            let Some(l) = nth_i64_or_null(args, 2)? else {
                return Ok(Value::Null);
            };
            Some(l)
        } else {
            None
        };

        Ok(Value::String(substring_by_char_start_length(
            &s, start, length,
        )?))
    }

    fn name(&self) -> &str {
        "substring"
    }
}

impl CustomFunc for StartsWithFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_strings_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_string_to_bool(args, |a, b| a.starts_with(b))
    }

    fn name(&self) -> &str {
        "startswith"
    }
}

impl CustomFunc for SplitValueFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[3])?;
        nth_string_or_null(args, 0)?;
        nth_string_or_null(args, 1)?;
        nth_i64_or_null(args, 2)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[3])?;

        let Some(s) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(delim) = nth_string_or_null(args, 1)? else {
            return Ok(Value::Null);
        };
        let Some(idx) = nth_i64_or_null(args, 2)? else {
            return Ok(Value::Null);
        };

        let parts: Vec<&str> = s.split(&delim).collect();
        let len = parts.len() as i64;

        let actual = if idx >= 0 { idx } else { len + idx };
        if actual < 0 || actual >= len {
            return Err(EvalError::TypeMismatch {
                expected: format!("index in range [0, {}) or negative within bounds", len),
                actual: idx.to_string(),
            });
        }

        Ok(Value::String(parts[actual as usize].to_string()))
    }

    fn name(&self) -> &str {
        "split_value"
    }
}

impl CustomFunc for TrimFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_string_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_string_to_string(args, |s| s.trim().to_string())
    }

    fn name(&self) -> &str {
        "trim"
    }
}

impl CustomFunc for UpperFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_string_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_string_to_string(args, |s| s.to_uppercase())
    }

    fn name(&self) -> &str {
        "upper"
    }
}

fn parse_format_locale(locale: &str) -> Result<LanguageIdentifier, EvalError> {
    let normalized = locale.replace('_', "-");

    let langid =
        LanguageIdentifier::try_from_str(&normalized).map_err(|_| EvalError::TypeMismatch {
            expected: "supported locale, e.g. en_US".to_string(),
            actual: locale.to_string(),
        })?;

    if langid.region.is_none() {
        return Err(EvalError::TypeMismatch {
            expected: "locale with country/region, e.g. en_US".to_string(),
            actual: locale.to_string(),
        });
    }

    Ok(langid)
}

fn format_decimal_with_locale(
    value: f64,
    precision: usize,
    locale: &str,
    use_grouping: bool,
) -> Result<String, EvalError> {
    if !value.is_finite() {
        return Err(EvalError::TypeMismatch {
            expected: "finite numeric value".to_string(),
            actual: value.to_string(),
        });
    }

    let langid = parse_format_locale(locale)?;

    let formatter = DecimalFormatter::try_new(langid.into(), Default::default()).map_err(|_| {
        EvalError::TypeMismatch {
            expected: "locale supported by ICU decimal formatter".to_string(),
            actual: locale.to_string(),
        }
    })?;

    let rounded = format!("{:.*}", precision, value);

    let decimal = IcuDecimal::from_str(&rounded).map_err(|_| EvalError::TypeMismatch {
        expected: "parsable decimal after rounding".to_string(),
        actual: rounded.clone(),
    })?;

    let mut out = formatter.format(&decimal).to_string();
    if !use_grouping {
        out.retain(|ch| {
            ch != ','
                && ch != '.'
                && ch != '\''
                && ch != ' '
                && ch != '\u{00A0}'
                && ch != '\u{202F}'
        });

        let (sign, digits) = if let Some(rest) = rounded.strip_prefix('-') {
            ("-", rest)
        } else {
            ("", rounded.as_str())
        };

        return Ok(format!("{sign}{digits}"));
    }

    Ok(out)
}

impl CustomFunc for FormatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2, 3])?;
        nth_f64_or_null(args, 0)?;
        nth_i64_or_null(args, 1)?;
        if args.len() == 3 {
            nth_string_or_null(args, 2)?;
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2, 3])?;

        let Some(v1) = nth_f64_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(v2) = nth_i64_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        if v2 < 0 {
            return Err(EvalError::TypeMismatch {
                expected: "decimal places >= 0".to_string(),
                actual: v2.to_string(),
            });
        }

        let precision = v2 as usize;

        if args.len() == 2 {
            let s = format!("{:.*}", precision, v1);
            return Ok(Value::String(s));
        }

        let Some(locale_str) = nth_string_or_null(args, 2)? else {
            return Ok(Value::Null);
        };

        let s = format_decimal_with_locale(v1, precision, &locale_str, true)?;
        Ok(Value::String(s))
    }

    fn name(&self) -> &str {
        "format"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::custom_func::helpers::{
        a, assert_array, assert_bool, assert_float, assert_int, assert_map, assert_null,
        assert_string, b, f, i, m, n, s,
    };
    use datatypes::Value;

    #[test]
    fn test_string_functions_correctness() {
        // concat
        let func = ConcatFunc;
        assert_string(
            func.eval_row(&[s("hello"), s("world")]).unwrap(),
            "helloworld",
        );

        // endswith
        let func = EndsWithFunc;
        assert_bool(func.eval_row(&[s("abcdef"), s("def")]).unwrap(), true);
        assert_bool(func.eval_row(&[s("abcdef"), s("abc")]).unwrap(), false);

        // indexof
        let func = IndexOfFunc;
        assert_int(func.eval_row(&[s("abcdef"), s("cd")]).unwrap(), 2);
        assert_int(func.eval_row(&[s("abcdef"), s("x")]).unwrap(), -1);

        // length
        let func = LengthFunc;
        assert_int(func.eval_row(&[s("abc")]).unwrap(), 3);
        assert_int(func.eval_row(&[s("")]).unwrap(), 0);

        // lower
        let func = LowerFunc;
        assert_string(func.eval_row(&[s("AbCXYZ")]).unwrap(), "abcxyz");

        // lpad
        let func = LPadFunc;
        assert_string(func.eval_row(&[s("abc"), i(2)]).unwrap(), "  abc");
        assert_string(func.eval_row(&[s("abc"), i(0)]).unwrap(), "abc");

        // ltrim
        let func = LTrimFunc;
        assert_string(func.eval_row(&[s("   \t abc  ")]).unwrap(), "abc  ");

        // numbytes
        let func = NumBytesFunc;
        assert_int(func.eval_row(&[s("abc")]).unwrap(), 3);

        // regexp_matches
        let func = RegexpMatchesFunc;
        assert_bool(func.eval_row(&[s("abc123"), s(r"\d+")]).unwrap(), true);
        assert_bool(func.eval_row(&[s("abcdef"), s(r"\d+")]).unwrap(), false);

        // regexp_replace
        let func = RegexpReplaceFunc;
        assert_string(
            func.eval_row(&[s("abc123def456"), s(r"\d+"), s("#")])
                .unwrap(),
            "abc#def#",
        );
        assert_string(
            func.eval_row(&[s("hello world"), s("world"), s("ekuiper")])
                .unwrap(),
            "hello ekuiper",
        );

        // regexp_substr
        let func = RegexpSubstrFunc;
        assert_string(func.eval_row(&[s("abc123def"), s(r"\d+")]).unwrap(), "123");
        assert_string(func.eval_row(&[s("abcdef"), s(r"\d+")]).unwrap(), "");

        // reverse
        let func = ReverseFunc;
        assert_string(func.eval_row(&[s("abc")]).unwrap(), "cba");
        assert_string(func.eval_row(&[s("abcd")]).unwrap(), "dcba");

        // rpad
        let func = RPadFunc;
        assert_string(func.eval_row(&[s("abc"), i(2)]).unwrap(), "abc  ");
        assert_string(func.eval_row(&[s("abc"), i(0)]).unwrap(), "abc");

        // rtrim
        let func = RTrimFunc;
        assert_string(func.eval_row(&[s("  abc \t  ")]).unwrap(), "  abc");

        // substring
        let func = SubstringFunc;
        assert_string(func.eval_row(&[s("abcdef"), i(2), i(3)]).unwrap(), "cde");
        assert_string(func.eval_row(&[s("abcdef"), i(2)]).unwrap(), "cdef");
        assert_string(func.eval_row(&[s("abcdef"), i(10), i(3)]).unwrap(), "");

        // startswith
        let func = StartsWithFunc;
        assert_bool(func.eval_row(&[s("abcdef"), s("abc")]).unwrap(), true);
        assert_bool(func.eval_row(&[s("abcdef"), s("def")]).unwrap(), false);

        // split_value
        let func = SplitValueFunc;
        assert_string(
            func.eval_row(&[s("/test/device001/message"), s("/"), i(0)])
                .unwrap(),
            "",
        );
        assert_string(
            func.eval_row(&[s("/test/device001/message"), s("/"), i(3)])
                .unwrap(),
            "message",
        );
        assert_string(func.eval_row(&[s("a,b,c"), s(","), i(1)]).unwrap(), "b");
        assert_string(func.eval_row(&[s("a,b,c"), s(","), i(-1)]).unwrap(), "c");

        // trim
        let func = TrimFunc;
        assert_string(func.eval_row(&[s("   abc \t  ")]).unwrap(), "abc");

        // upper
        let func = UpperFunc;
        assert_string(func.eval_row(&[s("abcxyz")]).unwrap(), "ABCXYZ");

        // format
        let func = FormatFunc;

        // basic rounding (no locale)
        assert_string(
            func.eval_row(&[f(12332.1234567), i(4)]).unwrap(),
            "12332.1235",
        );
        assert_string(func.eval_row(&[f(12.6), i(0)]).unwrap(), "13");
        assert_string(func.eval_row(&[f(-12.345), i(2)]).unwrap(), "-12.35");
        assert_string(func.eval_row(&[f(0.0), i(3)]).unwrap(), "0.000");

        // no locale: no grouping
        assert_string(func.eval_row(&[f(123456.78), i(2)]).unwrap(), "123456.78");
        assert_string(func.eval_row(&[f(1234567.0), i(0)]).unwrap(), "1234567");

        // locale-aware
        assert_string(
            func.eval_row(&[f(12332.1234567), i(4), s("en_US")])
                .unwrap(),
            "12,332.1235",
        );

        assert_string(
            func.eval_row(&[f(123456.78), i(2), s("de_DE")]).unwrap(),
            "123.456,78",
        );

        assert_string(
            func.eval_row(&[f(-123456.78), i(2), s("en_US")]).unwrap(),
            "-123,456.78",
        );
    }

    #[test]
    fn test_string_functions_arity() {
        // concat
        let func = ConcatFunc;
        assert!(func.validate_row(&[s("a")]).is_err());
        assert!(func.validate_row(&[s("a"), s("b"), s("c")]).is_err());

        // regexp_replace
        let func = RegexpReplaceFunc;
        assert!(func.validate_row(&[s("abc"), s("a")]).is_err());
        assert!(func
            .validate_row(&[s("abc"), s("a"), s("b"), s("c")])
            .is_err());

        // substring
        let func = SubstringFunc;
        assert!(func.validate_row(&[s("abc")]).is_err());
        assert!(func.validate_row(&[s("abc"), i(1), i(2), i(3)]).is_err());

        // split_value
        let func = SplitValueFunc;
        assert!(func.validate_row(&[s("a,b,c"), s(",")]).is_err());
        assert!(func
            .validate_row(&[s("a,b,c"), s(","), i(1), i(2)])
            .is_err());

        // format
        let func = FormatFunc;
        assert!(func.validate_row(&[f(1.23)]).is_err());
        assert!(func
            .validate_row(&[f(1.23), i(2), s("en_US"), s("extra")])
            .is_err());
    }

    #[test]
    fn test_string_functions_null_and_edge_cases() {
        // endswith
        let func = EndsWithFunc;
        assert_eq!(func.eval_row(&[n(), s("x")]).unwrap(), Value::Null);

        // length
        let func = LengthFunc;
        assert_eq!(func.eval_row(&[n()]).unwrap(), Value::Null);

        // regexp_matches
        let func = RegexpMatchesFunc;
        assert!(func.eval_row(&[s("abc"), s(r"(")]).is_err());

        // regexp_substr
        let func = RegexpSubstrFunc;
        assert!(func.eval_row(&[s("abc"), s(r"(")]).is_err());

        // lpad / rpad
        let func = LPadFunc;
        assert!(func.eval_row(&[s("abc"), i(-1)]).is_err());

        let func = RPadFunc;
        assert!(func.eval_row(&[s("abc"), i(-1)]).is_err());

        // substring
        let func = SubstringFunc;
        assert!(func.eval_row(&[s("abcdef"), i(-1), i(3)]).is_err());
        assert!(func.eval_row(&[s("abcdef"), i(1), i(-3)]).is_err());
        assert_eq!(func.eval_row(&[n(), i(1), i(2)]).unwrap(), Value::Null);

        // split_value
        let func = SplitValueFunc;
        assert!(func.eval_row(&[s("a,b,c"), s(","), i(3)]).is_err());
        assert!(func.eval_row(&[s("a,b,c"), s(","), i(-4)]).is_err());
        assert_eq!(func.eval_row(&[n(), s(","), i(0)]).unwrap(), Value::Null);

        // format
        let func = FormatFunc;
        assert!(func.eval_row(&[f(12.3), i(-1)]).is_err());
        assert!(func.eval_row(&[f(12.3), i(2), s("bad")]).is_err());
        assert_eq!(func.eval_row(&[n(), i(2)]).unwrap(), Value::Null);
    }
}
