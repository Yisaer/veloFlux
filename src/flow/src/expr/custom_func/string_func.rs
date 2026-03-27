use super::helpers::*;
use crate::catalog::FunctionDef;
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use crate::expr::scalar::ScalarExpr;
use datatypes::Value;
use icu::decimal::input::Decimal as IcuDecimal;
use icu::decimal::DecimalFormatter;
use icu::locale::LanguageIdentifier;
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;

// format_time function is moved to "cast" function in Transform Functions

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

#[derive(Clone)]
struct CompiledRegexpMatchesFunc {
    regex: Regex,
}

#[derive(Clone)]
struct CompiledRegexpReplaceFunc {
    regex: Regex,
}

#[derive(Clone)]
struct CompiledRegexpSubstrFunc {
    regex: Regex,
}

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

pub fn builtin_function_defs() -> Vec<FunctionDef> {
    vec![
        format_function_def(),
        concat_function_def(),
        endswith_function_def(),
        indexof_function_def(),
        length_function_def(),
        lower_function_def(),
        lpad_function_def(),
        ltrim_function_def(),
        numbytes_function_def(),
        regexp_matches_function_def(),
        regexp_replace_function_def(),
        regexp_substr_function_def(),
        reverse_function_def(),
        rpad_function_def(),
        rtrim_function_def(),
        substring_function_def(),
        startswith_function_def(),
        split_value_function_def(),
        trim_function_def(),
        upper_function_def(),
    ]
}

fn reverse_by_chars(s: &str) -> String {
    s.chars().rev().collect()
}

fn literal_pattern_arg(args: &[ScalarExpr], idx: usize) -> Option<&str> {
    match args.get(idx) {
        Some(ScalarExpr::Literal(Value::String(pattern), _)) => Some(pattern.as_str()),
        _ => None,
    }
}

pub fn maybe_specialize_literal_regex_func(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<Option<Arc<dyn CustomFunc>>, EvalError> {
    let Some(pattern) = literal_pattern_arg(args, 1) else {
        return Ok(None);
    };

    let func: Arc<dyn CustomFunc> = match func_name {
        "regexp_matches" => Arc::new(CompiledRegexpMatchesFunc {
            regex: compile_regex(pattern)?,
        }),
        "regexp_replace" => Arc::new(CompiledRegexpReplaceFunc {
            regex: compile_regex(pattern)?,
        }),
        "regexp_substring" => Arc::new(CompiledRegexpSubstrFunc {
            regex: compile_regex(pattern)?,
        }),
        _ => return Ok(None),
    };

    Ok(Some(func))
}

impl std::fmt::Debug for CompiledRegexpMatchesFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompiledRegexpMatchesFunc")
    }
}

impl std::fmt::Debug for CompiledRegexpReplaceFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompiledRegexpReplaceFunc")
    }
}

impl std::fmt::Debug for CompiledRegexpSubstrFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompiledRegexpSubstrFunc")
    }
}

pub fn format_function_def() -> FunctionDef {
    scalar_function_def(
        "format",
        vec![
            req_arg("value", float_type()),
            req_arg("precision", int_type()),
            opt_arg("locale", string_type()),
        ],
        string_type(),
        "Format a numeric value as a string, optionally using a locale.",
        vec![
            "Requires 2 or 3 arguments.",
            "The first argument must be numeric.",
            "The second argument must be an integer precision >= 0.",
            "The optional third argument must be a locale string such as en_US.",
            "Returns NULL if any provided argument is NULL.",
        ],
        vec![
            "SELECT format(12.3456, 2) AS s",
            "SELECT format(price, 2, 'en_US')",
        ],
    )
}

pub fn concat_function_def() -> FunctionDef {
    scalar_function_def(
        "concat",
        vec![req_arg("a", string_type()), req_arg("b", string_type())],
        string_type(),
        "Concatenate two strings.",
        vec![
            "Requires exactly 2 arguments.",
            "Both arguments must be strings.",
            "NULL is not accepted as a string argument in the current implementation.",
        ],
        vec![
            "SELECT concat('hello', 'world') AS s",
            "SELECT concat(a, b)",
        ],
    )
}

pub fn endswith_function_def() -> FunctionDef {
    scalar_function_def(
        "endswith",
        vec![
            req_arg("value", string_type()),
            req_arg("suffix", string_type()),
        ],
        bool_type(),
        "Return whether a string ends with the given suffix.",
        vec![
            "Requires exactly 2 arguments.",
            "Both arguments must be strings or NULL.",
            "Returns NULL if any argument is NULL.",
        ],
        vec![
            "SELECT endswith('hello', 'lo')",
            "SELECT endswith(name, '.csv')",
        ],
    )
}

pub fn indexof_function_def() -> FunctionDef {
    scalar_function_def(
        "indexof",
        vec![
            req_arg("value", string_type()),
            req_arg("substr", string_type()),
        ],
        int_type(),
        "Return the character index of the first occurrence of a substring, or -1 if not found.",
        vec![
            "Requires exactly 2 arguments.",
            "Both arguments must be strings or NULL.",
            "Returns NULL if any argument is NULL.",
            "Character positions are counted by chars, not raw bytes.",
        ],
        vec![
            "SELECT indexof('banana', 'na')",
            "SELECT indexof(message, ':')",
        ],
    )
}

pub fn length_function_def() -> FunctionDef {
    unary_string_fn_def(
        "length",
        int_type(),
        "Return the number of characters in a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT length('hello')", "SELECT length(name)"],
    )
}

pub fn lower_function_def() -> FunctionDef {
    unary_string_fn_def(
        "lower",
        string_type(),
        "Convert a string to lowercase.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT lower('Hello')", "SELECT lower(name)"],
    )
}

pub fn lpad_function_def() -> FunctionDef {
    scalar_function_def(
        "lpad",
        vec![
            req_arg("value", string_type()),
            req_arg("count", int_type()),
        ],
        string_type(),
        "Pad a string on the left with spaces.",
        vec![
            "Requires exactly 2 arguments.",
            "The first argument must be a string or NULL.",
            "The second argument must be a non-negative integer or NULL.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT lpad('x', 3)", "SELECT lpad(code, 5)"],
    )
}

pub fn ltrim_function_def() -> FunctionDef {
    unary_string_fn_def(
        "ltrim",
        string_type(),
        "Trim leading whitespace from a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT ltrim('  hello')", "SELECT ltrim(name)"],
    )
}

pub fn numbytes_function_def() -> FunctionDef {
    unary_string_fn_def(
        "numbytes",
        int_type(),
        "Return the UTF-8 byte length of a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT numbytes('hello')", "SELECT numbytes(name)"],
    )
}

pub fn regexp_matches_function_def() -> FunctionDef {
    scalar_function_def(
        "regexp_matches",
        vec![
            req_arg("value", string_type()),
            req_arg("pattern", string_type()),
        ],
        bool_type(),
        "Return whether a string matches a regular expression.",
        vec![
            "Requires exactly 2 arguments.",
            "Both arguments must be strings or NULL.",
            "Returns NULL if any argument is NULL.",
            "Invalid regex patterns return an evaluation error.",
        ],
        vec![
            "SELECT regexp_matches('abc123', '[0-9]+')",
            "SELECT regexp_matches(message, '^ERR')",
        ],
    )
}

pub fn regexp_replace_function_def() -> FunctionDef {
    scalar_function_def(
        "regexp_replace",
        vec![
            req_arg("value", string_type()),
            req_arg("pattern", string_type()),
            req_arg("replacement", string_type()),
        ],
        string_type(),
        "Replace all matches of a regular expression in a string.",
        vec![
            "Requires exactly 3 arguments.",
            "All arguments must be strings or NULL.",
            "Returns NULL if any argument is NULL.",
            "Invalid regex patterns return an evaluation error.",
        ],
        vec![
            "SELECT regexp_replace('abc123', '[0-9]+', '_')",
            "SELECT regexp_replace(message, '\\\\s+', ' ')",
        ],
    )
}

pub fn regexp_substr_function_def() -> FunctionDef {
    scalar_function_def_with_aliases(
        "regexp_substring",
        vec!["regexp_substr"],
        vec![
            req_arg("value", string_type()),
            req_arg("pattern", string_type()),
        ],
        string_type(),
        "Return the first substring matching a regular expression.",
        vec![
            "Requires exactly 2 arguments.",
            "Both arguments must be strings or NULL.",
            "Returns NULL if any argument is NULL.",
            "Returns an empty string if there is no match.",
            "Invalid regex patterns return an evaluation error.",
        ],
        vec![
            "SELECT regexp_substring('abc123', '[0-9]+')",
            "SELECT regexp_substring(message, 'ERR[0-9]+')",
        ],
    )
}

pub fn reverse_function_def() -> FunctionDef {
    unary_string_fn_def(
        "reverse",
        string_type(),
        "Reverse a string by characters.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT reverse('abc')", "SELECT reverse(name)"],
    )
}

pub fn rpad_function_def() -> FunctionDef {
    scalar_function_def(
        "rpad",
        vec![
            req_arg("value", string_type()),
            req_arg("count", int_type()),
        ],
        string_type(),
        "Pad a string on the right with spaces.",
        vec![
            "Requires exactly 2 arguments.",
            "The first argument must be a string or NULL.",
            "The second argument must be a non-negative integer or NULL.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT rpad('x', 3)", "SELECT rpad(code, 5)"],
    )
}

pub fn rtrim_function_def() -> FunctionDef {
    unary_string_fn_def(
        "rtrim",
        string_type(),
        "Trim trailing whitespace from a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT rtrim('hello  ')", "SELECT rtrim(name)"],
    )
}

pub fn substring_function_def() -> FunctionDef {
    scalar_function_def(
        "substring",
        vec![
            req_arg("value", string_type()),
            req_arg("start", int_type()),
            opt_arg("length", int_type()),
        ],
        string_type(),
        "Extract a substring starting at a character index, with an optional length.",
        vec![
            "Requires 2 or 3 arguments.",
            "The first argument must be a string or NULL.",
            "The start argument must be an integer or NULL.",
            "The optional length argument must be an integer or NULL.",
            "Returns NULL if any provided argument is NULL.",
        ],
        vec![
            "SELECT substring('abcdef', 2)",
            "SELECT substring('abcdef', 2, 3)",
        ],
    )
}

pub fn startswith_function_def() -> FunctionDef {
    binary_string_fn_def(
        "startswith",
        bool_type(),
        "Return whether a string starts with the given prefix.",
        vec![
            "Requires exactly 2 arguments.",
            "Both arguments must be strings or NULL.",
            "Returns NULL if any argument is NULL.",
        ],
        vec![
            "SELECT startswith('hello', 'he')",
            "SELECT startswith(name, 'tmp_')",
        ],
    )
}

pub fn split_value_function_def() -> FunctionDef {
    scalar_function_def(
        "split_value",
        vec![
            req_arg("value", string_type()),
            req_arg("delimiter", string_type()),
            req_arg("index", int_type()),
        ],
        string_type(),
        "Split a string by a delimiter and return one element by index.",
        vec![
            "Requires exactly 3 arguments.",
            "The first two arguments must be strings or NULL.",
            "The third argument must be an integer or NULL.",
            "Returns NULL if any argument is NULL.",
            "Negative indexes are allowed if they stay within bounds.",
        ],
        vec![
            "SELECT split_value('a,b,c', ',', 1)",
            "SELECT split_value(path, '/', -1)",
        ],
    )
}

pub fn trim_function_def() -> FunctionDef {
    unary_string_fn_def(
        "trim",
        string_type(),
        "Trim leading and trailing whitespace from a string.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT trim('  hello  ')", "SELECT trim(name)"],
    )
}

pub fn upper_function_def() -> FunctionDef {
    unary_string_fn_def(
        "upper",
        string_type(),
        "Convert a string to uppercase.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be a string or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT upper('Hello')", "SELECT upper(name)"],
    )
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

    fn aliases(&self) -> &'static [&'static str] {
        &["regexp_substr"]
    }
}

impl CustomFunc for CompiledRegexpMatchesFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_strings_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_string_to_bool_result(args, |s, _pat| Ok(self.regex.is_match(s)))
    }

    fn name(&self) -> &str {
        "regexp_matches"
    }
}

impl CustomFunc for CompiledRegexpReplaceFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[3])?;
        nth_string_or_null(args, 0)?;
        nth_string_or_null(args, 1)?;
        nth_string_or_null(args, 2)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        ternary_string_to_string(args, |s, _pat, repl| {
            Ok(self.regex.replace_all(s, repl).to_string())
        })
    }

    fn name(&self) -> &str {
        "regexp_replace"
    }
}

impl CustomFunc for CompiledRegexpSubstrFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_strings_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_string_to_string_result(args, |s, _pat| {
            Ok(self
                .regex
                .find(s)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default())
        })
    }

    fn name(&self) -> &str {
        "regexp_substring"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["regexp_substr"]
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
    use crate::expr::custom_func::helpers::{assert_bool, assert_int, assert_string, f, i, n, s};
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
