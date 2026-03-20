use super::helpers::*;
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use datatypes::Value;
use rug::{Float, Integer};

macro_rules! unary_f64_func {
    ($struct:ident,$name:expr,$op:expr) => {
        #[derive(Debug, Clone)]
        pub struct $struct;

        impl CustomFunc for $struct {
            fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
                validate_arity(args, &[1])
            }

            fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
                unary_numeric_to_f64(args, $op)
            }

            fn name(&self) -> &str {
                $name
            }
        }
    };
}

macro_rules! binary_f64_func {
    ($struct:ident,$name:expr,$op:expr) => {
        #[derive(Debug, Clone)]
        pub struct $struct;

        impl CustomFunc for $struct {
            fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
                validate_arity(args, &[2])
            }

            fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
                binary_numeric_to_f64(args, $op)
            }

            fn name(&self) -> &str {
                $name
            }
        }
    };
}

unary_f64_func!(AcosFunc, "acos", |x: f64| x.acos());
unary_f64_func!(AsinFunc, "asin", |x: f64| x.asin());
unary_f64_func!(AtanFunc, "atan", |x: f64| x.atan());
unary_f64_func!(CosFunc, "cos", |x: f64| x.cos());
unary_f64_func!(CoshFunc, "cosh", |x: f64| x.cosh());
unary_f64_func!(ExpFunc, "exp", |x: f64| x.exp());
unary_f64_func!(FloorFunc, "floor", |x: f64| x.floor());
unary_f64_func!(LnFunc, "ln", |x: f64| x.ln());
unary_f64_func!(SinFunc, "sin", |x: f64| x.sin());
unary_f64_func!(SinhFunc, "sinh", |x: f64| x.sinh());
unary_f64_func!(SqrtFunc, "sqrt", |x: f64| x.sqrt());
unary_f64_func!(TanFunc, "tan", |x: f64| x.tan());
unary_f64_func!(TanhFunc, "tanh", |x: f64| x.tanh());
unary_f64_func!(CeilingFunc, "ceiling", |x: f64| x.ceil());
unary_f64_func!(CeilFunc, "ceil", |x: f64| x.ceil());
unary_f64_func!(RadiansFunc, "radians", |x: f64| x.to_radians());
unary_f64_func!(DegreesFunc, "degrees", |x: f64| x.to_degrees());

binary_f64_func!(Atan2Func, "atan2", |a: f64, b: f64| a.atan2(b));
binary_f64_func!(ModFunc, "mod", |a: f64, b: f64| a % b);
binary_f64_func!(PowerFunc, "power", |a: f64, b: f64| a.powf(b));
binary_f64_func!(PowFunc, "pow", |a: f64, b: f64| a.powf(b));

#[derive(Debug, Clone)]
pub struct AbsFunc;

impl CustomFunc for AbsFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        abs_value(&args[0])
    }

    fn name(&self) -> &str {
        "abs"
    }
}

pub fn abs_value(v: &Value) -> Result<Value, EvalError> {
    map_numeric_value(
        v,
        |x| {
            x.checked_abs().ok_or(EvalError::TypeMismatch {
                expected: "integer whose abs is representable".to_string(),
                actual: format!("{x}"),
            })
        },
        Ok,
        |x| Ok(x.abs()),
    )
}

#[derive(Debug, Clone)]
pub struct BitAndFunc;

impl CustomFunc for BitAndFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_i64(args, |a, b| a & b)
    }

    fn name(&self) -> &str {
        "bitand"
    }
}

#[derive(Debug, Clone)]
pub struct BitOrFunc;

impl CustomFunc for BitOrFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_i64(args, |a, b| a | b)
    }

    fn name(&self) -> &str {
        "bitor"
    }
}

#[derive(Debug, Clone)]
pub struct BitXorFunc;

impl CustomFunc for BitXorFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_i64(args, |a, b| a ^ b)
    }

    fn name(&self) -> &str {
        "bitxor"
    }
}

#[derive(Debug, Clone)]
pub struct BitNotFunc;

impl CustomFunc for BitNotFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_i64(args, |a| !a)
    }

    fn name(&self) -> &str {
        "bitnot"
    }
}

#[derive(Debug, Clone)]
pub struct SignFunc;

impl CustomFunc for SignFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        sign_numeric(args)
    }

    fn name(&self) -> &str {
        "sign"
    }
}

#[derive(Debug, Clone)]
pub struct PiFunc;

impl CustomFunc for PiFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        nullary_f64(args, || std::f64::consts::PI)
    }

    fn name(&self) -> &str {
        "pi"
    }
}

#[derive(Debug, Clone)]
pub struct RandFunc;

impl CustomFunc for RandFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        nullary_f64(args, rand::random::<f64>)
    }

    fn name(&self) -> &str {
        "rand"
    }
}

#[derive(Debug, Clone)]
pub struct CotFunc;

impl CustomFunc for CotFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_numeric_to_f64_inf_err(args, |x| 1.0 / x.tan())
    }

    fn name(&self) -> &str {
        "cot"
    }
}

#[derive(Debug, Clone)]
pub struct LogFunc;

impl CustomFunc for LogFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1, 2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1, 2])?;

        match args.len() {
            1 => {
                let Some(x) = nth_f64_or_null(args, 0)? else {
                    return Ok(Value::Null);
                };
                Ok(f64_to_value_nan_null(x.log10()))
            }
            2 => {
                let Some(base) = nth_f64_or_null(args, 0)? else {
                    return Ok(Value::Null);
                };
                let Some(x) = nth_f64_or_null(args, 1)? else {
                    return Ok(Value::Null);
                };
                Ok(f64_to_value_nan_null(x.ln() / base.ln()))
            }
            _ => unreachable!(),
        }
    }

    fn name(&self) -> &str {
        "log"
    }
}

#[derive(Debug, Clone)]
pub struct RoundFunc;

impl CustomFunc for RoundFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1, 2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1, 2])?;

        let Some(x) = nth_f64_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let precision = precision_arg_or_default(args, 1, 0)?;

        let factor = 10_f64.powi(precision);
        let scaled = x * factor;

        let result = if factor.is_infinite() || scaled.is_infinite() || factor == 0.0 {
            round_with_big_float(x, precision)
        } else {
            scaled.round() / factor
        };

        Ok(Value::Float64(result))
    }

    fn name(&self) -> &str {
        "round"
    }
}

fn round_with_big_float(v: f64, precision: i32) -> f64 {
    const BIG_FLOAT_PREC: u32 = 256;

    let bf = Float::with_val(BIG_FLOAT_PREC, v);
    let ten = Float::with_val(BIG_FLOAT_PREC, 10);

    let multiplier = if precision == 0 {
        Float::with_val(BIG_FLOAT_PREC, 1)
    } else if precision > 0 {
        let mut m = Float::with_val(BIG_FLOAT_PREC, 10);
        for _ in 1..precision {
            m *= &ten;
        }
        m
    } else {
        let mut m = Float::with_val(BIG_FLOAT_PREC, 1);
        for _ in 0..(-precision) {
            m /= &ten;
        }
        m
    };

    let scaled = Float::with_val(BIG_FLOAT_PREC, &bf * &multiplier);

    let mut int_part = trunc_toward_zero(&scaled, BIG_FLOAT_PREC);

    let frac_part = Float::with_val(
        BIG_FLOAT_PREC,
        &scaled - Float::with_val(BIG_FLOAT_PREC, &int_part),
    );

    let half = Float::with_val(BIG_FLOAT_PREC, 0.5);
    let neg_half = Float::with_val(BIG_FLOAT_PREC, -0.5);

    if frac_part >= half {
        int_part += 1;
    } else if frac_part <= neg_half {
        int_part -= 1;
    }

    let result = Float::with_val(BIG_FLOAT_PREC, int_part) / multiplier;
    result.to_f64()
}

fn trunc_toward_zero(x: &Float, prec: u32) -> Integer {
    let zero = Float::with_val(prec, 0);

    let truncated = if x >= &zero {
        Float::with_val(prec, x).floor()
    } else {
        Float::with_val(prec, x).ceil()
    };

    truncated
        .to_integer()
        .expect("floor/ceil of finite Float should be integral")
}

#[derive(Debug, Clone)]
pub struct ConvFunc;

impl CustomFunc for ConvFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[3])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[3])?;

        if any_null(args) {
            return Ok(Value::Null);
        }

        let s = value_to_string(&args[0])?;
        let from_base = value_to_i64(&args[1])?;
        let to_base = value_to_i64(&args[2])?;

        conv_impl(&s, from_base, to_base)
    }

    fn name(&self) -> &str {
        "conv"
    }
}

fn conv_impl(input: &str, mut from_base: i64, mut to_base: i64) -> Result<Value, EvalError> {
    let mut signed = false;
    let mut ignore_sign = false;

    if from_base < 0 {
        from_base = -from_base;
        signed = true;
    }

    if to_base < 0 {
        to_base = -to_base;
        ignore_sign = true;
    }

    if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base) {
        return Ok(Value::Null);
    }

    let trimmed = input.trim();
    let prefix = get_valid_prefix(trimmed, from_base as u32);

    if prefix.is_empty() {
        return Ok(Value::String("0".to_string()));
    }

    let mut negative_input = false;
    let digits = if let Some(rest) = prefix.strip_prefix('-') {
        negative_input = true;
        rest
    } else {
        prefix.as_str()
    };

    let mut val =
        u64::from_str_radix(digits, from_base as u32).map_err(|e| EvalError::TypeMismatch {
            expected: "valid number string for base conversion".to_string(),
            actual: e.to_string(),
        })?;

    if signed {
        let min_int64_abs = (i64::MIN as i128).unsigned_abs() as u64;
        if negative_input && val > min_int64_abs {
            val = min_int64_abs;
        }
        if !negative_input && val > i64::MAX as u64 {
            val = i64::MAX as u64;
        }
    }

    if negative_input {
        val = 0u64.wrapping_sub(val);
    }

    let signed_val = val as i64;
    let negative_result = signed_val < 0;

    if ignore_sign && negative_result {
        val = 0u64.wrapping_sub(val);
    }

    let mut s = to_base_string(val, to_base as u32);
    if negative_result && ignore_sign {
        s = format!("-{}", s);
    }

    Ok(Value::String(s.to_uppercase()))
}

fn get_valid_prefix(s: &str, base: u32) -> String {
    let mut end = 0usize;

    for (i, ch) in s.char_indices() {
        if i == 0 && (ch == '+' || ch == '-') {
            end = i + ch.len_utf8();
            continue;
        }

        match char_to_digit(ch) {
            Some(d) if d < base => {
                end = i + ch.len_utf8();
            }
            _ => break,
        }
    }

    let out = &s[..end];
    if out.len() > 1 && out.starts_with('+') {
        out[1..].to_string()
    } else {
        out.to_string()
    }
}

fn char_to_digit(c: char) -> Option<u32> {
    c.to_digit(36)
}

fn to_base_string(mut value: u64, base: u32) -> String {
    debug_assert!((2..=36).contains(&base));

    if value == 0 {
        return "0".to_string();
    }

    let mut buf = Vec::new();
    while value > 0 {
        let digit = (value % base as u64) as u8;
        let ch = match digit {
            0..=9 => (b'0' + digit) as char,
            10..=35 => (b'A' + (digit - 10)) as char,
            _ => unreachable!(),
        };
        buf.push(ch);
        value /= base as u64;
    }

    buf.iter().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::Value;

    const EPS: f64 = 1e-9;

    fn eval<F: CustomFunc>(f: &F, args: &[Value]) -> Value {
        f.eval_row(args).unwrap()
    }

    fn eval_err<F: CustomFunc>(f: &F, args: &[Value]) {
        assert!(f.eval_row(args).is_err(), "expected error, got ok");
    }

    #[test]
    fn test_unary_math_correctness() {
        assert_float(eval(&AcosFunc, &[Value::Float64(1.0)]), 0.0);
        assert_float(eval(&AsinFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(
            eval(&AtanFunc, &[Value::Float64(1.0)]),
            std::f64::consts::FRAC_PI_4,
        );
        assert_float(eval(&CosFunc, &[Value::Float64(0.0)]), 1.0);
        assert_float(eval(&CoshFunc, &[Value::Float64(0.0)]), 1.0);
        assert_float(eval(&ExpFunc, &[Value::Float64(1.0)]), std::f64::consts::E);
        assert_float(eval(&FloorFunc, &[Value::Float64(3.8)]), 3.0);
        assert_float(eval(&LnFunc, &[Value::Float64(1.0)]), 0.0);
        assert_float(eval(&SinFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(eval(&SinhFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(eval(&SqrtFunc, &[Value::Float64(9.0)]), 3.0);
        assert_float(eval(&TanFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(eval(&TanhFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(eval(&CeilingFunc, &[Value::Float64(3.2)]), 4.0);
        assert_float(eval(&CeilFunc, &[Value::Float64(3.2)]), 4.0);
        assert_float(
            eval(&RadiansFunc, &[Value::Float64(180.0)]),
            std::f64::consts::PI,
        );
        assert_float(
            eval(&DegreesFunc, &[Value::Float64(std::f64::consts::PI)]),
            180.0,
        );
        assert_float(
            eval(&CotFunc, &[Value::Float64(std::f64::consts::FRAC_PI_4)]),
            1.0,
        );
    }

    #[test]
    fn test_binary_math_correctness() {
        assert_float(
            eval(&Atan2Func, &[Value::Float64(1.0), Value::Float64(1.0)]),
            std::f64::consts::FRAC_PI_4,
        );
        assert_float(
            eval(&ModFunc, &[Value::Float64(10.5), Value::Float64(3.0)]),
            10.5 % 3.0,
        );
        assert_float(
            eval(&PowerFunc, &[Value::Float64(2.0), Value::Float64(3.0)]),
            8.0,
        );
        assert_float(
            eval(&PowFunc, &[Value::Float64(2.0), Value::Float64(4.0)]),
            16.0,
        );
    }

    #[test]
    fn test_abs_bit_sign_pi_rand_correctness() {
        assert_int(eval(&AbsFunc, &[Value::Int64(-12)]), 12);
        assert_float(eval(&AbsFunc, &[Value::Float64(-12.5)]), 12.5);

        assert_int(eval(&BitAndFunc, &[Value::Int64(6), Value::Int64(3)]), 2);
        assert_int(eval(&BitOrFunc, &[Value::Int64(6), Value::Int64(3)]), 7);
        assert_int(eval(&BitXorFunc, &[Value::Int64(6), Value::Int64(3)]), 5);
        assert_int(eval(&BitNotFunc, &[Value::Int64(6)]), !6);

        assert_int(eval(&SignFunc, &[Value::Int64(9)]), 1);
        assert_int(eval(&SignFunc, &[Value::Int64(-9)]), -1);
        assert_int(eval(&SignFunc, &[Value::Int64(0)]), 0);

        assert_float(eval(&PiFunc, &[]), std::f64::consts::PI);

        let rand_v = eval(&RandFunc, &[]);
        match rand_v {
            Value::Float64(v) => assert!((0.0..1.0).contains(&v)),
            other => panic!("expected Float64 in [0,1), got {:?}", other),
        }
    }

    #[test]
    fn test_round_large_precision_matches_bigfloat_behavior() {
        let f = RoundFunc;
        let v = f.eval_row(&[Value::Float64(1.2345), Value::Int64(400)]);

        assert!(v.is_ok(), "expected Ok result, got {:?}", v);

        match v.unwrap() {
            Value::Float64(x) => {
                assert!(x.is_finite(), "expected finite float result, got {}", x);
                assert_eq!(x, 1.2345);
            }
            other => panic!("expected Float64 result, got {:?}", other),
        }
    }

    #[test]
    fn test_log_round_conv_correctness() {
        assert_float(eval(&LogFunc, &[Value::Float64(1000.0)]), 3.0);

        assert_float(
            eval(&LogFunc, &[Value::Float64(2.0), Value::Float64(8.0)]),
            3.0,
        );

        assert_float(eval(&RoundFunc, &[Value::Float64(3.6)]), 4.0);
        assert_float(
            eval(&RoundFunc, &[Value::Float64(3.14159), Value::Int64(2)]),
            3.14,
        );
        assert_float(
            eval(&RoundFunc, &[Value::Float64(1234.56), Value::Int64(-2)]),
            1200.0,
        );

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("A".to_string()),
                    Value::Int64(16),
                    Value::Int64(10),
                ],
            ),
            "10",
        );

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("1010".to_string()),
                    Value::Int64(2),
                    Value::Int64(16),
                ],
            ),
            "A",
        );
    }

    #[test]
    fn test_semantics_for_null_and_domain_cases() {
        assert_null(eval(&SqrtFunc, &[Value::Float64(-1.0)]));
        assert_null(eval(&LnFunc, &[Value::Float64(-1.0)]));
        assert_null(eval(&AcosFunc, &[Value::Float64(2.0)]));
        assert_null(eval(&AsinFunc, &[Value::Float64(2.0)]));

        assert_null(eval(&LogFunc, &[Value::Float64(-10.0)]));
        assert_null(eval(
            &LogFunc,
            &[Value::Float64(10.0), Value::Float64(-1.0)],
        ));

        assert_null(eval(
            &ConvFunc,
            &[
                Value::String("10".to_string()),
                Value::Int64(1),
                Value::Int64(10),
            ],
        ));

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("XYZ".to_string()),
                    Value::Int64(10),
                    Value::Int64(16),
                ],
            ),
            "0",
        );

        eval_err(&CotFunc, &[Value::Float64(0.0)]);
    }

    #[test]
    fn test_conv_edge_cases() {
        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("   +1GZ".to_string()),
                    Value::Int64(17),
                    Value::Int64(10),
                ],
            ),
            "33",
        );

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("-10".to_string()),
                    Value::Int64(-10),
                    Value::Int64(16),
                ],
            ),
            "FFFFFFFFFFFFFFF6",
        );

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("-10".to_string()),
                    Value::Int64(-10),
                    Value::Int64(-16),
                ],
            ),
            "-A",
        );
    }

    #[test]
    fn test_abs_overflow_errors() {
        eval_err(&AbsFunc, &[Value::Int64(i64::MIN)]);
    }

    #[test]
    fn test_nullary_arity_validation() {
        assert!(PiFunc.validate_row(&[]).is_ok());
        assert!(RandFunc.validate_row(&[]).is_ok());

        assert!(PiFunc.validate_row(&[Value::Int64(1)]).is_err());
        assert!(RandFunc.validate_row(&[Value::Int64(1)]).is_err());
    }

    #[test]
    fn test_unary_arity_validation() {
        assert!(AcosFunc.validate_row(&[Value::Float64(1.0)]).is_ok());
        assert!(AbsFunc.validate_row(&[Value::Int64(1)]).is_ok());
        assert!(BitNotFunc.validate_row(&[Value::Int64(1)]).is_ok());
        assert!(SignFunc.validate_row(&[Value::Int64(1)]).is_ok());
        assert!(CotFunc.validate_row(&[Value::Float64(1.0)]).is_ok());

        assert!(AcosFunc.validate_row(&[]).is_err());
        assert!(AcosFunc
            .validate_row(&[Value::Float64(1.0), Value::Float64(2.0)])
            .is_err());
    }

    #[test]
    fn test_binary_arity_validation() {
        assert!(Atan2Func
            .validate_row(&[Value::Float64(1.0), Value::Float64(1.0)])
            .is_ok());
        assert!(ModFunc
            .validate_row(&[Value::Float64(1.0), Value::Float64(1.0)])
            .is_ok());
        assert!(PowerFunc
            .validate_row(&[Value::Float64(1.0), Value::Float64(1.0)])
            .is_ok());
        assert!(PowFunc
            .validate_row(&[Value::Float64(1.0), Value::Float64(1.0)])
            .is_ok());
        assert!(BitAndFunc
            .validate_row(&[Value::Int64(1), Value::Int64(1)])
            .is_ok());
        assert!(BitOrFunc
            .validate_row(&[Value::Int64(1), Value::Int64(1)])
            .is_ok());
        assert!(BitXorFunc
            .validate_row(&[Value::Int64(1), Value::Int64(1)])
            .is_ok());

        assert!(Atan2Func.validate_row(&[Value::Float64(1.0)]).is_err());
        assert!(Atan2Func
            .validate_row(&[
                Value::Float64(1.0),
                Value::Float64(1.0),
                Value::Float64(1.0)
            ])
            .is_err());
    }

    #[test]
    fn test_variable_arity_functions() {
        assert!(LogFunc.validate_row(&[Value::Float64(10.0)]).is_ok());
        assert!(LogFunc
            .validate_row(&[Value::Float64(2.0), Value::Float64(8.0)])
            .is_ok());
        assert!(LogFunc.validate_row(&[]).is_err());
        assert!(LogFunc
            .validate_row(&[
                Value::Float64(1.0),
                Value::Float64(2.0),
                Value::Float64(3.0)
            ])
            .is_err());

        assert!(RoundFunc.validate_row(&[Value::Float64(3.14)]).is_ok());
        assert!(RoundFunc
            .validate_row(&[Value::Float64(3.14), Value::Int64(2)])
            .is_ok());
        assert!(RoundFunc.validate_row(&[]).is_err());
        assert!(RoundFunc
            .validate_row(&[Value::Float64(3.14), Value::Int64(2), Value::Int64(3)])
            .is_err());

        assert!(ConvFunc
            .validate_row(&[
                Value::String("10".to_string()),
                Value::Int64(10),
                Value::Int64(2)
            ])
            .is_ok());
        assert!(ConvFunc.validate_row(&[]).is_err());
        assert!(ConvFunc
            .validate_row(&[Value::String("10".to_string())])
            .is_err());
        assert!(ConvFunc
            .validate_row(&[
                Value::String("10".to_string()),
                Value::Int64(10),
                Value::Int64(2),
                Value::Int64(8),
            ])
            .is_err());
    }

    #[test]
    fn test_mixed_numeric_types_for_unary_funcs() {
        assert_float(eval(&SinFunc, &[Value::Int64(0)]), 0.0);
        assert_float(eval(&SqrtFunc, &[Value::Int64(16)]), 4.0);
        assert_float(eval(&CeilingFunc, &[Value::Int64(3)]), 3.0);

        assert_int(eval(&AbsFunc, &[Value::Int64(-7)]), 7);
        assert_float(eval(&AbsFunc, &[Value::Float64(-7.25)]), 7.25);
    }

    #[test]
    fn test_mixed_numeric_types_for_binary_funcs() {
        assert_float(
            eval(&Atan2Func, &[Value::Int64(1), Value::Float64(1.0)]),
            std::f64::consts::FRAC_PI_4,
        );
        assert_float(
            eval(&ModFunc, &[Value::Int64(10), Value::Float64(4.0)]),
            10.0 % 4.0,
        );
        assert_float(
            eval(&PowerFunc, &[Value::Float64(2.5), Value::Int64(2)]),
            6.25,
        );
        assert_float(eval(&PowFunc, &[Value::Int64(9), Value::Float64(0.5)]), 3.0);
    }

    #[test]
    fn test_mixed_numeric_types_for_variable_arity_funcs() {
        assert_float(eval(&LogFunc, &[Value::Int64(10)]), 1.0);
        assert_float(eval(&LogFunc, &[Value::Int64(2), Value::Float64(8.0)]), 3.0);

        assert_float(
            eval(&RoundFunc, &[Value::Int64(1234), Value::Int64(-2)]),
            1200.0,
        );
        assert_float(
            eval(&RoundFunc, &[Value::Float64(12.345), Value::Int64(2)]),
            12.35,
        );
    }

    #[test]
    fn test_type_errors_for_non_numeric_inputs() {
        eval_err(&SinFunc, &[Value::String("abc".to_string())]);
        eval_err(
            &Atan2Func,
            &[Value::String("1".to_string()), Value::Float64(1.0)],
        );
        eval_err(
            &RoundFunc,
            &[Value::Float64(1.23), Value::String("2".to_string())],
        );
        eval_err(&BitAndFunc, &[Value::Int64(1), Value::Float64(2.0)]);
        eval_err(
            &BitOrFunc,
            &[Value::String("1".to_string()), Value::Int64(2)],
        );
        eval_err(&BitXorFunc, &[Value::Float64(1.0), Value::Int64(2)]);
        eval_err(&BitNotFunc, &[Value::Float64(1.0)]);
        eval_err(
            &ConvFunc,
            &[
                Value::String("10".to_string()),
                Value::Float64(10.0),
                Value::Int64(2),
            ],
        );
    }

    #[test]
    fn test_null_propagation_behavior() {
        assert_null(eval(&SinFunc, &[Value::Null]));
        assert_null(eval(&Atan2Func, &[Value::Null, Value::Float64(1.0)]));
        assert_null(eval(&Atan2Func, &[Value::Float64(1.0), Value::Null]));
        assert_null(eval(&AbsFunc, &[Value::Null]));
        assert_null(eval(&BitAndFunc, &[Value::Null, Value::Int64(1)]));
        assert_null(eval(&BitNotFunc, &[Value::Null]));
        assert_null(eval(&SignFunc, &[Value::Null]));
        assert_null(eval(&LogFunc, &[Value::Null]));
        assert_null(eval(&LogFunc, &[Value::Float64(10.0), Value::Null]));
        assert_null(eval(&RoundFunc, &[Value::Null]));
        assert_null(eval(
            &ConvFunc,
            &[Value::Null, Value::Int64(10), Value::Int64(2)],
        ));
    }

    #[test]
    fn test_round_half_away_from_zero() {
        assert_float(eval(&RoundFunc, &[Value::Float64(2.5)]), 3.0);
        assert_float(eval(&RoundFunc, &[Value::Float64(-2.5)]), -3.0);
    }

    #[test]
    fn test_mod_zero_returns_null() {
        assert_null(eval(&ModFunc, &[Value::Float64(10.0), Value::Float64(0.0)]));
    }

    #[test]
    fn test_inf_behavior() {
        match eval(&LnFunc, &[Value::Float64(0.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_negative()),
            other => panic!("expected -Inf, got {:?}", other),
        }

        match eval(&LogFunc, &[Value::Float64(0.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_negative()),
            other => panic!("expected -Inf, got {:?}", other),
        }

        match eval(&ExpFunc, &[Value::Float64(1000.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_positive()),
            other => panic!("expected +Inf, got {:?}", other),
        }
    }

    #[test]
    fn test_log_edge_cases() {
        match eval(&LogFunc, &[Value::Float64(1.0), Value::Float64(10.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_positive()),
            other => panic!("expected +Inf, got {:?}", other),
        }

        match eval(&LogFunc, &[Value::Float64(10.0), Value::Float64(0.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_negative()),
            other => panic!("expected -Inf, got {:?}", other),
        }
    }
}
