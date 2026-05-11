use super::helpers::*;
use crate::catalog::FunctionDef;
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use datatypes::{TimestampValue, Value};

#[derive(Debug, Clone)]
pub struct NowFunc;

#[derive(Debug, Clone)]
pub struct CurDateFunc;

#[derive(Debug, Clone)]
pub struct CurTimeFunc;

#[derive(Debug, Clone)]
pub struct FormatTimeFunc;

#[derive(Debug, Clone)]
pub struct DayNameFunc;

#[derive(Debug, Clone)]
pub struct DayOfMonthFunc;

#[derive(Debug, Clone)]
pub struct DayOfWeekFunc;

#[derive(Debug, Clone)]
pub struct DayOfYearFunc;

#[derive(Debug, Clone)]
pub struct FromUnixTimeFunc;

#[derive(Debug, Clone)]
pub struct HourFunc;

#[derive(Debug, Clone)]
pub struct LastDayFunc;

#[derive(Debug, Clone)]
pub struct MicrosecondFunc;

#[derive(Debug, Clone)]
pub struct MinuteFunc;

#[derive(Debug, Clone)]
pub struct MonthFunc;

#[derive(Debug, Clone)]
pub struct MonthNameFunc;

#[derive(Debug, Clone)]
pub struct SecondFunc;

pub fn builtin_function_defs() -> Vec<FunctionDef> {
    vec![
        now_function_def(),
        cur_date_function_def(),
        cur_time_function_def(),
        format_time_function_def(),
        day_name_function_def(),
        day_of_month_function_def(),
        day_of_week_function_def(),
        day_of_year_function_def(),
        from_unix_time_function_def(),
        hour_function_def(),
        last_day_function_def(),
        microsecond_function_def(),
        minute_function_def(),
        month_function_def(),
        month_name_function_def(),
        second_function_def(),
    ]
}

pub fn now_function_def() -> FunctionDef {
    scalar_function_def_with_aliases(
        "now",
        vec!["current_timestamp"],
        vec![],
        timestamp_type(),
        "Return the current UTC timestamp.",
        vec![
            "Requires exactly 0 arguments.",
            "The value is evaluated when each row is processed.",
        ],
        vec!["SELECT now()", "SELECT current_timestamp()"],
    )
}

pub fn cur_date_function_def() -> FunctionDef {
    scalar_function_def_with_aliases(
        "cur_date",
        vec!["current_date"],
        vec![],
        string_type(),
        "Return the current UTC date as YYYY-MM-DD.",
        vec!["Requires exactly 0 arguments."],
        vec!["SELECT cur_date()", "SELECT current_date()"],
    )
}

pub fn cur_time_function_def() -> FunctionDef {
    scalar_function_def_with_aliases(
        "cur_time",
        vec!["current_time"],
        vec![],
        string_type(),
        "Return the current UTC time as HH:MM:SS.ffffff.",
        vec!["Requires exactly 0 arguments."],
        vec!["SELECT cur_time()", "SELECT current_time()"],
    )
}

pub fn format_time_function_def() -> FunctionDef {
    scalar_function_def(
        "format_time",
        vec![
            req_arg("timestamp", timestamp_type()),
            req_arg("format", string_type()),
        ],
        string_type(),
        "Format a UTC timestamp using chrono strftime patterns.",
        vec![
            "Requires exactly 2 arguments.",
            "The first argument must be a timestamp or NULL.",
            "The second argument must be a string or NULL.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT format_time(ts, '%Y-%m-%d %H:%M:%S')"],
    )
}

pub fn day_name_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "day_name",
        string_type(),
        "Return the English weekday name of a UTC timestamp.",
        vec!["SELECT day_name(ts)"],
    )
}

pub fn day_of_month_function_def() -> FunctionDef {
    scalar_function_def_with_aliases(
        "day_of_month",
        vec!["day"],
        vec![req_arg("timestamp", timestamp_type())],
        int_type(),
        "Return the day of month from a UTC timestamp.",
        unary_timestamp_constraints(),
        vec!["SELECT day_of_month(ts)", "SELECT day(ts)"],
    )
}

pub fn day_of_week_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "day_of_week",
        int_type(),
        "Return the weekday number using Sunday=1 through Saturday=7.",
        vec!["SELECT day_of_week(ts)"],
    )
}

pub fn day_of_year_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "day_of_year",
        int_type(),
        "Return the day of year from a UTC timestamp.",
        vec!["SELECT day_of_year(ts)"],
    )
}

pub fn from_unix_time_function_def() -> FunctionDef {
    scalar_function_def(
        "from_unix_time",
        vec![req_arg("seconds", int_type())],
        timestamp_type(),
        "Convert Unix epoch seconds to a UTC timestamp.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be an integer or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT from_unix_time(1778505255)"],
    )
}

pub fn hour_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "hour",
        int_type(),
        "Return the hour component from a UTC timestamp.",
        vec!["SELECT hour(ts)"],
    )
}

pub fn last_day_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "last_day",
        string_type(),
        "Return the last day of the timestamp's UTC month as YYYY-MM-DD.",
        vec!["SELECT last_day(ts)"],
    )
}

pub fn microsecond_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "microsecond",
        int_type(),
        "Return the microsecond component from a UTC timestamp.",
        vec!["SELECT microsecond(ts)"],
    )
}

pub fn minute_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "minute",
        int_type(),
        "Return the minute component from a UTC timestamp.",
        vec!["SELECT minute(ts)"],
    )
}

pub fn month_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "month",
        int_type(),
        "Return the month number from a UTC timestamp.",
        vec!["SELECT month(ts)"],
    )
}

pub fn month_name_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "month_name",
        string_type(),
        "Return the English month name from a UTC timestamp.",
        vec!["SELECT month_name(ts)"],
    )
}

pub fn second_function_def() -> FunctionDef {
    unary_timestamp_fn_def(
        "second",
        int_type(),
        "Return the second component from a UTC timestamp.",
        vec!["SELECT second(ts)"],
    )
}

fn unary_timestamp_fn_def(
    name: &str,
    return_type: crate::catalog::TypeSpec,
    description: &str,
    examples: Vec<&str>,
) -> FunctionDef {
    scalar_function_def(
        name,
        vec![req_arg("timestamp", timestamp_type())],
        return_type,
        description,
        unary_timestamp_constraints(),
        examples,
    )
}

fn unary_timestamp_constraints() -> Vec<&'static str> {
    vec![
        "Requires exactly 1 argument.",
        "The argument must be a timestamp or NULL.",
        "Returns NULL if the argument is NULL.",
    ]
}

fn timestamp_to_datetime(timestamp: TimestampValue) -> Result<DateTime<Utc>, EvalError> {
    let secs = timestamp.epoch_micros().div_euclid(1_000_000);
    let micros = timestamp.epoch_micros().rem_euclid(1_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, micros * 1_000).ok_or_else(|| EvalError::TypeMismatch {
        expected: "timestamp representable as UTC datetime".to_string(),
        actual: format!("{:?}", timestamp),
    })
}

fn nth_timestamp_or_null(args: &[Value], idx: usize) -> Result<Option<TimestampValue>, EvalError> {
    nth_converted_or_null(args, idx, |value| match value {
        Value::Timestamp(timestamp) => Ok(*timestamp),
        other => Err(EvalError::TypeMismatch {
            expected: "timestamp".to_string(),
            actual: format!("{:?}", other),
        }),
    })
}

fn eval_unary_timestamp(
    args: &[Value],
    map: impl FnOnce(DateTime<Utc>) -> Result<Value, EvalError>,
) -> Result<Value, EvalError> {
    validate_arity(args, &[1])?;
    let Some(timestamp) = nth_timestamp_or_null(args, 0)? else {
        return Ok(Value::Null);
    };
    map(timestamp_to_datetime(timestamp)?)
}

fn current_timestamp_value() -> Value {
    Value::Timestamp(TimestampValue::from_epoch_micros(
        Utc::now().timestamp_micros(),
    ))
}

fn format_time_value(datetime: DateTime<Utc>) -> Value {
    Value::String(format!(
        "{:02}:{:02}:{:02}.{:06}",
        datetime.hour(),
        datetime.minute(),
        datetime.second(),
        datetime.timestamp_subsec_micros()
    ))
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

impl CustomFunc for NowFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[0])?;
        Ok(current_timestamp_value())
    }

    fn name(&self) -> &str {
        "now"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["current_timestamp"]
    }
}

impl CustomFunc for CurDateFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[0])?;
        Ok(Value::String(Utc::now().format("%Y-%m-%d").to_string()))
    }

    fn name(&self) -> &str {
        "cur_date"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["current_date"]
    }
}

impl CustomFunc for CurTimeFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[0])?;
        Ok(format_time_value(Utc::now()))
    }

    fn name(&self) -> &str {
        "cur_time"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["current_time"]
    }
}

impl CustomFunc for FormatTimeFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_timestamp_or_null(args, 0)?;
        nth_string_or_null(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(timestamp) = nth_timestamp_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(format) = nth_string_or_null(args, 1)? else {
            return Ok(Value::Null);
        };
        Ok(Value::String(
            timestamp_to_datetime(timestamp)?
                .format(&format)
                .to_string(),
        ))
    }

    fn name(&self) -> &str {
        "format_time"
    }
}

impl CustomFunc for DayNameFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| Ok(Value::String(dt.format("%A").to_string())))
    }

    fn name(&self) -> &str {
        "day_name"
    }
}

impl CustomFunc for DayOfMonthFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| Ok(Value::Int64(dt.day() as i64)))
    }

    fn name(&self) -> &str {
        "day_of_month"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["day"]
    }
}

impl CustomFunc for DayOfWeekFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| {
            Ok(Value::Int64(dt.weekday().number_from_sunday() as i64))
        })
    }

    fn name(&self) -> &str {
        "day_of_week"
    }
}

impl CustomFunc for DayOfYearFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| Ok(Value::Int64(dt.ordinal() as i64)))
    }

    fn name(&self) -> &str {
        "day_of_year"
    }
}

impl CustomFunc for FromUnixTimeFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_i64_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(seconds) = nth_i64_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let micros = seconds
            .checked_mul(1_000_000)
            .ok_or_else(|| EvalError::TypeMismatch {
                expected: "Unix seconds representable as timestamp".to_string(),
                actual: seconds.to_string(),
            })?;
        Ok(Value::Timestamp(TimestampValue::from_epoch_micros(micros)))
    }

    fn name(&self) -> &str {
        "from_unix_time"
    }
}

impl CustomFunc for HourFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| Ok(Value::Int64(dt.hour() as i64)))
    }

    fn name(&self) -> &str {
        "hour"
    }
}

impl CustomFunc for LastDayFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| {
            let year = dt.year();
            let month = dt.month();
            let day = days_in_month(year, month);
            let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
                EvalError::TypeMismatch {
                    expected: "valid UTC month".to_string(),
                    actual: format!("{year:04}-{month:02}"),
                }
            })?;
            Ok(Value::String(date.format("%Y-%m-%d").to_string()))
        })
    }

    fn name(&self) -> &str {
        "last_day"
    }
}

impl CustomFunc for MicrosecondFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| {
            Ok(Value::Int64(dt.timestamp_subsec_micros() as i64))
        })
    }

    fn name(&self) -> &str {
        "microsecond"
    }
}

impl CustomFunc for MinuteFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| Ok(Value::Int64(dt.minute() as i64)))
    }

    fn name(&self) -> &str {
        "minute"
    }
}

impl CustomFunc for MonthFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| Ok(Value::Int64(dt.month() as i64)))
    }

    fn name(&self) -> &str {
        "month"
    }
}

impl CustomFunc for MonthNameFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| Ok(Value::String(dt.format("%B").to_string())))
    }

    fn name(&self) -> &str {
        "month_name"
    }
}

impl CustomFunc for SecondFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_timestamp_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        eval_unary_timestamp(args, |dt| Ok(Value::Int64(dt.second() as i64)))
    }

    fn name(&self) -> &str {
        "second"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(value: &str) -> Value {
        Value::Timestamp(TimestampValue::parse_rfc3339(value).unwrap())
    }

    fn assert_timestamp_string(actual: Value, expected: &str) {
        let Value::Timestamp(timestamp) = actual else {
            panic!("expected timestamp, got {:?}", actual);
        };
        assert_eq!(timestamp.to_rfc3339_utc().as_deref(), Some(expected));
    }

    #[test]
    fn now_returns_timestamp() {
        let func = NowFunc;
        let before = Utc::now().timestamp_micros();
        let actual = func.eval_row(&[]).unwrap();
        let after = Utc::now().timestamp_micros();
        let Value::Timestamp(timestamp) = actual else {
            panic!("expected timestamp");
        };
        assert!(timestamp.epoch_micros() >= before);
        assert!(timestamp.epoch_micros() <= after);
    }

    #[test]
    fn current_zero_arg_strings_have_expected_shape() {
        let date = CurDateFunc.eval_row(&[]).unwrap();
        let time = CurTimeFunc.eval_row(&[]).unwrap();

        let Value::String(date) = date else {
            panic!("expected date string");
        };
        let Value::String(time) = time else {
            panic!("expected time string");
        };
        assert_eq!(date.len(), 10);
        assert_eq!(time.len(), 15);
    }

    // coverage-covers: stream.function.date_time_scalars
    #[test]
    fn format_time_formats_timestamp() {
        let func = FormatTimeFunc;
        assert_string(
            func.eval_row(&[
                ts("2026-05-11T13:14:15.123456Z"),
                s("%Y-%m-%d %H:%M:%S%.6f"),
            ])
            .unwrap(),
            "2026-05-11 13:14:15.123456",
        );
    }

    // coverage-covers: stream.function.date_time_scalars
    #[test]
    fn extractors_return_expected_parts() {
        let timestamp = ts("2026-05-11T13:14:15.123456Z");

        assert_string(
            DayNameFunc.eval_row(&[timestamp.clone()]).unwrap(),
            "Monday",
        );
        assert_int(DayOfMonthFunc.eval_row(&[timestamp.clone()]).unwrap(), 11);
        assert_int(DayOfWeekFunc.eval_row(&[timestamp.clone()]).unwrap(), 2);
        assert_int(DayOfYearFunc.eval_row(&[timestamp.clone()]).unwrap(), 131);
        assert_int(HourFunc.eval_row(&[timestamp.clone()]).unwrap(), 13);
        assert_int(
            MicrosecondFunc.eval_row(&[timestamp.clone()]).unwrap(),
            123456,
        );
        assert_int(MinuteFunc.eval_row(&[timestamp.clone()]).unwrap(), 14);
        assert_int(MonthFunc.eval_row(&[timestamp.clone()]).unwrap(), 5);
        assert_string(MonthNameFunc.eval_row(&[timestamp.clone()]).unwrap(), "May");
        assert_int(SecondFunc.eval_row(&[timestamp]).unwrap(), 15);
    }

    #[test]
    fn last_day_handles_month_lengths() {
        let func = LastDayFunc;
        assert_string(
            func.eval_row(&[ts("2026-02-11T00:00:00Z")]).unwrap(),
            "2026-02-28",
        );
        assert_string(
            func.eval_row(&[ts("2024-02-11T00:00:00Z")]).unwrap(),
            "2024-02-29",
        );
        assert_string(
            func.eval_row(&[ts("2026-05-11T00:00:00Z")]).unwrap(),
            "2026-05-31",
        );
    }

    #[test]
    fn from_unix_time_converts_seconds() {
        let func = FromUnixTimeFunc;
        assert_timestamp_string(
            func.eval_row(&[i(1778505255)]).unwrap(),
            "2026-05-11T13:14:15.000000Z",
        );
    }

    // coverage-covers: stream.function.date_time_scalars
    #[test]
    fn timestamp_functions_propagate_null() {
        assert_null(FormatTimeFunc.eval_row(&[n(), s("%Y")]).unwrap());
        assert_null(
            FormatTimeFunc
                .eval_row(&[ts("2026-05-11T00:00:00Z"), n()])
                .unwrap(),
        );
        assert_null(DayNameFunc.eval_row(&[n()]).unwrap());
        assert_null(FromUnixTimeFunc.eval_row(&[n()]).unwrap());
    }

    // coverage-covers: stream.function.date_time_scalars
    #[test]
    fn aliases_are_declared() {
        assert_eq!(NowFunc.aliases(), ["current_timestamp"]);
        assert_eq!(CurDateFunc.aliases(), ["current_date"]);
        assert_eq!(CurTimeFunc.aliases(), ["current_time"]);
        assert_eq!(DayOfMonthFunc.aliases(), ["day"]);
    }
}
