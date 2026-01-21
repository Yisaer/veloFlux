use datatypes::{DataType, Int64Type, Value};

#[test]
fn test_int64_type_casting_null() {
    let int64_type = Int64Type;
    assert_eq!(int64_type.try_cast(Value::Null), Some(Value::Null));
}

#[test]
fn test_int64_type_casting_integers() {
    let int64_type = Int64Type;

    assert_eq!(int64_type.try_cast(Value::Int8(-1)), Some(Value::Int64(-1)));
    assert_eq!(
        int64_type.try_cast(Value::Int16(-2)),
        Some(Value::Int64(-2))
    );
    assert_eq!(
        int64_type.try_cast(Value::Int32(-3)),
        Some(Value::Int64(-3))
    );
    assert_eq!(
        int64_type.try_cast(Value::Int64(-4)),
        Some(Value::Int64(-4))
    );

    assert_eq!(int64_type.try_cast(Value::Uint8(1)), Some(Value::Int64(1)));
    assert_eq!(int64_type.try_cast(Value::Uint16(2)), Some(Value::Int64(2)));
    assert_eq!(int64_type.try_cast(Value::Uint32(3)), Some(Value::Int64(3)));
    assert_eq!(
        int64_type.try_cast(Value::Uint64(i64::MAX as u64)),
        Some(Value::Int64(i64::MAX))
    );
    assert_eq!(
        int64_type.try_cast(Value::Uint64(i64::MAX as u64 + 1)),
        None
    );
}

#[test]
fn test_int64_type_casting_float_to_int_rules() {
    let int64_type = Int64Type;

    assert_eq!(
        int64_type.try_cast(Value::Float64(3.0)),
        Some(Value::Int64(3))
    );
    assert_eq!(int64_type.try_cast(Value::Float64(3.1)), None);
    assert_eq!(int64_type.try_cast(Value::Float64(f64::INFINITY)), None);
    assert_eq!(int64_type.try_cast(Value::Float64(f64::NAN)), None);

    assert_eq!(
        int64_type.try_cast(Value::Float32(4.0)),
        Some(Value::Int64(4))
    );
    assert_eq!(int64_type.try_cast(Value::Float32(4.25)), None);
}

#[test]
fn test_int64_type_casting_reject_string_bool() {
    let int64_type = Int64Type;

    assert_eq!(int64_type.try_cast(Value::Bool(true)), None);
    assert_eq!(int64_type.try_cast(Value::String("123".to_string())), None);
}
