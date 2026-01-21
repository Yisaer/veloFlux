use datatypes::{DataType, Float32Type, Value};

#[test]
fn test_float32_basic_functionality() {
    let float32_type = Float32Type;

    // Test name
    assert_eq!(float32_type.name(), "Float32");

    // Test default value
    let default_val = float32_type.default_value();
    assert_eq!(default_val, Value::Float32(0.0));
}

#[test]
fn test_float32_type_casting() {
    let float32_type = Float32Type;

    // Test casting from NULL
    assert_eq!(float32_type.try_cast(Value::Null), Some(Value::Null));

    // Test casting from integer types
    assert_eq!(
        float32_type.try_cast(Value::Int8(42)),
        Some(Value::Float32(42.0))
    );
    assert_eq!(
        float32_type.try_cast(Value::Int16(1000)),
        Some(Value::Float32(1000.0))
    );
    assert_eq!(
        float32_type.try_cast(Value::Int32(100000)),
        Some(Value::Float32(100000.0))
    );
    assert_eq!(
        float32_type.try_cast(Value::Int64(123456789)),
        Some(Value::Float32(123456789.0))
    );

    // Test casting from unsigned integer types
    assert_eq!(
        float32_type.try_cast(Value::Uint8(200)),
        Some(Value::Float32(200.0))
    );
    assert_eq!(
        float32_type.try_cast(Value::Uint16(50000)),
        Some(Value::Float32(50000.0))
    );
    assert_eq!(
        float32_type.try_cast(Value::Uint32(3000000000)),
        Some(Value::Float32(3000000000.0))
    );

    // Test casting from Float64
    assert_eq!(
        float32_type.try_cast(Value::Float64(3.15)),
        Some(Value::Float32(3.15))
    );

    // Test casting from unsupported types
    assert_eq!(float32_type.try_cast(Value::Bool(true)), None);
    assert_eq!(
        float32_type.try_cast(Value::String("123.45".to_string())),
        None
    );
}

#[test]
fn test_float32_edge_cases() {
    let float32_type = Float32Type;

    // Test zero values
    assert_eq!(
        float32_type.try_cast(Value::Int8(0)),
        Some(Value::Float32(0.0))
    );

    // Test negative values
    assert_eq!(
        float32_type.try_cast(Value::Int8(-42)),
        Some(Value::Float32(-42.0))
    );
    assert_eq!(
        float32_type.try_cast(Value::Float64(-123.45)),
        Some(Value::Float32(-123.45))
    );
}
