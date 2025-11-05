use datatypes::{Float32Type, Value, DataType};

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
    
    // Test casting from integer types
    assert_eq!(float32_type.try_cast(Value::Int8(42)), Some(Value::Float32(42.0)));
    assert_eq!(float32_type.try_cast(Value::Int16(1000)), Some(Value::Float32(1000.0)));
    assert_eq!(float32_type.try_cast(Value::Int32(100000)), Some(Value::Float32(100000.0)));
    assert_eq!(float32_type.try_cast(Value::Int64(123456789)), Some(Value::Float32(123456789.0)));
    
    // Test casting from unsigned integer types
    assert_eq!(float32_type.try_cast(Value::Uint8(200)), Some(Value::Float32(200.0)));
    assert_eq!(float32_type.try_cast(Value::Uint16(50000)), Some(Value::Float32(50000.0)));
    assert_eq!(float32_type.try_cast(Value::Uint32(3000000000)), Some(Value::Float32(3000000000.0)));
    
    // Test casting from boolean
    assert_eq!(float32_type.try_cast(Value::Bool(true)), Some(Value::Float32(1.0)));
    assert_eq!(float32_type.try_cast(Value::Bool(false)), Some(Value::Float32(0.0)));
    
    // Test casting from Float64
    assert_eq!(float32_type.try_cast(Value::Float64(3.14159)), Some(Value::Float32(3.14159)));
    
    // Test casting from string
    assert_eq!(float32_type.try_cast(Value::String("123.45".to_string())), Some(Value::Float32(123.45)));
    assert_eq!(float32_type.try_cast(Value::String("invalid".to_string())), None);
    
    // Test casting from unsupported types
    assert_eq!(float32_type.try_cast(Value::String("hello".to_string())), None);
}

#[test]
fn test_float32_precision_and_range() {
    let float32_type = Float32Type;
    
    // Test with fractional values
    assert_eq!(float32_type.try_cast(Value::String("123.456789".to_string())), Some(Value::Float32(123.456789)));
    
    // Test with scientific notation
    assert_eq!(float32_type.try_cast(Value::String("1.23e-4".to_string())), Some(Value::Float32(0.000123)));
    assert_eq!(float32_type.try_cast(Value::String("1.23e4".to_string())), Some(Value::Float32(12300.0)));
    
    // Test very small and very large numbers (within f32 range)
    assert_eq!(float32_type.try_cast(Value::String("1e-30".to_string())), Some(Value::Float32(1e-30)));
    assert_eq!(float32_type.try_cast(Value::String("1e30".to_string())), Some(Value::Float32(1e30)));
}

#[test]
fn test_float32_edge_cases() {
    let float32_type = Float32Type;
    
    // Test zero values
    assert_eq!(float32_type.try_cast(Value::Int8(0)), Some(Value::Float32(0.0)));
    assert_eq!(float32_type.try_cast(Value::String("0.0".to_string())), Some(Value::Float32(0.0)));
    assert_eq!(float32_type.try_cast(Value::String("-0.0".to_string())), Some(Value::Float32(-0.0)));
    
    // Test negative values
    assert_eq!(float32_type.try_cast(Value::Int8(-42)), Some(Value::Float32(-42.0)));
    assert_eq!(float32_type.try_cast(Value::String("-123.45".to_string())), Some(Value::Float32(-123.45)));
}