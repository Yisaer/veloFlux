use datatypes::{DataType, TimestampType, TimestampValue, Value};

#[test]
fn timestamp_rfc3339_offset_normalizes_to_same_epoch() {
    let utc = TimestampValue::parse_rfc3339("2026-05-08T10:20:30Z").expect("parse utc");
    let offset =
        TimestampValue::parse_rfc3339("2026-05-08T18:20:30+08:00").expect("parse offset timestamp");

    assert_eq!(utc, offset);
    assert_eq!(
        utc.to_rfc3339_utc().as_deref(),
        Some("2026-05-08T10:20:30.000000Z")
    );
}

#[test]
fn timestamp_preserves_microsecond_precision_on_format() {
    let ts = TimestampValue::parse_rfc3339("2026-05-08T10:20:30.123456Z")
        .expect("parse timestamp with micros");

    assert_eq!(
        ts.to_rfc3339_utc().as_deref(),
        Some("2026-05-08T10:20:30.123456Z")
    );
}

#[test]
fn timestamp_type_casts_rfc3339_strings_only() {
    let timestamp_type = TimestampType;

    assert!(matches!(
        timestamp_type.try_cast(Value::String("2026-05-08T10:20:30Z".to_string())),
        Some(Value::Timestamp(_))
    ));
    assert_eq!(
        timestamp_type.try_cast(Value::String("2026-05-08 10:20:30".to_string())),
        None
    );
    assert_eq!(timestamp_type.try_cast(Value::Int64(1)), None);
}
