//! Binary decoder — interprets raw byte slices according to AUTOSAR
//! data-type definitions.
//!
//! This is the core runtime component consumed by veloFlux pipeline.

mod plan;
pub mod value;

pub use plan::DecodePlan;

use std::collections::{HashMap, HashSet};

use crate::ast::types::{self, DataType, DataTypeKind};
use crate::util::convert;

pub use value::Value;

/// Decodes `&[u8]` payloads guided by a set of [`DataType`] definitions.
///
/// The type map supports exact implementation paths and legacy lowercased
/// short names from [`crate::parser::datatypes::DataTypesParser`].
#[derive(Debug, Default)]
pub struct Decoder;

enum DecodeError {
    Layout(String),
    Payload(String),
}

impl DecodeError {
    fn into_message(self) -> String {
        match self {
            Self::Layout(message) | Self::Payload(message) => message,
        }
    }
}

impl From<String> for DecodeError {
    fn from(message: String) -> Self {
        Self::Payload(message)
    }
}

impl Decoder {
    pub fn new() -> Self {
        Self
    }

    /// Decode `data` according to `dt`, returning the number of bytes
    /// consumed and the resulting [`Value`].
    ///
    /// For composite types (STRUCTURE, ARRAY, VECTOR) this method
    /// recurses into the referenced sub-types via the type map.
    pub fn decode(
        &self,
        types: &HashMap<String, DataType>,
        data: &[u8],
        dt: &DataType,
    ) -> Result<(usize, Value), String> {
        self.compile(types, dt).decode(data)
    }

    /// Compile an owned immutable layout. Reuse it when decoding repeatedly;
    /// decode() compiles a fresh plan to respect changes to a caller's registry.
    pub fn compile(&self, types: &HashMap<String, DataType>, dt: &DataType) -> DecodePlan {
        DecodePlan::compile(types, dt)
    }

    /// Read a primitive whose semantics have already been resolved.
    fn decode_basic_type(
        &self,
        data: &[u8],
        bt: crate::ast::resolver::BasicType,
    ) -> Result<(usize, Value), DecodeError> {
        use crate::ast::resolver::BasicType;

        let result = match bt {
            BasicType::Uint8 => read_u8(data).map(|(n, v)| (n, Value::U8(v))),
            BasicType::Uint16 => read_u16(data).map(|(n, v)| (n, Value::U16(v))),
            BasicType::Uint32 => read_u32(data).map(|(n, v)| (n, Value::U32(v))),
            BasicType::Uint64 => read_u64(data).map(|(n, v)| (n, Value::U64(v))),
            BasicType::Int8 => read_i8(data).map(|(n, v)| (n, Value::I8(v))),
            BasicType::Int16 => read_i16(data).map(|(n, v)| (n, Value::I16(v))),
            BasicType::Int32 => read_i32(data).map(|(n, v)| (n, Value::I32(v))),
            BasicType::Int64 => read_i64(data).map(|(n, v)| (n, Value::I64(v))),
            BasicType::Float => read_f32(data).map(|(n, v)| (n, Value::F32(v))),
            BasicType::Double => read_f64(data).map(|(n, v)| (n, Value::F64(v))),
            BasicType::Boolean => read_u8(data).map(|(n, v)| (n, Value::Bool(v != 0))),
            BasicType::String => {
                // Variable-length string: consume rest of buffer as UTF-8.
                let s =
                    std::str::from_utf8(data).map_err(|e| format!("invalid UTF-8 string: {e}"))?;
                Ok((data.len(), Value::Str(s.to_string())))
            }
            BasicType::FixedLengthString(len) => {
                let len = len as usize;
                if data.len() < len {
                    return Err(DecodeError::Payload(format!(
                        "need {len} bytes for fixed-length string, have {}",
                        data.len()
                    )));
                }
                let s = std::str::from_utf8(&data[..len])
                    .map_err(|e| format!("invalid UTF-8 fixed-string: {e}"))?;
                Ok((len, Value::Str(s.to_string())))
            }
        };
        result.map_err(DecodeError::Payload)
    }

    /// Resolve a type-reference path (e.g. `"/DataTypes/SpeedType"` or
    /// `"uint8"`) into a [`DataType`].
    ///
    /// The returned value is either a clone of an entry in the type
    /// map or a synthesised `DataType` for basic / built-in types. Stored
    /// aliases are followed with cycle detection; fixed string sizes are kept.
    /// Qualified paths require exact entries. Only unqualified names may use
    /// short-name compatibility lookup or builtin synthesis.
    pub fn resolve_ref(
        &self,
        types: &HashMap<String, DataType>,
        ref_path: &str,
    ) -> Option<DataType> {
        use crate::ast::resolver::resolve_basic_type;

        let key = convert::extract_last(ref_path).to_lowercase();

        // Preserve scoped identities before using legacy short-name entries.
        if let Some(mut dt) = lookup_type(types, ref_path) {
            let mut visited = HashSet::new();
            while let Some(target) = alias_target(types, dt) {
                let DataTypeKind::TypeReference(reference) = &dt.kind else {
                    unreachable!()
                };
                if !visited.insert(reference.type_name.as_str()) {
                    return None;
                }
                dt = target;
            }
            if let DataTypeKind::TypeReference(reference) = &dt.kind
                && reference.type_name.contains('/')
                && !types.contains_key(&reference.type_name)
            {
                return None;
            }
            return Some(dt.clone());
        }

        if ref_path.contains('/') {
            return None;
        }
        // 2) Fallback: treat as a basic-type reference.
        let tr = types::TypeReference {
            type_name: ref_path.to_string(),
            string_size: None,
        };
        if resolve_basic_type(&tr).is_some() {
            return Some(DataType::new_type_reference(
                key,
                "TYPE_REFERENCE".into(),
                ref_path.to_string(),
            ));
        }

        None
    }
}

fn lookup_type<'a>(types: &'a HashMap<String, DataType>, reference: &str) -> Option<&'a DataType> {
    types.get(reference).or_else(|| {
        (!reference.contains('/'))
            .then(|| types.get(&reference.to_lowercase()))
            .flatten()
    })
}

fn alias_target<'a>(types: &'a HashMap<String, DataType>, dt: &DataType) -> Option<&'a DataType> {
    use crate::ast::resolver::resolve_basic_type;
    // Declared bases are terminal primitives. Their canonical names may also
    // be used by application/implementation aliases in the compatibility map.
    if dt.category == "SW-BASE-TYPE" {
        return None;
    }
    let DataTypeKind::TypeReference(reference) = &dt.kind else {
        return None;
    };
    if reference.string_size.is_some() {
        return None;
    }
    if let Some(target) = types.get(&reference.type_name) {
        // Preserve existing VALUE identities when the old name already has
        // exactly the declared primitive semantics.
        if target.category == "SW-BASE-TYPE"
            && let DataTypeKind::TypeReference(base) = &target.kind
            && resolve_basic_type(reference).is_some()
            && resolve_basic_type(reference) == resolve_basic_type(base)
        {
            return None;
        }
        return Some(target);
    }
    // Primitive base references can share a short name with their owner.
    // Keep their existing primitive interpretation instead of making a
    // spurious self-alias through the short-name compatibility lookup.
    resolve_basic_type(reference)
        .is_none()
        .then(|| lookup_type(types, &reference.type_name))
        .flatten()
}

// ---------------------------------------------------------------------------
// Primitive readers — each returns (bytes_consumed, value)
// ---------------------------------------------------------------------------

fn read_u8(data: &[u8]) -> Result<(usize, u8), String> {
    ensure_len(data, 1, "u8")?;
    Ok((1, data[0]))
}

fn read_u16(data: &[u8]) -> Result<(usize, u16), String> {
    ensure_len(data, 2, "u16")?;
    Ok((2, u16::from_be_bytes([data[0], data[1]])))
}

fn read_u32(data: &[u8]) -> Result<(usize, u32), String> {
    ensure_len(data, 4, "u32")?;
    Ok((4, u32::from_be_bytes([data[0], data[1], data[2], data[3]])))
}

fn read_u64(data: &[u8]) -> Result<(usize, u64), String> {
    ensure_len(data, 8, "u64")?;
    Ok((
        8,
        u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]),
    ))
}

fn read_i8(data: &[u8]) -> Result<(usize, i8), String> {
    ensure_len(data, 1, "i8")?;
    Ok((1, data[0] as i8))
}

fn read_i16(data: &[u8]) -> Result<(usize, i16), String> {
    let (_, v) = read_u16(data)?;
    Ok((2, v as i16))
}

fn read_i32(data: &[u8]) -> Result<(usize, i32), String> {
    let (_, v) = read_u32(data)?;
    Ok((4, v as i32))
}

fn read_i64(data: &[u8]) -> Result<(usize, i64), String> {
    let (_, v) = read_u64(data)?;
    Ok((8, v as i64))
}

fn read_f32(data: &[u8]) -> Result<(usize, f32), String> {
    let (_, v) = read_u32(data)?;
    Ok((4, f32::from_bits(v)))
}

fn read_f64(data: &[u8]) -> Result<(usize, f64), String> {
    let (_, v) = read_u64(data)?;
    Ok((8, f64::from_bits(v)))
}

fn ensure_len(data: &[u8], need: usize, type_name: &str) -> Result<(), String> {
    if data.len() < need {
        Err(format!(
            "not enough bytes for {type_name}: need {need}, have {}",
            data.len()
        ))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::types::{ArrayType, StructureField};

    fn types_map() -> HashMap<String, DataType> {
        // A simple map with one sub-type: `Point { x: u32, y: u32 }`
        let point_fields = vec![
            StructureField {
                name: "x".into(),
                type_ref: "/Base/uint32".into(),
                in_place: false,
            },
            StructureField {
                name: "y".into(),
                type_ref: "/Base/uint32".into(),
                in_place: false,
            },
        ];
        let point_dt = DataType::new_structure("Point".into(), "STRUCTURE".into(), point_fields);
        [
            ("point".to_string(), point_dt),
            (
                "/Base/uint32".into(),
                DataType::new_type_reference(
                    "uint32".into(),
                    "SW-BASE-TYPE".into(),
                    "uint32".into(),
                ),
            ),
        ]
        .into_iter()
        .collect()
    }

    struct TestDecoder {
        decoder: Decoder,
        types: HashMap<String, DataType>,
    }

    impl TestDecoder {
        fn decode(&self, data: &[u8], dt: &DataType) -> Result<(usize, Value), String> {
            self.decoder.decode(&self.types, data, dt)
        }

        fn resolve_ref(&self, ref_path: &str) -> Option<DataType> {
            self.decoder.resolve_ref(&self.types, ref_path)
        }
    }

    fn decoder() -> TestDecoder {
        TestDecoder {
            decoder: Decoder::new(),
            types: types_map(),
        }
    }

    #[test]
    fn decode_uint8() {
        let dt = DataType::new_type_reference("u".into(), "VALUE".into(), "uint8".into());
        let d = decoder();
        let (n, v) = d.decode(&[0xAB], &dt).unwrap();
        assert_eq!(n, 1);
        assert_eq!(v, Value::U8(0xAB));
    }

    #[test]
    fn decode_numeric_aliases_and_preserve_next_field() {
        let cases: &[(&str, &[u8], usize, Value)] = &[
            ("sint8", &[0xFF, 0x7F], 1, Value::I8(-1)),
            ("int8", &[0x80, 0x7F], 1, Value::I8(-128)),
            ("int8_t", &[0xFF, 0x7F], 1, Value::I8(-1)),
            ("uint8", &[0xFF, 0x7F], 1, Value::U8(255)),
            ("uint8_t", &[0xFF, 0x7F], 1, Value::U8(255)),
            (
                "float64",
                &[0x3F, 0xF0, 0, 0, 0, 0, 0, 1, 0x7F],
                8,
                Value::F64(f64::from_bits(0x3FF0_0000_0000_0001)),
            ),
            (
                "float64_t",
                &[0x3F, 0xF0, 0, 0, 0, 0, 0, 1, 0x7F],
                8,
                Value::F64(f64::from_bits(0x3FF0_0000_0000_0001)),
            ),
            (
                "double",
                &[0x3F, 0xF0, 0, 0, 0, 0, 0, 1, 0x7F],
                8,
                Value::F64(f64::from_bits(0x3FF0_0000_0000_0001)),
            ),
            (
                "float32",
                &[0x3F, 0x80, 0, 1, 0x7F],
                4,
                Value::F32(f32::from_bits(0x3F80_0001)),
            ),
            (
                "float",
                &[0x3F, 0x80, 0, 1, 0x7F],
                4,
                Value::F32(f32::from_bits(0x3F80_0001)),
            ),
        ];
        let d = decoder();
        let sentinel = DataType::new_type_reference("next".into(), "VALUE".into(), "uint8".into());
        for (name, data, expected_size, expected) in cases {
            let dt = DataType::new_type_reference("number".into(), "VALUE".into(), (*name).into());
            let (consumed, value) = d.decode(data, &dt).unwrap();
            assert_eq!(consumed, *expected_size, "{name}");
            assert_eq!(&value, expected, "{name}");
            assert_eq!(
                d.decode(&data[consumed..], &sentinel).unwrap(),
                (1, Value::U8(0x7F)),
                "{name}: next field must remain aligned",
            );
        }
    }

    #[test]
    fn decode_uint16_big_endian() {
        let dt = DataType::new_type_reference("u".into(), "VALUE".into(), "uint16".into());
        let d = decoder();
        let (n, v) = d.decode(&[0x12, 0x34], &dt).unwrap();
        assert_eq!(n, 2);
        assert_eq!(v, Value::U16(0x1234));
    }

    #[test]
    fn decode_uint32_big_endian() {
        let dt = DataType::new_type_reference("u".into(), "VALUE".into(), "uint32".into());
        let d = decoder();
        let (n, v) = d.decode(&[0xAA, 0xBB, 0xCC, 0xDD], &dt).unwrap();
        assert_eq!(n, 4);
        assert_eq!(v, Value::U32(0xAABBCCDD));
    }

    #[test]
    fn decode_boolean_true() {
        let dt = DataType::new_type_reference("b".into(), "VALUE".into(), "bool".into());
        let d = decoder();
        let (n, v) = d.decode(&[1], &dt).unwrap();
        assert_eq!(n, 1);
        assert_eq!(v, Value::Bool(true));
    }

    #[test]
    fn decode_boolean_false() {
        let dt = DataType::new_type_reference("b".into(), "VALUE".into(), "bool".into());
        let d = decoder();
        let (_n, v) = d.decode(&[0], &dt).unwrap();
        assert_eq!(v, Value::Bool(false));
    }

    #[test]
    fn decode_float() {
        let dt = DataType::new_type_reference("f".into(), "VALUE".into(), "float".into());
        let d = decoder();
        let (n, v) = d.decode(&[0x40, 0x49, 0x0F, 0xDB], &dt).unwrap();
        assert_eq!(n, 4);
        if let Value::F32(x) = v {
            assert!((x - std::f32::consts::PI).abs() < 0.001);
        } else {
            panic!("expected F32");
        }
    }

    #[test]
    fn decode_structure() {
        // Point { x: u32, y: u32 } where x=1, y=2 (big-endian)
        let dt = decoder().resolve_ref("point").unwrap();
        let data = [0, 0, 0, 1, 0, 0, 0, 2];

        let d = decoder();
        let (n, v) = d.decode(&data, &dt).unwrap();
        assert_eq!(n, 8);

        if let Value::Struct(fields) = v {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "x");
            assert_eq!(fields[0].1, Value::U32(1));
            assert_eq!(fields[1].0, "y");
            assert_eq!(fields[1].1, Value::U32(2));
        } else {
            panic!("expected Struct");
        }
    }

    #[test]
    fn decode_fixed_array() {
        let arr_dt = DataType {
            short_name: "Arr".into(),
            category: "ARRAY".into(),
            kind: DataTypeKind::Array(ArrayType {
                size: 3,
                in_place: false,
                element_ref: "uint8".into(),
                bounds: None,
            }),
        };
        let d = decoder();
        let (n, v) = d.decode(&[10, 20, 30], &arr_dt).unwrap();
        assert_eq!(n, 3);
        assert_eq!(
            v,
            Value::Array(vec![Value::U8(10), Value::U8(20), Value::U8(30),])
        );
    }

    #[test]
    fn not_enough_bytes_error() {
        let dt = DataType::new_type_reference("u".into(), "VALUE".into(), "uint32".into());
        let d = decoder();
        let err = d.decode(&[0xAA], &dt).unwrap_err();
        assert!(err.contains("not enough bytes for u32"));
    }
}
