//! Basic type resolution — maps AUTOSAR type-reference strings to
//! concrete [`BasicType`] discriminants.
//!
//! Corresponds to the Go `ast.util.go` / `GetBasicTypeFromRef`.

use crate::ast::types::TypeReference;

/// Legacy primitive namespaces used by minimal fixtures and AP standard types.
/// Register these exact referenced identities at parse time, never after a
/// runtime path miss. Arbitrary namespaces and substring matches are excluded.
pub(crate) fn legacy_builtin_name(path: &str) -> Option<String> {
    let name = ["/BaseTypes/", "/PlatformTypes/", "/AUTOSAR/StdTypes/"]
        .iter()
        .find_map(|prefix| path.strip_prefix(prefix))?
        .to_lowercase();
    matches!(
        name.as_str(),
        "uint8"
            | "uint8_t"
            | "uint16"
            | "uint16_t"
            | "uint32"
            | "uint32_t"
            | "uint64"
            | "uint64_t"
            | "int8"
            | "int8_t"
            | "sint8"
            | "int16"
            | "int16_t"
            | "sint16"
            | "int32"
            | "int32_t"
            | "sint32"
            | "int64"
            | "int64_t"
            | "sint64"
            | "float"
            | "float32"
            | "float32_t"
            | "float64"
            | "float64_t"
            | "double"
            | "bool"
            | "boolean"
            | "string"
    )
    .then_some(name)
}

/// Enum representing every built-in basic type recognised by the converter.
///
/// The variants map 1:1 to the `typeref.TypeRef` constructors in the
/// Go `idl-parser` library.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BasicType {
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    Boolean,
    /// Variable-length string (no fixed bound).
    String,
    /// Fixed-length string with the given byte-size.
    FixedLengthString(u64),
}

/// Try to resolve a [`TypeReference`] into a known [`BasicType`].
///
/// Returns `None` when the reference does not name a recognised built-in.
pub fn resolve_basic_type(type_ref: &TypeReference) -> Option<BasicType> {
    let name = extract_type_name_from_ref(&type_ref.type_name);

    if name.contains("string") {
        return type_ref
            .string_size
            .filter(|&s| s > 0)
            .map(BasicType::FixedLengthString)
            .or(Some(BasicType::String));
    }

    // Match unsigned names before their signed substrings, and float64 before
    // the generic float alias. Retain substring matching for prefixed names.
    if name.contains("uint16") {
        Some(BasicType::Uint16)
    } else if name.contains("uint32") {
        Some(BasicType::Uint32)
    } else if name.contains("uint64") {
        Some(BasicType::Uint64)
    } else if name.contains("uint8") {
        Some(BasicType::Uint8)
    } else if name.contains("int8") {
        Some(BasicType::Int8)
    } else if name.contains("int16") {
        Some(BasicType::Int16)
    } else if name.contains("int32") {
        Some(BasicType::Int32)
    } else if name.contains("int64") {
        Some(BasicType::Int64)
    } else if name.contains("float64") || name.contains("double") {
        Some(BasicType::Double)
    } else if name.contains("float") {
        Some(BasicType::Float)
    } else if name.contains("bool") {
        Some(BasicType::Boolean)
    } else {
        None
    }
}

/// Extract the trailing segment of an ARXML type-path (e.g.
/// `"/PlatformTypes/uint32"` → `"uint32"`), lowercased.
pub fn extract_type_name_from_ref(r: &str) -> String {
    r.rsplit('/').next().unwrap_or(r).to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_uint32() {
        let tr = TypeReference {
            type_name: "/PlatformTypes/uint32".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), Some(BasicType::Uint32));
    }

    #[test]
    fn resolve_float_aliases() {
        for (name, expected) in [
            ("/BaseTypes/Float", BasicType::Float),
            ("/BaseTypes/float32", BasicType::Float),
            ("/BaseTypes/float32_t", BasicType::Float),
            ("/BaseTypes/float64", BasicType::Double),
            ("/BaseTypes/FLOAT64_T", BasicType::Double),
            ("/BaseTypes/double", BasicType::Double),
        ] {
            let tr = TypeReference {
                type_name: name.into(),
                string_size: None,
            };
            assert_eq!(resolve_basic_type(&tr), Some(expected), "{name}");
        }
    }

    #[test]
    fn resolve_string_variable() {
        let tr = TypeReference {
            type_name: "string".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), Some(BasicType::String));
    }

    #[test]
    fn resolve_fixed_length_string() {
        let tr = TypeReference {
            type_name: "/StringType".into(),
            string_size: Some(64),
        };
        assert_eq!(
            resolve_basic_type(&tr),
            Some(BasicType::FixedLengthString(64))
        );
    }

    #[test]
    fn resolve_boolean() {
        let tr = TypeReference {
            type_name: "boolean".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), Some(BasicType::Boolean));
    }

    #[test]
    fn resolve_8_bit_integer_aliases() {
        for (name, expected) in [
            ("/BaseTypes/int8", BasicType::Int8),
            ("/BaseTypes/sint8", BasicType::Int8),
            ("/BaseTypes/int8_t", BasicType::Int8),
            ("/BaseTypes/SINT8", BasicType::Int8),
            ("/BaseTypes/uint8", BasicType::Uint8),
            ("/BaseTypes/uint8_t", BasicType::Uint8),
        ] {
            let tr = TypeReference {
                type_name: name.into(),
                string_size: None,
            };
            assert_eq!(resolve_basic_type(&tr), Some(expected), "{name}");
        }
    }

    #[test]
    fn unknown_type_returns_none() {
        let tr = TypeReference {
            type_name: "/custom/SomeWeirdType".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), None);
    }

    #[test]
    fn extract_trailing_segment() {
        assert_eq!(extract_type_name_from_ref("/PlatformTypes/Float"), "float");
        assert_eq!(extract_type_name_from_ref("uint32"), "uint32");
    }
}
