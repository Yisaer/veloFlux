//! GBF (General Binary Format) schema definitions for packet decoding.
//!
//! This module defines the JSON schema format for describing binary packet structures.

use serde::{Deserialize, Serialize};

/// Root schema definition. Contains a single inline `structure` that describes the packet layout.
/// All sequence item types are defined inline via nested `structure` fields, not a named-type registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GbfSchema {
    /// Root packet structure definition. Required — serde will produce a clear
    /// 'missing field `structure`' error at load time for malformed schemas.
    pub structure: GbfStructure,
}

/// A structure definition (struct with fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GbfStructure {
    /// Type discriminator (usually "struct").
    #[serde(rename = "type")]
    pub type_name: String,
    /// Fields of the structure.
    #[serde(default)]
    pub fields: Vec<Field>,
}

/// A single field in a struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    /// Field name (used for output key and references).
    pub name: String,
    /// Field type (u8, u16be, u16le, u32be, u64be, bytes, sequence).
    #[serde(rename = "type")]
    pub field_type: String,
    /// Constant value constraint (for magic bytes).
    #[serde(rename = "const")]
    pub const_value: Option<u64>,
    /// Reference to another field for length.
    pub length_ref: Option<String>,
    /// Unit of length. Only `"bytes"` is supported.
    pub length_unit: Option<String>,
    /// For sequence types: the item structure.
    pub structure: Option<GbfStructure>,
    /// Format specification for payload decoding.
    pub format: Option<FormatSpec>,
    /// Bit mask to apply after reading the value (for integers).
    pub read_mask: Option<u64>,
    /// Bit shift to apply after masking.
    pub read_shift: Option<u32>,
}

/// Format specification for payload decoding.
/// Presence of this object marks a field as an embedded payload.
/// The actual format type is determined by decoder configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatSpec {
    /// Reference to the field containing the message ID.
    pub id_ref: Option<String>,
}

impl GbfSchema {
    /// Load a GBF schema from a JSON file.
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let schema: GbfSchema = serde_json::from_str(&content)?;
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schema() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    { 
                        "name": "frames", 
                        "type": "sequence", 
                        "length_ref": "total_len",
                        "length_unit": "bytes",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "magic", "type": "u8", "const": 85 },
                                { "name": "can_id", "type": "u16be" },
                                { "name": "data_len", "type": "u8" },
                                { 
                                    "name": "payload", 
                                    "type": "bytes",
                                    "length_ref": "data_len",
                                    "format": { "type": "dbc", "id_ref": "can_id" }
                                }
                            ]
                        }
                    }
                ]
            }
        }
        "#;

        let schema: GbfSchema = serde_json::from_str(json).expect("parse schema");
        let root = &schema.structure;

        assert_eq!(root.fields.len(), 3);

        let ts_field = &root.fields[0];
        assert_eq!(ts_field.name, "ts");
        assert_eq!(ts_field.field_type, "u64be");

        let frames_field = &root.fields[2];
        assert_eq!(frames_field.name, "frames");
        assert_eq!(frames_field.field_type, "sequence");
        let can_frame_item = frames_field.structure.as_ref().expect("sequence structure"); // Field::structure stays Option
        assert_eq!(can_frame_item.type_name, "struct");
        assert_eq!(can_frame_item.fields.len(), 4);

        let magic_field = &can_frame_item.fields[0];
        assert_eq!(magic_field.const_value, Some(85));

        let payload_field = &can_frame_item.fields[3];
        assert_eq!(
            payload_field
                .format
                .as_ref()
                .unwrap()
                .id_ref
                .as_ref()
                .unwrap(),
            "can_id"
        );
    }

    #[test]
    fn test_parse_minimal_schema() {
        let json = r#"{ "structure": { "type": "struct", "fields": [] } }"#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        assert_eq!(schema.structure.fields.len(), 0);
    }

    #[test]
    fn test_schema_with_read_mask_and_shift() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "flags", "type": "u8", "read_mask": 127, "read_shift": 1 }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let root = &schema.structure;
        let field = &root.fields[0];
        assert_eq!(field.read_mask, Some(127));
        assert_eq!(field.read_shift, Some(1));
    }

    #[test]
    fn test_schema_with_length_ref() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "len", "type": "u16be" },
                    { "name": "data", "type": "bytes", "length_ref": "len" }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let root = &schema.structure;
        let data_field = &root.fields[1];
        assert_eq!(data_field.length_ref.as_ref().unwrap(), "len");
    }
}
