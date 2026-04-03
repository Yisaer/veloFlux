//! GBF (General Binary Format) schema definitions for packet decoding.
//!
//! This module defines the JSON schema format for describing binary packet structures.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Root schema definition containing types and packet structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GbfSchema {
    /// Named type definitions that can be referenced by packet fields.
    #[serde(default)]
    pub types: HashMap<String, TypeDef>,
    /// The root packet structure definition.
    pub packet: TypeDef,
}

/// A type definition (struct with fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDef {
    /// List of fields in this struct.
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
    /// Unit of length (bytes or count).
    pub length_unit: Option<String>,
    /// For sequence types: the item type.
    pub item: Option<ItemRef>,
    /// Format specification for payload decoding.
    pub format: Option<FormatSpec>,
    /// Bit mask to apply after reading the value (for integers).
    pub read_mask: Option<u64>,
    /// Bit shift to apply after masking.
    pub read_shift: Option<u32>,
}

/// Reference to an item type (for sequences).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemRef {
    /// Type name reference.
    #[serde(rename = "type")]
    pub type_name: String,
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

    /// Get a type definition by name.
    pub fn get_type(&self, name: &str) -> Option<&TypeDef> {
        self.types.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schema() {
        let json = r#"
        {
            "types": {
                "can_frame": {
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
            },
            "packet": {
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    { 
                        "name": "frames", 
                        "type": "sequence", 
                        "item": { "type": "can_frame" },
                        "length_ref": "total_len",
                        "length_unit": "bytes"
                    }
                ]
            }
        }
        "#;

        let schema: GbfSchema = serde_json::from_str(json).expect("parse schema");

        assert!(schema.types.contains_key("can_frame"));
        assert_eq!(schema.packet.fields.len(), 3);

        let ts_field = &schema.packet.fields[0];
        assert_eq!(ts_field.name, "ts");
        assert_eq!(ts_field.field_type, "u64be");

        let frames_field = &schema.packet.fields[2];
        assert_eq!(frames_field.name, "frames");
        assert_eq!(frames_field.field_type, "sequence");
        assert_eq!(frames_field.item.as_ref().unwrap().type_name, "can_frame");

        let can_frame = schema.get_type("can_frame").unwrap();
        assert_eq!(can_frame.fields.len(), 4);

        let magic_field = &can_frame.fields[0];
        assert_eq!(magic_field.const_value, Some(85));

        let payload_field = &can_frame.fields[3];
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
    fn test_get_type_returns_none_for_missing() {
        let json = r#"{ "types": {}, "packet": { "fields": [] } }"#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        assert!(schema.get_type("nonexistent").is_none());
    }

    #[test]
    fn test_schema_with_read_mask_and_shift() {
        let json = r#"
        {
            "types": {},
            "packet": {
                "fields": [
                    { "name": "flags", "type": "u8", "read_mask": 127, "read_shift": 1 }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let field = &schema.packet.fields[0];
        assert_eq!(field.read_mask, Some(127));
        assert_eq!(field.read_shift, Some(1));
    }

    #[test]
    fn test_schema_with_length_ref() {
        let json = r#"
        {
            "types": {},
            "packet": {
                "fields": [
                    { "name": "len", "type": "u16be" },
                    { "name": "data", "type": "bytes", "length_ref": "len" }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let data_field = &schema.packet.fields[1];
        assert_eq!(data_field.length_ref.as_ref().unwrap(), "len");
    }

    #[test]
    fn test_schema_empty_types() {
        let json = r#"{ "types": {}, "packet": { "fields": [] } }"#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        assert!(schema.types.is_empty());
        assert!(schema.packet.fields.is_empty());
    }
}
