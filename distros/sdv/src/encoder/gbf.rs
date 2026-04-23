//! GBF (General Binary Format) encoder for generating binary packets from schema.
//!
//! This module provides schema-driven encoding of binary packets based on GBF schema definitions.
//! All encoding logic is derived from the schema - no hardcoded values.

use crate::schema::gbf::{Field, GbfSchema};
use rand::Rng;
use std::collections::HashMap;

/// Error type for GBF encoding operations.
#[derive(Debug)]
pub enum GbfEncodeError {
    /// Field type is not supported.
    UnsupportedType(String),
    /// Invalid field value type.
    InvalidValue(String),
}

impl std::fmt::Display for GbfEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GbfEncodeError::UnsupportedType(t) => write!(f, "unsupported field type: {}", t),
            GbfEncodeError::InvalidValue(m) => write!(f, "invalid field value: {}", m),
        }
    }
}

impl std::error::Error for GbfEncodeError {}

/// Value types for encoding fields.
#[derive(Debug, Clone)]
pub enum FieldValue {
    /// Integer value (auto-sized based on field type).
    Int(u64),
    /// Bytes payload.
    Bytes(Vec<u8>),
    /// Sequence of items (each item is a FieldValues map).
    Sequence(Vec<FieldValues>),
}

/// Field name to value mapping for encoding.
pub type FieldValues = HashMap<String, FieldValue>;

/// GBF encoder that generates binary packets from schema definitions.
pub struct GbfEncoder {
    schema: GbfSchema,
}

impl GbfEncoder {
    /// Create a new GBF encoder from a schema.
    pub fn new(schema: GbfSchema) -> Self {
        Self { schema }
    }

    /// Create a GBF encoder by loading a schema from a file.
    pub fn from_schema_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let schema = GbfSchema::load(path)?;
        Ok(Self::new(schema))
    }

    /// Encode a packet with provided field values.
    ///
    /// Fields with `const` values are auto-filled from schema.
    /// Missing fields use defaults (0 for integers, empty for bytes).
    pub fn encode(&self, values: &FieldValues) -> Result<Vec<u8>, GbfEncodeError> {
        self.encode_type_def_from_fields(&self.schema.structure.fields, values)
    }

    /// Encode data and return as uppercase hex string.
    pub fn encode_hex(&self, values: &FieldValues) -> Result<String, GbfEncodeError> {
        let binary = self.encode(values)?;
        Ok(binary.iter().map(|b| format!("{:02X}", b)).collect())
    }

    /// Generate random field values based on schema structure.
    pub fn generate_random<R: Rng>(&self, rng: &mut R, items_per_sequence: usize) -> FieldValues {
        self.generate_random_for_type_from_fields(
            &self.schema.structure.fields,
            rng,
            items_per_sequence,
        )
    }

    /// Encode a type definition with provided values.
    fn encode_type_def_from_fields(
        &self,
        fields_def: &[Field],
        values: &FieldValues,
    ) -> Result<Vec<u8>, GbfEncodeError> {
        let mut buffer = Vec::new();

        // First pass: encode all non-length-dependent fields to calculate sizes
        let mut field_sizes: HashMap<String, usize> = HashMap::new();

        // Pre-calculate sequence/bytes sizes for length_ref fields
        for field in fields_def {
            if let Some(ref length_ref) = field.length_ref {
                // This field has a length reference, calculate its size
                let size = self.calculate_field_size(field, values)?;
                field_sizes.insert(length_ref.clone(), size);
            }
        }

        // Second pass: encode fields
        for field in fields_def {
            let field_bytes = self.encode_field(field, values, &field_sizes)?;
            buffer.extend(field_bytes);
        }

        Ok(buffer)
    }

    /// Calculate size of a field that will be encoded.
    fn calculate_field_size(
        &self,
        field: &Field,
        values: &FieldValues,
    ) -> Result<usize, GbfEncodeError> {
        match field.field_type.as_str() {
            "bytes" => {
                if let Some(FieldValue::Bytes(data)) = values.get(&field.name) {
                    Ok(data.len())
                } else {
                    Ok(0)
                }
            }
            "sequence" => {
                if let Some(unit) = field.length_unit.as_deref()
                    && unit != "bytes"
                {
                    return Err(GbfEncodeError::InvalidValue(format!(
                        "sequence field '{}' has unsupported length_unit '{}'; \
                         only \"bytes\" is supported",
                        field.name, unit
                    )));
                }
                if let Some(FieldValue::Sequence(items)) = values.get(&field.name) {
                    let item_ref = field.structure.as_ref().ok_or_else(|| {
                        GbfEncodeError::InvalidValue(format!(
                            "sequence field '{}' is missing 'structure' definition",
                            field.name
                        ))
                    })?;

                    let mut total_size = 0;
                    for item_values in items {
                        let encoded =
                            self.encode_type_def_from_fields(&item_ref.fields, item_values)?;
                        total_size += encoded.len();
                    }
                    Ok(total_size)
                } else {
                    Ok(0)
                }
            }
            _ => Ok(self.get_type_size(&field.field_type)),
        }
    }

    /// Encode a single field.
    fn encode_field(
        &self,
        field: &Field,
        values: &FieldValues,
        field_sizes: &HashMap<String, usize>,
    ) -> Result<Vec<u8>, GbfEncodeError> {
        // Check if this field should use a const value
        if let Some(const_val) = field.const_value {
            return self.encode_integer(&field.field_type, const_val, field);
        }

        // Check if this field is a length_ref target (stores size of another field)
        if let Some(&size) = field_sizes.get(&field.name) {
            return self.encode_integer(&field.field_type, size as u64, field);
        }

        // Get value from provided values or use default
        match field.field_type.as_str() {
            "u8" | "u16be" | "u16le" | "u32be" | "u32le" | "u64be" | "u64le" => {
                let value = match values.get(&field.name) {
                    Some(FieldValue::Int(v)) => *v,
                    _ => 0,
                };
                self.encode_integer(&field.field_type, value, field)
            }
            "bytes" => {
                let data = match values.get(&field.name) {
                    Some(FieldValue::Bytes(d)) => d.clone(),
                    _ => Vec::new(),
                };
                Ok(data)
            }
            "sequence" => {
                if let Some(unit) = field.length_unit.as_deref()
                    && unit != "bytes"
                {
                    return Err(GbfEncodeError::InvalidValue(format!(
                        "sequence field '{}' has unsupported length_unit '{}'; \
                         only \"bytes\" is supported",
                        field.name, unit
                    )));
                }
                let items = match values.get(&field.name) {
                    Some(FieldValue::Sequence(s)) => s,
                    _ => return Ok(Vec::new()),
                };

                let item_ref = field.structure.as_ref().ok_or_else(|| {
                    GbfEncodeError::InvalidValue(format!(
                        "sequence field '{}' is missing 'structure' definition",
                        field.name
                    ))
                })?;

                let mut buffer = Vec::new();
                for item_values in items {
                    let encoded =
                        self.encode_type_def_from_fields(&item_ref.fields, item_values)?;
                    buffer.extend(encoded);
                }
                Ok(buffer)
            }
            other => Err(GbfEncodeError::UnsupportedType(other.to_string())),
        }
    }

    /// Encode an integer value with the specified type.
    fn encode_integer(
        &self,
        type_name: &str,
        value: u64,
        field: &Field,
    ) -> Result<Vec<u8>, GbfEncodeError> {
        // Apply write_mask and write_shift (reverse of read operations)
        let mut val = value;

        // For encoding, we reverse the read operations:
        // read: (raw & mask) >> shift = value
        // write: (value << shift) = raw (within mask bits)
        if let Some(shift) = field.read_shift {
            val <<= shift;
        }
        // Note: write_mask would be used for combining with other bits,
        // but for simple fields we just encode the shifted value

        match type_name {
            "u8" => Ok(vec![val as u8]),
            "u16be" => Ok((val as u16).to_be_bytes().to_vec()),
            "u16le" => Ok((val as u16).to_le_bytes().to_vec()),
            "u32be" => Ok((val as u32).to_be_bytes().to_vec()),
            "u32le" => Ok((val as u32).to_le_bytes().to_vec()),
            "u64be" => Ok(val.to_be_bytes().to_vec()),
            "u64le" => Ok(val.to_le_bytes().to_vec()),
            other => Err(GbfEncodeError::UnsupportedType(other.to_string())),
        }
    }

    /// Get the byte size of a primitive type.
    fn get_type_size(&self, type_name: &str) -> usize {
        match type_name {
            "u8" => 1,
            "u16be" | "u16le" => 2,
            "u32be" | "u32le" => 4,
            "u64be" | "u64le" => 8,
            _ => 0,
        }
    }

    /// Generate random values for a type definition.
    fn generate_random_for_type_from_fields<R: Rng>(
        &self,
        fields_def: &[Field],
        rng: &mut R,
        items_per_sequence: usize,
    ) -> FieldValues {
        let mut values = FieldValues::new();

        for field in fields_def {
            // Skip const fields (they're auto-filled)
            if field.const_value.is_some() {
                continue;
            }

            // Skip length_ref target fields (they're auto-calculated)
            let is_length_target = fields_def
                .iter()
                .any(|f| f.length_ref.as_ref().is_some_and(|lr| lr == &field.name));
            if is_length_target {
                continue;
            }

            let value = self.generate_random_field_value(field, rng, items_per_sequence);
            if let Some(v) = value {
                values.insert(field.name.clone(), v);
            }
        }

        values
    }

    /// Generate a random value for a field.
    fn generate_random_field_value<R: Rng>(
        &self,
        field: &Field,
        rng: &mut R,
        items_per_sequence: usize,
    ) -> Option<FieldValue> {
        match field.field_type.as_str() {
            "u8" => Some(FieldValue::Int(rng.r#gen::<u8>() as u64)),
            "u16be" | "u16le" => Some(FieldValue::Int(rng.r#gen::<u16>() as u64)),
            "u32be" | "u32le" => Some(FieldValue::Int(rng.r#gen::<u32>() as u64)),
            "u64be" | "u64le" => Some(FieldValue::Int(rng.r#gen::<u64>())),
            "bytes" => {
                // Generate random bytes (default 8 bytes like CAN payload)
                let len = 8;
                let data: Vec<u8> = (0..len).map(|_| rng.r#gen()).collect();
                Some(FieldValue::Bytes(data))
            }
            "sequence" => {
                let item_ref = field.structure.as_ref()?;

                let items: Vec<FieldValues> = (0..items_per_sequence)
                    .map(|_| {
                        self.generate_random_for_type_from_fields(
                            &item_ref.fields,
                            rng,
                            items_per_sequence,
                        )
                    })
                    .collect();

                Some(FieldValue::Sequence(items))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_schema() -> GbfSchema {
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
                        "structure": { 
                            "type": "struct",
                            "fields": [
                                { "name": "magic", "type": "u8", "const": 85 },
                                { "name": "can_id", "type": "u16be" },
                                { "name": "data_len", "type": "u8" },
                                {
                                    "name": "payload",
                                    "type": "bytes",
                                    "length_ref": "data_len"
                                }
                            ]
                        },
                        "length_unit": "bytes"
                    }
                ]
            }
        }
        "#;
        serde_json::from_str(json).expect("parse test schema")
    }

    #[test]
    fn test_encode_simple_packet() {
        let schema = get_test_schema();
        let encoder = GbfEncoder::new(schema);

        // Create a packet with one frame
        let mut frame1 = FieldValues::new();
        frame1.insert("can_id".to_string(), FieldValue::Int(0x586));
        frame1.insert(
            "payload".to_string(),
            FieldValue::Bytes(vec![0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11, 0x55]),
        );

        let mut values = FieldValues::new();
        values.insert("ts".to_string(), FieldValue::Int(0x00000190A5A0EC4A));
        values.insert("frames".to_string(), FieldValue::Sequence(vec![frame1]));

        let result = encoder.encode(&values).expect("encode packet");

        // Verify structure:
        // ts: 8 bytes
        // total_len: 2 bytes
        // frame: magic(1) + can_id(2) + data_len(1) + payload(8) = 12 bytes
        assert_eq!(result.len(), 8 + 2 + 12);

        // Verify timestamp (big-endian)
        assert_eq!(
            &result[0..8],
            &[0x00, 0x00, 0x01, 0x90, 0xA5, 0xA0, 0xEC, 0x4A]
        );

        // Verify total_len (should be 12 for one frame)
        assert_eq!(&result[8..10], &[0x00, 0x0C]);

        // Verify magic byte
        assert_eq!(result[10], 0x55);

        // Verify can_id (0x586)
        assert_eq!(&result[11..13], &[0x05, 0x86]);

        // Verify data_len (8)
        assert_eq!(result[13], 0x08);

        // Verify payload
        assert_eq!(
            &result[14..22],
            &[0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11, 0x55]
        );
    }

    #[test]
    fn test_encode_hex() {
        let schema = get_test_schema();
        let encoder = GbfEncoder::new(schema);

        let mut frame = FieldValues::new();
        frame.insert("can_id".to_string(), FieldValue::Int(0x100));
        frame.insert("payload".to_string(), FieldValue::Bytes(vec![0xAB, 0xCD]));

        let mut values = FieldValues::new();
        values.insert("ts".to_string(), FieldValue::Int(1000));
        values.insert("frames".to_string(), FieldValue::Sequence(vec![frame]));

        let hex = encoder.encode_hex(&values).expect("encode hex");

        // Should be uppercase hex
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(
            hex.chars()
                .filter(|c| c.is_ascii_alphabetic())
                .all(|c| c.is_uppercase())
        );
    }

    #[test]
    fn test_generate_random() {
        let schema = get_test_schema();
        let encoder = GbfEncoder::new(schema);
        let mut rng = rand::thread_rng();

        let values = encoder.generate_random(&mut rng, 2);

        // Should have ts and frames
        assert!(values.contains_key("ts"));
        assert!(values.contains_key("frames"));

        // ts should be present, and frames should have items
        if let Some(FieldValue::Sequence(items)) = values.get("frames") {
            assert_eq!(items.len(), 2);

            // Each frame should have can_id and payload
            for item in items {
                assert!(item.contains_key("can_id"));
                assert!(item.contains_key("payload"));
            }
        } else {
            panic!("frames should be a sequence");
        }

        // Encode should work with random values
        let result = encoder.encode(&values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_const_values_auto_filled() {
        let schema = get_test_schema();
        let encoder = GbfEncoder::new(schema);

        // Create frame without explicitly setting magic - it should auto-fill
        let mut frame = FieldValues::new();
        frame.insert("can_id".to_string(), FieldValue::Int(0x123));
        frame.insert("payload".to_string(), FieldValue::Bytes(vec![0x00]));

        let mut values = FieldValues::new();
        values.insert("ts".to_string(), FieldValue::Int(0));
        values.insert("frames".to_string(), FieldValue::Sequence(vec![frame]));

        let result = encoder.encode(&values).expect("encode");

        // Magic should be 0x55 (85) as defined in schema
        assert_eq!(result[10], 0x55);
    }
}
