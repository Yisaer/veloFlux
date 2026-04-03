//! GBF packet parser for schema-driven binary packet parsing.
//!
//! This module provides shared GBF packet parsing logic that can be reused
//! by both decoders (for decoding to records) and mergers (for repacking).

use crate::schema::gbf::{GbfSchema, TypeDef};
use flow::codec::CodecError;
use std::collections::HashMap;

/// A parsed frame from a GBF packet.
#[derive(Debug, Clone)]
pub struct GbfFrame {
    pub can_id: u16,
    pub payload: Vec<u8>,
}

/// Field name mappings extracted from schema for dynamic encoding/decoding.
#[derive(Debug, Clone)]
pub struct SchemaFieldMap {
    /// Timestamp field name in packet (e.g., "ts")
    pub timestamp_field: String,
    /// Sequence field name in packet (e.g., "frames")
    pub sequence_field: String,
    /// Item type name for the sequence (e.g., "can_frame")
    pub sequence_item_type: String,
    /// CAN ID field name in frame items (e.g., "can_id")  
    pub frame_id_field: String,
    /// Payload field name in frame items (e.g., "payload")
    pub frame_payload_field: String,
}

impl SchemaFieldMap {
    /// Extract field name mappings from a GBF schema.
    ///
    /// The timestamp field must be named "ts" or "timestamp".
    pub fn from_schema(schema: &GbfSchema) -> Result<Self, CodecError> {
        // Find timestamp field - must be named "ts" or "timestamp"
        let timestamp_field = schema
            .packet
            .fields
            .iter()
            .find(|f| f.name == "ts" || f.name == "timestamp")
            .map(|f| f.name.clone())
            .ok_or_else(|| {
                CodecError::Other(
                    "No timestamp field found in packet. Field must be named 'ts' or 'timestamp'."
                        .to_string(),
                )
            })?;

        // Find sequence field
        let sequence_field_def = schema
            .packet
            .fields
            .iter()
            .find(|f| f.field_type == "sequence")
            .ok_or_else(|| CodecError::Other("No sequence field found in packet".to_string()))?;

        let sequence_field = sequence_field_def.name.clone();
        let sequence_item_type = sequence_field_def
            .item
            .as_ref()
            .ok_or_else(|| CodecError::Other("Sequence field missing item type".to_string()))?
            .type_name
            .clone();

        // Get the frame type definition
        let frame_type = schema.get_type(&sequence_item_type).ok_or_else(|| {
            CodecError::Other(format!("Frame type '{}' not found", sequence_item_type))
        })?;

        // Find CAN ID field (typically u16 type or named "can_id" or contains "id")
        let frame_id_field = frame_type
            .fields
            .iter()
            .find(|f| f.name == "can_id" || f.name.contains("id"))
            .map(|f| f.name.clone())
            .ok_or_else(|| CodecError::Other("No ID field found in frame type".to_string()))?;

        // Find payload field (bytes type or named "payload"/"data")
        let frame_payload_field = frame_type
            .fields
            .iter()
            .find(|f| f.field_type == "bytes" || f.name == "payload" || f.name == "data")
            .map(|f| f.name.clone())
            .ok_or_else(|| CodecError::Other("No payload field found in frame type".to_string()))?;

        Ok(Self {
            timestamp_field,
            sequence_field,
            sequence_item_type,
            frame_id_field,
            frame_payload_field,
        })
    }
}

/// Internal optimized representation of a field.
#[derive(Debug, Clone)]
pub(crate) struct OptField {
    pub _name: String,
    pub kind: OptFieldType,
    pub index: usize,
    /// If true, this field's value is needed solely for calculating timestamp (ts).
    pub is_ts: bool,
    /// If set, the value must be stored because it is referenced by a length_ref or id_ref
    pub store_index: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) enum OptFieldType {
    U8 {
        const_val: Option<u64>,
        mask: Option<u64>,
        shift: Option<u32>,
    },
    U16Be {
        id_ref: bool,
    },
    U16Le {
        #[allow(dead_code)]
        id_ref: bool,
    },
    U32Be,
    U32Le,
    U64Be,
    U64Le,
    Sequence {
        length_ref_index: usize,
        item_type: String,
    },
    Bytes {
        length_ref_index: usize,
        format_id_ref_index: Option<usize>,
    },
}

pub(crate) struct OptTypeDef {
    pub fields: Vec<OptField>,
    /// Number of storage slots needed for this type's context
    pub storage_size: usize,
}

/// GBF parser that converts binary packets into structured frames based on JSON schema.
pub struct GbfParser {
    packet_def: OptTypeDef,
    type_defs: HashMap<String, OptTypeDef>,
    field_map: SchemaFieldMap,
}

impl GbfParser {
    /// Create a new GBF parser from a schema.
    pub fn new(gbf_schema: GbfSchema) -> Self {
        // Extract field name mappings first
        let field_map = SchemaFieldMap::from_schema(&gbf_schema)
            .expect("Failed to extract field mappings from schema");

        // Compile schema into optimized form
        let type_defs: HashMap<String, OptTypeDef> = gbf_schema
            .types
            .iter()
            .map(|(k, v)| (k.clone(), Self::compile_type_def(v)))
            .collect();

        let packet_def = Self::compile_type_def(&gbf_schema.packet);

        Self {
            packet_def,
            type_defs,
            field_map,
        }
    }

    /// Get the schema field name mappings.
    pub fn field_map(&self) -> &SchemaFieldMap {
        &self.field_map
    }

    fn compile_type_def(def: &TypeDef) -> OptTypeDef {
        let mut fields = Vec::with_capacity(def.fields.len());
        let mut name_to_index = HashMap::new();

        // First pass: assign storage indices to all fields that might be referenced
        for (i, field) in def.fields.iter().enumerate() {
            name_to_index.insert(field.name.clone(), i);
        }

        for (i, field) in def.fields.iter().enumerate() {
            let kind = match field.field_type.as_str() {
                "u8" => OptFieldType::U8 {
                    const_val: field.const_value,
                    mask: field.read_mask,
                    shift: field.read_shift,
                },
                "u16be" => {
                    let is_id_ref = field.format.as_ref().is_some_and(|f| f.id_ref.is_some())
                        || def.fields.iter().any(|f| {
                            f.format.as_ref().and_then(|fmt| fmt.id_ref.as_ref())
                                == Some(&field.name)
                        })
                        || field.name == "can_id"; // Also treat can_id as id_ref by convention
                    OptFieldType::U16Be { id_ref: is_id_ref }
                }
                "u16le" => {
                    let is_id_ref = field.format.as_ref().is_some_and(|f| f.id_ref.is_some())
                        || def.fields.iter().any(|f| {
                            f.format.as_ref().and_then(|fmt| fmt.id_ref.as_ref())
                                == Some(&field.name)
                        });
                    OptFieldType::U16Le { id_ref: is_id_ref }
                }
                "u32be" => OptFieldType::U32Be,
                "u32le" => OptFieldType::U32Le,
                "u64be" => OptFieldType::U64Be,
                "u64le" => OptFieldType::U64Le,
                "sequence" => {
                    let ref_name = field
                        .length_ref
                        .as_ref()
                        .expect("sequence must have length_ref");
                    let ref_idx = *name_to_index.get(ref_name).expect("invalid length_ref");
                    OptFieldType::Sequence {
                        length_ref_index: ref_idx,
                        item_type: field
                            .item
                            .as_ref()
                            .map(|i| i.type_name.clone())
                            .unwrap_or_default(),
                    }
                }
                "bytes" => {
                    let ref_name = field
                        .length_ref
                        .as_ref()
                        .expect("bytes must have length_ref");
                    let ref_idx = *name_to_index.get(ref_name).expect("invalid length_ref");
                    let id_ref_idx = field
                        .format
                        .as_ref()
                        .and_then(|f| f.id_ref.as_ref())
                        .map(|name| *name_to_index.get(name).expect("invalid id_ref"));

                    OptFieldType::Bytes {
                        length_ref_index: ref_idx,
                        format_id_ref_index: id_ref_idx,
                    }
                }
                _ => OptFieldType::U8 {
                    const_val: None,
                    mask: None,
                    shift: None,
                }, // fallback
            };

            fields.push(OptField {
                _name: field.name.clone(),
                kind,
                index: i,
                is_ts: field.name == "ts"
                    || field.name == "timestamp"
                    || field.name.contains("time"),
                store_index: Some(i), // Store everything for now
            });
        }

        OptTypeDef {
            fields,
            storage_size: def.fields.len(),
        }
    }

    /// Split a payload into individual packets based on the schema.
    pub fn split_packets<'a>(&self, payload: &'a [u8]) -> Vec<&'a [u8]> {
        let mut packets = Vec::new();
        let mut cursor = 0;

        // Calculate packet header size (all fixed fields before the sequence)
        let header_size = self.calculate_header_size(&self.packet_def);

        while cursor + header_size <= payload.len() {
            if let Some((len_offset, len_size)) = self.find_length_field_offset(&self.packet_def) {
                let len_bytes = &payload[cursor + len_offset..cursor + len_offset + len_size];
                let body_len = self.parse_uint(len_bytes, len_size) as usize;
                let packet_size = header_size + body_len;

                if cursor + packet_size > payload.len() {
                    break;
                }

                packets.push(&payload[cursor..cursor + packet_size]);
                cursor += packet_size;
            } else {
                packets.push(&payload[cursor..]);
                break;
            }
        }

        packets
    }

    /// Parse a single packet into timestamp and frames based on the schema.
    pub fn parse_packet(&self, packet: &[u8]) -> Result<(u64, Vec<GbfFrame>), CodecError> {
        let mut cursor = 0;
        let mut context = vec![0u64; self.packet_def.storage_size];
        let mut timestamp = 0u64;
        let mut frames = Vec::new();

        for field in &self.packet_def.fields {
            if cursor >= packet.len() {
                break;
            }

            match &field.kind {
                OptFieldType::U8 {
                    const_val: _,
                    mask,
                    shift,
                } => {
                    let mut value = packet[cursor] as u64;
                    if let Some(m) = mask {
                        value &= m;
                    }
                    if let Some(s) = shift {
                        value >>= s;
                    }
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 1;
                }
                OptFieldType::U16Be { .. } => {
                    if cursor + 2 > packet.len() {
                        break;
                    }
                    let value = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as u64;
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 2;
                }
                OptFieldType::U16Le { .. } => {
                    if cursor + 2 > packet.len() {
                        break;
                    }
                    let value = u16::from_le_bytes([packet[cursor], packet[cursor + 1]]) as u64;
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 2;
                }
                OptFieldType::U32Be => {
                    if cursor + 4 > packet.len() {
                        break;
                    }
                    let value =
                        u32::from_be_bytes(packet[cursor..cursor + 4].try_into().unwrap()) as u64;
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 4;
                }
                OptFieldType::U32Le => {
                    if cursor + 4 > packet.len() {
                        break;
                    }
                    let value =
                        u32::from_le_bytes(packet[cursor..cursor + 4].try_into().unwrap()) as u64;
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 4;
                }
                OptFieldType::U64Be => {
                    if cursor + 8 > packet.len() {
                        break;
                    }
                    let value = u64::from_be_bytes(packet[cursor..cursor + 8].try_into().unwrap());
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 8;
                }
                OptFieldType::U64Le => {
                    if cursor + 8 > packet.len() {
                        break;
                    }
                    let value = u64::from_le_bytes(packet[cursor..cursor + 8].try_into().unwrap());
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 8;
                }
                OptFieldType::Sequence {
                    length_ref_index,
                    item_type,
                } => {
                    let seq_len = context[*length_ref_index] as usize;
                    let seq_end = (cursor + seq_len).min(packet.len());

                    if let Some(frame_type) = self.type_defs.get(item_type) {
                        frames = self.parse_frames(&packet[cursor..seq_end], frame_type)?;
                    }
                    cursor = seq_end;
                }
                _ => {}
            }
        }

        Ok((timestamp, frames))
    }

    /// Parse frames from a sequence buffer.
    fn parse_frames(
        &self,
        buffer: &[u8],
        frame_type: &OptTypeDef,
    ) -> Result<Vec<GbfFrame>, CodecError> {
        // Estimate capacity: assume minimum 4 bytes per frame
        let estimated_frames = (buffer.len() / 4).max(16);
        let mut frames = Vec::with_capacity(estimated_frames);
        let mut cursor = 0;

        // Pre-allocate context once, reuse for each frame
        let mut context = vec![0u64; frame_type.storage_size];

        while cursor < buffer.len() {
            // Skip padding (0x00 bytes)
            while cursor < buffer.len() && buffer[cursor] == 0 {
                cursor += 1;
            }
            if cursor >= buffer.len() {
                break;
            }

            let mut frame_cursor = cursor;
            // Reset context for this frame
            context.fill(0);
            let mut can_id: Option<u32> = None;
            let mut payload_start = 0usize;
            let mut payload_len = 0usize;
            let mut magic_ok = true;

            for field in &frame_type.fields {
                if frame_cursor >= buffer.len() {
                    break;
                }

                match &field.kind {
                    OptFieldType::U8 {
                        const_val,
                        mask,
                        shift,
                    } => {
                        let mut value = buffer[frame_cursor] as u64;
                        if let Some(m) = mask {
                            value &= m;
                        }
                        if let Some(s) = shift {
                            value >>= s;
                        }

                        if let Some(expected) = const_val
                            && value != *expected
                        {
                            magic_ok = false;
                            break;
                        }
                        context[field.store_index.unwrap()] = value;
                        frame_cursor += 1;
                    }
                    OptFieldType::U16Be { id_ref } => {
                        if frame_cursor + 2 > buffer.len() {
                            break;
                        }
                        let value =
                            u16::from_be_bytes([buffer[frame_cursor], buffer[frame_cursor + 1]])
                                as u64;
                        context[field.store_index.unwrap()] = value;
                        if *id_ref {
                            can_id = Some(value as u32);
                        }
                        frame_cursor += 2;
                    }
                    OptFieldType::U32Be => {
                        if frame_cursor + 4 > buffer.len() {
                            break;
                        }
                        let value = u32::from_be_bytes(
                            buffer[frame_cursor..frame_cursor + 4].try_into().unwrap(),
                        ) as u64;
                        context[field.store_index.unwrap()] = value;
                        // For u32be fields containing "id" in name, treat as can_id
                        if field._name.contains("id") {
                            can_id = Some(value as u32);
                        }
                        frame_cursor += 4;
                    }
                    OptFieldType::Bytes {
                        length_ref_index,
                        format_id_ref_index,
                    } => {
                        let len = context[*length_ref_index] as usize;
                        payload_start = frame_cursor;
                        payload_len = len.min(buffer.len() - frame_cursor);

                        // Check if we need to get can_id from a reference field
                        if let Some(id_idx) = format_id_ref_index {
                            can_id = Some(context[*id_idx] as u32);
                        }

                        frame_cursor += payload_len;
                    }
                    _ => {}
                }
            }

            if !magic_ok {
                cursor += 1;
                continue;
            }

            if frame_cursor > cursor
                && let Some(cid) = can_id
            {
                frames.push(GbfFrame {
                    can_id: cid as u16, // Cast to u16 for GbfFrame compatibility
                    payload: buffer[payload_start..payload_start + payload_len].to_vec(),
                });
            }

            if frame_cursor == cursor {
                break;
            }
            cursor = frame_cursor;
        }

        Ok(frames)
    }

    /// Calculate the size of fixed header fields before the sequence.
    fn calculate_header_size(&self, type_def: &OptTypeDef) -> usize {
        let mut size = 0;
        for field in &type_def.fields {
            match field.kind {
                OptFieldType::U8 { .. } => size += 1,
                OptFieldType::U16Be { .. } | OptFieldType::U16Le { .. } => size += 2,
                OptFieldType::U32Be | OptFieldType::U32Le => size += 4,
                OptFieldType::U64Be | OptFieldType::U64Le => size += 8,
                OptFieldType::Sequence { .. } | OptFieldType::Bytes { .. } => break,
            }
        }
        size
    }

    /// Find the offset and size of the length field.
    fn find_length_field_offset(&self, type_def: &OptTypeDef) -> Option<(usize, usize)> {
        let mut offset = 0;
        // Find which field is used as length ref for the sequence
        let length_ref_idx = type_def
            .fields
            .iter()
            .filter_map(|f| match &f.kind {
                OptFieldType::Sequence {
                    length_ref_index, ..
                } => Some(*length_ref_index),
                _ => None,
            })
            .next();

        if let Some(target_idx) = length_ref_idx {
            for field in &type_def.fields {
                let size = match field.kind {
                    OptFieldType::U8 { .. } => 1,
                    OptFieldType::U16Be { .. } => 2,
                    OptFieldType::U32Be => 4,
                    OptFieldType::U64Be => 8,
                    _ => return None,
                };

                if field.index == target_idx {
                    return Some((offset, size));
                }
                offset += size;
            }
        }
        None
    }

    /// Parse an unsigned integer from bytes.
    fn parse_uint(&self, bytes: &[u8], size: usize) -> u64 {
        match size {
            1 => bytes[0] as u64,
            2 => u16::from_be_bytes([bytes[0], bytes[1]]) as u64,
            4 => u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as u64,
            8 => u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::gbf::GbfSchema;

    fn get_standard_schema() -> GbfSchema {
        let json = r#"
        {
            "types": {
                "can_frame": {
                    "fields": [
                        { "name": "magic", "type": "u8", "const": 85 },
                        { "name": "can_id", "type": "u16be" },
                        { "name": "data_len", "type": "u8" },
                        { "name": "payload", "type": "bytes", "length_ref": "data_len", "format": { "id_ref": "can_id" } }
                    ]
                }
            },
            "packet": {
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    { "name": "frames", "type": "sequence", "length_ref": "total_len", "item": { "type": "can_frame" } }
                ]
            }
        }
        "#;
        serde_json::from_str(json).expect("parse schema")
    }

    // Test Issue #1: Timestamp detection should prioritize name over type
    #[test]
    fn test_timestamp_detection_prioritizes_name_over_type() {
        // Schema where total_len (u32) comes before timestamp
        let json = r#"
        {
            "types": {
                "frame": {
                    "fields": [
                        { "name": "id", "type": "u16be" },
                        { "name": "data", "type": "bytes", "length_ref": "id" }
                    ]
                }
            },
            "packet": {
                "fields": [
                    { "name": "total_len", "type": "u32be" },
                    { "name": "ts", "type": "u64be" },
                    { "name": "frames", "type": "sequence", "length_ref": "total_len", "item": { "type": "frame" } }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let field_map = SchemaFieldMap::from_schema(&schema).expect("field map");

        // Should detect "ts" as timestamp, not "total_len" (even though total_len is first u32)
        assert_eq!(field_map.timestamp_field, "ts");
    }

    // Test that non-ts/timestamp field names are rejected
    #[test]
    fn test_timestamp_requires_ts_or_timestamp_name() {
        let json = r#"
        {
            "types": {
                "frame": {
                    "fields": [
                        { "name": "id", "type": "u16be" },
                        { "name": "data", "type": "bytes", "length_ref": "id" }
                    ]
                }
            },
            "packet": {
                "fields": [
                    { "name": "epoch", "type": "u64be" },
                    { "name": "count", "type": "u16be" },
                    { "name": "items", "type": "sequence", "length_ref": "count", "item": { "type": "frame" } }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let result = SchemaFieldMap::from_schema(&schema);

        // Should error because "epoch" is not "ts" or "timestamp"
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ts"));
    }

    // Test Issue #2: Empty packets handling
    #[test]
    fn test_split_packets_empty_payload() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema);

        let packets = parser.split_packets(&[]);
        assert!(packets.is_empty());
    }

    // Test Issue #2: Payload smaller than header
    #[test]
    fn test_split_packets_payload_smaller_than_header() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema);

        // Only 5 bytes, but header needs 10 (8 for ts + 2 for total_len)
        let packets = parser.split_packets(&[0, 0, 0, 0, 0]);
        assert!(packets.is_empty());
    }

    // Test parse_packet with valid data
    #[test]
    fn test_parse_packet_extracts_timestamp_and_frames() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema);

        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 100]); // ts = 100
        packet.extend_from_slice(&[0, 12]); // total_len = 12
        packet.push(0x55); // magic
        packet.extend_from_slice(&[0x01, 0x00]); // can_id = 256
        packet.push(0x08); // data_len = 8
        packet.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]); // payload

        let (timestamp, frames) = parser.parse_packet(&packet).expect("parse");
        assert_eq!(timestamp, 100);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].can_id, 256);
        assert_eq!(frames[0].payload, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    // Test parse_packet with no frames (heartbeat)
    #[test]
    fn test_parse_packet_heartbeat_no_frames() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema);

        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 50]); // ts = 50
        packet.extend_from_slice(&[0, 0]); // total_len = 0 (no frames)

        let (timestamp, frames) = parser.parse_packet(&packet).expect("parse");
        assert_eq!(timestamp, 50);
        assert!(frames.is_empty());
    }

    // Test field_map accessor
    #[test]
    fn test_field_map_returns_correct_mappings() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema);
        let map = parser.field_map();

        assert_eq!(map.timestamp_field, "ts");
        assert_eq!(map.sequence_field, "frames");
        assert_eq!(map.sequence_item_type, "can_frame");
        assert_eq!(map.frame_id_field, "can_id");
        assert_eq!(map.frame_payload_field, "payload");
    }

    // Test multiple packets in single payload
    #[test]
    fn test_split_multiple_packets() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema);

        let mut payload = Vec::new();
        // Packet 1
        payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]); // ts = 1
        payload.extend_from_slice(&[0, 0]); // total_len = 0
        // Packet 2
        payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 2]); // ts = 2
        payload.extend_from_slice(&[0, 0]); // total_len = 0

        let packets = parser.split_packets(&payload);
        assert_eq!(packets.len(), 2);
    }

    // Test invalid magic byte skipped
    #[test]
    fn test_parse_packet_skips_invalid_magic() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema);

        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 100]); // ts
        packet.extend_from_slice(&[0, 12]); // total_len
        packet.push(0xAA); // WRONG magic (not 0x55)
        packet.extend_from_slice(&[0x01, 0x00]); // can_id
        packet.push(0x08); // data_len
        packet.extend_from_slice(&[0; 8]); // payload

        let (_, frames) = parser.parse_packet(&packet).expect("parse");
        // Frame with wrong magic should be skipped
        assert!(frames.is_empty());
    }

    // Test SchemaFieldMap error cases
    #[test]
    fn test_schema_field_map_no_sequence_error() {
        let json = r#"
        {
            "types": {},
            "packet": {
                "fields": [
                    { "name": "ts", "type": "u64be" }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let result = SchemaFieldMap::from_schema(&schema);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No sequence field")
        );
    }
}
