//! GBF Merger - Accumulates raw GBF packets and repacks them into a merged binary packet.
//!
//! This merger is used by the Sampler processor's "Packer" strategy to
//! accumulate CAN frames from raw GBF packets over an interval and pack them
//! into a single merged GBF packet.
//!
//! Data flow: Raw GBF bytes → merge() → trigger() → Merged GBF bytes → Decoder

use std::collections::HashMap;

use flow::Merger;
use flow::codec::CodecError;

use crate::codec::gbf_parser::GbfParser;
use crate::encoder::gbf::{FieldValue, FieldValues, GbfEncoder};
use crate::schema::gbf::GbfSchema;

/// GBF Merger that accumulates raw GBF packets and repacks them on trigger.
///
/// Similar to `spi.go` behavior in eKuiper:
/// - Receives raw GBF binary packets
/// - Extracts and accumulates frames indexed by CAN ID
/// - On trigger, packs all accumulated frames into a new GBF packet
pub struct GbfMerger {
    /// Parser for decoding incoming GBF packets
    parser: GbfParser,
    /// Encoder for packing accumulated frames
    encoder: GbfEncoder,
    /// Accumulated frames: CAN ID -> raw frame bytes
    frame_set: HashMap<u32, Vec<u8>>,
    /// Last timestamp seen
    last_ts: u64,
}

impl GbfMerger {
    /// Create a new GBF Merger from a schema.
    pub fn new(schema: GbfSchema) -> Self {
        let parser = GbfParser::new(schema.clone());
        Self {
            parser,
            encoder: GbfEncoder::new(schema),
            frame_set: HashMap::with_capacity(64),
            last_ts: 0,
        }
    }

    /// Create a GBF Merger by loading schema from file.
    pub fn from_schema_file(schema_path: &str) -> Result<Self, CodecError> {
        let schema = GbfSchema::load(schema_path)
            .map_err(|e| CodecError::Other(format!("failed to load schema: {e}")))?;
        Ok(Self::new(schema))
    }

    /// Reset the merger state (useful for benchmarking)
    pub fn reset(&mut self) {
        self.frame_set.clear();
        self.last_ts = 0;
    }
}

impl Merger for GbfMerger {
    fn merge(&mut self, data: &[u8]) -> Result<(), CodecError> {
        // Parse packet using schema-driven parser
        let (timestamp, frames) = self.parser.parse_packet(data)?;
        self.last_ts = timestamp;

        // Accumulate frames by CAN ID
        for frame in frames {
            self.frame_set.insert(frame.can_id as u32, frame.payload);
        }

        Ok(())
    }

    fn trigger(&mut self) -> Result<Option<Vec<u8>>, CodecError> {
        if self.frame_set.is_empty() {
            return Ok(None);
        }

        let field_map = self.parser.field_map();

        // Build frames from accumulated data (take ownership to avoid cloning)
        let frames: Vec<FieldValues> = std::mem::take(&mut self.frame_set)
            .into_iter()
            .map(|(can_id, payload)| {
                let mut frame = FieldValues::new();
                frame.insert(
                    field_map.frame_id_field.clone(),
                    FieldValue::Int(can_id as u64),
                );
                frame.insert(
                    field_map.frame_payload_field.clone(),
                    FieldValue::Bytes(payload),
                );
                frame
            })
            .collect();

        // Build packet values
        let mut values = FieldValues::new();
        values.insert(
            field_map.timestamp_field.clone(),
            FieldValue::Int(self.last_ts),
        );
        values.insert(
            field_map.sequence_field.clone(),
            FieldValue::Sequence(frames),
        );

        // Encode to binary
        let packed = self
            .encoder
            .encode(&values)
            .map_err(|e| CodecError::Other(format!("encoding failed: {e}")))?;

        Ok(Some(packed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_schema() -> GbfSchema {
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
                            "length_ref": "data_len"
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
        serde_json::from_str(json).expect("parse test schema")
    }

    #[test]
    fn test_gbf_merger_trigger_empty() {
        let schema = get_test_schema();
        let mut merger = GbfMerger::new(schema);

        let result = merger.trigger().expect("trigger");
        assert!(result.is_none());
    }

    #[test]
    fn test_gbf_merger_parse_and_repack() {
        // Create a GBF packet that matches our test schema:
        // - ts: u64be (8 bytes)
        // - total_len: u16be (2 bytes)
        // - frames: sequence of can_frame
        //   - magic: u8 (0x55)
        //   - can_id: u16be (2 bytes)
        //   - data_len: u8 (1 byte)
        //   - payload: bytes[data_len]

        // Frame 1: can_id=0x0100, payload="ABCD" (4 bytes)
        // Frame 2: can_id=0x0200, payload="EF" (2 bytes)
        // Total frame bytes: (1+2+1+4) + (1+2+1+2) = 8 + 6 = 14 bytes

        let mut data = Vec::new();
        // Timestamp: 1000000000 = 0x3B9ACA00
        data.extend_from_slice(&0x3B9ACA00u64.to_be_bytes());
        // Total len: 14
        data.extend_from_slice(&14u16.to_be_bytes());
        // Frame 1: magic, can_id, data_len, payload
        data.push(0x55);
        data.extend_from_slice(&0x0100u16.to_be_bytes());
        data.push(4);
        data.extend_from_slice(b"ABCD");
        // Frame 2: magic, can_id, data_len, payload
        data.push(0x55);
        data.extend_from_slice(&0x0200u16.to_be_bytes());
        data.push(2);
        data.extend_from_slice(b"EF");

        let schema = get_test_schema();
        let mut merger = GbfMerger::new(schema);

        // Merge the packet
        merger.merge(&data).expect("merge");

        // Check that frames were accumulated
        assert_eq!(merger.frame_set.len(), 2);
        assert!(merger.frame_set.contains_key(&0x0100));
        assert!(merger.frame_set.contains_key(&0x0200));

        // Trigger to get repacked output
        let result = merger.trigger().expect("trigger");
        assert!(result.is_some());

        let packed = result.unwrap();
        // Verify it's a valid GBF packet (starts with timestamp, has frames)
        assert!(packed.len() > 10);

        // The first 8 bytes should be the timestamp
        let ts = u64::from_be_bytes([
            packed[0], packed[1], packed[2], packed[3], packed[4], packed[5], packed[6], packed[7],
        ]);
        assert_eq!(ts, 1000000000);
    }

    #[test]
    fn test_gbf_merger_different_schema() {
        // Test with completely different schema to verify schema-driven approach
        // Different: field names, types, sizes, and field order
        let schema_json = r#"
        {
            "types": {
                "message": {
                    "fields": [
                        { "name": "header", "type": "u8", "const": 170 },
                        { "name": "length", "type": "u8" },
                        { "name": "data", "type": "bytes", "length_ref": "length" },
                        { "name": "msg_id", "type": "u32be" }
                    ]
                }
            },
            "packet": {
                "fields": [
                    { "name": "timestamp", "type": "u32be" },
                    { "name": "count", "type": "u16be" },
                    {
                        "name": "messages",
                        "type": "sequence",
                        "item": { "type": "message" },
                        "length_ref": "count",
                        "length_unit": "bytes"
                    }
                ]
            }
        }
        "#;

        let schema: GbfSchema = serde_json::from_str(schema_json).expect("parse schema");
        let mut merger = GbfMerger::new(schema);

        // Create a packet with different structure:
        // - timestamp: u32be (4 bytes) instead of u64be
        // - count: u16be (2 bytes) instead of total_len
        // - messages with: header(1) + length(1) + data(N) + msg_id(4)
        let mut data = Vec::new();

        // Timestamp: 123456 = 0x0001E240
        data.extend_from_slice(&123456u32.to_be_bytes());

        // Count: 18 bytes total for 2 messages
        // Message 1: 1 + 1 + 3 + 4 = 9 bytes
        // Message 2: 1 + 1 + 2 + 4 = 8 bytes
        // Total: 17 bytes
        data.extend_from_slice(&17u16.to_be_bytes());

        // Message 1: msg_id=0x00001111, data="ABC" (3 bytes)
        data.push(0xAA); // header
        data.push(3); // length
        data.extend_from_slice(b"ABC"); // data
        data.extend_from_slice(&0x00001111u32.to_be_bytes()); // msg_id

        // Message 2: msg_id=0x00002222, data="XY" (2 bytes)
        data.push(0xAA); // header
        data.push(2); // length
        data.extend_from_slice(b"XY"); // data
        data.extend_from_slice(&0x00002222u32.to_be_bytes()); // msg_id

        // Merge the packet
        merger.merge(&data).expect("merge");

        // Check that frames were accumulated (using msg_id as key)
        assert_eq!(merger.frame_set.len(), 2);
        assert!(merger.frame_set.contains_key(&0x00001111));
        assert!(merger.frame_set.contains_key(&0x00002222));

        // Verify payloads
        assert_eq!(merger.frame_set.get(&0x00001111).unwrap(), b"ABC");
        assert_eq!(merger.frame_set.get(&0x00002222).unwrap(), b"XY");

        // Trigger to get repacked output
        let result = merger.trigger().expect("trigger");
        assert!(result.is_some());

        let packed = result.unwrap();

        // Verify structure: 4-byte timestamp + 2-byte count + messages
        assert!(packed.len() >= 6);

        // The first 4 bytes should be the timestamp (u32be)
        let ts = u32::from_be_bytes([packed[0], packed[1], packed[2], packed[3]]);
        assert_eq!(ts, 123456);

        // The next 2 bytes should be the count
        let count = u16::from_be_bytes([packed[4], packed[5]]);
        assert!(count > 0, "Count should be non-zero");
    }
}
