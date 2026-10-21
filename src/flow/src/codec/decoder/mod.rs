//! Decoder abstractions for turning raw bytes into RecordBatch collections.

pub mod proto;
pub mod proto_bundle;

use crate::model::{CollectionError, Message, RecordBatch, Tuple};
use crate::planner::decode_projection::{DecodeProjection, ProjectionNode};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use datatypes::{
    ColumnSchema, ConcreteDatatype, ListType, ListValue, Schema, StringType, StructField,
    StructType, StructValue, TimestampValue, Value,
};
use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer};
use serde::de::{
    Deserialize, DeserializeSeed, Deserializer, Error as DeError, IgnoredAny, MapAccess, SeqAccess,
    Visitor,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

pub const FILE_LINE_DECODER_KIND: &str = "file_line";
const FILE_LINE_FRAME_FILENAME_LEN_BYTES: usize = 4;

/// Errors that can occur while decoding payloads.
#[derive(thiserror::Error, Debug)]
pub enum CodecError {
    /// Payload was not valid UTF-8 (used by the simple string decoder).
    #[error("invalid utf8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// Payload was not valid JSON.
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    /// RecordBatch construction failed.
    #[error("collection error: {0}")]
    Collection(#[from] CollectionError),
    /// Custom decoder-specific failure.
    #[error("{0}")]
    Other(String),
}

/// Trait implemented by all record decoders.
pub trait RecordDecoder: Send + Sync + 'static {
    /// Prepare immutable execution state for one projection snapshot.
    /// Custom decoders can retain the existing per-call interface.
    fn prepare(
        &self,
        _projection: Option<&DecodeProjection>,
    ) -> Option<Arc<dyn PreparedRecordDecoder>> {
        None
    }

    /// Direct decoder instances default to isolated execution.
    fn supports_inline_execution(&self) -> bool {
        false
    }

    /// Convert raw bytes into a RecordBatch, optionally applying a decode projection.
    ///
    /// Projection semantics:
    /// - `None`: decode all schema columns.
    /// - `Some(p)`: only decode columns present in `p`; missing columns are treated as `NULL`.
    ///   Nested projection nodes (struct fields / list indexes) are applied when present.
    fn decode_with_projection(
        &self,
        payload: &[u8],
        projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError>;

    /// Convert raw bytes into a RecordBatch (no projection).
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError> {
        self.decode_with_projection(payload, None)
    }
}

/// A decoder bound to one immutable schema and projection snapshot.
pub trait PreparedRecordDecoder: Send + Sync + 'static {
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError>;
}

/// Decoder that converts JSON documents (object or array) into a RecordBatch.
#[derive(Clone)]
pub struct JsonDecoder {
    stream_name: Arc<str>,
    schema: Arc<Schema>,
    schema_keys: Arc<[Arc<str>]>,
    #[allow(dead_code)]
    props: Arc<JsonMap<String, JsonValue>>,
    /// Immutable full-schema plan, reused without allocation or locking.
    /// Non-flat schemas always use the DOM-based path.
    flat_plan: Option<Arc<FlatRowPlan>>,
}

impl JsonDecoder {
    pub fn new(
        stream_name: impl Into<String>,
        schema: Arc<Schema>,
        props: JsonMap<String, JsonValue>,
    ) -> Self {
        let stream_name: Arc<str> = Arc::<str>::from(stream_name.into());
        let schema_keys: Arc<[Arc<str>]> = Arc::from(
            schema
                .column_schemas()
                .iter()
                .map(|col| Arc::<str>::from(col.name.as_str()))
                .collect::<Vec<_>>(),
        );
        let flat_plan = plan_flat_row(&schema, None, &schema_keys).map(Arc::new);
        Self {
            stream_name,
            schema,
            schema_keys,
            props: Arc::new(props),
            flat_plan,
        }
    }

    fn decode_value_with_decode_projection(
        &self,
        json: JsonValue,
        decode_projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        match json {
            JsonValue::Object(map) => {
                self.build_from_object_rows_with_decode_projection(vec![map], decode_projection)
            }
            JsonValue::Array(items) => {
                self.decode_array_with_decode_projection(items, decode_projection)
            }
            other => Err(CodecError::Other(format!(
                "JSON root must be object or array, got {other:?}"
            ))),
        }
    }

    fn decode_array_with_decode_projection(
        &self,
        items: Vec<JsonValue>,
        decode_projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        if items.is_empty() {
            return Ok(RecordBatch::empty());
        }

        if !items.iter().all(|v| v.is_object()) {
            return Err(CodecError::Other(
                "JSON array must contain only objects".to_string(),
            ));
        }

        let rows: Vec<JsonMap<String, JsonValue>> = items
            .into_iter()
            .map(|v| match v {
                JsonValue::Object(map) => Ok(map),
                _ => Err(CodecError::Other(
                    "internal: expected JSON object after validation".to_string(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.build_from_object_rows_with_decode_projection(rows, decode_projection)
    }

    fn build_from_object_rows_with_decode_projection(
        &self,
        rows: Vec<JsonMap<String, JsonValue>>,
        decode_projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        if rows.is_empty() {
            return Ok(RecordBatch::empty());
        }

        let tuples =
            self.build_tuples_from_object_rows_with_decode_projection(rows, decode_projection)?;
        Ok(RecordBatch::new(tuples)?)
    }

    fn build_tuples_from_object_rows_with_decode_projection(
        &self,
        rows: Vec<JsonMap<String, JsonValue>>,
        decode_projection: Option<&DecodeProjection>,
    ) -> Result<Vec<Tuple>, CodecError> {
        if let Some(output_slots) =
            decode_projection.and_then(|projection| projection.output_slots())
        {
            let columns_by_name: HashMap<&str, _> = self
                .schema
                .column_schemas()
                .iter()
                .map(|column| (column.name.as_str(), column))
                .collect();
            let mut tuples = Vec::with_capacity(rows.len());
            for mut row in rows {
                let mut values = Vec::with_capacity(output_slots.len());
                for slot in output_slots.iter() {
                    let Some(column) = columns_by_name.get(slot.as_ref()) else {
                        values.push(Value::Null);
                        continue;
                    };
                    let projection_node =
                        decode_projection.and_then(|p| p.column(column.name.as_str()));
                    let value = projection_node
                        .and_then(|node| {
                            row.remove(&column.name).map(|json| {
                                json_to_value_with_datatype_and_projection(
                                    &json,
                                    &column.data_type,
                                    Some(node),
                                )
                            })
                        })
                        .unwrap_or(Value::Null);
                    values.push(value);
                }
                drop(row);
                tuples.push(assemble_row(
                    &self.stream_name,
                    &Arc::clone(output_slots),
                    values,
                ));
            }
            return Ok(tuples);
        }

        let mut tuples = Vec::with_capacity(rows.len());
        for mut row in rows {
            let mut values = Vec::with_capacity(self.schema_keys.len());
            for column in self.schema.column_schemas().iter() {
                let projection_node =
                    decode_projection.and_then(|p| p.column(column.name.as_str()));
                let value = match decode_projection {
                    Some(_) => {
                        if let Some(node) = projection_node {
                            row.remove(&column.name)
                                .map(|json| {
                                    json_to_value_with_datatype_and_projection(
                                        &json,
                                        &column.data_type,
                                        Some(node),
                                    )
                                })
                                .unwrap_or(Value::Null)
                        } else {
                            let _ = row.remove(&column.name);
                            Value::Null
                        }
                    }
                    None => row
                        .remove(&column.name)
                        .map(|json| {
                            json_to_value_with_datatype_and_projection(
                                &json,
                                &column.data_type,
                                None,
                            )
                        })
                        .unwrap_or(Value::Null),
                };
                values.push(value);
            }
            drop(row);
            tuples.push(assemble_row(
                &self.stream_name,
                &Arc::clone(&self.schema_keys),
                values,
            ));
        }
        Ok(tuples)
    }
}

/// Assemble one output row shared by the slow and flat-fast paths.
pub(crate) fn assemble_row(
    stream: &Arc<str>,
    slots: &Arc<[Arc<str>]>,
    values: Vec<Value>,
) -> Tuple {
    let message = Arc::new(Message::new_shared_keys(
        Arc::clone(stream),
        Arc::clone(slots),
        values,
    ));
    Tuple::new(vec![message])
}

/// Returns true for scalar datatypes the streaming fast path decodes
/// inline. List/Struct (and any future composite) stay on the DOM-based
/// slow path.
fn is_flat_datatype(datatype: &ConcreteDatatype) -> bool {
    !matches!(
        datatype,
        ConcreteDatatype::List(_) | ConcreteDatatype::Struct(_)
    )
}

/// Per-slot decode operation for one payload: convert with the datatype,
/// or always emit Null (slot not backed by a schema column under the
/// active projection).
pub(crate) struct FlatRowPlan {
    pub(crate) slots: Arc<[Arc<str>]>,
    schema: Arc<Schema>,
    /// Reuse Schema's name index, then translate directly to the output layout.
    schema_to_output: Vec<Option<usize>>,
    ops: Vec<Option<ScalarConversion>>,
}

/// Derive the flat-row plan for one decode call. Returns None (slow path)
/// when the schema is not flat or any emitted slot carries a nested
/// projection node.
pub(crate) fn plan_flat_row(
    schema: &Arc<Schema>,
    projection: Option<&DecodeProjection>,
    fallback_slots: &Arc<[Arc<str>]>,
) -> Option<FlatRowPlan> {
    let columns = schema.column_schemas();
    // Duplicate schema names need the DOM path's schema-order consumption.
    if columns.is_empty()
        || !columns.iter().enumerate().all(|(index, column)| {
            is_flat_datatype(&column.data_type) && schema.column_index(&column.name) == Some(index)
        })
    {
        return None;
    }
    let slots = projection
        .and_then(|projection| projection.output_slots())
        .cloned()
        .unwrap_or_else(|| Arc::clone(fallback_slots));
    let mut ops = Vec::with_capacity(slots.len());
    let mut schema_to_output = vec![None; columns.len()];
    for (output_index, slot) in slots.iter().enumerate() {
        let column = schema.column_index(slot.as_ref()).map(|schema_index| {
            // The DOM path consumes a field at its first output occurrence.
            schema_to_output[schema_index].get_or_insert(output_index);
            &columns[schema_index]
        });
        // Mirrors the slow path exactly: without a projection every schema
        // column converts; with one, only columns registered in it convert
        // (pruned columns yield Null even when present in the payload), and
        // any nested projection node forces the slow path.
        let op = match column {
            None => None,
            Some(column) => match projection {
                None => ScalarConversion::for_datatype(&column.data_type),
                Some(p) => match p.column(slot.as_ref()) {
                    Some(ProjectionNode::All) => ScalarConversion::for_datatype(&column.data_type),
                    Some(_) => return None,
                    None => None,
                },
            },
        };
        ops.push(op);
    }
    Some(FlatRowPlan {
        slots,
        schema: Arc::clone(schema),
        schema_to_output,
        ops,
    })
}

/// Exact copy of the generic number arm of `json_to_value`: as_i64, else
/// as_u64, else as_f64, else Null.
fn number_value(number: &serde_json::Number) -> Value {
    if let Some(i) = number.as_i64() {
        Value::Int64(i)
    } else if let Some(u) = number.as_u64() {
        Value::Uint64(u)
    } else if let Some(f) = number.as_f64() {
        Value::Float64(f)
    } else {
        Value::Null
    }
}

/// The scalar conversion contract shared by DOM and streaming decode.
#[derive(Clone, Copy)]
enum ScalarConversion {
    Null,
    Bool,
    Int64,
    Uint64,
    Timestamp,
    Bytes,
    Native,
}

enum JsonScalar<'a> {
    Null,
    Bool(bool),
    Number(Cow<'a, serde_json::Number>),
    String(&'a str),
}

impl<'a> JsonScalar<'a> {
    fn from_json(value: &'a JsonValue) -> Option<Self> {
        match value {
            JsonValue::Null => Some(Self::Null),
            JsonValue::Bool(value) => Some(Self::Bool(*value)),
            JsonValue::Number(value) => Some(Self::Number(Cow::Borrowed(value))),
            JsonValue::String(value) => Some(Self::String(value)),
            _ => None,
        }
    }
}

impl ScalarConversion {
    fn for_datatype(datatype: &ConcreteDatatype) -> Option<Self> {
        Some(match datatype {
            ConcreteDatatype::Null => Self::Null,
            ConcreteDatatype::Bool(_) => Self::Bool,
            ConcreteDatatype::Int64(_) => Self::Int64,
            ConcreteDatatype::Uint64(_) => Self::Uint64,
            ConcreteDatatype::Timestamp(_) => Self::Timestamp,
            ConcreteDatatype::Bytes(_) => Self::Bytes,
            ConcreteDatatype::List(_) | ConcreteDatatype::Struct(_) => return None,
            ConcreteDatatype::Int8(_)
            | ConcreteDatatype::Int16(_)
            | ConcreteDatatype::Int32(_)
            | ConcreteDatatype::Uint8(_)
            | ConcreteDatatype::Uint16(_)
            | ConcreteDatatype::Uint32(_)
            | ConcreteDatatype::Float32(_)
            | ConcreteDatatype::Float64(_)
            | ConcreteDatatype::String(_) => Self::Native,
        })
    }

    fn convert(self, value: JsonScalar<'_>) -> Value {
        match (self, value) {
            (Self::Native | Self::Bool, JsonScalar::Bool(value)) => Value::Bool(value),
            (Self::Native, JsonScalar::Number(value)) => number_value(&value),
            (Self::Int64, JsonScalar::Number(value)) => {
                value.as_i64().map(Value::Int64).unwrap_or(Value::Null)
            }
            (Self::Uint64, JsonScalar::Number(value)) => {
                value.as_u64().map(Value::Uint64).unwrap_or(Value::Null)
            }
            (Self::Native, JsonScalar::String(value)) => Value::String(value.to_owned().into()),
            (Self::Timestamp, JsonScalar::String(value)) => TimestampValue::parse_rfc3339(value)
                .map(Value::Timestamp)
                .unwrap_or(Value::Null),
            (Self::Bytes, JsonScalar::String(value)) => BASE64_STANDARD
                .decode(value.as_bytes())
                .map(Bytes::from)
                .map(|bytes| Value::Bytes(bytes.into()))
                .unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }

    fn convert_json(self, value: &JsonValue) -> Value {
        match JsonScalar::from_json(value) {
            Some(value) => self.convert(value),
            None if matches!(self, Self::Native) => json_to_value(value),
            None => Value::Null,
        }
    }
}

struct TypedSeed<'a> {
    op: &'a Option<ScalarConversion>,
}

impl<'de> DeserializeSeed<'de> for TypedSeed<'_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        match self.op {
            None => {
                IgnoredAny::deserialize(deserializer)?;
                Ok(Value::Null)
            }
            Some(conversion) => deserializer.deserialize_any(TypedVisitor {
                conversion: *conversion,
            }),
        }
    }
}

struct TypedVisitor {
    conversion: ScalarConversion,
}

impl<'de> Visitor<'de> for TypedVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON scalar")
    }

    fn visit_bool<E: DeError>(self, value: bool) -> Result<Value, E> {
        Ok(self.conversion.convert(JsonScalar::Bool(value)))
    }

    fn visit_i64<E: DeError>(self, value: i64) -> Result<Value, E> {
        Ok(self
            .conversion
            .convert(JsonScalar::Number(Cow::Owned(value.into()))))
    }

    fn visit_u64<E: DeError>(self, value: u64) -> Result<Value, E> {
        Ok(self
            .conversion
            .convert(JsonScalar::Number(Cow::Owned(value.into()))))
    }

    fn visit_f64<E: DeError>(self, value: f64) -> Result<Value, E> {
        Ok(serde_json::Number::from_f64(value)
            .map(|value| {
                self.conversion
                    .convert(JsonScalar::Number(Cow::Owned(value)))
            })
            .unwrap_or(Value::Null))
    }

    fn visit_str<E: DeError>(self, value: &str) -> Result<Value, E> {
        Ok(self.conversion.convert(JsonScalar::String(value)))
    }

    fn visit_borrowed_str<E: DeError>(self, value: &'de str) -> Result<Value, E> {
        self.visit_str(value)
    }

    fn visit_string<E: DeError>(self, value: String) -> Result<Value, E> {
        self.visit_str(&value)
    }

    fn visit_unit<E: DeError>(self) -> Result<Value, E> {
        Ok(self.conversion.convert(JsonScalar::Null))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        if matches!(self.conversion, ScalarConversion::Native) {
            let value = JsonValue::deserialize(SeqAccessDeserializer::new(seq))?;
            Ok(self.conversion.convert_json(&value))
        } else {
            while seq.next_element::<IgnoredAny>()?.is_some() {}
            Ok(Value::Null)
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        if matches!(self.conversion, ScalarConversion::Native) {
            let value = JsonValue::deserialize(MapAccessDeserializer::new(map))?;
            Ok(self.conversion.convert_json(&value))
        } else {
            while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
            Ok(Value::Null)
        }
    }
}

struct RowSeed<'a> {
    plan: &'a FlatRowPlan,
    stream: &'a Arc<str>,
}

impl<'de, 'a> DeserializeSeed<'de> for RowSeed<'a> {
    type Value = Tuple;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Tuple, D::Error> {
        deserializer.deserialize_map(RowVisitor {
            plan: self.plan,
            stream: self.stream,
            values: vec![Value::Null; self.plan.slots.len()],
        })
    }
}

struct RowVisitor<'a> {
    plan: &'a FlatRowPlan,
    stream: &'a Arc<str>,
    values: Vec<Value>,
}

impl<'de, 'a> Visitor<'de> for RowVisitor<'a> {
    type Value = Tuple;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Tuple, A::Error> {
        let mut this = self;
        while let Some(key) = map.next_key::<Cow<'_, str>>()? {
            let slot = this
                .plan
                .schema
                .column_index(key.as_ref())
                .and_then(|index| this.plan.schema_to_output[index]);
            match slot {
                Some(index) => {
                    let value = map.next_value_seed(TypedSeed {
                        op: &this.plan.ops[index],
                    })?;
                    this.values[index] = value;
                }
                None => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(assemble_row(this.stream, &this.plan.slots, this.values))
    }
}

struct FlatTopSeed<'a> {
    plan: &'a FlatRowPlan,
    stream: &'a Arc<str>,
}

impl<'de, 'a> DeserializeSeed<'de> for FlatTopSeed<'a> {
    type Value = Vec<Tuple>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Vec<Tuple>, D::Error> {
        deserializer.deserialize_any(TopVisitor {
            plan: self.plan,
            stream: self.stream,
        })
    }
}

struct TopVisitor<'a> {
    plan: &'a FlatRowPlan,
    stream: &'a Arc<str>,
}

impl<'de, 'a> Visitor<'de> for TopVisitor<'a> {
    type Value = Vec<Tuple>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object or array")
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Vec<Tuple>, A::Error> {
        Ok(vec![RowVisitor {
            plan: self.plan,
            stream: self.stream,
            values: vec![Value::Null; self.plan.slots.len()],
        }
        .visit_map(map)?])
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<Tuple>, A::Error> {
        let mut rows = Vec::new();
        while let Some(row) = seq.next_element_seed(RowSeed {
            plan: self.plan,
            stream: self.stream,
        })? {
            rows.push(row);
        }
        Ok(rows)
    }
}

/// Decode one payload through a pre-derived flat-row plan: no DOM, no
/// second conversion pass. Rows stream straight into slot-ordered values.
pub(crate) fn decode_flat_rows(
    plan: &FlatRowPlan,
    stream: &Arc<str>,
    payload: &[u8],
) -> Result<RecordBatch, CodecError> {
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let rows = FlatTopSeed { plan, stream }
        .deserialize(&mut deserializer)
        .map_err(CodecError::Json)?;
    deserializer.end().map_err(CodecError::Json)?;
    if rows.is_empty() {
        return Ok(RecordBatch::empty());
    }
    Ok(RecordBatch::new(rows)?)
}

/// Decoder for file source stream payloads.
pub struct FileLineDecoder {
    stream_name: Arc<str>,
    schema_keys: Arc<[Arc<str>]>,
}

impl FileLineDecoder {
    pub fn new(stream_name: impl Into<String>, schema: Arc<Schema>) -> Result<Self, CodecError> {
        validate_file_line_schema(&schema)?;
        let stream_name: Arc<str> = Arc::<str>::from(stream_name.into());
        let schema_keys: Arc<[Arc<str>]> = Arc::from(
            schema
                .column_schemas()
                .iter()
                .map(|col| Arc::<str>::from(col.name.as_str()))
                .collect::<Vec<_>>(),
        );
        Ok(Self {
            stream_name,
            schema_keys,
        })
    }
}

impl RecordDecoder for FileLineDecoder {
    fn supports_inline_execution(&self) -> bool {
        true
    }
    fn decode_with_projection(
        &self,
        payload: &[u8],
        projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        let (filename, line) = parse_file_line_frame(payload)?;
        let filename = String::from_utf8(filename.to_vec())?;
        let line = String::from_utf8(line.to_vec())?;

        let keys = projection
            .and_then(|projection| projection.output_slots())
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.schema_keys));
        let mut values = Vec::with_capacity(keys.len());
        for key in keys.iter() {
            let value = match key.as_ref() {
                "line" => Value::String(line.clone().into()),
                "filename" => Value::String(filename.clone().into()),
                _ => Value::Null,
            };
            values.push(value);
        }

        let message = Arc::new(Message::new_shared_keys(
            Arc::clone(&self.stream_name),
            keys,
            values,
        ));
        Ok(RecordBatch::new(vec![Tuple::new(vec![message])])?)
    }
}

fn validate_file_line_schema(schema: &Schema) -> Result<(), CodecError> {
    let columns = schema.column_schemas();
    let expected = [
        ColumnSchema::new(
            String::new(),
            "line".to_string(),
            ConcreteDatatype::String(StringType),
        ),
        ColumnSchema::new(
            String::new(),
            "filename".to_string(),
            ConcreteDatatype::String(StringType),
        ),
    ];
    if columns.len() != expected.len() {
        return Err(CodecError::Other(
            "file_line decoder requires schema columns `line string, filename string`".to_string(),
        ));
    }
    for (column, expected) in columns.iter().zip(expected.iter()) {
        if column.name != expected.name || column.data_type != expected.data_type {
            return Err(CodecError::Other(
                "file_line decoder requires schema columns `line string, filename string`"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn parse_file_line_frame(payload: &[u8]) -> Result<(&[u8], &[u8]), CodecError> {
    if payload.len() < FILE_LINE_FRAME_FILENAME_LEN_BYTES {
        return Err(CodecError::Other(
            "file_line payload frame is shorter than filename length header".to_string(),
        ));
    }
    let mut len_bytes = [0_u8; FILE_LINE_FRAME_FILENAME_LEN_BYTES];
    len_bytes.copy_from_slice(&payload[..FILE_LINE_FRAME_FILENAME_LEN_BYTES]);
    let filename_len = u32::from_le_bytes(len_bytes) as usize;
    let filename_start = FILE_LINE_FRAME_FILENAME_LEN_BYTES;
    let filename_end = filename_start + filename_len;
    if payload.len() < filename_end {
        return Err(CodecError::Other(
            "file_line payload frame has truncated filename".to_string(),
        ));
    }
    Ok((
        &payload[filename_start..filename_end],
        &payload[filename_end..],
    ))
}

struct PreparedJsonDecoder {
    decoder: JsonDecoder,
    projection: Option<DecodeProjection>,
    flat_plan: Option<Arc<FlatRowPlan>>,
}

impl PreparedRecordDecoder for PreparedJsonDecoder {
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError> {
        if let Some(plan) = &self.flat_plan {
            return decode_flat_rows(plan, &self.decoder.stream_name, payload);
        }
        self.decoder.decode_value_with_decode_projection(
            serde_json::from_slice(payload)?,
            self.projection.as_ref(),
        )
    }
}

impl RecordDecoder for JsonDecoder {
    fn prepare(
        &self,
        projection: Option<&DecodeProjection>,
    ) -> Option<Arc<dyn PreparedRecordDecoder>> {
        let flat_plan = match projection {
            None => self.flat_plan.clone(),
            Some(_) if self.flat_plan.is_some() => {
                plan_flat_row(&self.schema, projection, &self.schema_keys).map(Arc::new)
            }
            Some(_) => None,
        };
        Some(Arc::new(PreparedJsonDecoder {
            decoder: self.clone(),
            projection: projection.cloned(),
            flat_plan,
        }))
    }

    fn supports_inline_execution(&self) -> bool {
        true
    }
    fn decode_with_projection(
        &self,
        payload: &[u8],
        projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        if let Some(default_plan) = &self.flat_plan {
            if projection.is_none() {
                return decode_flat_rows(default_plan, &self.stream_name, payload);
            }
            if let Some(plan) = plan_flat_row(&self.schema, projection, &self.schema_keys) {
                return decode_flat_rows(&plan, &self.stream_name, payload);
            }
        }
        let json = serde_json::from_slice(payload)?;
        self.decode_value_with_decode_projection(json, projection)
    }
}

fn json_to_value(value: &JsonValue) -> Value {
    if let Some(value) = JsonScalar::from_json(value) {
        return ScalarConversion::Native.convert(value);
    }
    match value {
        JsonValue::Array(items) => {
            let converted: Vec<Value> = items.iter().map(json_to_value).collect();
            let element_type = converted
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            Value::List(ListValue::new(converted, Arc::new(element_type)))
        }
        JsonValue::Object(map) => {
            let mut fields = Vec::with_capacity(map.len());
            let mut values = Vec::with_capacity(map.len());

            for (key, val) in map {
                let converted = json_to_value(val);
                let datatype = converted.datatype();
                fields.push(StructField::new(key.clone(), datatype, true));
                values.push(converted);
            }

            Value::Struct(StructValue::new(values, StructType::new(Arc::new(fields))))
        }
        _ => Value::Null,
    }
}

fn json_to_value_with_datatype(value: &JsonValue, datatype: &ConcreteDatatype) -> Value {
    json_to_value_with_datatype_and_projection(value, datatype, None)
}

fn json_to_value_with_datatype_and_projection(
    value: &JsonValue,
    datatype: &ConcreteDatatype,
    projection: Option<&ProjectionNode>,
) -> Value {
    if let Some(conversion) = ScalarConversion::for_datatype(datatype) {
        return conversion.convert_json(value);
    }
    match datatype {
        ConcreteDatatype::List(list_type) => {
            json_to_list_value_with_datatype_and_projection(value, list_type, projection)
        }
        ConcreteDatatype::Struct(struct_type) => {
            json_to_struct_value_with_datatype_and_projection(value, struct_type, projection)
        }
        _ => Value::Null,
    }
}

fn json_to_list_value_with_datatype(value: &JsonValue, list_type: &ListType) -> Value {
    let JsonValue::Array(items) = value else {
        return Value::Null;
    };

    let element_type = list_type.item_type();
    let converted: Vec<Value> = items
        .iter()
        .map(|item| json_to_value_with_datatype(item, element_type))
        .collect();
    Value::List(ListValue::new(converted, Arc::new(element_type.clone())))
}

fn json_to_list_value_with_datatype_and_projection(
    value: &JsonValue,
    list_type: &ListType,
    projection: Option<&ProjectionNode>,
) -> Value {
    let JsonValue::Array(items) = value else {
        return Value::Null;
    };

    let element_type = list_type.item_type();
    match projection {
        Some(ProjectionNode::List { indexes, element }) => match indexes {
            crate::planner::decode_projection::ListIndexSelection::Indexes(required) => {
                let mut converted = Vec::with_capacity(items.len());
                for (idx, item) in items.iter().enumerate() {
                    if required.contains(&idx) {
                        converted.push(json_to_value_with_datatype_and_projection(
                            item,
                            element_type,
                            Some(element.as_ref()),
                        ));
                    } else {
                        converted.push(Value::Null);
                    }
                }
                Value::List(ListValue::new(converted, Arc::new(element_type.clone())))
            }
            crate::planner::decode_projection::ListIndexSelection::All => {
                let converted: Vec<Value> = items
                    .iter()
                    .map(|item| {
                        json_to_value_with_datatype_and_projection(
                            item,
                            element_type,
                            Some(element.as_ref()),
                        )
                    })
                    .collect();
                Value::List(ListValue::new(converted, Arc::new(element_type.clone())))
            }
        },
        _ => json_to_list_value_with_datatype(value, list_type),
    }
}

fn json_to_struct_value_with_datatype_and_projection(
    value: &JsonValue,
    struct_type: &StructType,
    projection: Option<&ProjectionNode>,
) -> Value {
    let JsonValue::Object(map) = value else {
        return Value::Null;
    };

    let projection_fields = match projection {
        Some(ProjectionNode::Struct(fields)) => Some(fields),
        _ => None,
    };

    let values: Vec<Value> = struct_type
        .fields()
        .iter()
        .map(|field| {
            let child_proj = projection_fields.and_then(|fields| fields.get(field.name()));
            map.get(field.name())
                .map(|v| {
                    json_to_value_with_datatype_and_projection(v, field.data_type(), child_proj)
                })
                .unwrap_or(Value::Null)
        })
        .collect();

    Value::Struct(StructValue::new(values, struct_type.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::source::file::encode_file_line_frame;
    use datatypes::{
        BooleanType, BytesType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type,
        Int16Type, Int32Type, Int64Type, Int8Type, Schema, StringType, StructField, StructType,
        TimestampType, TimestampValue, Uint16Type, Uint32Type, Uint64Type, Uint8Type, Value,
    };
    use serde_json::Map as JsonMap;
    use std::path::Path;

    fn decode_one(decoder: &JsonDecoder, payload: &[u8]) -> Tuple {
        let mut rows = decoder.decode(payload).expect("decode batch").into_rows();
        assert_eq!(rows.len(), 1, "expected exactly one decoded tuple");
        rows.pop().expect("one row")
    }

    fn file_line_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "logs".to_string(),
                "line".to_string(),
                ConcreteDatatype::String(StringType),
            ),
            ColumnSchema::new(
                "logs".to_string(),
                "filename".to_string(),
                ConcreteDatatype::String(StringType),
            ),
        ]))
    }

    #[test]
    fn file_line_decoder_decodes_frame() {
        let decoder = FileLineDecoder::new("logs", file_line_schema()).expect("decoder");
        let payload = encode_file_line_frame(Path::new("/tmp/app.log"), b"hello")
            .expect("encode file line frame");

        let tuple = decoder
            .decode(&payload)
            .expect("decode file line frame")
            .into_rows()
            .pop()
            .expect("one tuple");

        assert_eq!(
            tuple.value_by_name("logs", "line"),
            Some(&Value::String("hello".to_string().into()))
        );
        assert_eq!(
            tuple.value_by_name("logs", "filename"),
            Some(&Value::String("app.log".to_string().into()))
        );
    }

    #[test]
    fn file_line_decoder_honors_output_slots() {
        let decoder = FileLineDecoder::new("logs", file_line_schema()).expect("decoder");
        let payload = encode_file_line_frame(Path::new("/tmp/app.log"), b"hello")
            .expect("encode file line frame");
        let projection =
            DecodeProjection::from_top_level_columns_with_version(&["line".to_string()], 1)
                .with_output_slots(Arc::from(vec![Arc::<str>::from("line")]));

        let tuple = decoder
            .decode_with_projection(&payload, Some(&projection))
            .expect("decode file line frame")
            .into_rows()
            .pop()
            .expect("one tuple");

        let entries = tuple.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, ("logs", "line"));
        assert_eq!(entries[0].1, &Value::String("hello".to_string().into()));
    }

    #[test]
    fn file_line_decoder_rejects_malformed_frame() {
        let decoder = FileLineDecoder::new("logs", file_line_schema()).expect("decoder");
        let err = decoder.decode(&[1, 0, 0]).expect_err("malformed frame");
        assert!(err.to_string().contains("shorter than filename length"));
    }

    #[test]
    fn json_decoder_decodes_single_tuple() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "orders".to_string(),
                "amount".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "orders".to_string(),
                "status".to_string(),
                ConcreteDatatype::String(StringType),
            ),
        ]));
        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"{"amount":10,"status":"ok"}"#.as_ref();
        let tuple = decode_one(&decoder, payload);

        let mut columns: Vec<_> = tuple
            .entries()
            .into_iter()
            .map(|((src, col), _)| (src.to_string(), col.to_string()))
            .collect();
        columns.sort();
        assert_eq!(
            columns,
            vec![
                ("orders".to_string(), "amount".to_string()),
                ("orders".to_string(), "status".to_string())
            ]
        );
        assert_eq!(
            tuple.value_by_name("orders", "amount"),
            Some(&Value::Int64(10))
        );
        assert_eq!(
            tuple.value_by_name("orders", "status"),
            Some(&Value::String("ok".to_string().into()))
        );
    }

    #[test]
    fn json_decoder_ignores_unknown_fields() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "orders".to_string(),
                "amount".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "orders".to_string(),
                "status".to_string(),
                ConcreteDatatype::String(StringType),
            ),
        ]));
        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"{"amount":10,"status":"ok","extra":42,"extra2":"ignored"}"#.as_ref();

        let batch = decoder.decode(payload).expect("decode batch");
        let tuples = batch.into_rows();
        assert_eq!(tuples.len(), 1);
        let tuple = &tuples[0];

        let mut columns: Vec<_> = tuple
            .entries()
            .into_iter()
            .map(|((src, col), _)| (src.to_string(), col.to_string()))
            .collect();
        columns.sort();
        assert_eq!(
            columns,
            vec![
                ("orders".to_string(), "amount".to_string()),
                ("orders".to_string(), "status".to_string())
            ]
        );

        assert_eq!(tuple.value_by_name("orders", "extra"), None);
        assert_eq!(tuple.value_by_name("orders", "extra2"), None);
    }

    #[test]
    fn json_decoder_decodes_timestamp_rfc3339_strings() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "events".to_string(),
            "event_time".to_string(),
            ConcreteDatatype::Timestamp(TimestampType),
        )]));
        let decoder = JsonDecoder::new("events", schema, JsonMap::new());
        let tuple = decode_one(
            &decoder,
            br#"{"event_time":"2026-05-08T18:20:30+08:00"}"#.as_ref(),
        );

        assert_eq!(
            tuple.value_by_name("events", "event_time"),
            Some(&Value::Timestamp(
                TimestampValue::parse_rfc3339("2026-05-08T10:20:30Z").expect("valid timestamp")
            ))
        );
    }

    #[test]
    fn json_decoder_rejects_unsupported_timestamp_inputs_as_null() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "events".to_string(),
            "event_time".to_string(),
            ConcreteDatatype::Timestamp(TimestampType),
        )]));
        let decoder = JsonDecoder::new("events", schema, JsonMap::new());

        for payload in [
            br#"{"event_time":1715150000000}"#.as_ref(),
            br#"{"event_time":"2026-05-08 10:20:30"}"#.as_ref(),
        ] {
            let tuple = decode_one(&decoder, payload);
            assert_eq!(
                tuple.value_by_name("events", "event_time"),
                Some(&Value::Null)
            );
        }
    }

    #[test]
    fn json_decoder_decodes_typed_bytes_from_base64_strings() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "frames".to_string(),
            "payload".to_string(),
            ConcreteDatatype::Bytes(BytesType),
        )]));
        let decoder = JsonDecoder::new("frames", schema, JsonMap::new());
        let tuple = decode_one(&decoder, br#"{"payload":"aGVsbG8="}"#);

        assert_eq!(
            tuple.value_by_name("frames", "payload"),
            Some(&Value::Bytes(Bytes::from_static(b"hello").into()))
        );
    }

    #[test]
    fn json_decoder_decodes_invalid_typed_bytes_as_null() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "frames".to_string(),
            "payload".to_string(),
            ConcreteDatatype::Bytes(BytesType),
        )]));
        let decoder = JsonDecoder::new("frames", schema, JsonMap::new());
        let tuple = decode_one(&decoder, br#"{"payload":"not base64!"}"#);

        assert_eq!(tuple.value_by_name("frames", "payload"), Some(&Value::Null));
    }

    #[test]
    fn json_decoder_decodes_multiple_rows() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "amount".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]));
        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"[{"amount":10},{"amount":20}]"#.as_ref();
        let rows = decoder.decode(payload).expect("decode batch").into_rows();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn flat_fast_path_matches_slow_path_on_adversarial_corpus() {
        // Flat schema covering every scalar arm of the conversion table.
        let mut columns = vec![
            ColumnSchema::new(
                "s".to_string(),
                "s".to_string(),
                ConcreteDatatype::String(StringType),
            ),
            ColumnSchema::new(
                "s".to_string(),
                "i".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "s".to_string(),
                "u".to_string(),
                ConcreteDatatype::Uint64(Uint64Type),
            ),
            ColumnSchema::new(
                "s".to_string(),
                "f".to_string(),
                ConcreteDatatype::Float64(Float64Type),
            ),
            ColumnSchema::new(
                "s".to_string(),
                "b".to_string(),
                ConcreteDatatype::Bool(BooleanType),
            ),
            ColumnSchema::new(
                "s".to_string(),
                "ts".to_string(),
                ConcreteDatatype::Timestamp(TimestampType),
            ),
            ColumnSchema::new(
                "s".to_string(),
                "by".to_string(),
                ConcreteDatatype::Bytes(BytesType),
            ),
        ];
        columns.extend(
            [
                ("i8", ConcreteDatatype::Int8(Int8Type)),
                ("i16", ConcreteDatatype::Int16(Int16Type)),
                ("i32", ConcreteDatatype::Int32(Int32Type)),
                ("u8", ConcreteDatatype::Uint8(Uint8Type)),
                ("u16", ConcreteDatatype::Uint16(Uint16Type)),
                ("u32", ConcreteDatatype::Uint32(Uint32Type)),
                ("f32", ConcreteDatatype::Float32(Float32Type)),
            ]
            .into_iter()
            .map(|(name, datatype)| ColumnSchema::new("s".to_string(), name.to_string(), datatype)),
        );
        let schema = Arc::new(Schema::new(columns));
        let decoder = JsonDecoder::new("s", schema, JsonMap::new());
        let subset = DecodeProjection::from_top_level_columns_with_version(
            &["s".to_string(), "i".to_string(), "f".to_string()],
            1,
        )
        .with_output_slots(Arc::from(vec![
            Arc::<str>::from("f"),
            Arc::<str>::from("s"),
            Arc::<str>::from("zzz"),
            Arc::<str>::from("i"),
            Arc::<str>::from("b"),
        ]));

        // (payload, run against subset projection too)
        let corpus: &[(&str, bool)] = &[
            (
                r#"{"s":"a","i":1,"u":2,"f":2.5,"b":true,"ts":"2024-01-01T00:00:00Z","by":"aGk="}"#,
                true,
            ),
            (r#"{}"#, true),
            (r#"{"s":"x"}"#, true),
            (r#"{"f":"offline"}"#, true),
            (
                r#"{"i8":"a","i16":"b","i32":"c","u8":"d","u16":"e","u32":"f","f32":"g","f":"h"}"#,
                true,
            ),
            (r#"{"s":"x","zzz":[1,{"a":2}],"i":3}"#, true),
            // Surprising-but-locked generic conversions: number into a
            // string column yields Int64, bool into a float column yields Bool.
            (
                r#"{"s":5,"i":"x","u":-3,"f":true,"b":0,"ts":123,"by":42}"#,
                true,
            ),
            (
                r#"{"i":9223372036854775807,"u":18446744073709551615}"#,
                true,
            ),
            // Overflow and float-into-int edges.
            (r#"{"i":9223372036854775808,"u":-1}"#, true),
            (r#"{"i":1.0,"u":1e2,"f":5}"#, true),
            (r#"{"s":"héllo wörld \u0041"}"#, true),
            (r#"{"weird\"key":1,"s":"v"}"#, true),
            // Composites into generic (List/Struct values) and strict (Null) columns.
            (r#"{"s":[1,2],"f":{"a":1}}"#, true),
            (r#"{"i":[1,2],"b":{"a":1}}"#, true),
            (r#"{"b":null,"s":null,"i":null}"#, true),
            (r#"{"s":"a","s":"b","i":1,"i":2}"#, true),
            (r#"{"ts":"not-a-date","by":"!!!not-base64!!!"}"#, true),
            (r#"{"ts":123,"by":42}"#, true),
            // Roots: arrays (valid, empty, bad element) and scalars.
            (r#"[{"s":"a"},{"s":"b","i":7}]"#, false),
            (r#"[]"#, false),
            (r#"[{"s":"a"},5]"#, false),
            (r#"[1,2]"#, false),
            (r#"5"#, false),
            (r#""x""#, false),
            (r#"true"#, false),
            (r#"null"#, false),
            // Invalid JSON: same parse error on both paths.
            (r#""#, false),
            (r#"{"#, false),
            (r#"{"a":}"#, false),
            (r#"{"s":"a"} trailing"#, false),
        ];
        for (payload, with_subset) in corpus {
            for projection in [None, with_subset.then_some(&subset)] {
                let raw = serde_json::from_slice::<JsonValue>(payload.as_bytes());
                let slow = raw
                    .map_err(CodecError::Json)
                    .and_then(|json| decoder.decode_value_with_decode_projection(json, projection));
                let plan = plan_flat_row(&decoder.schema, projection, &decoder.schema_keys);
                // Tuple construction stamps SystemTime::now; compare messages only.
                fn messages_debug(batch: RecordBatch) -> String {
                    format!(
                        "{:?}",
                        batch
                            .into_rows()
                            .iter()
                            .map(|tuple| &tuple.messages)
                            .collect::<Vec<_>>()
                    )
                }
                let fast = match plan {
                    Some(plan) => {
                        assert_eq!(
                            plan.slots.len(),
                            if projection.is_some() {
                                5
                            } else {
                                decoder.schema_keys.len()
                            },
                            "corpus must take the fast path with expected width"
                        );
                        decode_flat_rows(&plan, &decoder.stream_name, payload.as_bytes())
                    }
                    None => panic!("corpus must take the fast path: {payload}"),
                };
                match (slow, fast) {
                    (Ok(slow_batch), Ok(fast_batch)) => {
                        let expected = messages_debug(slow_batch);
                        assert_eq!(expected, messages_debug(fast_batch), "payload {payload}");
                        let prepared = decoder.prepare(projection).expect("prepared JSON");
                        assert_eq!(
                            expected,
                            messages_debug(
                                prepared
                                    .decode(payload.as_bytes())
                                    .expect("prepared decode")
                            ),
                            "prepared payload {payload}"
                        );
                    }
                    (Err(slow_err), Err(fast_err)) => {
                        if serde_json::from_slice::<JsonValue>(payload.as_bytes()).is_err() {
                            assert_eq!(
                                slow_err.to_string(),
                                fast_err.to_string(),
                                "parse errors must match: {payload}"
                            );
                        }
                    }
                    (slow, fast) => {
                        panic!("divergent outcomes for {payload}: slow={slow:?} fast={fast:?}")
                    }
                }
            }
        }
    }

    #[test]
    #[ignore = "manual release-mode JSON decode cost comparison"]
    fn json_decode_cost_comparison() {
        use std::hint::black_box;
        use std::time::Instant;
        for width in [8, 64, 256] {
            let schema = Arc::new(Schema::new(
                (0..width)
                    .map(|index| {
                        ColumnSchema::new(
                            "s".into(),
                            format!("c{index}"),
                            ConcreteDatatype::Int64(Int64Type),
                        )
                    })
                    .collect(),
            ));
            let decoder = JsonDecoder::new("s", schema, JsonMap::new());
            let object = JsonValue::Object(
                (0..width)
                    .map(|index| (format!("c{index}"), JsonValue::from(index)))
                    .collect(),
            );
            for selected in [width, 3] {
                let columns: Vec<String> = (0..selected).map(|index| format!("c{index}")).collect();
                let projection = DecodeProjection::from_top_level_columns_with_version(&columns, 1)
                    .with_output_slots(Arc::from(
                        columns
                            .iter()
                            .map(|key| Arc::<str>::from(key.as_str()))
                            .collect::<Vec<_>>(),
                    ));
                let prepared = decoder.prepare(Some(&projection)).expect("prepared");
                for rows in [1, 32] {
                    let payload = serde_json::to_vec(&if rows == 1 {
                        object.clone()
                    } else {
                        JsonValue::Array(vec![object.clone(); rows])
                    })
                    .expect("payload");
                    let run = |mode| -> RecordBatch {
                        match mode {
                            0 => decoder
                                .decode_value_with_decode_projection(
                                    serde_json::from_slice(black_box(&payload)).expect("JSON"),
                                    Some(&projection),
                                )
                                .expect("DOM"),
                            1 => decoder
                                .decode_with_projection(black_box(&payload), Some(&projection))
                                .expect("per-call plan"),
                            _ => prepared
                                .decode(black_box(&payload))
                                .expect("prepared decode"),
                        }
                    };
                    let expected = format!("{:?}", run(0).rows()[0].messages);
                    for mode in [1, 2] {
                        assert_eq!(expected, format!("{:?}", run(mode).rows()[0].messages));
                    }
                    let iterations = 2048 / rows;
                    let mut samples: [Vec<u128>; 3] = std::array::from_fn(|_| Vec::new());
                    // Rotate execution order and report medians to reduce order/noise effects.
                    for round in 0..5 {
                        for offset in 0..3 {
                            let mode = (round + offset) % 3;
                            let start = Instant::now();
                            for _ in 0..iterations {
                                black_box(run(mode));
                            }
                            samples[mode].push(start.elapsed().as_nanos() / iterations as u128);
                        }
                    }
                    let medians = samples.map(|mut values| {
                        values.sort_unstable();
                        values[2]
                    });
                    println!("width={width} selected={selected} rows={rows} ns/payload: DOM={} per_call={} prepared={}",
                        medians[0], medians[1], medians[2]);
                }
            }
        }
    }

    #[test]
    fn flat_fast_path_matches_slow_path_across_wide_projection_changes() {
        for width in [8, 64, 256] {
            let schema = Arc::new(Schema::new(
                (0..width)
                    .map(|index| {
                        ColumnSchema::new(
                            "s".to_string(),
                            format!("c{index}"),
                            ConcreteDatatype::Int64(Int64Type),
                        )
                    })
                    .collect(),
            ));
            let decoder = JsonDecoder::new("s", schema, JsonMap::new());
            let last = format!("c{}", width - 1);
            // Deliberately retain the same version across different projections.
            let first = DecodeProjection::from_top_level_columns_with_version(
                &["c0".to_string(), last.clone()],
                1,
            )
            .with_output_slots(Arc::from(vec![
                Arc::<str>::from(last),
                Arc::<str>::from("c0"),
                Arc::<str>::from("unknown"),
                Arc::<str>::from("c1"),
                Arc::<str>::from("c0"),
            ]));
            let second = DecodeProjection::from_top_level_columns_with_version(
                &["c1".to_string(), "c2".to_string()],
                1,
            )
            .with_output_slots(Arc::from(vec![
                Arc::<str>::from("c2"),
                Arc::<str>::from("c0"),
                Arc::<str>::from("c1"),
            ]));
            let full_width =
                DecodeProjection::from_top_level_columns_with_version(&["c0".to_string()], 2);
            // Reverse field order, omit a known column, and repeat c0 (last wins).
            let fields = (0..width)
                .rev()
                .filter(|index| *index != 1)
                .map(|index| format!(r#""c{index}":{index}"#))
                .chain([r#""extra":99"#.to_string(), r#""c0":-1"#.to_string()])
                .collect::<Vec<_>>()
                .join(",");
            let payload = format!("[{{{fields}}},{{}}]");
            for projection in [None, Some(&first), Some(&second), Some(&full_width), None] {
                let slow = decoder
                    .decode_value_with_decode_projection(
                        serde_json::from_str(&payload).expect("valid JSON"),
                        projection,
                    )
                    .expect("DOM decode");
                let fast = decoder
                    .decode_with_projection(payload.as_bytes(), projection)
                    .expect("streaming decode");
                assert_eq!(fast.rows().len(), slow.rows().len());
                for (fast_row, slow_row) in fast.rows().iter().zip(slow.rows()) {
                    assert_eq!(
                        format!("{:?}", fast_row.messages),
                        format!("{:?}", slow_row.messages),
                        "width={width}, projection={projection:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn json_decoder_keeps_output_slots_when_projection_prunes_columns() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "orders".to_string(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "orders".to_string(),
                "b".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
        ]));
        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let projection =
            DecodeProjection::from_top_level_columns_with_version(&["a".to_string()], 1)
                .with_output_slots(Arc::from(vec![
                    Arc::<str>::from("a"),
                    Arc::<str>::from("b"),
                ]));

        let tuple = decoder
            .decode_with_projection(br#"{"a":10,"b":20}"#, Some(&projection))
            .expect("decode batch")
            .into_rows()
            .into_iter()
            .next()
            .expect("one row");

        let message = tuple.messages.first().expect("one message");
        assert_eq!(
            message
                .entry_by_index(0)
                .map(|(key, value)| (key.as_ref(), value)),
            Some(("a", &Value::Int64(10)))
        );
        assert_eq!(
            message
                .entry_by_index(1)
                .map(|(key, value)| (key.as_ref(), value)),
            Some(("b", &Value::Null))
        );
        assert_eq!(message.entry_by_index(2), None);
    }

    #[test]
    fn json_decoder_respects_struct_schema_fields() {
        let struct_type =
            ConcreteDatatype::Struct(StructType::new(Arc::new(vec![StructField::new(
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                false,
            )])));

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "b".to_string(),
            struct_type,
        )]));

        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"{"b":{"c":10,"d":"ignore"}}"#.as_ref();
        let tuple = decode_one(&decoder, payload);

        let Some(Value::Struct(struct_val)) = tuple.value_by_name("orders", "b") else {
            panic!("expected struct value");
        };
        assert_eq!(struct_val.get_field("c"), Some(&Value::Int64(10)));
        assert_eq!(struct_val.get_field("d"), None);
    }

    #[test]
    fn json_decoder_respects_list_struct_schema_fields() {
        let element_type =
            ConcreteDatatype::Struct(StructType::new(Arc::new(vec![StructField::new(
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                false,
            )])));

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "items".to_string(),
            ConcreteDatatype::List(datatypes::ListType::new(Arc::new(element_type))),
        )]));

        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"{"items":[{"c":10,"d":"ignore"},{"c":20,"d":"ignore2"}]}"#.as_ref();
        let tuple = decode_one(&decoder, payload);

        let Some(Value::List(list_val)) = tuple.value_by_name("orders", "items") else {
            panic!("expected list value");
        };
        assert_eq!(list_val.len(), 2);

        let Some(Value::Struct(first)) = list_val.get(0) else {
            panic!("expected struct element");
        };
        assert_eq!(first.get_field("c"), Some(&Value::Int64(10)));
        assert_eq!(first.get_field("d"), None);
    }

    #[test]
    fn json_decoder_decode_projection_prunes_list_indexes_without_shrinking() {
        use crate::planner::decode_projection::{
            DecodeProjection, FieldPath, FieldPathSegment, ListIndex, ListIndexSelection,
            ProjectionNode,
        };
        use std::collections::{BTreeMap, BTreeSet};

        let element_type =
            ConcreteDatatype::Struct(StructType::new(Arc::new(vec![StructField::new(
                "x".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                false,
            )])));

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "items".to_string(),
            ConcreteDatatype::List(datatypes::ListType::new(Arc::new(element_type))),
        )]));

        let decoder = JsonDecoder::new("orders", Arc::clone(&schema), JsonMap::new());

        let mut indexes = BTreeSet::new();
        indexes.insert(0usize);
        indexes.insert(3usize);
        let mut fields = BTreeMap::new();
        fields.insert("x".to_string(), ProjectionNode::All);
        let element = ProjectionNode::Struct(fields);
        let list_node = ProjectionNode::List {
            indexes: ListIndexSelection::Indexes(indexes),
            element: Box::new(element),
        };
        let mut projection = DecodeProjection::default();
        projection.mark_field_path_used(&FieldPath {
            column: "items".to_string(),
            segments: vec![
                FieldPathSegment::ListIndex(ListIndex::Const(0)),
                FieldPathSegment::StructField("x".to_string()),
            ],
        });
        projection.mark_field_path_used(&FieldPath {
            column: "items".to_string(),
            segments: vec![
                FieldPathSegment::ListIndex(ListIndex::Const(3)),
                FieldPathSegment::StructField("x".to_string()),
            ],
        });
        // Ensure the test uses the same semantics as mark_field_path_used.
        assert_eq!(projection.column("items"), Some(&list_node));

        let payload =
            br#"{"items":[{"x":10,"y":"ignore"},{"x":20},{"x":30},{"x":40},{"x":50}]}"#.as_ref();
        let tuple = decoder
            .decode_with_projection(payload, Some(&projection))
            .expect("decode batch")
            .into_rows()
            .into_iter()
            .next()
            .expect("one row");

        let Some(Value::List(list_val)) = tuple.value_by_name("orders", "items") else {
            panic!("expected list value");
        };
        assert_eq!(list_val.len(), 5);

        let Some(Value::Struct(first)) = list_val.get(0) else {
            panic!("expected struct element at 0");
        };
        assert_eq!(first.get_field("x"), Some(&Value::Int64(10)));

        assert_eq!(list_val.get(1), Some(&Value::Null));
        assert_eq!(list_val.get(2), Some(&Value::Null));

        let Some(Value::Struct(fourth)) = list_val.get(3) else {
            panic!("expected struct element at 3");
        };
        assert_eq!(fourth.get_field("x"), Some(&Value::Int64(40)));

        assert_eq!(list_val.get(4), Some(&Value::Null));
    }
}
