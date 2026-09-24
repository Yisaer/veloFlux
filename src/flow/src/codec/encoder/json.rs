use super::{EncodeError, SinkEncoder, SinkEncoderFactory};
use crate::model::{Collection, Tuple};
use crate::planner::physical::output_layout::{OutputLayout, OutputValueRef};
use crate::planner::sink::{JsonOutputFormat, SinkEncoderConfig};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use datatypes::{StructField, StructType, Value};
use serde::Serialize;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

const MAX_STRUCT_WRITE_PLANS: usize = 64;

/// Encoder that emits row objects using the configured JSON delivery format.
pub struct JsonEncoder {
    id: String,
    props: JsonMap<String, JsonValue>,
    output_format: JsonOutputFormat,
    omit_null_columns: bool,
    write_plan: Option<Arc<JsonWritePlan>>,
}

impl JsonEncoder {
    /// Create a new JSON encoder with the provided identifier.
    pub fn new(id: impl Into<String>, config: &SinkEncoderConfig) -> Result<Self, EncodeError> {
        let id = id.into();
        let omit_null_columns = config
            .json_omit_null_columns()
            .map_err(EncodeError::Other)?;
        let output_format = config.json_output_format().map_err(EncodeError::Other)?;
        Ok(Self {
            id,
            output_format,
            omit_null_columns,
            props: config.props().clone(),
            write_plan: None,
        })
    }

    /// Access encoder props (currently unused by JSON encoder).
    pub fn props(&self) -> &JsonMap<String, JsonValue> {
        &self.props
    }
}

impl SinkEncoderFactory for JsonEncoder {
    fn id(&self) -> &str {
        &self.id
    }

    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
        Ok(Box::new(JsonEncoderRuntime::new(
            self.output_format,
            self.omit_null_columns,
            self.write_plan.clone(),
        )))
    }

    fn with_output_layout(
        self: Arc<Self>,
        output_layout: Arc<OutputLayout>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        Ok(Arc::new(Self {
            id: self.id.clone(),
            props: self.props.clone(),
            output_format: self.output_format,
            omit_null_columns: self.omit_null_columns,
            write_plan: Some(Arc::new(JsonWritePlan::build(&output_layout)?)),
        }))
    }
}

struct JsonEncoderRuntime {
    output_format: JsonOutputFormat,
    is_first_row: bool,
    omit_null_columns: bool,
    write_plan: Option<Arc<JsonWritePlan>>,
    struct_write_plans: JsonStructWritePlanCache,
}

impl JsonEncoderRuntime {
    fn new(
        output_format: JsonOutputFormat,
        omit_null_columns: bool,
        write_plan: Option<Arc<JsonWritePlan>>,
    ) -> Self {
        Self {
            output_format,
            is_first_row: true,
            omit_null_columns,
            write_plan,
            struct_write_plans: JsonStructWritePlanCache::default(),
        }
    }
}

struct JsonWritePlan {
    columns: Vec<JsonOutputField>,
    layout_width: usize,
}

struct JsonOutputField {
    prefix: Vec<u8>,
    slots: Vec<JsonOutputSlot>,
}

struct JsonOutputSlot {
    layout_index: usize,
    value_ref: OutputValueRef,
}

struct JsonStructWritePlan {
    struct_type: StructType,
    fields: Vec<JsonStructOutputField>,
}

struct JsonStructOutputField {
    prefix: Vec<u8>,
    indices: Vec<usize>,
}

#[derive(Default)]
struct JsonStructWritePlanCache {
    plans: HashMap<u64, Vec<Arc<JsonStructWritePlan>>>,
    len: usize,
}

impl JsonStructWritePlanCache {
    fn get_or_build(
        &mut self,
        struct_type: &StructType,
    ) -> Result<Arc<JsonStructWritePlan>, EncodeError> {
        let fields = struct_type.fields();
        // Only names affect field order and serialized key prefixes.
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        fields.len().hash(&mut hasher);
        for field in fields.iter() {
            field.name().hash(&mut hasher);
        }
        let key = hasher.finish();

        if let Some(candidates) = self.plans.get(&key) {
            for plan in candidates {
                let cached_fields = plan.struct_type.fields();
                if fields.len() == cached_fields.len()
                    && fields
                        .iter()
                        .zip(cached_fields.iter())
                        .all(|(left, right)| left.name() == right.name())
                {
                    return Ok(Arc::clone(plan));
                }
            }
        }

        let plan = Arc::new(JsonStructWritePlan::build(struct_type, &fields)?);
        if self.len >= MAX_STRUCT_WRITE_PLANS {
            // Dynamically shaped structs must not grow the runtime cache without bound.
            self.plans.clear();
            self.len = 0;
        }
        self.plans.entry(key).or_default().push(Arc::clone(&plan));
        self.len += 1;
        Ok(plan)
    }
}

impl JsonStructWritePlan {
    fn build(struct_type: &StructType, fields: &[StructField]) -> Result<Self, EncodeError> {
        let mut ordered = (0..fields.len()).collect::<Vec<_>>();
        ordered.sort_unstable_by(|left, right| {
            fields[*left]
                .name()
                .cmp(fields[*right].name())
                .then(left.cmp(right))
        });

        let mut output_fields: Vec<JsonStructOutputField> = Vec::new();
        let mut current_name = None;
        for index in ordered {
            let name = fields[index].name();
            if current_name != Some(name) {
                let mut prefix = serde_json::to_vec(name).map_err(EncodeError::Serialization)?;
                prefix.push(b':');
                output_fields.push(JsonStructOutputField {
                    prefix,
                    indices: Vec::new(),
                });
                current_name = Some(name);
            }
            output_fields
                .last_mut()
                .expect("field was inserted")
                .indices
                .push(index);
        }

        Ok(Self {
            struct_type: struct_type.clone(),
            fields: output_fields,
        })
    }
}

impl JsonWritePlan {
    fn build(layout: &OutputLayout) -> Result<Self, EncodeError> {
        let mut ordered = layout.columns.iter().enumerate().collect::<Vec<_>>();
        // Keep duplicate names in layout order so a mask can select an earlier value.
        ordered.sort_unstable_by(|(left_index, left), (right_index, right)| {
            left.name.cmp(&right.name).then(left_index.cmp(right_index))
        });

        let mut columns: Vec<JsonOutputField> = Vec::new();
        let mut current_name = None;
        for (layout_index, column) in ordered {
            let name = column.name.as_ref();
            if current_name != Some(name) {
                let mut prefix = serde_json::to_vec(name).map_err(EncodeError::Serialization)?;
                prefix.push(b':');
                columns.push(JsonOutputField {
                    prefix,
                    slots: Vec::new(),
                });
                current_name = Some(name);
            }
            columns
                .last_mut()
                .expect("column was inserted")
                .slots
                .push(JsonOutputSlot {
                    layout_index,
                    value_ref: column.value_ref.clone(),
                });
        }

        Ok(Self {
            columns,
            layout_width: layout.columns.len(),
        })
    }
}

impl SinkEncoder for JsonEncoderRuntime {
    fn begin_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        self.is_first_row = true;
        match self.output_format {
            JsonOutputFormat::Array => Ok(Some(bytes::Bytes::from_static(b"["))),
            JsonOutputFormat::Ndjson => Ok(None),
        }
    }

    fn append(&mut self, record: &dyn Collection) -> Result<Option<bytes::Bytes>, EncodeError> {
        if record.num_rows() == 0 {
            return Ok(None);
        }

        let mut chunk = Vec::new();
        for tuple in record.rows() {
            if matches!(self.output_format, JsonOutputFormat::Array) {
                if !self.is_first_row {
                    chunk.push(b',');
                }
                self.is_first_row = false;
            }
            append_tuple_json(
                &mut chunk,
                tuple,
                self.omit_null_columns,
                self.write_plan.as_deref(),
                &mut self.struct_write_plans,
            )?;
            if matches!(self.output_format, JsonOutputFormat::Ndjson) {
                chunk.push(b'\n');
            }
        }
        Ok(Some(chunk.into()))
    }

    fn finish_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        match self.output_format {
            JsonOutputFormat::Array => Ok(Some(bytes::Bytes::from_static(b"]"))),
            JsonOutputFormat::Ndjson => Ok(None),
        }
    }

    fn abort_delivery(&mut self) {
        self.is_first_row = true;
    }
}

fn append_tuple_json(
    payload: &mut Vec<u8>,
    tuple: &Tuple,
    omit_null_columns: bool,
    write_plan: Option<&JsonWritePlan>,
    struct_write_plans: &mut JsonStructWritePlanCache,
) -> Result<(), EncodeError> {
    let write_plan = write_plan.ok_or_else(|| {
        EncodeError::Other("JSON encoding requires the final output layout".to_string())
    })?;
    let output_mask = tuple.output_mask();
    if output_mask.is_some_and(|mask| mask.len() != write_plan.layout_width) {
        return Err(EncodeError::Other(format!(
            "output_mask width {} does not match output schema width {}",
            output_mask.map_or(0, <[bool]>::len),
            write_plan.layout_width
        )));
    }
    let keep_nulls = output_mask.is_some() || !omit_null_columns;

    payload.push(b'{');
    let mut first = true;
    for field in &write_plan.columns {
        let mut selected = None;
        for slot in &field.slots {
            if output_mask.is_some_and(|mask| !mask[slot.layout_index]) {
                continue;
            }
            let value = slot.value_ref.resolve(tuple).map_err(EncodeError::Other)?;
            if keep_nulls || !matches!(value, Value::Null) {
                selected = Some(value);
            }
        }
        if let Some(value) = selected {
            if !first {
                payload.push(b',');
            }
            first = false;
            payload.extend_from_slice(&field.prefix);
            write_json_value(payload, value, keep_nulls, struct_write_plans)?;
        }
    }
    payload.push(b'}');
    Ok(())
}

fn write_json_serde<T: Serialize + ?Sized>(
    payload: &mut Vec<u8>,
    value: &T,
) -> Result<(), EncodeError> {
    serde_json::to_writer(payload, value).map_err(EncodeError::Serialization)
}

fn write_json_float(payload: &mut Vec<u8>, value: f64) -> Result<(), EncodeError> {
    if let Some(number) = JsonNumber::from_f64(value) {
        write_json_serde(payload, &number)
    } else {
        payload.extend_from_slice(b"null");
        Ok(())
    }
}

fn write_json_value(
    payload: &mut Vec<u8>,
    value: &Value,
    keep_nulls: bool,
    struct_write_plans: &mut JsonStructWritePlanCache,
) -> Result<(), EncodeError> {
    match value {
        Value::Null => payload.extend_from_slice(b"null"),
        Value::Bool(value) => {
            payload.extend_from_slice(if *value { b"true" } else { b"false" });
        }
        Value::String(value) => write_json_serde(payload, value)?,
        Value::Bytes(value) => {
            let encoded = BASE64_STANDARD.encode(value.as_ref());
            write_json_serde(payload, &encoded)?;
        }
        Value::Float32(value) => write_json_float(payload, *value as f64)?,
        Value::Float64(value) => write_json_float(payload, *value)?,
        Value::Int8(value) => write_json_serde(payload, value)?,
        Value::Int16(value) => write_json_serde(payload, value)?,
        Value::Int32(value) => write_json_serde(payload, value)?,
        Value::Int64(value) => write_json_serde(payload, value)?,
        Value::Uint8(value) => write_json_serde(payload, value)?,
        Value::Uint16(value) => write_json_serde(payload, value)?,
        Value::Uint32(value) => write_json_serde(payload, value)?,
        Value::Uint64(value) => write_json_serde(payload, value)?,
        Value::Timestamp(value) => {
            if let Some(text) = value.to_rfc3339_utc() {
                write_json_serde(payload, &text)?;
            } else {
                payload.extend_from_slice(b"null");
            }
        }
        Value::List(list) => {
            payload.push(b'[');
            for (index, item) in list.items().iter().enumerate() {
                if index != 0 {
                    payload.push(b',');
                }
                write_json_value(payload, item, keep_nulls, struct_write_plans)?;
            }
            payload.push(b']');
        }
        Value::Struct(struct_value) => {
            let struct_type = struct_value.fields();
            let items = struct_value.items();
            let plan = struct_write_plans.get_or_build(struct_type)?;

            payload.push(b'{');
            let mut first = true;
            for field in &plan.fields {
                let mut selected = None;
                for &index in field.indices.iter().rev() {
                    if let Some(item) = items.get(index) {
                        if keep_nulls || !matches!(item, Value::Null) {
                            selected = Some(item);
                            break;
                        }
                    }
                }
                if let Some(item) = selected {
                    if !first {
                        payload.push(b',');
                    }
                    first = false;
                    payload.extend_from_slice(&field.prefix);
                    write_json_value(payload, item, keep_nulls, struct_write_plans)?;
                }
            }
            payload.push(b'}');
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{batch_from_columns_simple, Collection, Message};
    use crate::planner::physical::output_layout::OutputColumnLayout;
    use crate::planner::sink::SinkEncoderConfig;
    use datatypes::{
        ConcreteDatatype, Int64Type, ListType, ListValue, StringType, StructField, StructType,
        StructValue, TimestampValue,
    };
    use std::sync::Arc;

    fn extend_optional(payload: &mut Vec<u8>, chunk: Option<bytes::Bytes>) {
        if let Some(chunk) = chunk {
            payload.extend_from_slice(&chunk);
        }
    }

    fn test_output_layout(collection: &dyn Collection) -> Arc<OutputLayout> {
        let Some(tuple) = collection.rows().first() else {
            return Arc::new(OutputLayout::new(Vec::new()));
        };
        let mut columns = Vec::new();
        for (message_index, message) in tuple.messages().iter().enumerate() {
            columns.extend(
                message
                    .entries()
                    .enumerate()
                    .map(|(value_index, (name, value))| OutputColumnLayout {
                        name: Arc::from(name),
                        data_type: value.datatype(),
                        value_ref: OutputValueRef::Message {
                            message_index,
                            value_index,
                        },
                    }),
            );
        }
        if let Some(affiliate) = tuple.affiliate() {
            columns.extend(affiliate.entries().enumerate().map(
                |(affiliate_index, (name, value))| OutputColumnLayout {
                    name: Arc::clone(name),
                    data_type: value.datatype(),
                    value_ref: OutputValueRef::Affiliate { affiliate_index },
                },
            ));
        }
        Arc::new(OutputLayout::new(columns))
    }

    fn test_runtime(encoder: &JsonEncoder, collection: &dyn Collection) -> JsonEncoderRuntime {
        let layout = test_output_layout(collection);
        JsonEncoderRuntime::new(
            encoder.output_format,
            encoder.omit_null_columns,
            Some(Arc::new(JsonWritePlan::build(&layout).expect("write plan"))),
        )
    }

    fn json_config_with_format(format: &str) -> SinkEncoderConfig {
        let mut props = JsonMap::new();
        props.insert("format".to_string(), JsonValue::String(format.to_string()));
        SinkEncoderConfig::new("json", props)
    }

    fn encode_collection(encoder: &JsonEncoder, collection: &dyn Collection) -> Vec<u8> {
        let mut runtime = test_runtime(encoder, collection);
        let mut payload = Vec::new();
        extend_optional(
            &mut payload,
            runtime.begin_delivery().expect("begin delivery"),
        );
        extend_optional(
            &mut payload,
            runtime.append(collection).expect("append record"),
        );
        extend_optional(
            &mut payload,
            runtime.finish_delivery().expect("finish delivery"),
        );
        payload
    }

    #[test]
    fn json_encoder_emits_single_payload() {
        let batch = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(10), Value::Int64(20)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![
                    Value::String("ok".to_string()),
                    Value::String("fail".to_string()),
                ],
            ),
        ])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"amount":10, "status":"ok"},
                {"amount":20, "status":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_explicit_array_matches_default_bytes() {
        let batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(10), Value::Int64(20)],
        )])
        .expect("valid batch");
        let default_encoder =
            JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("default encoder");
        let array_encoder =
            JsonEncoder::new("json", &json_config_with_format("array")).expect("array encoder");

        let default_payload = encode_collection(&default_encoder, &batch);
        let array_payload = encode_collection(&array_encoder, &batch);

        assert_eq!(default_payload, br#"[{"amount":10},{"amount":20}]"#);
        assert_eq!(array_payload, default_payload);
    }

    #[test]
    fn json_encoder_writes_fields_in_sorted_order() {
        let batch = batch_from_columns_simple(vec![
            ("orders".to_string(), "b".to_string(), vec![Value::Int64(1)]),
            ("orders".to_string(), "a".to_string(), vec![Value::Int64(2)]),
        ])
        .expect("valid batch");
        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");

        assert_eq!(encode_collection(&encoder, &batch), br#"[{"a":2,"b":1}]"#);
    }

    #[test]
    fn json_encoder_duplicate_names_respect_output_mask() {
        let name: Arc<str> = Arc::from("dup");
        let message = Arc::new(Message::new(
            "orders",
            vec![Arc::clone(&name), Arc::clone(&name)],
            vec![Arc::new(Value::Int64(1)), Arc::new(Value::Int64(2))],
        ));
        let mut tuple = Tuple::new(vec![message]);
        let layout = OutputLayout::new(
            (0..2)
                .map(|value_index| OutputColumnLayout {
                    name: Arc::clone(&name),
                    data_type: ConcreteDatatype::Int64(Int64Type),
                    value_ref: OutputValueRef::Message {
                        message_index: 0,
                        value_index,
                    },
                })
                .collect(),
        );
        let plan = JsonWritePlan::build(&layout).expect("write plan");

        tuple.set_output_mask(vec![true, false]);
        let mut payload = Vec::new();
        let mut struct_write_plans = JsonStructWritePlanCache::default();
        append_tuple_json(
            &mut payload,
            &tuple,
            true,
            Some(&plan),
            &mut struct_write_plans,
        )
        .expect("encode first slot");
        assert_eq!(payload, br#"{"dup":1}"#);

        tuple.set_output_mask(vec![true, true]);
        payload.clear();
        append_tuple_json(
            &mut payload,
            &tuple,
            true,
            Some(&plan),
            &mut struct_write_plans,
        )
        .expect("encode last slot");
        assert_eq!(payload, br#"{"dup":2}"#);
    }

    #[test]
    fn json_encoder_ndjson_emits_one_compact_line_per_row() {
        let batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(10), Value::Int64(20)],
        )])
        .expect("valid batch");
        let encoder =
            JsonEncoder::new("json", &json_config_with_format("ndjson")).expect("NDJSON encoder");

        let payload = encode_collection(&encoder, &batch);

        assert_eq!(payload, b"{\"amount\":10}\n{\"amount\":20}\n");
    }

    #[test]
    fn json_encoder_ndjson_streams_across_appends_without_shared_row_state() {
        let batch1 = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("batch1");
        let batch2 = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(2)],
        )])
        .expect("batch2");
        let encoder =
            JsonEncoder::new("json", &json_config_with_format("ndjson")).expect("NDJSON encoder");
        let mut runtime = test_runtime(&encoder, &batch1);
        let mut payload = Vec::new();

        extend_optional(
            &mut payload,
            runtime.begin_delivery().expect("begin delivery"),
        );
        extend_optional(
            &mut payload,
            runtime.append(&batch1).expect("append batch1"),
        );
        extend_optional(
            &mut payload,
            runtime.append(&batch2).expect("append batch2"),
        );
        extend_optional(
            &mut payload,
            runtime.finish_delivery().expect("finish delivery"),
        );

        assert_eq!(payload, b"{\"amount\":1}\n{\"amount\":2}\n");
    }

    #[test]
    fn json_encoder_ndjson_empty_delivery_is_empty() {
        let encoder =
            JsonEncoder::new("json", &json_config_with_format("ndjson")).expect("NDJSON encoder");
        let empty = crate::model::RecordBatch::new(Vec::new()).expect("empty batch");

        assert!(encode_collection(&encoder, &empty).is_empty());
    }

    #[test]
    fn json_encoder_ndjson_escapes_record_delimiters_inside_strings() {
        let batch = batch_from_columns_simple(vec![(
            "events".to_string(),
            "text".to_string(),
            vec![Value::String("雪\n\r".to_string())],
        )])
        .expect("valid batch");
        let encoder =
            JsonEncoder::new("json", &json_config_with_format("ndjson")).expect("NDJSON encoder");

        let payload = encode_collection(&encoder, &batch);

        assert_eq!(payload, "{\"text\":\"雪\\n\\r\"}\n".as_bytes());
        assert!(!payload[..payload.len() - 1].contains(&b'\n'));
        assert!(!payload.contains(&b'\r'));
    }

    #[test]
    fn json_encoder_formats_timestamp_as_rfc3339_utc() {
        let batch = batch_from_columns_simple(vec![(
            "events".to_string(),
            "event_time".to_string(),
            vec![Value::Timestamp(
                TimestampValue::parse_rfc3339("2026-05-08T10:20:30.123456Z")
                    .expect("valid timestamp"),
            )],
        )])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"event_time": "2026-05-08T10:20:30.123456Z"}
            ])
        );
    }

    #[test]
    fn json_encoder_omits_top_level_null_columns_by_default() {
        let batch = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(10)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::Null],
            ),
        ])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([{ "amount": 10 }]));
    }

    #[test]
    fn json_encoder_keeps_top_level_null_columns_when_disabled() {
        let batch = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(10)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::Null],
            ),
        ])
        .expect("valid batch");

        let config = SinkEncoderConfig::json().with_json_omit_null_columns(false);
        let encoder = JsonEncoder::new("json", &config).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([{ "amount": 10, "status": null }]));
    }

    #[test]
    fn json_encoder_emits_bytes_as_base64_strings() {
        let batch = batch_from_columns_simple(vec![(
            "frames".to_string(),
            "payload".to_string(),
            vec![Value::Bytes(bytes::Bytes::from_static(b"hello"))],
        )])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([{ "payload": "aGVsbG8=" }]));
    }

    #[test]
    fn json_encoder_omits_nested_null_object_fields_and_keeps_array_nulls() {
        let nested_type = StructType::new(Arc::new(vec![
            StructField::new("b".to_string(), ConcreteDatatype::Int64(Int64Type), false),
            StructField::new("c".to_string(), ConcreteDatatype::String(StringType), true),
            StructField::new(
                "items".to_string(),
                ConcreteDatatype::List(ListType::new(Arc::new(ConcreteDatatype::Int64(Int64Type)))),
                true,
            ),
        ]));
        let nested_value = Value::Struct(StructValue::new(
            vec![
                Value::Int64(1),
                Value::Null,
                Value::List(ListValue::new(
                    vec![Value::Int64(1), Value::Null, Value::Int64(2)],
                    Arc::new(ConcreteDatatype::Int64(Int64Type)),
                )),
            ],
            nested_type,
        ));
        let batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "payload".to_string(),
            vec![nested_value],
        )])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([{
                "payload": {
                    "b": 1,
                    "items": [1, null, 2]
                }
            }])
        );
    }

    #[test]
    fn json_encoder_rejects_non_boolean_omit_null_columns() {
        let mut props = JsonMap::new();
        props.insert(
            "omit_null_columns".to_string(),
            JsonValue::String("false".to_string()),
        );
        let config = SinkEncoderConfig::new("json", props);

        let err = match JsonEncoder::new("json", &config) {
            Ok(_) => panic!("invalid config should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("omit_null_columns must be a boolean"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn json_encoder_streaming() {
        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let batch1 = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(1)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::String("ok".to_string())],
            ),
        ])
        .expect("batch1");
        let mut runtime = test_runtime(&encoder, &batch1);
        let mut payload = Vec::new();
        extend_optional(
            &mut payload,
            runtime.begin_delivery().expect("begin delivery"),
        );
        if let Some(chunk) = runtime.append(&batch1).expect("append batch1") {
            payload.extend_from_slice(&chunk);
        }

        let batch2 = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(2)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::String("fail".to_string())],
            ),
        ])
        .expect("batch2");
        if let Some(chunk) = runtime.append(&batch2).expect("append batch2") {
            payload.extend_from_slice(&chunk);
        }

        extend_optional(
            &mut payload,
            runtime.finish_delivery().expect("finish delivery"),
        );
        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"amount":1, "status":"ok"},
                {"amount":2, "status":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_streaming_empty_payload_is_array() {
        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let empty = crate::model::RecordBatch::new(Vec::new()).expect("empty batch");
        let mut runtime = test_runtime(&encoder, &empty);
        let mut payload = Vec::new();
        extend_optional(
            &mut payload,
            runtime.begin_delivery().expect("begin delivery"),
        );
        extend_optional(
            &mut payload,
            runtime.finish_delivery().expect("finish delivery"),
        );

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([]));
    }
}
