use super::template_transform::JsonTemplateTransform;
use super::{CollectionEncoder, CollectionEncoderStream, EncodeError};
use crate::model::{Collection, Tuple};
use crate::planner::physical::output_schema::{OutputSchema, OutputValueGetter};
use crate::planner::physical::ByIndexProjection;
use crate::planner::sink::{SinkEncoderConfig, SinkEncoderTransformConfig};
use datatypes::Value;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::sync::Arc;

/// Encoder that emits the entire collection as a JSON array of row objects.
pub struct JsonEncoder {
    id: String,
    props: JsonMap<String, JsonValue>,
    omit_null_columns: bool,
    transform: Option<Arc<JsonTemplateTransform>>,
    by_index_projection: Option<Arc<ByIndexProjection>>,
    output_schema: Option<Arc<OutputSchema>>,
}

impl JsonEncoder {
    /// Create a new JSON encoder with the provided identifier.
    pub fn new(id: impl Into<String>, config: &SinkEncoderConfig) -> Result<Self, EncodeError> {
        let id = id.into();
        Ok(Self {
            id,
            omit_null_columns: config.json_omit_null_columns(),
            transform: json_template_transform_from_config(config)?,
            props: config.props().clone(),
            by_index_projection: None,
            output_schema: None,
        })
    }

    /// Access encoder props (currently unused by JSON encoder).
    pub fn props(&self) -> &JsonMap<String, JsonValue> {
        &self.props
    }
}

impl CollectionEncoder for JsonEncoder {
    fn id(&self) -> &str {
        &self.id
    }

    fn encode(&self, collection: &dyn Collection) -> Result<Vec<u8>, EncodeError> {
        if self.transform.is_some() {
            let mut stream = JsonStreamingEncoder::new(
                self.omit_null_columns,
                None,
                self.transform.clone(),
                self.output_schema.clone(),
            );
            stream.append_collection(collection)?;
            return Box::new(stream).finish();
        }

        let rows = collection.rows();
        let payload = if rows.is_empty() {
            serde_json::to_vec(&JsonValue::Array(Vec::new()))
        } else {
            let mut json_rows = Vec::with_capacity(rows.len());
            let options = JsonRowEncodeOptions::native(
                self.by_index_projection.as_deref(),
                self.output_schema.as_deref(),
                if self.omit_null_columns {
                    NullColumnPolicy::OmitNullObjectFields
                } else {
                    NullColumnPolicy::KeepNulls
                },
            );
            for tuple in rows {
                json_rows.push(tuple_to_json_with_options(tuple, options)?);
            }
            serde_json::to_vec(&JsonValue::Array(json_rows))
        }
        .map_err(EncodeError::Serialization)?;
        Ok(payload)
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_index_lazy_materialization(&self) -> bool {
        self.transform.is_none()
    }

    fn with_by_index_projection(
        self: Arc<Self>,
        spec: Arc<ByIndexProjection>,
    ) -> Result<Arc<dyn CollectionEncoder>, EncodeError> {
        if self.transform.is_some() {
            return Err(EncodeError::Other(
                "index lazy materialization is not supported when encoder.transform=template"
                    .to_string(),
            ));
        }
        Ok(Arc::new(Self {
            id: self.id.clone(),
            props: self.props.clone(),
            omit_null_columns: self.omit_null_columns,
            transform: self.transform.clone(),
            by_index_projection: Some(spec),
            output_schema: self.output_schema.clone(),
        }))
    }

    fn with_output_schema(
        self: Arc<Self>,
        output_schema: Arc<OutputSchema>,
    ) -> Result<Arc<dyn CollectionEncoder>, EncodeError> {
        Ok(Arc::new(Self {
            id: self.id.clone(),
            props: self.props.clone(),
            omit_null_columns: self.omit_null_columns,
            transform: self.transform.clone(),
            by_index_projection: self.by_index_projection.clone(),
            output_schema: Some(output_schema),
        }))
    }

    fn start_stream(&self) -> Option<Box<dyn CollectionEncoderStream>> {
        Some(Box::new(JsonStreamingEncoder::new(
            self.omit_null_columns,
            self.by_index_projection.clone(),
            self.transform.clone(),
            self.output_schema.clone(),
        )))
    }
}

struct JsonStreamingEncoder {
    payload: Vec<u8>,
    is_first_row: bool,
    omit_null_columns: bool,
    by_index_projection: Option<Arc<ByIndexProjection>>,
    transform: Option<Arc<JsonTemplateTransform>>,
    output_schema: Option<Arc<OutputSchema>>,
}

impl JsonStreamingEncoder {
    fn new(
        omit_null_columns: bool,
        by_index_projection: Option<Arc<ByIndexProjection>>,
        transform: Option<Arc<JsonTemplateTransform>>,
        output_schema: Option<Arc<OutputSchema>>,
    ) -> Self {
        Self {
            payload: vec![b'['],
            is_first_row: true,
            omit_null_columns,
            by_index_projection,
            transform,
            output_schema,
        }
    }
}

#[derive(Clone, Copy)]
struct JsonRowEncodeOptions<'a> {
    by_index_projection: Option<&'a ByIndexProjection>,
    output_schema: Option<&'a OutputSchema>,
    output_mask_mode: OutputMaskMode,
    null_policy: NullColumnPolicy,
}

impl<'a> JsonRowEncodeOptions<'a> {
    fn native(
        by_index_projection: Option<&'a ByIndexProjection>,
        output_schema: Option<&'a OutputSchema>,
        null_policy: NullColumnPolicy,
    ) -> Self {
        Self {
            by_index_projection,
            output_schema,
            output_mask_mode: OutputMaskMode::HonorMask,
            null_policy,
        }
    }

    fn transform(
        by_index_projection: Option<&'a ByIndexProjection>,
        output_schema: Option<&'a OutputSchema>,
    ) -> Self {
        Self {
            by_index_projection,
            output_schema,
            output_mask_mode: OutputMaskMode::DenseForTemplate,
            null_policy: NullColumnPolicy::KeepNulls,
        }
    }
}

#[derive(Clone, Copy)]
enum OutputMaskMode {
    HonorMask,
    DenseForTemplate,
}

#[derive(Clone, Copy)]
enum NullColumnPolicy {
    KeepNulls,
    OmitNullObjectFields,
}

impl CollectionEncoderStream for JsonStreamingEncoder {
    fn append(&mut self, tuple: &Tuple) -> Result<(), EncodeError> {
        append_tuple_to_payload(
            &mut self.payload,
            &mut self.is_first_row,
            self.omit_null_columns,
            tuple,
            self.by_index_projection.as_deref(),
            self.transform.as_deref(),
            self.output_schema.as_deref(),
        )
    }

    fn finish(self: Box<Self>) -> Result<Vec<u8>, EncodeError> {
        let mut encoder = *self;
        encoder.payload.push(b']');
        Ok(encoder.payload)
    }
}

fn value_to_json(value: &Value, null_policy: NullColumnPolicy) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Float32(v) => number_from_f64(*v as f64),
        Value::Float64(v) => number_from_f64(*v),
        Value::Int8(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int16(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int32(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint8(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint16(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint32(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Struct(struct_value) => {
            let mut map = JsonMap::new();
            let fields = struct_value.fields().fields();
            for (field, item) in fields.iter().zip(struct_value.items().iter()) {
                insert_json_field(&mut map, field.name().to_string(), item, null_policy);
            }
            JsonValue::Object(map)
        }
        Value::List(list) => {
            let values = list
                .items()
                .iter()
                .map(|item| value_to_json(item, null_policy))
                .collect::<Vec<_>>();
            JsonValue::Array(values)
        }
    }
}

fn insert_json_field(
    json_row: &mut JsonMap<String, JsonValue>,
    key: String,
    value: &Value,
    null_policy: NullColumnPolicy,
) {
    if matches!(null_policy, NullColumnPolicy::OmitNullObjectFields) && matches!(value, Value::Null)
    {
        return;
    }

    json_row.insert(key, value_to_json(value, null_policy));
}

fn tuple_to_json_with_options(
    tuple: &Tuple,
    options: JsonRowEncodeOptions<'_>,
) -> Result<JsonValue, EncodeError> {
    if tuple.output_mask().is_some() {
        let output_schema = options.output_schema.ok_or_else(|| {
            let message = match options.output_mask_mode {
                OutputMaskMode::HonorMask => {
                    "output_mask-aware JSON encoding requires the final output schema"
                }
                OutputMaskMode::DenseForTemplate => {
                    "output_mask-aware JSON template encoding requires the final output schema"
                }
            };
            EncodeError::Other(message.to_string())
        })?;
        return encode_row_from_output_schema(tuple, output_schema, options);
    }

    encode_row_from_tuple_layout(tuple, options.by_index_projection, options.null_policy)
}

fn resolve_by_index_output_value<'a>(
    tuple: &'a Tuple,
    by_index_projection: &ByIndexProjection,
    output_name: &str,
) -> Option<&'a Value> {
    by_index_projection.columns().iter().find_map(|column| {
        if column.output_name.as_ref() != output_name {
            return None;
        }
        tuple.messages().iter().find_map(|message| {
            if message.source() != column.source_name.as_ref() {
                return None;
            }
            message.value(column.source_column_display.as_ref())
        })
    })
}

fn encode_row_from_output_schema(
    tuple: &Tuple,
    output_schema: &OutputSchema,
    options: JsonRowEncodeOptions<'_>,
) -> Result<JsonValue, EncodeError> {
    let mut json_row = JsonMap::new();
    match options.output_mask_mode {
        OutputMaskMode::HonorMask => {
            let output_mask = tuple.output_mask().ok_or_else(|| {
                EncodeError::Other(
                    "output_mask is required for mask-aware JSON encoding".to_string(),
                )
            })?;
            if output_mask.len() != output_schema.columns.len() {
                return Err(EncodeError::Other(format!(
                    "output_mask width {} does not match output schema width {}",
                    output_mask.len(),
                    output_schema.columns.len()
                )));
            }

            for (column, selected) in output_schema.columns.iter().zip(output_mask.iter()) {
                if !selected {
                    continue;
                }
                let value = resolve_output_value_for_schema_column(
                    tuple,
                    options.by_index_projection,
                    column.name.as_ref(),
                    &column.getter,
                )?;
                json_row.insert(
                    column.name.as_ref().to_string(),
                    value_to_json(value, options.null_policy),
                );
            }
        }
        OutputMaskMode::DenseForTemplate => {
            // Row-diff branches still expose `.row` as the dense current output row. Until the
            // template contract becomes mask-aware, unchanged tracked columns remain visible as
            // `null` here.
            for column in output_schema.columns.iter() {
                let value = resolve_output_value(tuple, &column.getter).unwrap_or(&Value::Null);
                json_row.insert(
                    column.name.as_ref().to_string(),
                    value_to_json(value, options.null_policy),
                );
            }
        }
    }

    Ok(JsonValue::Object(json_row))
}

fn resolve_output_value<'a>(tuple: &'a Tuple, getter: &OutputValueGetter) -> Option<&'a Value> {
    match getter {
        OutputValueGetter::Affiliate { column_name } => tuple
            .affiliate()
            .and_then(|affiliate| affiliate.value(column_name)),
        OutputValueGetter::MessageByName {
            source_name,
            column_name,
        } => tuple.messages().iter().find_map(|message| {
            if message.source() != source_name.as_ref() {
                return None;
            }
            message.value(column_name)
        }),
    }
}

fn resolve_output_value_for_schema_column<'a>(
    tuple: &'a Tuple,
    by_index_projection: Option<&ByIndexProjection>,
    output_name: &str,
    getter: &OutputValueGetter,
) -> Result<&'a Value, EncodeError> {
    by_index_projection
        .and_then(|projection| resolve_by_index_output_value(tuple, projection, output_name))
        .or_else(|| resolve_output_value(tuple, getter))
        .ok_or_else(|| {
            EncodeError::Other(format!(
                "output schema column `{output_name}` could not be resolved from runtime tuple"
            ))
        })
}

fn encode_row_from_tuple_layout(
    tuple: &Tuple,
    by_index_projection: Option<&ByIndexProjection>,
    null_policy: NullColumnPolicy,
) -> Result<JsonValue, EncodeError> {
    let mut json_row = match by_index_projection {
        Some(by_index_projection) => JsonMap::with_capacity(by_index_projection.columns().len()),
        None => JsonMap::with_capacity(tuple.len()),
    };

    if let Some(by_index_projection) = by_index_projection {
        if let Some(affiliate) = tuple.affiliate() {
            for (key, value) in affiliate.entries() {
                insert_json_field(&mut json_row, key.as_ref().to_string(), value, null_policy);
            }
        }

        for column in by_index_projection.columns().iter() {
            let value = tuple
                .value_by_index(column.source_name.as_ref(), column.column_index)
                .ok_or_else(|| {
                    EncodeError::Other(format!(
                        "by_index_projection column not found: {}#{}",
                        column.source_name.as_ref(),
                        column.column_index
                    ))
                })?;
            insert_json_field(
                &mut json_row,
                column.output_name.as_ref().to_string(),
                value,
                null_policy,
            );
        }
    } else {
        for ((_, column_name), value) in tuple.entries() {
            insert_json_field(&mut json_row, column_name.to_string(), value, null_policy);
        }
    }

    Ok(JsonValue::Object(json_row))
}

fn append_tuple_to_payload(
    payload: &mut Vec<u8>,
    is_first_row: &mut bool,
    omit_null_columns: bool,
    tuple: &Tuple,
    by_index_projection: Option<&ByIndexProjection>,
    transform: Option<&JsonTemplateTransform>,
    output_schema: Option<&OutputSchema>,
) -> Result<(), EncodeError> {
    if *is_first_row {
        *is_first_row = false;
    } else {
        payload.push(b',');
    }

    let options = if transform.is_some() {
        JsonRowEncodeOptions::transform(by_index_projection, output_schema)
    } else {
        JsonRowEncodeOptions::native(
            by_index_projection,
            output_schema,
            if omit_null_columns {
                NullColumnPolicy::OmitNullObjectFields
            } else {
                NullColumnPolicy::KeepNulls
            },
        )
    };
    let row = tuple_to_json_with_options(tuple, options)?;
    if let Some(transform) = transform {
        payload.extend(transform.render_item(row)?);
    } else {
        serde_json::to_writer(payload, &row).map_err(EncodeError::Serialization)?;
    }
    Ok(())
}

fn json_template_transform_from_config(
    config: &SinkEncoderConfig,
) -> Result<Option<Arc<JsonTemplateTransform>>, EncodeError> {
    match config.transform() {
        Some(SinkEncoderTransformConfig::Template { template }) => {
            JsonTemplateTransform::compile(template)
                .map(Arc::new)
                .map(Some)
        }
        None => Ok(None),
    }
}

fn number_from_f64(value: f64) -> JsonValue {
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::batch_from_columns_simple;
    use crate::planner::sink::SinkEncoderConfig;
    use datatypes::{
        ConcreteDatatype, Int64Type, ListType, ListValue, StringType, StructField, StructType,
        StructValue,
    };
    use std::sync::Arc;

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
        let payload = encoder.encode(&batch).expect("encode collection");

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
        let payload = encoder.encode(&batch).expect("encode collection");

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
        let payload = encoder.encode(&batch).expect("encode collection");

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([{ "amount": 10, "status": null }]));
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
        let payload = encoder.encode(&batch).expect("encode collection");

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
    fn json_encoder_streaming() {
        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        assert!(
            encoder.supports_streaming(),
            "json encoder should be streaming"
        );
        let mut stream = encoder.start_stream().expect("stream");

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
        stream.append_collection(&batch1).expect("append batch1");

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
        for tuple in batch2.rows() {
            stream.append(tuple).expect("stream append");
        }

        let payload = stream.finish().expect("stream finish");
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
        let stream = encoder.start_stream().expect("stream");
        let payload = stream.finish().expect("stream finish");

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([]));
    }

    #[test]
    fn json_encoder_transform_renders_each_row() {
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

        let config = SinkEncoderConfig::json_with_transform_template(
            "{\"value\":{{ json(.row.amount) }},\"label\":{{ json(.row.status) }} }",
        );
        let encoder = JsonEncoder::new("json", &config).expect("encoder");
        let payload = encoder.encode(&batch).expect("encode collection");

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"value":10, "label":"ok"},
                {"value":20, "label":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_transform_streaming_renders_each_row() {
        let config = SinkEncoderConfig::json_with_transform_template(
            "{\"value\":{{ json(.row.amount) }},\"label\":{{ json(.row.status) }} }",
        );
        let encoder = JsonEncoder::new("json", &config).expect("encoder");
        let mut stream = encoder.start_stream().expect("stream");

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
        stream.append_collection(&batch1).expect("append batch1");

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
        for tuple in batch2.rows() {
            stream.append(tuple).expect("append row");
        }

        let payload = stream.finish().expect("stream finish");
        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"value":1, "label":"ok"},
                {"value":2, "label":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_rejects_malformed_transform_template() {
        let config = SinkEncoderConfig::json_with_transform_template("{{ json(.row.amount) ");
        let err = match JsonEncoder::new("json", &config) {
            Ok(_) => panic!("invalid template should fail"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid template"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn json_encoder_transform_disables_index_lazy_materialization() {
        let config =
            SinkEncoderConfig::json_with_transform_template("{\"value\":{{ json(.row.amount) }} }");
        let encoder = Arc::new(JsonEncoder::new("json", &config).expect("encoder"));
        assert!(
            !encoder.supports_index_lazy_materialization(),
            "transform-enabled encoder should disable by-index lazy materialization"
        );
        let spec = Arc::new(ByIndexProjection::new(vec![]));
        let err = match encoder.with_by_index_projection(spec) {
            Ok(_) => panic!("should reject by-index"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("index lazy materialization is not supported"),
            "unexpected error: {err}"
        );
    }
}
