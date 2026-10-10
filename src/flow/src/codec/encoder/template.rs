//! Row-oriented Upon template encoder.

use super::{EncodeError, SinkEncoder, SinkEncoderFactory};
use crate::model::{Collection, Tuple};
use crate::planner::physical::output_layout::OutputLayout;
use crate::planner::sink::SinkEncoderConfig;
use crate::{PropertyContext, PropertyRegistry};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use datatypes::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

const TEMPLATE_NAME: &str = "row_template";

/// Compiled template source shared by processor-local runtimes.
#[derive(Clone)]
struct CompiledTemplate {
    source: String,
}

/// Factory for row-oriented templates with delivery-level framing.
pub struct TemplateEncoder {
    id: String,
    template: CompiledTemplate,
    begin: Bytes,
    delimiter: Bytes,
    end: Bytes,
    property_registry: PropertyRegistry,
    output_layout: Option<Arc<OutputLayout>>,
}

impl TemplateEncoder {
    /// Create and validate a template encoder factory from sink configuration.
    pub fn new(id: impl Into<String>, config: &SinkEncoderConfig) -> Result<Self, EncodeError> {
        let template = CompiledTemplate {
            source: config
                .template_source()
                .map_err(EncodeError::Other)?
                .to_string(),
        };
        let property_registry = config.property_registry();
        compile_engine(&template, property_registry.snapshot())?;

        Ok(Self {
            id: id.into(),
            template,
            begin: Bytes::from(
                config
                    .template_begin()
                    .map_err(EncodeError::Other)?
                    .as_bytes()
                    .to_vec(),
            ),
            delimiter: Bytes::from(
                config
                    .template_delimiter()
                    .map_err(EncodeError::Other)?
                    .as_bytes()
                    .to_vec(),
            ),
            end: Bytes::from(
                config
                    .template_end()
                    .map_err(EncodeError::Other)?
                    .as_bytes()
                    .to_vec(),
            ),
            property_registry,
            output_layout: None,
        })
    }
}

impl SinkEncoderFactory for TemplateEncoder {
    fn id(&self) -> &str {
        &self.id
    }

    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
        let output_layout = self.output_layout.clone().ok_or_else(|| {
            EncodeError::Other("template encoding requires the final output layout".to_string())
        })?;
        Ok(Box::new(TemplateEncoderRuntime {
            template: self.template.clone(),
            begin: self.begin.clone(),
            delimiter: self.delimiter.clone(),
            end: self.end.clone(),
            property_registry: self.property_registry.clone(),
            output_layout,
            engine: None,
            is_first_item: true,
            property_revision: None,
        }))
    }

    fn with_output_layout(
        self: Arc<Self>,
        output_layout: Arc<OutputLayout>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        Ok(Arc::new(Self {
            id: self.id.clone(),
            template: self.template.clone(),
            begin: self.begin.clone(),
            delimiter: self.delimiter.clone(),
            end: self.end.clone(),
            property_registry: self.property_registry.clone(),
            output_layout: Some(output_layout),
        }))
    }
}

struct TemplateEncoderRuntime {
    template: CompiledTemplate,
    begin: Bytes,
    delimiter: Bytes,
    end: Bytes,
    property_registry: PropertyRegistry,
    output_layout: Arc<OutputLayout>,
    engine: Option<upon::Engine<'static>>,
    is_first_item: bool,
    property_revision: Option<u64>,
}

impl SinkEncoder for TemplateEncoderRuntime {
    fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
        self.is_first_item = true;
        let properties = self.property_registry.snapshot();
        let revision = properties.revision();
        if self.engine.is_none() || self.property_revision != Some(revision) {
            self.engine = Some(compile_engine(&self.template, properties)?);
            self.property_revision = Some(revision);
        }
        Ok(Some(self.begin.clone()))
    }

    fn append(&mut self, record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
        if record.num_rows() == 0 {
            return Ok(None);
        }
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| EncodeError::Other("template delivery has not begun".to_string()))?;
        let mut chunk = Vec::new();
        for tuple in record.rows() {
            let context = tuple_context(tuple, &self.output_layout)?;
            let rendered = engine
                .template(TEMPLATE_NAME)
                .render(context)
                .to_string()
                .map_err(|err| EncodeError::Other(format!("template render error: {err}")))?;
            if !self.is_first_item {
                chunk.extend_from_slice(&self.delimiter);
            }
            chunk.extend_from_slice(rendered.as_bytes());
            self.is_first_item = false;
        }
        Ok(Some(chunk.into()))
    }

    fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
        Ok(Some(self.end.clone()))
    }

    fn abort_delivery(&mut self) {
        self.is_first_item = true;
    }
}

fn compile_engine(
    template: &CompiledTemplate,
    properties: PropertyContext,
) -> Result<upon::Engine<'static>, EncodeError> {
    let (mut engine, _) = crate::template::engine_with_properties(properties);
    engine.add_function("json", |value: &upon::Value| -> String {
        upon_value_to_json(value).to_string()
    });
    engine
        .add_template(TEMPLATE_NAME, template.source.clone())
        .map_err(|err| EncodeError::Other(format!("invalid template: {err}")))?;
    Ok(engine)
}

fn tuple_context(tuple: &Tuple, output_layout: &OutputLayout) -> Result<upon::Value, EncodeError> {
    let mut row = BTreeMap::new();
    for column in output_layout.columns.iter() {
        let value = column
            .value_ref
            .resolve(tuple)
            .map_err(EncodeError::Other)?;
        row.insert(column.name.to_string(), value_to_upon(value)?);
    }
    Ok(upon::Value::Map(row))
}

fn value_to_upon(value: &Value) -> Result<upon::Value, EncodeError> {
    match value {
        Value::Null => Ok(upon::Value::None),
        Value::Bool(value) => Ok(upon::Value::Bool(*value)),
        Value::String(value) => Ok(upon::Value::String(value.as_ref().clone())),
        Value::Bytes(value) => Ok(upon::Value::String(BASE64_STANDARD.encode(value.as_ref()))),
        Value::Float32(value) => Ok(upon::Value::Float(*value as f64)),
        Value::Float64(value) => Ok(upon::Value::Float(*value)),
        Value::Int8(value) => Ok(upon::Value::Integer(i64::from(*value))),
        Value::Int16(value) => Ok(upon::Value::Integer(i64::from(*value))),
        Value::Int32(value) => Ok(upon::Value::Integer(i64::from(*value))),
        Value::Int64(value) => Ok(upon::Value::Integer(*value)),
        Value::Uint8(value) => Ok(upon::Value::Integer(i64::from(*value))),
        Value::Uint16(value) => Ok(upon::Value::Integer(i64::from(*value))),
        Value::Uint32(value) => Ok(upon::Value::Integer(i64::from(*value))),
        Value::Uint64(value) => i64::try_from(*value)
            .map(upon::Value::Integer)
            .map_err(|_| {
                EncodeError::Other(format!(
                    "uint64 value {value} cannot be represented by the template engine"
                ))
            }),
        Value::Timestamp(value) => {
            value
                .to_rfc3339_utc()
                .map(upon::Value::String)
                .ok_or_else(|| {
                    EncodeError::Other(format!(
                        "timestamp value {value:?} cannot be represented by the template engine"
                    ))
                })
        }
        Value::Struct(value) => {
            let fields = value.fields().fields();
            let items = value.items();
            let values = fields
                .iter()
                .zip(items.iter())
                .map(|(field, item)| Ok((field.name().to_string(), value_to_upon(item)?)))
                .collect::<Result<BTreeMap<_, _>, EncodeError>>()?;
            Ok(upon::Value::Map(values))
        }
        Value::List(value) => value
            .items()
            .iter()
            .map(value_to_upon)
            .collect::<Result<Vec<_>, EncodeError>>()
            .map(upon::Value::List),
    }
}

fn upon_value_to_json(value: &upon::Value) -> serde_json::Value {
    match value {
        upon::Value::None => serde_json::Value::Null,
        upon::Value::Bool(value) => serde_json::Value::Bool(*value),
        upon::Value::Integer(value) => serde_json::Value::Number((*value).into()),
        upon::Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        upon::Value::String(value) => serde_json::Value::String(value.clone()),
        upon::Value::List(values) => {
            serde_json::Value::Array(values.iter().map(upon_value_to_json).collect())
        }
        upon::Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), upon_value_to_json(value)))
                .collect(),
        ),
    }
}
