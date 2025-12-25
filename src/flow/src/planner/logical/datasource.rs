use crate::catalog::EventtimeDefinition;
use crate::catalog::StreamDecoderConfig;
use crate::planner::decode_projection::DecodeProjection;
use crate::planner::logical::BaseLogicalPlan;
use datatypes::Schema;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DataSource {
    pub base: BaseLogicalPlan,
    pub source_name: String,
    pub alias: Option<String>,
    pub decoder: StreamDecoderConfig,
    pub schema: Arc<Schema>,
    pub decode_projection: Option<DecodeProjection>,
    /// For shared sources, this stores the per-pipeline required top-level columns as a
    /// projection view (column name list). The full `schema` is preserved to keep
    /// `ColumnRef::ByIndex` semantics stable.
    pub shared_required_schema: Option<Vec<String>>,
    pub eventtime: Option<EventtimeDefinition>,
}

impl DataSource {
    pub fn new(
        source_name: String,
        alias: Option<String>,
        decoder: StreamDecoderConfig,
        index: i64,
        schema: Arc<Schema>,
        eventtime: Option<EventtimeDefinition>,
    ) -> Self {
        let base = BaseLogicalPlan::new(vec![], index);
        Self {
            base,
            source_name,
            alias,
            decoder,
            schema,
            decode_projection: None,
            shared_required_schema: None,
            eventtime,
        }
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn decode_projection(&self) -> Option<&DecodeProjection> {
        self.decode_projection.as_ref()
    }

    pub fn shared_required_schema(&self) -> Option<&[String]> {
        self.shared_required_schema.as_deref()
    }

    pub fn eventtime(&self) -> Option<&EventtimeDefinition> {
        self.eventtime.as_ref()
    }
}
