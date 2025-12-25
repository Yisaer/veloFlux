use crate::catalog::StreamDecoderConfig;
use crate::planner::physical::BasePhysicalPlan;
use crate::planner::physical::PhysicalPlan;
use datatypes::Schema;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalSharedStream {
    pub base: BasePhysicalPlan,
    stream_name: String,
    alias: Option<String>,
    schema: Arc<Schema>,
    required_columns: Vec<String>,
    decoder: StreamDecoderConfig,
    explain_ingest_plan: Option<Arc<PhysicalPlan>>,
}

impl PhysicalSharedStream {
    pub fn new(
        stream_name: String,
        alias: Option<String>,
        schema: Arc<Schema>,
        required_columns: Vec<String>,
        decoder: StreamDecoderConfig,
        explain_ingest_plan: Option<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new_leaf(index);
        Self {
            base,
            stream_name,
            alias,
            schema,
            required_columns,
            decoder,
            explain_ingest_plan,
        }
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn required_columns(&self) -> &[String] {
        &self.required_columns
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }

    pub fn explain_ingest_plan(&self) -> Option<Arc<PhysicalPlan>> {
        self.explain_ingest_plan.as_ref().cloned()
    }
}
