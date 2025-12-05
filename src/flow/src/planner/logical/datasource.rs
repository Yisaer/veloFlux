use crate::catalog::StreamDecoderConfig;
use crate::planner::logical::BaseLogicalPlan;

#[derive(Debug, Clone)]
pub struct DataSource {
    pub base: BaseLogicalPlan,
    pub source_name: String,
    pub alias: Option<String>,
    pub decoder: StreamDecoderConfig,
}

impl DataSource {
    pub fn new(
        source_name: String,
        alias: Option<String>,
        decoder: StreamDecoderConfig,
        index: i64,
    ) -> Self {
        let base = BaseLogicalPlan::new(vec![], index);
        Self {
            base,
            source_name,
            alias,
            decoder,
        }
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }
}
