use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use datatypes::Schema;
use parser::JoinType;
use std::sync::Arc;

/// A resolved lookup key slot used by the physical lookup join.
#[derive(Debug, Clone)]
pub struct LookupJoinKeySlot {
    pub probe_relation: String,
    pub probe_column: String,
    pub probe_message_index: usize,
    pub probe_value_index: usize,
    pub table_relation: String,
    pub table_column: String,
    pub table_value_index: usize,
}

/// Physical stream-to-table lookup join.
#[derive(Debug, Clone)]
pub struct PhysicalLookupJoin {
    pub base: BasePhysicalPlan,
    pub join_type: JoinType,
    pub table_name: String,
    pub table_schema: Arc<Schema>,
    pub table_projection: Arc<[String]>,
    pub key_slots: Arc<[LookupJoinKeySlot]>,
    pub table_message_index: usize,
}

#[derive(Debug, Clone)]
pub struct PhysicalLookupJoinConfig {
    pub join_type: JoinType,
    pub table_name: String,
    pub table_schema: Arc<Schema>,
    pub table_projection: Vec<String>,
    pub key_slots: Vec<LookupJoinKeySlot>,
    pub table_message_index: usize,
}

impl PhysicalLookupJoin {
    pub fn new(config: PhysicalLookupJoinConfig, child: Arc<PhysicalPlan>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            join_type: config.join_type,
            table_name: config.table_name,
            table_schema: config.table_schema,
            table_projection: config.table_projection.into(),
            key_slots: config.key_slots.into(),
            table_message_index: config.table_message_index,
        }
    }
}
