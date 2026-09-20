use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use datatypes::Schema;
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
    pub table_name: String,
    pub table_schema: Arc<Schema>,
    pub table_projection: Arc<[String]>,
    pub key_slots: Arc<[LookupJoinKeySlot]>,
    pub table_message_index: usize,
}

impl PhysicalLookupJoin {
    pub fn new(
        table_name: String,
        table_schema: Arc<Schema>,
        table_projection: Vec<String>,
        key_slots: Vec<LookupJoinKeySlot>,
        table_message_index: usize,
        child: Arc<PhysicalPlan>,
        index: i64,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            table_name,
            table_schema,
            table_projection: table_projection.into(),
            key_slots: key_slots.into(),
            table_message_index,
        }
    }
}
