//! Synchronous projection and output layout application.

use crate::expr::{EvalRowContext, ScalarExpr};
use crate::model::{Collection, RecordBatch, Tuple};
use crate::planner::physical::{PhysicalProject, PhysicalProjectField, PipelineStateUsage};
use crate::processor::pipeline_state_runtime::update_row_hit_state;
use crate::processor::processor_state::ProcessorState;
use crate::processor::{ProcessorError, ProcessorStats};
use std::sync::Arc;

/// Task-facing projection configuration: the compiled fields plus the
/// pipeline-state handles they may update. Built once from the plan node;
/// the raw SQL ASTs inside the fields and the plan topology never cross
/// into tasks separately.
#[derive(Clone, Debug)]
pub(crate) struct ProjectStage {
    pub fields: Arc<[PhysicalProjectField]>,
    pub state: Option<Arc<ProcessorState>>,
    pub usage: PipelineStateUsage,
}

impl ProjectStage {
    pub(crate) fn from_plan(project: &PhysicalProject) -> Self {
        Self {
            fields: Arc::clone(&project.fields),
            state: project.processor_state.clone(),
            usage: project.pipeline_state_usage,
        }
    }
}

/// Apply projection to a collection
pub(crate) fn apply_project(
    input_collection: &dyn Collection,
    project: &ProjectStage,
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let fields = &project.fields;
    let state = project.state.as_deref();
    let state_usage = project.usage;
    let mut projected_rows = Vec::with_capacity(input_collection.num_rows());
    'rows: for tuple in input_collection.rows() {
        let mut projected_tuple =
            Tuple::with_timestamp(Arc::clone(&tuple.messages), tuple.timestamp);
        for field in fields.iter() {
            if matches!(
                field.compiled_expr,
                ScalarExpr::Column(crate::expr::scalar::ColumnRef::ByIndex { .. })
                    | ScalarExpr::Wildcard { .. }
            ) {
                continue;
            }
            let context = EvalRowContext {
                tuple,
                collection_metadata: input_collection.metadata(),
            };
            let value = match field.compiled_expr.eval_with_context(&context) {
                Ok(value) => value,
                Err(error) => {
                    if let Some(stats) = stats {
                        stats.record_error_logged("project processor error", error.to_string());
                    }
                    continue 'rows;
                }
            };
            projected_tuple
                .add_affiliate_column(Arc::new(field.field_name.as_ref().to_string()), value);
        }
        if let Some(state) = state {
            if let Err(error) = update_row_hit_state(state, state_usage, tuple.timestamp) {
                if let Some(stats) = stats {
                    stats.record_error_logged("project processor error", error.to_string());
                }
                continue;
            }
        }
        projected_rows.push(projected_tuple);
    }

    RecordBatch::new_with_metadata_from(projected_rows, input_collection)
        .map(|batch| Box::new(batch) as Box<dyn Collection>)
        .map_err(|error| ProcessorError::ProcessingError(error.to_string()))
}

#[cfg(test)]
mod tests {

    use crate::planner::physical::*;

    use super::*;
    use crate::expr::ProcStateField;
    use crate::model::Message;
    use datatypes::Value;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn empty_tuple_at_ms(ms: u64) -> Tuple {
        Tuple::with_timestamp(
            Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(ms),
        )
    }

    #[test]
    fn empty_projection_drops_upstream_affiliate() {
        let keys = vec![Arc::<str>::from("a")];
        let values = vec![Value::Int64(1)];
        let message = Arc::new(Message::new(Arc::<str>::from("stream"), keys, values));
        let timestamp = SystemTime::now();
        let mut input_tuple = Tuple::with_timestamp(Arc::from(vec![message]), timestamp);
        input_tuple.add_affiliate_column(Arc::new("tmp".to_string()), Value::Int64(999));
        assert!(input_tuple.affiliate().is_some(), "precondition");

        let input = RecordBatch::new(vec![input_tuple]).expect("record batch");
        let empty_stage = ProjectStage {
            fields: Arc::new([]),
            state: None,
            usage: PipelineStateUsage::default(),
        };
        let output = apply_project(&input, &empty_stage, None).expect("projection succeeds");
        let out_rows = output.rows();
        assert_eq!(out_rows.len(), 1);
        assert!(
            out_rows[0].affiliate().is_none(),
            "upstream affiliate should not leak in passthrough mode"
        );
        assert_eq!(out_rows[0].messages().len(), 1);
        assert_eq!(out_rows[0].timestamp, timestamp);
    }

    #[test]
    fn projection_updates_last_hit_time_after_projected_row() {
        let state = Arc::new(ProcessorState::new());
        let input = RecordBatch::new(vec![empty_tuple_at_ms(1000), empty_tuple_at_ms(2000)])
            .expect("record batch");
        let field_name = "last_hit_time_unix_ms()";
        let fields = vec![PhysicalProjectField::new(
            field_name,
            sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new(field_name)),
            ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            },
        )];
        let usage = PipelineStateUsage {
            last_hit_time_unix_ms: true,
            ..PipelineStateUsage::default()
        };
        let stage = ProjectStage {
            fields: fields.into(),
            state: Some(state.clone()),
            usage,
        };

        let output = apply_project(&input, &stage, None).expect("projection should succeed");

        let rows = output.rows();
        assert_eq!(
            rows[0].affiliate().and_then(|row| row.value(field_name)),
            Some(&Value::Int64(0))
        );
        assert_eq!(
            rows[1].affiliate().and_then(|row| row.value(field_name)),
            Some(&Value::Int64(1000))
        );
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 2000);
    }
}
