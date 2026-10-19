//! Synchronous filtering and predicate evaluation.

use crate::expr::ScalarExpr;
use crate::model::Collection;
use crate::model::RecordBatch;
use crate::planner::physical::{PhysicalFilter, PipelineStateUsage};
use crate::processor::pipeline_state_runtime::{update_collection_hit_state, update_row_hit_state};
use crate::processor::processor_state::ProcessorState;
use crate::processor::{ProcessorError, ProcessorStats};
use datatypes::Value;
use std::sync::Arc;

/// Task-facing filter configuration: the compiled predicate plus the
/// pipeline-state handles it may update. Built once from the plan node
/// (or held directly); the raw SQL AST and plan topology never cross
/// into tasks.
#[derive(Clone, Debug)]
pub(crate) struct FilterStage {
    pub expr: ScalarExpr,
    pub state: Option<Arc<ProcessorState>>,
    pub usage: PipelineStateUsage,
}

impl FilterStage {
    pub(crate) fn from_plan(filter: &PhysicalFilter) -> Self {
        Self {
            expr: filter.scalar_predicate.clone(),
            state: filter.processor_state.clone(),
            usage: filter.pipeline_state_usage,
        }
    }
}

/// Apply filter to a collection, optionally tracking pipeline state updates.
///
/// `last_hit_count()` is row-scoped and increments after each accepted row.
/// `last_hit_time_unix_ms()` is row-scoped and updates after each accepted row.
/// `last_agg_hit_count()` is collection-scoped and increments once after a
/// non-empty filtered collection is produced.
pub(crate) fn apply_filter(
    input_collection: &dyn Collection,
    filter: &FilterStage,
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let filter_expr = &filter.expr;
    let state = filter.state.as_deref();
    let state_usage = filter.usage;
    let mut kept = Vec::with_capacity(input_collection.num_rows());
    for tuple in input_collection.rows() {
        let result = match filter_expr.eval_with_tuple(tuple) {
            Ok(result) => result,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged("filter processor error", error.to_string());
                }
                continue;
            }
        };

        match result {
            Value::Bool(true) => {
                if let Some(state) = state {
                    if let Err(error) = update_row_hit_state(state, state_usage, tuple.timestamp) {
                        if let Some(stats) = stats {
                            stats.record_error_logged("filter processor error", error.to_string());
                        }
                        continue;
                    }
                }
                kept.push(tuple.clone());
            }
            Value::Bool(false) => {}
            other => {
                if let Some(stats) = stats {
                    stats.record_error_logged(
                        "filter processor error",
                        format!("Filter expression must return boolean values, got {other:?}"),
                    );
                }
            }
        }
    }

    if !kept.is_empty() {
        if let Some(state) = state {
            update_collection_hit_state(state, state_usage);
        }
    }

    Ok(Box::new(
        RecordBatch::new_with_metadata_from(kept, input_collection)
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?,
    ))
}

#[cfg(test)]
mod tests {

    use crate::planner::physical::*;

    use super::*;
    use crate::expr::func::BinaryFunc;
    use crate::expr::ProcStateField;
    use crate::model::Tuple;
    use datatypes::{ConcreteDatatype, Int64Type};
    use std::sync::atomic::Ordering;
    use std::time::{Duration, UNIX_EPOCH};

    fn tuple_at_ms(ms: u64) -> Tuple {
        Tuple::with_timestamp(
            Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(ms),
        )
    }

    #[test]
    fn filter_updates_last_hit_time_after_accepted_rows() {
        let state = Arc::new(ProcessorState::new());
        let input = RecordBatch::new(vec![
            tuple_at_ms(1000),
            tuple_at_ms(2000),
            tuple_at_ms(3000),
        ])
        .expect("record batch");
        let expr = ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            }),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(1500),
                ConcreteDatatype::Int64(Int64Type),
            )),
        };
        let usage = PipelineStateUsage {
            last_hit_time_unix_ms: true,
            ..PipelineStateUsage::default()
        };
        let stage = FilterStage {
            expr,
            state: Some(state.clone()),
            usage,
        };

        let output = apply_filter(&input, &stage, None).expect("filter should succeed");

        assert_eq!(output.num_rows(), 2);
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 2000);
    }
}
