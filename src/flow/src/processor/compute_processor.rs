//! Synchronous computed-column evaluation.

use crate::expr::{EvalRowContext, ScalarExpr};
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::PhysicalCompute;
use crate::processor::{ProcessorError, ProcessorStats};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ComputeExpr {
    output_name: Arc<String>,
    field_name: String,
    expr: ScalarExpr,
}

/// Build task-local expression configuration once, shared by standalone and region execution.
pub(crate) fn compute_fields(plan: &PhysicalCompute) -> Vec<ComputeExpr> {
    plan.fields
        .iter()
        .map(|field| ComputeExpr {
            output_name: Arc::new(field.field_name.clone()),
            field_name: field.field_name.clone(),
            expr: field.compiled_expr.clone(),
        })
        .collect()
}

pub(crate) fn apply_compute(
    input_collection: &dyn Collection,
    fields: &[ComputeExpr],
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    if fields.is_empty() {
        return Ok(input_collection.clone_box());
    }

    let mut out_rows = Vec::with_capacity(input_collection.num_rows());
    'rows: for tuple in input_collection.rows() {
        let mut out_tuple = tuple.clone();
        for field in fields {
            // Evaluate against the current tuple so compute fields can reference
            // earlier computed fields in the same node.
            let context = EvalRowContext {
                tuple: &out_tuple,
                collection_metadata: input_collection.metadata(),
            };
            let value = match field.expr.eval_with_context(&context) {
                Ok(value) => value,
                Err(eval_error) => {
                    let message = format!(
                        "Failed to evaluate expression for field '{}': {}",
                        field.field_name, eval_error
                    );
                    if let Some(stats) = stats {
                        stats.record_error_logged("compute processor error", message);
                    }
                    continue 'rows;
                }
            };
            out_tuple.add_affiliate_column(Arc::clone(&field.output_name), value);
        }
        out_rows.push(out_tuple);
    }

    let out_batch =
        RecordBatch::new_with_metadata_from(out_rows, input_collection).map_err(|err| {
            ProcessorError::ProcessingError(format!("Failed to build compute output: {err}"))
        })?;
    Ok(Box::new(out_batch))
}
