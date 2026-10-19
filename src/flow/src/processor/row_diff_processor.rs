//! Synchronous sink-side row diffs preserving the stable row schema.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::planner::physical::PhysicalRowDiff;
use crate::processor::output_row_accessor::OutputRowAccessor;
use crate::processor::{ProcessorError, ProcessorStats};
use datatypes::Value;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct RowDiffState {
    previous_output_row: Option<Vec<Option<Value>>>,
}

type RowDiffOutput = (Vec<Value>, Arc<[bool]>);

pub(crate) type RowDiffFields = (OutputRowAccessor, Arc<[Arc<str>]>, Arc<[bool]>);

pub(crate) fn row_diff_fields(
    physical_row_diff: &PhysicalRowDiff,
) -> Result<RowDiffFields, ProcessorError> {
    if physical_row_diff.base.children().len() != 1 {
        return Err(ProcessorError::InvalidConfiguration(
            "row diff processor requires exactly one input child".to_string(),
        ));
    }

    let row_accessor =
        OutputRowAccessor::from_output_layout(physical_row_diff.input_layout.as_ref());
    let output_width = row_accessor.width();
    let output_keys = physical_row_diff
        .output_layout
        .columns
        .iter()
        .map(|column| Arc::clone(&column.name))
        .collect::<Vec<_>>()
        .into();
    let tracked_flags = build_tracked_flags(
        output_width,
        physical_row_diff.tracked_column_indexes.as_ref(),
    )?;
    Ok((row_accessor, output_keys, tracked_flags))
}

fn build_tracked_flags(
    column_count: usize,
    tracked_column_indexes: &[usize],
) -> Result<Arc<[bool]>, ProcessorError> {
    let mut flags = vec![false; column_count];
    for &index in tracked_column_indexes {
        if index >= column_count {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "row diff tracked column index {index} out of bounds for output width {column_count}"
            )));
        }
        flags[index] = true;
    }
    Ok(flags.into())
}

fn extract_output_row(
    row_accessor: &OutputRowAccessor,
    tuple: &Tuple,
) -> Result<Vec<Option<Value>>, ProcessorError> {
    Ok(row_accessor.extract_row(tuple)?.into_optional_values())
}

fn build_diff_row(
    tracked_current_values: &[Option<Value>],
    previous_tracked_row: Option<&[Option<Value>]>,
    tracked_flags: &[bool],
    output_column_names: &[Arc<str>],
) -> Result<RowDiffOutput, ProcessorError> {
    let missing_tracked_columns = tracked_current_values
        .iter()
        .enumerate()
        .filter_map(|(idx, current_value)| {
            tracked_flags
                .get(idx)
                .copied()
                .unwrap_or(false)
                .then_some((idx, current_value))
        })
        .filter(|(_, current_value)| current_value.as_ref().is_none())
        .map(|(idx, _)| {
            output_column_names
                .get(idx)
                .map(|name| name.to_string())
                .unwrap_or_else(|| format!("#{idx}"))
        })
        .collect::<Vec<_>>();
    if !missing_tracked_columns.is_empty() {
        return Err(ProcessorError::ProcessingError(format!(
            "row diff processor failed to resolve tracked output columns [{}] from runtime tuple",
            missing_tracked_columns.join(", ")
        )));
    }

    let mut diff_values = Vec::with_capacity(tracked_current_values.len());
    let mut output_mask = Vec::with_capacity(tracked_current_values.len());

    for (idx, current_value) in tracked_current_values.iter().enumerate() {
        let is_tracked = tracked_flags.get(idx).copied().unwrap_or(false);
        if !is_tracked {
            diff_values.push(current_value.as_ref().cloned().unwrap_or(Value::Null));
            output_mask.push(true);
            continue;
        }

        let Some(current_value) = current_value.as_ref() else {
            return Err(ProcessorError::ProcessingError(format!(
                "row diff processor failed to resolve tracked output column {} from runtime tuple",
                output_column_names
                    .get(idx)
                    .map(|name| name.as_ref())
                    .unwrap_or("unknown")
            )));
        };

        let changed = previous_tracked_row
            .and_then(|previous| previous.get(idx))
            .and_then(|previous| previous.as_ref())
            .is_none_or(|previous| previous != current_value);

        if changed {
            diff_values.push(current_value.clone());
            output_mask.push(true);
        } else {
            diff_values.push(Value::Null);
            output_mask.push(false);
        }
    }

    Ok((diff_values, output_mask.into()))
}

fn materialize_diff_tuple(
    base_tuple: &Tuple,
    output_keys: &Arc<[Arc<str>]>,
    diff_values: Vec<Value>,
    output_mask: Arc<[bool]>,
) -> Tuple {
    let msg = Arc::new(Message::new_shared_keys(
        Arc::<str>::from(""),
        Arc::clone(output_keys),
        diff_values,
    ));
    let mut output_tuple = Tuple::with_timestamp(Arc::from(vec![msg]), base_tuple.timestamp);
    output_tuple.set_output_mask_shared(output_mask);
    output_tuple
}

pub(crate) fn apply_row_diff(
    input_collection: Box<dyn Collection>,
    row_accessor: &OutputRowAccessor,
    output_keys: &Arc<[Arc<str>]>,
    tracked_flags: &[bool],
    state: &mut RowDiffState,
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let metadata = input_collection.metadata().clone();
    let input_rows = input_collection.into_rows().map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to materialize row diff input: {err}"))
    })?;
    let mut output_rows = Vec::with_capacity(input_rows.len());

    for tuple in input_rows {
        let current_values = match extract_output_row(row_accessor, &tuple) {
            Ok(current_values) => current_values,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged("row diff processor error", error.to_string());
                }
                continue;
            }
        };
        let (diff_values, output_mask) = match build_diff_row(
            current_values.as_slice(),
            state.previous_output_row.as_deref(),
            tracked_flags,
            output_keys.as_ref(),
        ) {
            Ok(diff) => diff,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged("row diff processor error", error.to_string());
                }
                continue;
            }
        };
        state.previous_output_row = Some(current_values);

        output_rows.push(materialize_diff_tuple(
            &tuple,
            output_keys,
            diff_values,
            output_mask,
        ));
    }

    let output = RecordBatch::new_with_metadata(output_rows, metadata).map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to build row diff output: {err}"))
    })?;
    Ok(Box::new(output))
}

#[cfg(test)]
mod tests {

    use crate::processor::base::*;

    use super::*;

    #[test]
    fn build_diff_row_returns_processing_error_for_missing_tracked_columns() {
        let output_column_names = [Arc::<str>::from("x")];
        let err = build_diff_row(&[None], None, &[true], &output_column_names).unwrap_err();

        match err {
            ProcessorError::ProcessingError(message) => {
                assert_eq!(
                    message,
                    "row diff processor failed to resolve tracked output columns [x] from runtime tuple"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}
