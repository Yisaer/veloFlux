//! Synchronous change detection for tracked source columns.

use crate::model::Tuple;
use crate::processor::{ProcessorError, ProcessorStats};
use datatypes::Value;

#[derive(Default)]
pub(crate) struct SourceChangeGateState {
    previous_tracked_row: Option<Vec<Option<Value>>>,
}

fn extract_tracked_row(
    tuple: &Tuple,
    source_name: &str,
    tracked_column_indexes: &[usize],
) -> Result<Vec<Option<Value>>, ProcessorError> {
    let message = tuple.message_by_source(source_name).ok_or_else(|| {
        ProcessorError::ProcessingError(format!(
            "source change gate processor failed to resolve source `{source_name}` from runtime tuple"
        ))
    })?;

    tracked_column_indexes
        .iter()
        .map(|&index| {
            message
                .entry_by_index(index)
                .map(|(_, value)| Some(value.clone()))
                .ok_or_else(|| {
                    ProcessorError::ProcessingError(format!(
                        "source change gate processor failed to resolve source `{source_name}` column index {index} from runtime tuple"
                    ))
                })
        })
        .collect()
}

pub(crate) fn gate_collection(
    collection: Box<dyn crate::model::Collection>,
    source_name: &str,
    tracked_column_indexes: &[usize],
    state: &mut SourceChangeGateState,
    stats: Option<&ProcessorStats>,
) -> Result<Option<Box<dyn crate::model::Collection>>, ProcessorError> {
    let num_rows = collection.num_rows();
    if num_rows == 0 {
        return Ok(None);
    }

    let mut selected_indices = Vec::with_capacity(num_rows);
    for (row_index, tuple) in collection.rows().iter().enumerate() {
        let tracked_row = match extract_tracked_row(tuple, source_name, tracked_column_indexes) {
            Ok(tracked_row) => tracked_row,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged(
                        "source change gate processor error",
                        error.to_string(),
                    );
                }
                continue;
            }
        };
        let changed = match state.previous_tracked_row.as_ref() {
            Some(previous) => previous != &tracked_row,
            None => true,
        };

        if changed {
            state.previous_tracked_row = Some(tracked_row);
            selected_indices.push(row_index);
        }
    }

    if selected_indices.is_empty() {
        return Ok(None);
    }

    if selected_indices.len() == num_rows {
        return Ok(Some(collection));
    }

    collection
        .take(&selected_indices)
        .map(Some)
        .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
}
