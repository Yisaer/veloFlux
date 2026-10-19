//! Synchronous tuple materialization for collection sinks.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::processor::output_row_accessor::OutputRowAccessor;
use crate::processor::{ProcessorError, ProcessorStats};
use std::sync::Arc;

pub(crate) fn materialize_collection(
    output_source_name: &Arc<str>,
    shared_keys: &Arc<[Arc<str>]>,
    row_accessor: &mut OutputRowAccessor,
    input: &dyn Collection,
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let mut rows = Vec::with_capacity(input.num_rows());
    for tuple in input.rows() {
        let extracted = match row_accessor.extract_row(tuple) {
            Ok(extracted) => extracted,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged(
                        "memory collection materialize processor error",
                        error.to_string(),
                    );
                }
                continue;
            }
        };
        let values = extracted.into_values_with_null_fill();

        let msg = Arc::new(Message::new_shared_keys(
            Arc::clone(output_source_name),
            Arc::clone(shared_keys),
            values,
        ));
        let mut output_tuple = Tuple::with_timestamp(Arc::from(vec![msg]), tuple.timestamp);
        if let Some(mask) = tuple.output_mask_shared() {
            output_tuple.set_output_mask_shared(mask);
        }
        rows.push(output_tuple);
    }

    let batch = RecordBatch::new_with_metadata_from(rows, input)
        .map_err(|err| ProcessorError::ProcessingError(format!("invalid record batch: {err}")))?;
    Ok(Box::new(batch))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::planner::physical::output_layout::{
        OutputColumnLayout, OutputLayout, OutputValueRef,
    };
    use datatypes::{ConcreteDatatype, Int64Type, Value};
    use std::time::{Duration, SystemTime};

    fn build_message(source: &str, keys: &[&str], values: Vec<Value>) -> Arc<Message> {
        let shared_keys: Arc<[Arc<str>]> = Arc::from(
            keys.iter()
                .map(|key| Arc::<str>::from(*key))
                .collect::<Vec<_>>(),
        );
        Arc::new(Message::new_shared_keys(source, shared_keys, values))
    }

    #[test]
    fn materialize_collection_fills_planned_null_columns_and_preserves_mask() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(456);
        let mut tuple = Tuple::with_timestamp(
            Arc::from(vec![build_message("stream", &["a"], vec![Value::Int64(7)])]),
            timestamp,
        );
        tuple.add_affiliate_column(Arc::new("tmp".to_string()), Value::Int64(999));
        tuple.set_output_mask(vec![true, false, true]);

        let input = RecordBatch::new(vec![tuple]).expect("build input collection");
        let output_layout = OutputLayout::new(vec![
            OutputColumnLayout {
                name: Arc::from("a"),
                data_type: ConcreteDatatype::Int64(Int64Type),
                value_ref: OutputValueRef::Message {
                    message_index: 0,
                    value_index: 0,
                },
            },
            OutputColumnLayout {
                name: Arc::from("missing_msg"),
                data_type: ConcreteDatatype::Int64(Int64Type),
                value_ref: OutputValueRef::Null,
            },
            OutputColumnLayout {
                name: Arc::from("missing_aff"),
                data_type: ConcreteDatatype::Int64(Int64Type),
                value_ref: OutputValueRef::Null,
            },
        ]);
        let mut row_accessor = OutputRowAccessor::from_output_layout(&output_layout);
        let shared_keys = row_accessor.column_names();

        let materialized = materialize_collection(
            &Arc::<str>::from(""),
            &shared_keys,
            &mut row_accessor,
            &input,
            None,
        )
        .expect("materialize collection");

        assert_eq!(
            materialized.num_rows(),
            1,
            "materialized row count mismatch"
        );

        let output_tuple = &materialized.rows()[0];
        assert!(
            output_tuple.affiliate().is_none(),
            "materialize should clear affiliate",
        );
        assert_eq!(
            output_tuple.output_mask(),
            Some([true, false, true].as_slice()),
            "output mask should be preserved",
        );
        assert_eq!(
            output_tuple.timestamp, timestamp,
            "timestamp should be preserved",
        );
        assert_eq!(
            output_tuple.messages().len(),
            1,
            "materialize should emit one message",
        );

        let msg = &output_tuple.messages()[0];
        assert_eq!(msg.source(), "", "message source should be empty");
        let entries: Vec<(&str, Value)> = msg
            .entries()
            .map(|(key, value)| (key, value.clone()))
            .collect();
        assert_eq!(
            entries,
            vec![
                ("a", Value::Int64(7)),
                ("missing_msg", Value::Null),
                ("missing_aff", Value::Null),
            ],
            "materialized values mismatch",
        );
    }
}
