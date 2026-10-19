//! Synchronous collection source/layout normalization.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::processor::ProcessorError;
use datatypes::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

fn value_by_name_with_index(msg: &Message, expected: &str) -> Option<(usize, Value)> {
    let mut idx = 0;
    while let Some((key, value)) = msg.entry_by_index(idx) {
        if key.as_ref() == expected {
            return Some((idx, value.clone()));
        }
        idx += 1;
    }
    None
}

pub(crate) fn normalize_collection(
    schema_keys: &[Arc<str>],
    output_source_name: &Arc<str>,
    shared_keys: &Arc<[Arc<str>]>,
    input: &dyn Collection,
    processor_id: &str,
) -> Result<Box<dyn Collection>, ProcessorError> {
    if input.is_empty() {
        let batch = RecordBatch::new_with_metadata_from(Vec::new(), input).map_err(|err| {
            ProcessorError::ProcessingError(format!("invalid record batch: {err}"))
        })?;
        return Ok(Box::new(batch));
    }

    let mut key_indices = vec![None; schema_keys.len()];
    let mut saw_non_single_message = false;
    let mut saw_empty_messages = false;

    let mut schema_pos = HashMap::<&str, usize>::with_capacity(schema_keys.len());
    for (idx, key) in schema_keys.iter().enumerate() {
        schema_pos.insert(key.as_ref(), idx);
    }

    if let Some(sample) = input.rows().first() {
        if sample.messages().len() != 1 {
            saw_non_single_message = true;
        }
        if let Some(msg) = sample.messages().first() {
            for (idx, (key, _)) in msg.entries().enumerate() {
                if let Some(pos) = schema_pos.get(key).copied() {
                    key_indices[pos] = Some(idx);
                }
            }
        } else {
            saw_empty_messages = true;
        }
    }

    let mut missing = BTreeSet::<String>::new();
    let mut rows = Vec::with_capacity(input.num_rows());
    for tuple in input.rows() {
        if tuple.messages().len() != 1 {
            saw_non_single_message = true;
        }
        let input_msg = tuple.messages().first();
        if input_msg.is_none() {
            saw_empty_messages = true;
        }

        let mut values = Vec::with_capacity(schema_keys.len());
        for (idx, col_name) in schema_keys.iter().enumerate() {
            let expected = col_name.as_ref();

            let mut value = input_msg.and_then(|msg| {
                if let Some(key_idx) = key_indices.get(idx).and_then(|v| *v) {
                    msg.entry_by_index(key_idx).and_then(|(key, value)| {
                        if key.as_ref() == expected {
                            Some(value.clone())
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            });

            if value.is_none() {
                if let Some(msg) = input_msg {
                    if let Some((found_idx, found)) = value_by_name_with_index(msg, expected) {
                        value = Some(found);
                        if let Some(slot) = key_indices.get_mut(idx) {
                            *slot = Some(found_idx);
                        }
                    }
                }
            }

            if value.is_none() {
                missing.insert(expected.to_string());
            }
            values.push(value.unwrap_or(Value::Null));
        }

        let msg = Arc::new(Message::new_shared_keys(
            Arc::clone(output_source_name),
            Arc::clone(shared_keys),
            values,
        ));
        rows.push(Tuple::with_timestamp(Arc::from(vec![msg]), tuple.timestamp));
    }

    if !missing.is_empty() {
        tracing::warn!(
            processor_id = %processor_id,
            missing_columns = ?missing,
            "collection layout normalize filled NULL for missing columns"
        );
    }
    if saw_non_single_message {
        tracing::warn!(
            processor_id = %processor_id,
            "collection layout normalize expected single-message tuples; extra messages are ignored"
        );
    }
    if saw_empty_messages {
        tracing::warn!(
            processor_id = %processor_id,
            "collection layout normalize received tuples without messages"
        );
    }

    let batch = RecordBatch::new_with_metadata_from(rows, input)
        .map_err(|err| ProcessorError::ProcessingError(format!("invalid record batch: {err}")))?;
    Ok(Box::new(batch))
}

#[cfg(test)]
mod tests {

    use super::*;
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
    fn normalize_collection_rewrites_source_and_stabilizes_schema_order() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(123);
        let input = RecordBatch::new(vec![
            Tuple::with_timestamp(
                Arc::from(vec![build_message(
                    "topic_binding",
                    &["b", "a"],
                    vec![Value::Int64(2), Value::Int64(1)],
                )]),
                timestamp,
            ),
            Tuple::with_timestamp(
                Arc::from(vec![build_message(
                    "topic_binding",
                    &["a"],
                    vec![Value::Int64(100)],
                )]),
                timestamp,
            ),
        ])
        .expect("build input collection");

        let schema_keys = vec![Arc::<str>::from("a"), Arc::<str>::from("b")];
        let shared_keys: Arc<[Arc<str>]> = Arc::from(schema_keys.clone());
        let output_source_name = Arc::<str>::from("stream");

        let normalized = normalize_collection(
            &schema_keys,
            &output_source_name,
            &shared_keys,
            &input,
            "normalize_test",
        )
        .expect("normalize collection");

        assert_eq!(normalized.num_rows(), 2, "normalized row count mismatch");

        let first = &normalized.rows()[0];
        assert!(
            first.affiliate().is_none(),
            "normalize should clear affiliate"
        );
        assert_eq!(first.timestamp, timestamp, "timestamp should be preserved");
        assert_eq!(
            first.messages().len(),
            1,
            "normalize should keep one message"
        );
        let first_msg = &first.messages()[0];
        assert_eq!(
            first_msg.source(),
            "stream",
            "message source should be rewritten to stream binding",
        );
        let first_entries: Vec<(&str, Value)> = first_msg
            .entries()
            .map(|(key, value)| (key, value.clone()))
            .collect();
        assert_eq!(
            first_entries,
            vec![("a", Value::Int64(1)), ("b", Value::Int64(2))],
            "first row should be reordered to schema order",
        );

        let second = &normalized.rows()[1];
        assert!(
            second.affiliate().is_none(),
            "normalize should clear affiliate"
        );
        assert_eq!(second.timestamp, timestamp, "timestamp should be preserved");
        assert_eq!(
            second.messages().len(),
            1,
            "normalize should keep one message"
        );
        let second_msg = &second.messages()[0];
        assert_eq!(
            second_msg.source(),
            "stream",
            "message source should be rewritten to stream binding",
        );
        let second_entries: Vec<(&str, Value)> = second_msg
            .entries()
            .map(|(key, value)| (key, value.clone()))
            .collect();
        assert_eq!(
            second_entries,
            vec![("a", Value::Int64(100)), ("b", Value::Null)],
            "missing columns should be filled with null in schema order",
        );
    }
}
