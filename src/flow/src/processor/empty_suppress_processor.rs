//! Synchronous suppression of effectively empty collections.

use crate::model::Collection;

pub(crate) fn collection_is_effectively_empty(collection: &dyn Collection) -> bool {
    if collection.num_rows() == 0 {
        return true;
    }

    collection.rows().iter().all(tuple_is_effectively_empty)
}

fn tuple_is_effectively_empty(tuple: &crate::model::Tuple) -> bool {
    match tuple.output_mask() {
        Some(mask) => mask.iter().all(|selected| !selected),
        None => false,
    }
}

#[cfg(test)]
mod tests {

    use super::collection_is_effectively_empty;
    use crate::model::{batch_from_columns_simple, Message, RecordBatch, Tuple};
    use datatypes::Value;
    use std::sync::Arc;

    #[test]
    fn zero_row_collection_is_empty() {
        let batch = RecordBatch::empty();
        assert!(collection_is_effectively_empty(&batch));
    }

    #[test]
    fn output_mask_only_makes_collection_empty() {
        let message = Arc::new(Message::new(
            Arc::<str>::from("stream"),
            vec![Arc::<str>::from("a")],
            vec![Value::Int64(1)],
        ));
        let mut tuple = Tuple::new(vec![message]);
        tuple.set_output_mask(vec![false]);

        let batch = RecordBatch::new(vec![tuple]).expect("batch");
        assert!(collection_is_effectively_empty(&batch));
    }

    #[test]
    fn output_mask_with_selected_field_keeps_collection_non_empty() {
        let message = Arc::new(Message::new(
            Arc::<str>::from("stream"),
            vec![Arc::<str>::from("a"), Arc::<str>::from("b")],
            vec![Value::Int64(1), Value::Int64(2)],
        ));
        let mut tuple = Tuple::new(vec![message]);
        tuple.set_output_mask(vec![false, true]);

        let batch = RecordBatch::new(vec![tuple]).expect("batch");
        assert!(!collection_is_effectively_empty(&batch));
    }

    #[test]
    fn non_empty_collection_without_mask_is_not_empty() {
        let batch = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "a".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("batch");
        assert!(!collection_is_effectively_empty(&batch));
    }
}
