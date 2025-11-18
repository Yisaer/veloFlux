use crate::model::{Collection, RecordBatch};
use std::any::Any;
use std::sync::Arc;

/// Rewrite all tuple source identifiers within the provided collection.
///
/// Attempts to mutate in-place when the collection is a unique `RecordBatch`
/// reference; otherwise falls back to cloning the rows.
pub fn rewrite_collection_sources(
    mut collection: Arc<dyn Collection>,
    source_name: &str,
) -> Arc<dyn Collection> {
    if let Some(batch) = Arc::get_mut(&mut collection).and_then(|col| {
        let any = col as &mut dyn Collection as &mut dyn Any;
        any.downcast_mut::<RecordBatch>()
    }) {
        for tuple in batch.rows_mut() {
            tuple.rewrite_sources(source_name);
        }
        return collection;
    }

    let mut rows = collection.rows().to_vec();
    for tuple in rows.iter_mut() {
        tuple.rewrite_sources(source_name);
    }
    Arc::new(RecordBatch::new(rows).expect("valid rows"))
}
