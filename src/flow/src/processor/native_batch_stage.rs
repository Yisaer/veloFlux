//! Shared collection construction; historical batch transport exists only in tests.
use crate::model::{Collection, RecordBatch, Tuple};
use crate::processor::ProcessorError;
pub(crate) struct NativeBatchStage;
impl NativeBatchStage {
    pub(crate) fn collection(rows: Vec<Tuple>) -> Result<Box<dyn Collection>, ProcessorError> {
        RecordBatch::new(rows)
            .map(|batch| Box::new(batch) as Box<dyn Collection>)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }
}
