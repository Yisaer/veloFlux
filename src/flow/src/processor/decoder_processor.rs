//! Synchronous decoding from StreamData::Bytes into collections.

use crate::codec::RecordDecoder;
use crate::eventtime::{EventtimeParseError, EventtimeTypeParser};
use crate::planner::decode_projection::DecodeProjection;
use crate::processor::{ProcessorStats, StreamData};
use crate::shared_stream::AppliedDecodeState;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Clone)]
pub struct EventtimeDecodeConfig {
    pub source_name: String,
    pub column_name: String,
    pub column_index: usize,
    pub type_key: String,
    pub parser: Arc<dyn EventtimeTypeParser>,
}

/// Shared synchronous decoder and timestamp preparation, independent of transport.
#[derive(Clone)]
pub(crate) struct DecoderStage {
    decoder: Arc<dyn RecordDecoder>,
    shared_decode_state: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
    decode_projection: Option<DecodeProjection>,
    eventtime: Option<EventtimeDecodeConfig>,
}

impl DecoderStage {
    pub(crate) fn new(decoder: Arc<dyn RecordDecoder>) -> Self {
        Self {
            decoder,
            shared_decode_state: None,
            decode_projection: None,
            eventtime: None,
        }
    }
    pub(crate) fn with_shared_decode_state(
        mut self,
        state: Arc<parking_lot::RwLock<AppliedDecodeState>>,
    ) -> Self {
        self.shared_decode_state = Some(state);
        self
    }
    pub(crate) fn with_decode_projection(mut self, projection: DecodeProjection) -> Self {
        self.decode_projection = Some(projection);
        self
    }
    pub(crate) fn with_eventtime(mut self, eventtime: EventtimeDecodeConfig) -> Self {
        self.eventtime = Some(eventtime);
        self
    }

    pub(crate) fn process(&self, data: StreamData, stats: &ProcessorStats) -> Option<StreamData> {
        let batch = match data {
            StreamData::Bytes(payload) => {
                let decoded = if let Some(projection) = &self.decode_projection {
                    self.decoder
                        .decode_with_projection(&payload, Some(projection))
                } else if let Some(state) = &self.shared_decode_state {
                    let projection = Arc::clone(&state.read().decode_projection);
                    self.decoder
                        .decode_with_projection(&payload, Some(&projection))
                } else {
                    self.decoder.decode(&payload)
                };
                match decoded {
                    Ok(batch) => batch,
                    Err(error) => {
                        stats.record_error_logged(
                            "decoder processor error",
                            format!("decode error: {error}"),
                        );
                        return None;
                    }
                }
            }
            StreamData::Collection(collection) if self.eventtime.is_some() => {
                match crate::model::RecordBatch::new(collection.rows().to_vec()) {
                    Ok(batch) => batch,
                    Err(error) => {
                        stats.record_error_logged(
                            "decoder processor error",
                            format!("eventtime batch build error: {error}"),
                        );
                        return None;
                    }
                }
            }
            other => return Some(other),
        };
        let result = apply_eventtime(batch, &self.eventtime);
        if let Some(last) = result.errors.last() {
            stats.record_error_count_logged(
                "decoder processor error",
                result.errors.len() as u64,
                last.clone(),
            );
        }
        result
            .batch
            .map(|batch| StreamData::collection(Box::new(batch)))
    }
}

struct EventtimeApplyResult {
    batch: Option<crate::model::RecordBatch>,
    errors: Vec<String>,
}

fn apply_eventtime(
    batch: crate::model::RecordBatch,
    cfg: &Option<EventtimeDecodeConfig>,
) -> EventtimeApplyResult {
    let Some(cfg) = cfg else {
        return EventtimeApplyResult {
            batch: Some(batch),
            errors: Vec::new(),
        };
    };

    let mut errors = Vec::new();
    let mut rows = batch.into_rows();
    rows.retain_mut(|tuple| match extract_timestamp(tuple, cfg) {
        Ok(ts) => {
            tuple.timestamp = ts;
            true
        }
        Err(err) => {
            errors.push(format!(
                "eventtime parse error: source={}, column={}, type={}, {}",
                cfg.source_name, cfg.column_name, cfg.type_key, err
            ));
            false
        }
    });

    if rows.is_empty() {
        return EventtimeApplyResult {
            batch: None,
            errors,
        };
    }
    match crate::model::RecordBatch::new(rows) {
        Ok(batch) => EventtimeApplyResult {
            batch: Some(batch),
            errors,
        },
        Err(err) => {
            errors.push(format!("eventtime batch build error: {err}"));
            EventtimeApplyResult {
                batch: None,
                errors,
            }
        }
    }
}

fn extract_timestamp(
    tuple: &crate::model::Tuple,
    cfg: &EventtimeDecodeConfig,
) -> Result<SystemTime, EventtimeParseError> {
    let value = tuple
        .value_by_index(cfg.source_name.as_str(), cfg.column_index)
        .ok_or_else(|| {
            EventtimeParseError::new(format!(
                "eventtime column `{}` (index={}) missing in tuple",
                cfg.column_name, cfg.column_index
            ))
        })?;
    cfg.parser.parse(value)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::eventtime::EventtimeTypeRegistry;
    use crate::model::{Message, RecordBatch, Tuple};
    use datatypes::Value;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn apply_eventtime_updates_collection_rows() {
        let registry = EventtimeTypeRegistry::with_builtin_types();
        let parser = registry.resolve("unixtimestamp_ms").expect("parser");
        let message = Message::new(
            Arc::<str>::from("s"),
            vec![Arc::<str>::from("value"), Arc::<str>::from("event_ts")],
            vec![Value::Int64(7), Value::Int64(1_500)],
        );
        let batch = RecordBatch::new(vec![Tuple::new(vec![Arc::new(message)])]).expect("batch");

        let result = apply_eventtime(
            batch,
            &Some(EventtimeDecodeConfig {
                source_name: "s".to_string(),
                column_name: "event_ts".to_string(),
                column_index: 1,
                type_key: "unixtimestamp_ms".to_string(),
                parser,
            }),
        );

        assert!(result.errors.is_empty());
        let batch = result.batch.expect("batch");
        assert_eq!(
            batch.rows()[0].timestamp,
            UNIX_EPOCH + Duration::from_millis(1_500)
        );
    }
}
