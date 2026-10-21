//! Synchronous decoding from StreamData::Bytes into collections.

use crate::codec::{PreparedRecordDecoder, RecordDecoder};
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
    decode_projection: Option<Arc<DecodeProjection>>,
    prepared: Option<Arc<dyn PreparedRecordDecoder>>,
    eventtime: Option<EventtimeDecodeConfig>,
}

impl DecoderStage {
    pub(crate) fn new(decoder: Arc<dyn RecordDecoder>) -> Self {
        let prepared = decoder.prepare(None);
        Self {
            decoder,
            shared_decode_state: None,
            decode_projection: None,
            prepared,
            eventtime: None,
        }
    }
    pub(crate) fn with_shared_decode_state(
        mut self,
        state: Arc<parking_lot::RwLock<AppliedDecodeState>>,
    ) -> Self {
        let projection = Arc::clone(&state.read().decode_projection);
        self.prepared = self.decoder.prepare(Some(&projection));
        self.decode_projection = Some(projection);
        self.shared_decode_state = Some(state);
        self
    }
    pub(crate) fn with_decode_projection(mut self, projection: DecodeProjection) -> Self {
        self.prepared = self.decoder.prepare(Some(&projection));
        self.decode_projection = Some(Arc::new(projection));
        self.shared_decode_state = None;
        self
    }
    pub(crate) fn with_eventtime(mut self, eventtime: EventtimeDecodeConfig) -> Self {
        self.eventtime = Some(eventtime);
        self
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> Option<StreamData> {
        let batch = match data {
            StreamData::Bytes(payload) => {
                if let Some(state) = &self.shared_decode_state {
                    let projection = Arc::clone(&state.read().decode_projection);
                    if !self
                        .decode_projection
                        .as_ref()
                        .is_some_and(|previous| Arc::ptr_eq(previous, &projection))
                    {
                        self.prepared = self.decoder.prepare(Some(&projection));
                        self.decode_projection = Some(projection);
                    }
                }
                let decoded = if let Some(prepared) = &self.prepared {
                    prepared.decode(&payload)
                } else {
                    self.decoder
                        .decode_with_projection(&payload, self.decode_projection.as_deref())
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
    fn prepared_json_follows_projection_snapshots_and_reuses_stable_state() {
        use crate::codec::JsonDecoder;
        use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, StringType};

        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("s".into(), "a".into(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("s".into(), "b".into(), ConcreteDatatype::String(StringType)),
        ]));
        let decoder: Arc<dyn RecordDecoder> =
            Arc::new(JsonDecoder::new("s", schema, Default::default()));
        let stats = ProcessorStats::new("test", "prepared-json", "decoder");
        let payload = br#"{"b":"text","a":7}"#;
        let check = |stage: &mut DecoderStage, projection: Option<&DecodeProjection>| {
            let expected = decoder
                .decode_with_projection(payload, projection)
                .expect("decode");
            let output = stage
                .process(StreamData::bytes(payload.to_vec()), &stats)
                .expect("output");
            let StreamData::Collection(output) = output else {
                panic!("expected collection");
            };
            assert_eq!(
                format!("{:?}", output.rows()[0].messages),
                format!("{:?}", expected.rows()[0].messages),
            );
        };

        let mut full = DecoderStage::new(Arc::clone(&decoder));
        let prepared = full.prepared.clone().expect("prepared JSON");
        for _ in 0..2 {
            check(&mut full, None);
            assert!(Arc::ptr_eq(
                full.prepared.as_ref().expect("prepared"),
                &prepared
            ));
        }

        let first = DecodeProjection::from_top_level_columns_with_version(&["a".into()], 1);
        let mut fixed =
            DecoderStage::new(Arc::clone(&decoder)).with_decode_projection(first.clone());
        let prepared = fixed.prepared.clone().expect("prepared fixed projection");
        for _ in 0..2 {
            check(&mut fixed, Some(&first));
            assert!(Arc::ptr_eq(
                fixed.prepared.as_ref().expect("prepared"),
                &prepared
            ));
        }

        let state = Arc::new(parking_lot::RwLock::new(AppliedDecodeState {
            decoding_columns: vec!["a".into()],
            decode_projection: Arc::new(first.clone()),
            slot_version: 0,
        }));
        let mut shared =
            DecoderStage::new(Arc::clone(&decoder)).with_shared_decode_state(Arc::clone(&state));
        let previous = shared.prepared.clone().expect("prepared initial snapshot");
        check(&mut shared, Some(&first));
        assert!(Arc::ptr_eq(
            shared.prepared.as_ref().expect("prepared"),
            &previous
        ));

        // A different snapshot with the same numeric version must replace the plan.
        let second = DecodeProjection::from_top_level_columns_with_version(&["b".into()], 1)
            .with_output_slots(Arc::from(vec![Arc::<str>::from("b")]));
        state.write().decode_projection = Arc::new(second.clone());
        check(&mut shared, Some(&second));
        let updated = shared.prepared.clone().expect("prepared updated snapshot");
        assert!(!Arc::ptr_eq(&previous, &updated));
        check(&mut shared, Some(&second));
        assert!(Arc::ptr_eq(
            shared.prepared.as_ref().expect("prepared"),
            &updated
        ));
    }

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
