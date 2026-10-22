//! Synchronous sampling and merger state.

use crate::processor::{ProcessorError, ProcessorStats, StreamData};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use crate::codec::{Merger, MergerRegistry};
use crate::shared_stream::AppliedDecodeState;
use datatypes::Schema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackerProps {
    pub merger: MergerConfig,
}

/// Sampling strategy for the sampler stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SamplingStrategy {
    /// Keep only the latest value, discard intermediate values (lossy).
    #[default]
    Latest,
    /// Accumulate values and pack using a Merger.
    Packer {
        /// Packer properties.
        props: PackerProps,
    },
}

/// Configuration for a merger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergerConfig {
    /// Type of merger (e.g., "gbf").
    #[serde(rename = "type")]
    pub merger_type: String,
    /// Merger-specific properties.
    #[serde(default)]
    pub props: JsonMap<String, JsonValue>,
}

/// Configuration for the sampler stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplerConfig {
    /// Interval between emissions (e.g., "100ms", "1s").
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Sampling strategy to use.
    #[serde(default)]
    pub strategy: SamplingStrategy,
}

impl SamplerConfig {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            strategy: SamplingStrategy::Latest,
        }
    }

    pub fn with_strategy(mut self, strategy: SamplingStrategy) -> Self {
        self.strategy = strategy;
        self
    }
}

/// Synchronous sampling state shared by standalone and region execution.
pub(crate) struct SamplerStage {
    interval: Duration,
    state: StrategyState,
}
impl SamplerStage {
    pub(crate) fn new(
        config: SamplerConfig,
        registry: Option<Arc<MergerRegistry>>,
        schema: Option<Arc<Schema>>,
        artifact: Option<Arc<dyn Any + Send + Sync>>,
        applied: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
    ) -> Result<Self, ProcessorError> {
        if config.interval.is_zero() {
            return Err(ProcessorError::InvalidConfiguration(
                "sampler interval must be positive".into(),
            ));
        }
        Ok(Self {
            interval: config.interval,
            state: StrategyState::new(config.strategy, registry, schema, artifact, applied)?,
        })
    }

    pub(crate) fn period(&self) -> Duration {
        self.interval
    }
    pub(crate) fn on_tick(&mut self) -> Option<StreamData> {
        self.state.on_tick()
    }
    pub(crate) fn finish(&mut self) -> Option<StreamData> {
        self.state.flush()
    }
    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> [Option<StreamData>; 2] {
        if should_sample_data(&data) {
            if let Err(error) = self.state.on_sampled_data(data) {
                stats.record_error_logged("sampler input error", error.to_string());
            }
            [None, None]
        } else if data.is_terminal() {
            [self.finish(), Some(data)]
        } else {
            [Some(data), None]
        }
    }
}

enum StrategyState {
    Latest {
        latest: Option<StreamData>,
    },
    Packer {
        merger: Box<dyn Merger>,
        /// Shared decode state for projection pushdown when the merger supports
        /// fused decode. `None` falls back to decoding all columns.
        applied_decode_state: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
    },
}

impl StrategyState {
    fn new(
        strategy: SamplingStrategy,
        registry: Option<Arc<MergerRegistry>>,
        schema: Option<Arc<Schema>>,
        schema_artifact: Option<Arc<dyn Any + Send + Sync>>,
        applied_decode_state: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
    ) -> Result<Self, ProcessorError> {
        match strategy {
            SamplingStrategy::Latest => Ok(StrategyState::Latest { latest: None }),
            SamplingStrategy::Packer { props } => {
                let registry = registry.ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(
                        "Packer strategy requires MergerRegistry".to_string(),
                    )
                })?;
                let schema = schema.ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(
                        "Packer strategy requires an output schema".to_string(),
                    )
                })?;
                let merger_instance = registry
                    .instantiate_with_schema_artifact(
                        &props.merger.merger_type,
                        &props.merger.props,
                        schema,
                        schema_artifact,
                    )
                    .map_err(|e| ProcessorError::InvalidConfiguration(e.to_string()))?;
                Ok(StrategyState::Packer {
                    merger: merger_instance,
                    applied_decode_state,
                })
            }
        }
    }

    fn on_sampled_data(&mut self, data: StreamData) -> Result<(), ProcessorError> {
        match self {
            StrategyState::Latest { latest } => {
                *latest = Some(match data {
                    StreamData::BytesBatch(mut payloads) => match payloads.pop() {
                        Some(payload) => StreamData::Bytes(payload),
                        None => return Ok(()),
                    },
                    other => other,
                });
                Ok(())
            }
            StrategyState::Packer { merger, .. } => match data {
                StreamData::Bytes(bytes) => merger
                    .merge(&bytes)
                    .map_err(|e| ProcessorError::ProcessingError(e.to_string())),
                StreamData::BytesBatch(payloads) => {
                    let mut first_error = None;
                    for payload in payloads {
                        if let Err(error) = merger.merge(&payload) {
                            first_error.get_or_insert_with(|| {
                                ProcessorError::ProcessingError(error.to_string())
                            });
                        }
                    }
                    first_error.map_or(Ok(()), Err)
                }
                _ => {
                    tracing::warn!("Packer received non-bytes data: variant mismatch");
                    Ok(())
                }
            },
        }
    }

    fn on_tick(&mut self) -> Option<StreamData> {
        match self {
            StrategyState::Latest { latest } => latest.take(),
            StrategyState::Packer {
                merger,
                applied_decode_state,
            } => {
                if merger.supports_fused_decode() {
                    // Fused path: decode accumulated frames directly to a
                    // collection, skipping the binary re-encode + re-parse.
                    let projection = applied_decode_state
                        .as_ref()
                        .map(|state| Arc::clone(&state.read().decode_projection));
                    match merger.trigger_decoded(projection.as_deref()) {
                        Ok(Some(collection)) => Some(StreamData::collection(collection)),
                        Ok(None) => None,
                        Err(e) => {
                            tracing::error!(error = ?e, "merger trigger_decoded error");
                            None
                        }
                    }
                } else {
                    match merger.trigger() {
                        Ok(Some(bytes)) if !bytes.is_empty() => Some(StreamData::bytes(bytes)),
                        Ok(_) => None,
                        Err(e) => {
                            tracing::error!(error = ?e, "merger trigger error");
                            None
                        }
                    }
                }
            }
        }
    }

    fn flush(&mut self) -> Option<StreamData> {
        self.on_tick()
    }
}

fn should_sample_data(item: &StreamData) -> bool {
    matches!(
        item,
        StreamData::Bytes(_)
            | StreamData::BytesBatch(_)
            | StreamData::Collection(_)
            | StreamData::EncodedDelivery { .. }
    )
}

#[cfg(test)]
mod tests {

    use crate::planner::physical::*;
    use crate::processor::base::*;
    use crate::processor::data_metrics::*;

    use super::*;
    use crate::model::{Message, RecordBatch, Tuple};
    use crate::processor::StreamData;
    use crate::runtime::TaskSpawner;
    use datatypes::Value;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::time::sleep;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn create_test_data(value: i64) -> StreamData {
        let message = Arc::new(Message::new(
            "test",
            vec![Arc::from("value")],
            vec![Value::Int64(value)],
        ));
        let tuple = Tuple::new(vec![message]);
        let batch = RecordBatch::new(vec![tuple]).expect("create batch");
        StreamData::Collection(Box::new(batch))
    }

    #[tokio::test]
    async fn test_sampler_emits_latest_value() {
        let spawner = test_spawner();
        let config = SamplerConfig::new(Duration::from_millis(200));
        let mut resources = crate::processor::region_processor::RegionResources::default();
        resources.samplers.insert(
            0,
            SamplerStage::new(config.clone(), None, None, None, None).unwrap(),
        );
        resources.stage_default_domains =
            DataMetricDomains::NONE.passthrough(DataDomain::Collection);
        let mut sampler = crate::processor::region_processor::RegionProcessor::new(
            "latest_test".into(),
            &[PhysicalPlan::Sampler(PhysicalSampler::new(
                config.interval,
                config.strategy,
                None,
                vec![],
                0,
            ))],
            resources,
            default_channel_capacities(),
        )
        .unwrap();

        let (input_tx, input_rx) = broadcast::channel::<StreamData>(16);
        sampler.add_input(input_rx);

        let mut output_rx = sampler.subscribe_output().unwrap();
        let _handle = sampler.start(&spawner);

        sleep(Duration::from_millis(10)).await;

        // Send values 1, 2, 3 rapidly
        let _ = input_tx.send(create_test_data(1));
        let _ = input_tx.send(create_test_data(2));
        let _ = input_tx.send(create_test_data(3));

        sleep(Duration::from_millis(250)).await;

        let received = output_rx.try_recv();
        assert!(received.is_ok(), "Expected to receive data");

        if let Ok(StreamData::Collection(collection)) = received {
            let rows = collection.rows();
            assert_eq!(rows.len(), 1);
            let message = &rows[0].messages()[0];
            let (_, value) = message.entry_by_index(0).expect("get value");
            assert_eq!(*value, Value::Int64(3));
        } else {
            panic!("Expected Collection data");
        }
    }

    #[test]
    fn test_sampler_config_deserialization_packer() {
        let json = serde_json::json!({
            "interval": "1s",
            "strategy": {
                "type": "packer",
                "props": {
                    "merger": {
                        "type": "can_merger",
                        "props": {
                            "schema": "/etc/can.dbc"
                        }
                    }
                }
            }
        });

        let config: SamplerConfig = serde_json::from_value(json).expect("deserialize config");
        match config.strategy {
            SamplingStrategy::Packer { props } => {
                assert_eq!(props.merger.merger_type, "can_merger");
                assert_eq!(props.merger.props["schema"], "/etc/can.dbc");
            }
            _ => panic!("Expected Packer strategy"),
        }
    }
}

#[cfg(test)]
mod batching_tests {
    use super::*;
    use crate::codec::CodecError;
    use std::sync::{Arc, Mutex};

    struct RecordingMerger {
        attempts: Arc<Mutex<Vec<Vec<u8>>>>,
        successful: Vec<u8>,
    }

    impl Merger for RecordingMerger {
        fn merge(&mut self, data: &[u8]) -> Result<(), CodecError> {
            self.attempts.lock().unwrap().push(data.to_vec());
            if data == b"bad" {
                return Err(CodecError::Other("bad frame".into()));
            }
            self.successful.extend_from_slice(data);
            Ok(())
        }
        fn trigger(&mut self) -> Result<Option<Vec<u8>>, CodecError> {
            Ok(Some(std::mem::take(&mut self.successful)))
        }
    }

    #[test]
    fn latest_keeps_last_payload_and_empty_batch_does_not_clear_it() {
        let mut state = StrategyState::Latest { latest: None };
        state
            .on_sampled_data(StreamData::bytes_batch(vec!["first".into(), "last".into()]))
            .unwrap();
        state
            .on_sampled_data(StreamData::bytes_batch(vec![]))
            .unwrap();
        let Some(StreamData::Bytes(bytes)) = state.on_tick() else {
            panic!("expected latest payload")
        };
        assert_eq!(bytes.as_ref(), b"last");
        assert!(state.on_tick().is_none());
    }

    #[test]
    fn packer_attempts_every_payload_in_order_and_preserves_successful_merges() {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let mut state = StrategyState::Packer {
            merger: Box::new(RecordingMerger {
                attempts: attempts.clone(),
                successful: Vec::new(),
            }),
            applied_decode_state: None,
        };
        assert!(state
            .on_sampled_data(StreamData::bytes_batch(vec![
                "a".into(),
                "bad".into(),
                "c".into()
            ]))
            .is_err());
        assert_eq!(
            *attempts.lock().unwrap(),
            vec![b"a".to_vec(), b"bad".to_vec(), b"c".to_vec()]
        );
        let Some(StreamData::Bytes(bytes)) = state.on_tick() else {
            panic!("expected merged payload")
        };
        assert_eq!(bytes.as_ref(), b"ac");
    }
}
