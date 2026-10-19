//! SamplerProcessor - Samples a stream by emitting data based on configurable strategies.
//!
//! This processor is designed for downsampling high-frequency streams at the source level.
//! All pipelines using a stream with a sampler receive the same downsampled data.
//!
//! Strategies:
//! - Latest: Keep only the latest value, emit at fixed intervals (lossy downsampling)
//! - Future: Merge strategy for combining multiple records

use crate::planner::physical::DataDomain;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::data_metrics::{DataMetricDomains, DataMetrics, SAMPLER_METRICS};
use crate::processor::stage_clock::StageClock;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::{future::poll_fn, stream::StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::codec::{Merger, MergerRegistry};
use crate::shared_stream::AppliedDecodeState;
use datatypes::Schema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackerProps {
    pub merger: MergerConfig,
}

/// Sampling strategy for the SamplerProcessor.
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

/// Configuration for the SamplerProcessor.
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

/// Processor that samples a stream based on configurable strategies.
///
/// Currently supports:
/// - Latest: Emits the most recent value at fixed intervals
/// - Packer: Accumulates values and emits packed binary
pub struct SamplerProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    config: SamplerConfig,
    stats: Arc<ProcessorStats>,
    merger_registry: Option<Arc<MergerRegistry>>,
    /// Output schema used to build a fused (decode-capable) Packer merger.
    merger_schema: Option<Arc<Schema>>,
    merger_schema_artifact: Option<Arc<dyn Any + Send + Sync>>,
    /// Shared decode state for projection pushdown into the fused decode path.
    applied_decode_state: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
    metric_domains: DataMetricDomains,
}

impl SamplerProcessor {
    pub fn new(id: impl Into<String>, config: SamplerConfig) -> Self {
        Self::new_with_channel_capacities(id, config, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        config: SamplerConfig,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            config,
            stats: Arc::new(ProcessorStats::default()),
            merger_registry: None,
            merger_schema: None,
            merger_schema_artifact: None,
            applied_decode_state: None,
            metric_domains: DataMetricDomains::NONE.passthrough(DataDomain::Message),
        }
    }

    /// Convenience constructor for Latest strategy with interval.
    pub fn latest(id: impl Into<String>, interval: Duration) -> Self {
        Self::new(id, SamplerConfig::new(interval))
    }

    fn create_stage(&self) -> Result<SamplerStage, ProcessorError> {
        if self.config.interval.is_zero() {
            return Err(ProcessorError::InvalidConfiguration(
                "sampler interval must be positive".into(),
            ));
        }
        Ok(SamplerStage {
            interval: self.config.interval,
            state: StrategyState::new(
                self.config.strategy.clone(),
                self.merger_registry.clone(),
                self.merger_schema.clone(),
                self.merger_schema_artifact.clone(),
                self.applied_decode_state.clone(),
            )?,
        })
    }
    pub(crate) fn into_stage(self) -> Result<SamplerStage, ProcessorError> {
        self.create_stage()
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    pub fn set_merger_registry(&mut self, registry: Arc<MergerRegistry>) {
        self.merger_registry = Some(registry);
    }

    pub(crate) fn set_metric_domains(&mut self, metric_domains: DataMetricDomains) {
        self.metric_domains = metric_domains;
    }

    /// Provide the output schema used to construct a Packer merger (required for
    /// mergers that build an embedded decoder for the fused path).
    pub fn set_merger_schema(&mut self, schema: Arc<Schema>) {
        self.merger_schema = Some(schema);
    }

    pub fn set_merger_schema_artifact(&mut self, artifact: Arc<dyn Any + Send + Sync>) {
        self.merger_schema_artifact = Some(artifact);
    }

    /// Provide shared decode state so the fused decode path can apply the
    /// current column projection.
    pub(crate) fn set_applied_decode_state(
        &mut self,
        state: Arc<parking_lot::RwLock<AppliedDecodeState>>,
    ) {
        self.applied_decode_state = Some(state);
    }
}

impl Processor for SamplerProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let emit_interval = self.config.interval;
        let processor_id = self.id.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        let mut stage = match self.create_stage() {
            Ok(stage) => stage,
            Err(error) => return ProcessorStart::failed(spawner, error),
        };
        let data_metrics = DataMetrics::new(stats.as_ref(), SAMPLER_METRICS, self.metric_domains);

        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            let mut clock = StageClock::new(Some((emit_interval, MissedTickBehavior::Skip)));
            let mut control_active = control_active;
            loop {
                let (outputs, terminal) = tokio::select! {
                    biased;
                    item = control_streams.next(), if control_active => {
                        match item {
                            Some(Ok(signal)) => {
                                let terminal = signal.is_terminal();
                                send_control_with_backpressure(&control_output, channel_capacities.control, signal).await?;
                                if terminal { ([stage.finish(), None], true) } else { continue; }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => { log_broadcast_lagged(&processor_id, n, "control"); continue; }
                            None => { control_active = false; continue; }
                        }
                    }
                    _ = poll_fn(|cx| clock.poll_next(cx)) => ([stage.on_tick(), None], false),
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                data_metrics.record_input(stats.as_ref(), &data)?;
                                let terminal = data.is_terminal();
                                let started = std::time::Instant::now();
                                let outputs = stage.process(data, &stats);
                                stats.record_handle_duration(started.elapsed());
                                (outputs, terminal)
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => { log_broadcast_lagged(&processor_id, n, "data"); continue; }
                            None => ([stage.finish(), None], true),
                        }
                    }
                };
                for data in outputs.into_iter().flatten() {
                    emit_data(&output, channel_capacities.data, &stats, &data_metrics, data).await?;
                }
                if terminal { return Ok(()); }
            }
        }, processor_span)))
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }

    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>,
    {
        self.inputs.push(receiver.into());
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        self.control_inputs.push(receiver.into());
    }
}

/// Synchronous sampling state shared by standalone and region execution.
pub(crate) struct SamplerStage {
    interval: Duration,
    state: StrategyState,
}
impl SamplerStage {
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
                *latest = Some(data);
                Ok(())
            }
            StrategyState::Packer { merger, .. } => match data {
                StreamData::Bytes(bytes) => merger
                    .merge(&bytes)
                    .map_err(|e| ProcessorError::ProcessingError(e.to_string())),
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
        StreamData::Bytes(_) | StreamData::Collection(_) | StreamData::EncodedDelivery { .. }
    )
}

async fn emit_data(
    output: &LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: &Arc<ProcessorStats>,
    data_metrics: &DataMetrics,
    data: StreamData,
) -> Result<(), ProcessorError> {
    let measurement = DataMetrics::measure(&data);
    send_with_backpressure(output, data_channel_capacity, data, Some(stats.as_ref())).await?;
    data_metrics.record_output(stats.as_ref(), measurement)?;
    Ok(())
}

#[cfg(test)]
mod tests {
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
        let mut sampler = SamplerProcessor::latest("latest_test", Duration::from_millis(200));
        sampler.set_metric_domains(DataMetricDomains::NONE.passthrough(DataDomain::Collection));

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
