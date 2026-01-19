//! SamplerProcessor - Samples a stream by emitting data based on configurable strategies.
//!
//! This processor is designed for downsampling high-frequency streams at the source level.
//! All pipelines using a stream with a sampler receive the same downsampled data.
//!
//! Strategies:
//! - Latest: Keep only the latest value, emit at fixed intervals (lossy downsampling)
//! - Future: Merge strategy for combining multiple records

use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{interval, MissedTickBehavior};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Sampling strategy for the SamplerProcessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SamplingStrategy {
    /// Keep only the latest value, discard intermediate values (lossy).
    #[default]
    Latest,
    // Future: Merge { merger: String } - merge using custom logic
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
pub struct SamplerProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    config: SamplerConfig,
    stats: Arc<ProcessorStats>,
}

impl SamplerProcessor {
    pub fn new(id: impl Into<String>, config: SamplerConfig) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            config,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    /// Convenience constructor for Latest strategy with interval.
    pub fn latest(id: impl Into<String>, interval: Duration) -> Self {
        Self::new(id, SamplerConfig::new(interval))
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for SamplerProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let emit_interval = self.config.interval;
        let processor_id = self.id.clone();
        let stats = Arc::clone(&self.stats);
        let strategy = self.config.strategy.clone();

        tracing::info!(
            processor_id = %processor_id,
            interval = ?emit_interval,
            strategy = ?strategy,
            "sampler processor starting"
        );

        tokio::spawn(async move {
            match strategy {
                SamplingStrategy::Latest => {
                    Self::run_latest_strategy(
                        processor_id,
                        emit_interval,
                        input_streams,
                        control_streams,
                        control_active,
                        output,
                        control_output,
                        stats,
                    )
                    .await
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}

impl SamplerProcessor {
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    async fn run_latest_strategy(
        processor_id: String,
        emit_interval: Duration,
        mut input_streams: futures::stream::SelectAll<
            tokio_stream::wrappers::BroadcastStream<StreamData>,
        >,
        mut control_streams: futures::stream::SelectAll<
            tokio_stream::wrappers::BroadcastStream<ControlSignal>,
        >,
        mut control_active: bool,
        output: broadcast::Sender<StreamData>,
        control_output: broadcast::Sender<ControlSignal>,
        stats: Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        let mut ticker = interval(emit_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut latest: Option<StreamData> = None;

        loop {
            tokio::select! {
                biased;
                control_item = control_streams.next(), if control_active => {
                    if let Some(Ok(control_signal)) = control_item {
                        let is_terminal = control_signal.is_terminal();
                        send_control_with_backpressure(&control_output, control_signal).await?;
                        if is_terminal {
                            // Emit latest before shutdown if present
                            if let Some(data) = latest.take() {
                                let row_count = data.num_rows_hint().unwrap_or(0);
                                send_with_backpressure(&output, data).await?;
                                stats.record_out(row_count);
                            }
                            tracing::info!(processor_id = %processor_id, "sampler received terminal signal, shutting down");
                            return Ok(());
                        }
                    } else if control_item.is_none() {
                        control_active = false;
                    } else if let Some(Err(BroadcastStreamRecvError::Lagged(n))) = control_item {
                        log_broadcast_lagged(&processor_id, n, "control");
                    }
                }
                _ = ticker.tick() => {
                    // Emit latest value if present
                    if let Some(data) = latest.take() {
                        let row_count = data.num_rows_hint().unwrap_or(0);
                        send_with_backpressure(&output, data).await?;
                        stats.record_out(row_count);
                        tracing::trace!(processor_id = %processor_id, "sampler emitted latest value");
                    }
                }
                data_item = input_streams.next() => {
                    match data_item {
                        Some(Ok(data)) => {
                            log_received_data(&processor_id, &data);
                            stats.record_in(data.num_rows_hint().unwrap_or(0));
                            // Replace latest with new data (discard old)
                            latest = Some(data);
                        }
                        Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                            log_broadcast_lagged(&processor_id, n, "data");
                        }
                        None => {
                            // Input closed, emit final value and exit
                            if let Some(data) = latest.take() {
                                let row_count = data.num_rows_hint().unwrap_or(0);
                                send_with_backpressure(&output, data).await?;
                                stats.record_out(row_count);
                            }
                            tracing::info!(processor_id = %processor_id, "sampler input closed, shutting down");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Message, RecordBatch, Tuple};
    use crate::processor::StreamData;
    use datatypes::Value;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::time::sleep;

    fn create_test_data(value: i64) -> StreamData {
        let message = Arc::new(Message::new(
            "test",
            vec![Arc::from("value")],
            vec![Arc::new(Value::Int64(value))],
        ));
        let tuple = Tuple::new(vec![message]);
        let batch = RecordBatch::new(vec![tuple]).expect("create batch");
        StreamData::Collection(Box::new(batch))
    }

    #[test]
    fn test_sampler_creation() {
        let sampler = SamplerProcessor::latest("test_sampler", Duration::from_millis(100));
        assert_eq!(sampler.id(), "test_sampler");
    }

    #[test]
    fn test_sampler_config_serialization() {
        let config = SamplerConfig::new(Duration::from_millis(100));
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("100ms"));
        assert!(json.contains("latest"));
    }

    #[tokio::test]
    async fn test_sampler_rate_limits_high_frequency_input() {
        let mut sampler = SamplerProcessor::latest("sampler_test", Duration::from_millis(50));

        let (input_tx, input_rx) = broadcast::channel::<StreamData>(16);
        sampler.add_input(input_rx);

        let mut output_rx = sampler.subscribe_output().unwrap();
        let handle = sampler.start();

        // Send 10 messages rapidly
        for i in 0..10 {
            let _ = input_tx.send(create_test_data(i));
            sleep(Duration::from_millis(5)).await;
        }

        sleep(Duration::from_millis(100)).await;

        let mut received_count = 0;
        while output_rx.try_recv().is_ok() {
            received_count += 1;
        }

        drop(input_tx);
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        assert!(
            received_count < 10,
            "Expected rate limiting, got {} messages",
            received_count
        );
        assert!(received_count >= 1, "Expected at least 1 message");
    }

    #[tokio::test]
    async fn test_sampler_emits_latest_value() {
        let mut sampler = SamplerProcessor::latest("latest_test", Duration::from_millis(200));

        let (input_tx, input_rx) = broadcast::channel::<StreamData>(16);
        sampler.add_input(input_rx);

        let mut output_rx = sampler.subscribe_output().unwrap();
        let _handle = sampler.start();

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
            assert_eq!(*value.as_ref(), Value::Int64(3));
        } else {
            panic!("Expected Collection data");
        }
    }
}
