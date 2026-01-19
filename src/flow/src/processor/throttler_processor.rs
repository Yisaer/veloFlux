//! ThrottlerProcessor - Rate limits a stream by emitting the latest value at fixed intervals.
//!
//! This processor is designed for downsampling high-frequency streams (Strategy 1 / Latest).
//! It works by:
//! 1. Constantly receiving input data.
//! 2. Storing only the latest received value.
//! 3. Periodically emitting the stored value (if any) based on the configured interval.

use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use tokio::sync::broadcast;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Processor that rate-limits a stream by emitting the latest value at fixed intervals.
pub struct ThrottlerProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    rate_limit: Duration,
}

impl ThrottlerProcessor {
    pub fn new(id: impl Into<String>, rate_limit: Duration) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            rate_limit,
        }
    }
}

impl Processor for ThrottlerProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let rate_limit = self.rate_limit;
        let processor_id = self.id.clone();

        tracing::info!(processor_id = %processor_id, rate_limit = ?rate_limit, "throttler processor starting");

        tokio::spawn(async move {
            let mut ticker = interval(rate_limit);
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
                                    send_with_backpressure(&output, data).await?;
                                }
                                tracing::info!(processor_id = %processor_id, "throttler received terminal signal, shutting down");
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
                            send_with_backpressure(&output, data).await?;
                            tracing::trace!(processor_id = %processor_id, "throttler emitted latest value");
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                // Replace latest with new data (discard old)
                                latest = Some(data);
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                log_broadcast_lagged(&processor_id, n, "data");
                            }
                            None => {
                                // Input closed, emit final value and exit
                                if let Some(data) = latest.take() {
                                    send_with_backpressure(&output, data).await?;
                                }
                                tracing::info!(processor_id = %processor_id, "throttler input closed, shutting down");
                                return Ok(());
                            }
                        }
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_throttler_creation() {
        let throttler = ThrottlerProcessor::new("test_throttler", Duration::from_millis(100));
        assert_eq!(throttler.id(), "test_throttler");
    }
}
