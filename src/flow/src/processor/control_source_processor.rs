//! ControlSourceProcessor - starting point for data flow
//!
//! This processor is responsible for receiving and sending control signals
//! that coordinate the entire stream processing pipeline.

use crate::processor::base::{forward_error, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY};
use crate::processor::{Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

/// ControlSourceProcessor - handles control signals for the pipeline
///
/// This processor acts as the starting point of the data flow. It:
/// - Receives StreamData from a single input (single-input)
/// - Forwards StreamData to multiple downstream processors (multi-output)
/// - Coordinates the start/end of stream processing
pub struct ControlSourceProcessor {
    /// Processor identifier
    id: String,
    /// Single input channel for receiving StreamData (single-input)
    input: Option<broadcast::Receiver<StreamData>>,
    /// Broadcast channel for all downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control channel for urgent control propagation
    control_output: broadcast::Sender<StreamData>,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor
    pub fn new(id: impl Into<String>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            input: None,
            output,
            control_output,
        }
    }

    /// Send StreamData to all downstream processors
    pub async fn send(&self, data: StreamData) -> Result<(), ProcessorError> {
        if let StreamData::Control(signal) = &data {
            if signal.routes_via_control() {
                send_with_backpressure(&self.control_output, data.clone()).await?;
            }
            if !signal.routes_via_data() {
                return Ok(());
            }
        }
        send_with_backpressure(&self.output, data).await
    }

    /// Send StreamData to a specific downstream processor by id.
    ///
    /// The broadcast-based fan-out no longer supports targeted sends, so this
    /// method now returns an error to signal that behavior.
    pub async fn send_stream_data(
        &self,
        _processor_id: &str,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        self.send(data).await
    }
}

impl Processor for ControlSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let input_result = self.input.take().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "ControlSourceProcessor input must be set before starting".to_string(),
            )
        });
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        println!("[ControlSourceProcessor:{processor_id}] starting");

        tokio::spawn(async move {
            let input = match input_result {
                Ok(input) => input,
                Err(e) => return Err(e),
            };
            let mut stream = BroadcastStream::new(input);

            while let Some(item) = stream.next().await {
                let data = match item {
                    Ok(data) => data,
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                        let message =
                            format!("Control source input lagged by {} messages", skipped);
                        println!(
                            "[ControlSourceProcessor:{processor_id}] input lagged: {}",
                            message
                        );
                        forward_error(&output, &processor_id, message).await?;
                        continue;
                    }
                };

                let is_terminal = data.is_terminal();
                let mut deliver_to_data = true;

                if let StreamData::Control(signal) = &data {
                    if signal.routes_via_control() {
                        send_with_backpressure(&control_output, data.clone()).await?;
                    }
                    if !signal.routes_via_data() {
                        deliver_to_data = false;
                    }
                }

                if deliver_to_data {
                    send_with_backpressure(&output, data).await?;
                }

                if is_terminal {
                    println!("[ControlSourceProcessor:{processor_id}] received StreamEnd");
                    return Ok(());
                }
            }

            // Input closed without explicit StreamEnd, propagate shutdown.
            println!("[ControlSourceProcessor:{processor_id}] stopping (input closed)");
            send_with_backpressure(&output, StreamData::stream_end()).await?;
            Ok(())
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.input = Some(receiver);
    }

    fn add_control_input(&mut self, _receiver: broadcast::Receiver<StreamData>) {}
}
