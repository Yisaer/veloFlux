//! ControlSourceProcessor - starting point for data flow
//!
//! This processor is responsible for receiving and sending control signals
//! that coordinate the entire stream processing pipeline.

use tokio::sync::mpsc;
use crate::processor::{Processor, ProcessorError, StreamData};

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
    input: Option<mpsc::Receiver<StreamData>>,
    /// Output channels for sending StreamData downstream (multi-output)
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            input: None,
            outputs: Vec::new(),
        }
    }
    
    /// Send StreamData to all downstream processors
    pub async fn send(&self, data: StreamData) -> Result<(), ProcessorError> {
        for output in &self.outputs {
            output
                .send(data.clone())
                .await
                .map_err(|_| ProcessorError::ChannelClosed)?;
        }
        Ok(())
    }
}

impl Processor for ControlSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let _id = self.id.clone();
        let input_result = self.input.take()
            .ok_or_else(|| ProcessorError::InvalidConfiguration(
                "ControlSourceProcessor input must be set before starting".to_string()
            ));
        let outputs = self.outputs.clone();
        
        tokio::spawn(async move {
            let mut input = match input_result {
                Ok(input) => input,
                Err(e) => return Err(e),
            };
            
            loop {
                match input.try_recv() {
                    Ok(data) => {
                        // Forward to all outputs
                        for output in &outputs {
                            if output.send(data.clone()).await.is_err() {
                                return Err(ProcessorError::ChannelClosed);
                            }
                        }
                        // Check if this is a terminal signal
                        if data.is_terminal() {
                            return Ok(());
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // Channel not empty but no data ready, continue
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        // Channel disconnected, send StreamEnd to all outputs and exit
                        for output in &outputs {
                            let _ = output.send(StreamData::stream_end()).await;
                        }
                        return Ok(());
                    }
                }
                
                // Yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
        })
    }
    
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        self.outputs.clone()
    }
    
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        // ControlSourceProcessor only supports single input
        // If input is already set, replace it
        self.input = Some(receiver);
    }
    
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.outputs.push(sender);
    }
}
