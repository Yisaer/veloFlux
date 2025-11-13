//! ControlSourceProcessor - starting point for data flow
//!
//! This processor is responsible for receiving and sending control signals
//! that coordinate the entire stream processing pipeline.

use tokio::sync::mpsc;
use std::collections::HashMap;
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
    /// Mapping from downstream processor id to output channel
    output_map: HashMap<String, mpsc::Sender<StreamData>>,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            input: None,
            outputs: Vec::new(),
            output_map: HashMap::new(),
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

    /// Register an output channel for a specific downstream processor id
    pub fn add_output_for_processor(
        &mut self,
        processor_id: impl Into<String>,
        sender: mpsc::Sender<StreamData>,
    ) {
        let id = processor_id.into();
        self.outputs.push(sender.clone());
        self.output_map.insert(id, sender);
    }

    /// Send StreamData to a specific downstream processor by id
    pub async fn send_stream_data(
        &self,
        processor_id: &str,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        if let Some(output) = self.output_map.get(processor_id) {
            output
                .send(data)
                .await
                .map_err(|_| ProcessorError::ChannelClosed)
        } else {
            Err(ProcessorError::InvalidConfiguration(format!(
                "Unknown processor id: {}",
                processor_id
            )))
        }
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
                match input.recv().await {
                    Some(data) => {
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
                    None => {
                        // Channel disconnected, send StreamEnd to all outputs and exit
                        for output in &outputs {
                            let _ = output.send(StreamData::stream_end()).await;
                        }
                        return Ok(());
                    }
                }
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
        let auto_id = format!("auto_output_{}", self.outputs.len());
        self.add_output_for_processor(auto_id, sender);
    }
}
