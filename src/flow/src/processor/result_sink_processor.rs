//! ResultSinkProcessor - final destination for data flow
//!
//! This processor receives data from upstream processors and forwards it to a single output.

use tokio::sync::{mpsc, Mutex};
use std::sync::Arc;
use std::pin::Pin;
use futures::stream::{FuturesUnordered, StreamExt};
use crate::processor::{Processor, ProcessorError, StreamData};

/// ResultSinkProcessor - forwards received data to a single output
///
/// This processor acts as the final destination in the data flow. It:
/// - Receives StreamData from multiple upstream processors (multi-input)
/// - Forwards all received data to a single output channel (single-output)
/// - Can be used to collect results or forward to external systems
pub struct ResultSinkProcessor {
    /// Processor identifier
    id: String,
    /// Input channels for receiving data (multi-input)
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Single output channel for forwarding received data (single-output)
    output: Option<mpsc::Sender<StreamData>>,
}

impl ResultSinkProcessor {
    /// Create a new ResultSinkProcessor
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            inputs: Vec::new(),
            output: None,
        }
    }
    
    /// Get the output receiver (for connecting to external systems)
    /// Returns None if output is not set
    pub fn output_receiver(&self) -> Option<&mpsc::Sender<StreamData>> {
        self.output.as_ref()
    }
}

impl Processor for ResultSinkProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let _id = self.id.clone();
        let inputs = std::mem::take(&mut self.inputs);
        let output = self.output.take()
            .ok_or_else(|| ProcessorError::InvalidConfiguration(
                "ResultSinkProcessor output must be set before starting".to_string()
            ));
        
        tokio::spawn(async move {
            let output = match output {
                Ok(output) => output,
                Err(e) => return Err(e),
            };
            
            // Wrap receivers in Arc<Mutex<>> to allow re-adding futures
            let receivers: Vec<Arc<Mutex<mpsc::Receiver<StreamData>>>> = 
                inputs.into_iter().map(|r| Arc::new(Mutex::new(r))).collect();
            let mut futures = FuturesUnordered::new();
            
            // Create initial futures for all receivers
            for (idx, receiver) in receivers.iter().enumerate() {
                let receiver_clone = receiver.clone();
                futures.push(Box::pin(async move {
                    let mut receiver_guard = receiver_clone.lock().await;
                    (idx, receiver_guard.recv().await)
                }) as Pin<Box<dyn std::future::Future<Output = (usize, Option<StreamData>)> + Send>>);
            }
            
            // Process data from any ready channel
            while let Some((idx, result)) = futures.next().await {
                match result {
                    Some(data) => {
                        // Forward data to the single output
                        if output.send(data.clone()).await.is_err() {
                            return Err(ProcessorError::ChannelClosed);
                        }
                        
                        // Check if this is a terminal signal
                        if data.is_terminal() {
                            // Forward StreamEnd to output before exiting
                            let _ = output.send(StreamData::stream_end()).await;
                            return Ok(());
                        }
                        
                        // Re-add future for this receiver
                        if let Some(receiver) = receivers.get(idx) {
                            let receiver_clone = receiver.clone();
                            futures.push(Box::pin(async move {
                                let mut receiver_guard = receiver_clone.lock().await;
                                (idx, receiver_guard.recv().await)
                            }) as Pin<Box<dyn std::future::Future<Output = (usize, Option<StreamData>)> + Send>>);
                        }
                    }
                    None => {
                        // Channel disconnected, continue with other channels
                    }
                }
            }
            
            // All channels closed, send StreamEnd to output before exiting
            let _ = output.send(StreamData::stream_end()).await;
            Ok(())
        })
    }
    
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        // Return single output as a vector (for compatibility with Processor trait)
        self.output.as_ref().map(|s| vec![s.clone()]).unwrap_or_default()
    }
    
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
    
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        // ResultSinkProcessor only supports single output
        // If output is already set, replace it
        self.output = Some(sender);
    }
}
