//! ProjectProcessor - processes projection operations
//!
//! This processor evaluates projection expressions and produces output with projected fields.

use tokio::sync::{mpsc, Mutex};
use std::sync::Arc;
use std::pin::Pin;
use futures::stream::{FuturesUnordered, StreamExt};
use crate::processor::{Processor, ProcessorError, StreamData};
use crate::planner::physical::{PhysicalProject, PhysicalProjectField};
use crate::model::Collection;

/// ProjectProcessor - evaluates projection expressions
///
/// This processor:
/// - Takes input data (Collection) and projection expressions
/// - Evaluates the expressions to create projected fields
/// - Sends the projected data downstream as StreamData::Collection
pub struct ProjectProcessor {
    /// Processor identifier
    id: String,
    /// Physical projection configuration
    physical_project: Arc<PhysicalProject>,
    /// Input channels for receiving data
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl ProjectProcessor {
    /// Create a new ProjectProcessor from PhysicalProject
    pub fn new(
        id: impl Into<String>,
        physical_project: Arc<PhysicalProject>,
    ) -> Self {
        Self {
            id: id.into(),
            physical_project,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
    
    /// Create a ProjectProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalProject
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<dyn crate::planner::physical::PhysicalPlan>,
    ) -> Option<Self> {
        plan.as_any().downcast_ref::<PhysicalProject>().map(|proj| Self::new(id, Arc::new(proj.clone())))
    }
}

/// Apply projection to a collection
fn apply_projection(input_collection: &dyn Collection, fields: &[PhysicalProjectField]) -> Result<Box<dyn Collection>, ProcessorError> {
    // Use the collection's apply_projection method
    input_collection.apply_projection(fields)
        .map_err(|e| ProcessorError::ProcessingError(format!("Failed to apply projection: {}", e)))
}

impl Processor for ProjectProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let inputs = std::mem::take(&mut self.inputs);
        let outputs = self.outputs.clone();
        let fields = self.physical_project.fields.clone();
        
        tokio::spawn(async move {
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
                        // Handle control signals
                        if let Some(control) = data.as_control() {
                            match control {
                                crate::processor::ControlSignal::StreamEnd => {
                                    // Forward StreamEnd to outputs
                                    for output in &outputs {
                                        let _ = output.send(data.clone()).await;
                                    }
                                    return Ok(());
                                }
                                _ => {
                                    // Forward other control signals
                                    for output in &outputs {
                                        if output.send(data.clone()).await.is_err() {
                                            return Err(ProcessorError::ChannelClosed);
                                        }
                                    }
                                }
                            }
                        } else if let Some(collection) = data.as_collection() {
                            // Apply projection to the collection
                            match apply_projection(collection, &fields) {
                                Ok(projected_collection) => {
                                    // Send projected data to all outputs
                                    let projected_data = StreamData::collection(projected_collection);
                                    for output in &outputs {
                                        if output.send(projected_data.clone()).await.is_err() {
                                            return Err(ProcessorError::ChannelClosed);
                                        }
                                    }
                                }
                                Err(e) => {
                                    // Send error downstream
                                    let error_data = StreamData::error(
                                        crate::processor::StreamError::new(e.to_string())
                                            .with_source(id.clone()),
                                    );
                                    for output in &outputs {
                                        if output.send(error_data.clone()).await.is_err() {
                                            return Err(ProcessorError::ChannelClosed);
                                        }
                                    }
                                }
                            }
                        } else {
                            // Forward non-collection data (errors, etc.)
                            for output in &outputs {
                                if output.send(data.clone()).await.is_err() {
                                    return Err(ProcessorError::ChannelClosed);
                                }
                            }
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
            
            // All channels closed
            Ok(())
        })
    }
    
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        self.outputs.clone()
    }
    
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
    
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.outputs.push(sender);
    }
}