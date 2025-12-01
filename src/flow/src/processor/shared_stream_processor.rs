use crate::processor::base::{
    fan_in_streams, forward_error, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use crate::shared_stream_registry;
use futures::stream::StreamExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

pub struct SharedStreamProcessor {
    id: String,
    plan_index: i64,
    stream_name: String,
    pipeline_id: Option<String>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<StreamData>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<StreamData>,
}

impl SharedStreamProcessor {
    pub fn new(plan_index: i64, stream_name: impl Into<String>) -> Self {
        let stream_name = stream_name.into();
        let id = format!("shared_source_{plan_index}");
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id,
            plan_index,
            stream_name,
            pipeline_id: None,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }

    pub fn set_pipeline_id(&mut self, pipeline_id: impl Into<String>) {
        self.pipeline_id = Some(pipeline_id.into());
    }
}

impl Processor for SharedStreamProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let has_data_inputs = !self.inputs.is_empty();
        let mut inputs = fan_in_streams(std::mem::take(&mut self.inputs));
        let mut input_active = has_data_inputs;
        let has_control_inputs = !self.control_inputs.is_empty();
        let mut control_inputs = fan_in_streams(std::mem::take(&mut self.control_inputs));
        let mut control_inputs_active = has_control_inputs;
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let stream_name = self.stream_name.clone();
        let plan_index = self.plan_index;
        let processor_id = self.id.clone();
        let pipeline_id = self
            .pipeline_id
            .clone()
            .unwrap_or_else(|| format!("pipeline-{}", Uuid::new_v4()));
        tokio::spawn(async move {
            println!(
                "[SharedStreamProcessor:{processor_id}#{plan_index}] subscribing to {stream_name} (pipeline {pipeline_id})"
            );
            let registry = shared_stream_registry();
            let mut subscription = registry
                .subscribe(&stream_name, format!("{pipeline_id}-{processor_id}"))
                .await
                .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
            let (shared_data_rx, shared_control_rx) = subscription.take_receivers();
            let mut shared_data = BroadcastStream::new(shared_data_rx);
            let mut shared_control = BroadcastStream::new(shared_control_rx);
            loop {
                tokio::select! {
                    biased;
                    control_msg = control_inputs.next(), if control_inputs_active => {
                        match control_msg {
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&control_output, data.clone()).await?;
                                if is_terminal {
                                    return Ok(());
                                }
                            }
                            Some(Err(err)) => {
                                let message =
                                    format!("SharedStreamProcessor control input lagged: {err}");
                                println!(
                                    "[SharedStreamProcessor:{processor_id}] control input lagged: {err}"
                                );
                                forward_error(&output, &processor_id, message.clone()).await?;
                                send_with_backpressure(
                                    &control_output,
                                    StreamData::error(
                                        StreamError::new(message).with_source(processor_id.clone()),
                                    ),
                                )
                                .await?;
                                continue;
                            }
                            None => {
                                control_inputs_active = false;
                            }
                        }
                    }
                    shared_control_msg = shared_control.next() => {
                        match shared_control_msg {
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&control_output, data.clone()).await?;
                                if is_terminal {
                                    return Ok(());
                                }
                            }
                            Some(Err(err)) => {
                                let message = format!("shared control lagged: {err}");
                                println!(
                                    "[SharedStreamProcessor:{processor_id}] shared control lagged: {err}"
                                );
                                send_with_backpressure(
                                    &control_output,
                                    StreamData::error(
                                        StreamError::new(message.clone())
                                            .with_source(processor_id.clone()),
                                    ),
                                )
                                .await?;
                                forward_error(&output, &processor_id, message).await?;
                                continue;
                            }
                            None => {
                                return Ok(());
                            }
                        }
                    }
                    data_msg = inputs.next(), if input_active => {
                        if let Some(Ok(data)) = data_msg {
                            let is_terminal = data.is_terminal();
                            send_with_backpressure(&output, data).await?;
                            if is_terminal {
                                return Ok(());
                            }
                        } else {
                            input_active = false;
                        }
                    }
                    shared_data_msg = shared_data.next() => {
                        match shared_data_msg {
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;
                                if is_terminal {
                                    return Ok(());
                                }
                            }
                            Some(Err(err)) => {
                                let message = format!("shared data lagged: {err}");
                                println!(
                                    "[SharedStreamProcessor:{processor_id}] shared data lagged: {err}"
                                );
                                forward_error(&output, &processor_id, message).await?;
                                continue;
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.control_inputs.push(receiver);
    }
}
