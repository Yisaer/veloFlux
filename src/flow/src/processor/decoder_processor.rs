//! DecoderProcessor - decodes StreamData::Bytes into StreamData::Collection.

use crate::codec::RecordDecoder;
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct DecoderProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    decoder: Arc<dyn RecordDecoder>,
    projection: Option<Arc<std::sync::RwLock<Vec<String>>>>,
}

impl DecoderProcessor {
    pub fn new(plan_name: impl Into<String>, decoder: Arc<dyn RecordDecoder>) -> Self {
        let plan_name = plan_name.into();
        Self::with_custom_id(plan_name.clone(), decoder)
    }

    pub fn with_custom_id(id: impl Into<String>, decoder: Arc<dyn RecordDecoder>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            decoder,
            projection: None,
        }
    }

    pub fn with_projection(mut self, projection: Arc<std::sync::RwLock<Vec<String>>>) -> Self {
        self.projection = Some(projection);
        self
    }
}

impl Processor for DecoderProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let decoder = Arc::clone(&self.decoder);
        let projection = self.projection.clone();
        let processor_id = self.id.clone();
        let log_prefix = format!("[DecoderProcessor:{processor_id}]");
        let base_inputs = std::mem::take(&mut self.inputs);
        let mut input_streams = fan_in_streams(base_inputs);
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        println!("{log_prefix} starting");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                println!("{log_prefix} received StreamEnd (control)");
                                println!("{log_prefix} stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(mut data)) => {
                                log_received_data(&processor_id, &data);
                                if let StreamData::Bytes(payload) = &data {
                                    let decoded = match &projection {
                                        Some(lock) => {
                                            let cols = lock
                                                .read()
                                                .expect("decoder projection lock poisoned")
                                                .clone();
                                            decoder.decode_with_projection(payload, Some(&cols))
                                        }
                                        None => decoder.decode(payload),
                                    };
                                    match decoded {
                                        Ok(batch) => {
                                            data = StreamData::collection(Box::new(batch));
                                        }
                                        Err(err) => {
                                            let message = format!("decode error: {}", err);
                                            forward_error(&output, &processor_id, message).await?;
                                            continue;
                                        }
                                    }
                                }
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;
                                if is_terminal {
                                    println!("{log_prefix} received StreamEnd (data)");
                                    println!("{log_prefix} stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message =
                                    format!("Decoder input lagged by {} messages", skipped);
                                println!("{log_prefix} input lagged by {skipped} messages");
                                forward_error(&output, &processor_id, message).await?;
                                continue;
                            }
                            None => {
                                println!("{log_prefix} stopped");
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
