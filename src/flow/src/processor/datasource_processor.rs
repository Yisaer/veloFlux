//! DataSourceProcessor - processes data from PhysicalDatasource
//!
//! This processor reads data from a PhysicalDatasource and sends it downstream
//! as StreamData::Collection.

use crate::codec::RecordDecoder;
use crate::connector::{ConnectorError, ConnectorEvent, SourceConnector};
use crate::processor::base::{
    fan_in_streams, forward_error, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use datatypes::Schema;
use futures::stream::StreamExt;
use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier (`datasource_{plan_index}` for plan-based processors)
    id: String,
    plan_index: Option<i64>,
    source_name: String,
    schema: Arc<Schema>,
    /// Input channels for receiving control signals
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Broadcast channel for downstream consumers
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<StreamData>,
    /// External source connectors that feed this processor
    connectors: Vec<ConnectorBinding>,
}

static DATASOURCE_RECORDS_IN: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "datasource_processor_records_in_total",
        "Rows received by datasource processors",
        &["processor"]
    )
    .expect("create datasource records_in counter vec")
});

static DATASOURCE_RECORDS_OUT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "datasource_processor_records_out_total",
        "Rows emitted by datasource processors",
        &["processor"]
    )
    .expect("create datasource records_out counter vec")
});

struct ConnectorBinding {
    connector: Box<dyn SourceConnector>,
    decoder: Arc<dyn RecordDecoder>,
    handle: Option<JoinHandle<()>>,
}

impl ConnectorBinding {
    fn activate(&mut self, processor_id: &str) -> broadcast::Receiver<StreamData> {
        let (sender, receiver) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let decoder = Arc::clone(&self.decoder);
        let processor_id = processor_id.to_string();
        let connector_id = self.connector.id().to_string();
        let mut stream = match self.connector.subscribe() {
            Ok(stream) => stream,
            Err(err) => {
                let _ = sender.send(StreamData::error(
                    StreamError::new(format!("connector subscribe error: {}", err))
                        .with_source(processor_id.clone()),
                ));
                return receiver;
            }
        };

        println!(
            "[DataSourceProcessor:{processor_id}] connector {} starting",
            connector_id
        );
        let sender_clone = sender.clone();
        self.handle = Some(tokio::spawn(async move {
            let sender = sender_clone;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(ConnectorEvent::Payload(bytes)) => match decoder.decode(&bytes) {
                        Ok(batch) => {
                            if send_with_backpressure(
                                &sender,
                                StreamData::collection(Box::new(batch)),
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        }
                        Err(err) => {
                            if send_with_backpressure(
                                &sender,
                                StreamData::error(
                                    StreamError::new(format!("decode error: {}", err))
                                        .with_source(processor_id.clone()),
                                ),
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        }
                    },
                    Ok(ConnectorEvent::EndOfStream) => break,
                    Err(err) => {
                        if send_with_backpressure(
                            &sender,
                            StreamData::error(
                                StreamError::new(format!("connector error: {}", err))
                                    .with_source(processor_id.clone()),
                            ),
                        )
                        .await
                        .is_err()
                        {
                            break;
                        }
                    }
                }
            }
            println!(
                "[DataSourceProcessor:{processor_id}] connector {} stopped",
                connector_id
            );
        }));

        receiver
    }

    async fn shutdown(&mut self) -> Result<(), ProcessorError> {
        let connector_id = self.connector.id().to_string();
        println!("[SourceConnector:{connector_id}] closing");
        if let Err(err) = self.connector.close() {
            return Err(Self::connector_error(self.connector.id(), err));
        }
        if let Some(handle) = self.handle.take() {
            if let Err(join_err) = handle.await {
                return Err(ProcessorError::ProcessingError(format!(
                    "Connector join error: {}",
                    join_err
                )));
            }
        }
        println!("[SourceConnector:{connector_id}] closed");
        Ok(())
    }

    fn connector_error(id: &str, err: ConnectorError) -> ProcessorError {
        ProcessorError::ProcessingError(format!("connector `{}` error: {}", id, err))
    }
}
impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(plan_index: i64, source_name: impl Into<String>, schema: Arc<Schema>) -> Self {
        Self::with_custom_id(
            Some(plan_index),
            format!("datasource_{plan_index}"),
            source_name,
            schema,
        )
    }

    pub fn with_custom_id(
        plan_index: Option<i64>,
        id: impl Into<String>,
        source_name: impl Into<String>,
        schema: Arc<Schema>,
    ) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            plan_index,
            source_name: source_name.into(),
            schema,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            connectors: Vec::new(),
        }
    }

    /// Register an external source connector and its decoder.
    pub fn add_connector(
        &mut self,
        connector: Box<dyn SourceConnector>,
        decoder: Arc<dyn RecordDecoder>,
    ) {
        self.connectors.push(ConnectorBinding {
            connector,
            decoder,
            handle: None,
        });
    }

    fn activate_connectors(
        connectors: &mut [ConnectorBinding],
        processor_id: &str,
    ) -> Vec<broadcast::Receiver<StreamData>> {
        connectors
            .iter_mut()
            .map(|binding| binding.activate(processor_id))
            .collect()
    }

    async fn shutdown_connectors(
        connectors: &mut [ConnectorBinding],
    ) -> Result<(), ProcessorError> {
        for binding in connectors.iter_mut() {
            binding.shutdown().await?;
        }
        Ok(())
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        let plan_label = self
            .plan_index
            .map(|idx| idx.to_string())
            .unwrap_or_else(|| "global".to_string());
        let source_name = self.source_name.clone();
        let log_prefix =
            format!("[DataSourceProcessor:{processor_id}#{plan_label}::{source_name}]");
        let mut base_inputs = std::mem::take(&mut self.inputs);
        let mut connectors = std::mem::take(&mut self.connectors);
        let connector_inputs = Self::activate_connectors(&mut connectors, &processor_id);
        base_inputs.extend(connector_inputs);
        let mut input_streams = fan_in_streams(base_inputs);
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        println!("{log_prefix} starting");
        tokio::spawn(async move {
            let mut connectors = connectors;
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(result) = control_item {
                            let control_data = match result {
                                Ok(data) => data,
                                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                                    let message = format!(
                                        "DataSource control input lagged by {} messages",
                                        skipped
                                    );
                                    println!("{log_prefix} control input lagged by {skipped} messages");
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
                            };
                            let is_terminal = control_data.is_terminal();
                            send_with_backpressure(&control_output, control_data.clone()).await?;
                            if is_terminal {
                                println!("{log_prefix} received StreamEnd (control)");
                                Self::shutdown_connectors(&mut connectors).await?;
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
                                if let StreamData::Collection(collection) = data {
                                    let rows = collection.num_rows() as u64;
                                    DATASOURCE_RECORDS_IN
                                        .with_label_values(&[processor_id.as_str()])
                                        .inc_by(rows);
                                    DATASOURCE_RECORDS_OUT
                                        .with_label_values(&[processor_id.as_str()])
                                        .inc_by(rows);
                                    data = StreamData::Collection(collection);
                                }
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;

                                if is_terminal {
                                    println!("{log_prefix} received StreamEnd (data)");
                                    Self::shutdown_connectors(&mut connectors).await?;
                                    println!("{log_prefix} stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "DataSource input lagged by {} messages",
                                    skipped
                                );
                                println!("{log_prefix} input lagged by {skipped} messages");
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
                                Self::shutdown_connectors(&mut connectors).await?;
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

impl DataSourceProcessor {
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
