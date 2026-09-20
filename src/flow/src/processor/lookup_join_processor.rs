//! Stream-to-table lookup join processor.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::planner::physical::PhysicalLookupJoin;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use crate::table::provider::{
    LookupKey, LookupOutput, LookupRequest, LookupResponse, TableProvider, TableProviderError,
};
use datatypes::Value;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::CancellationToken;

const OUTPUT_COLLECTION_ROWS: usize = 1024;
const LOOKUP_OUTPUT_CHANNEL_CAPACITY: usize = 1;

struct LookupTaskGuard {
    task: Option<tokio::task::JoinHandle<Result<LookupResponse, TableProviderError>>>,
}

impl LookupTaskGuard {
    fn new(task: tokio::task::JoinHandle<Result<LookupResponse, TableProviderError>>) -> Self {
        Self { task: Some(task) }
    }

    async fn finish(mut self) -> Result<LookupResponse, ProcessorError> {
        self.task
            .take()
            .expect("lookup task missing")
            .await
            .map_err(|error| ProcessorError::ProcessingError(error.to_string()))?
            .map_err(|error| ProcessorError::ProcessingError(error.to_string()))
    }

    async fn abort_and_wait(&mut self) {
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for LookupTaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }
}

async fn forward_control_signal(
    signal: ControlSignal,
    cancellation: &CancellationToken,
    output: &LinkOutput<ControlSignal>,
    channel_capacity: usize,
) -> Result<bool, ProcessorError> {
    let is_terminal = signal.is_terminal();
    if is_terminal {
        cancellation.cancel();
    }
    send_control_with_backpressure(output, channel_capacity, signal).await?;
    Ok(is_terminal)
}

async fn send_collection_with_cancellation(
    output: &LinkOutput<StreamData>,
    data_channel_capacity: usize,
    batch: RecordBatch,
    cancellation: &CancellationToken,
    stats: &ProcessorStats,
) -> Result<bool, ProcessorError> {
    let row_count = batch.num_rows();
    tokio::select! {
        result = send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::collection(Box::new(batch)),
            Some(stats),
        ) => {
            result?;
            stats.record_collection_out(row_count as u64);
            Ok(true)
        }
        _ = cancellation.cancelled() => Ok(false),
    }
}

/// Executes a stream-to-table lookup join for each input tuple.
pub struct LookupJoinProcessor {
    id: String,
    physical_join: Arc<PhysicalLookupJoin>,
    provider: Arc<dyn TableProvider>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl LookupJoinProcessor {
    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_join: Arc<PhysicalLookupJoin>,
        provider: Arc<dyn TableProvider>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            physical_join,
            provider,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        id: impl Into<String>,
        physical_join: Arc<PhysicalLookupJoin>,
        provider: Arc<dyn TableProvider>,
    ) -> Self {
        Self::new_with_channel_capacities(id, physical_join, provider, default_channel_capacities())
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }

    async fn process_collection(
        input: Box<dyn Collection>,
        physical_join: &PhysicalLookupJoin,
        provider: Arc<dyn TableProvider>,
        cancellation: &CancellationToken,
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: &ProcessorStats,
    ) -> Result<(), ProcessorError> {
        let projection = physical_join.table_projection.to_vec();
        let mut output_rows = Vec::new();
        let mut emitted_output = false;

        for tuple in input.rows() {
            let equality_keys = physical_join
                .key_slots
                .iter()
                .map(|key| {
                    let value = tuple
                        .messages()
                        .get(key.probe_message_index)
                        .and_then(|message| message.value_by_index(key.probe_value_index))
                        .ok_or_else(|| {
                            ProcessorError::ProcessingError(format!(
                                "lookup join probe slot is missing: message={}, value={}",
                                key.probe_message_index, key.probe_value_index
                            ))
                        })?;
                    Ok(LookupKey {
                        column: key.table_column.clone(),
                        value: value.clone(),
                    })
                })
                .collect::<Result<Vec<_>, ProcessorError>>()?;

            let (sender, mut receiver) = mpsc::channel(LOOKUP_OUTPUT_CHANNEL_CAPACITY);
            let request = LookupRequest {
                projection: projection.clone(),
                equality_keys,
                filters: Vec::new(),
                time_range: None,
                batch_size: OUTPUT_COLLECTION_ROWS,
                cancellation: cancellation.clone(),
            };
            let lookup_provider = Arc::clone(&provider);
            let lookup_task = tokio::spawn(async move {
                lookup_provider
                    .lookup(request, LookupOutput::new(sender))
                    .await
            });
            let mut lookup_task = LookupTaskGuard::new(lookup_task);

            loop {
                let Some(table_batch) = (tokio::select! {
                    _ = cancellation.cancelled() => {
                        lookup_task.abort_and_wait().await;
                        return Ok(());
                    }
                    batch = receiver.recv() => batch
                }) else {
                    break;
                };
                for table_tuple in table_batch {
                    if cancellation.is_cancelled() {
                        lookup_task.abort_and_wait().await;
                        return Ok(());
                    }
                    let table_message = table_tuple.messages().first().ok_or_else(|| {
                        ProcessorError::ProcessingError(format!(
                            "lookup provider `{}` returned a row without a message",
                            physical_join.table_name
                        ))
                    })?;
                    if table_tuple.messages().len() != 1 {
                        return Err(ProcessorError::ProcessingError(format!(
                        "lookup provider `{}` returned a row with {} messages; exactly one is required",
                        physical_join.table_name,
                        table_tuple.messages().len()
                    )));
                    }
                    if table_message.entries().count() != projection.len() {
                        return Err(ProcessorError::ProcessingError(format!(
                            "lookup provider `{}` returned {} columns, expected {}",
                            physical_join.table_name,
                            table_message.entries().count(),
                            projection.len()
                        )));
                    }

                    let mut messages = tuple.messages().to_vec();
                    let projected_values = projection
                        .iter()
                        .enumerate()
                        .map(|(value_index, column)| {
                            let value = table_message
                                .value_by_index(value_index)
                                .cloned()
                                .unwrap_or(Value::Null);
                            (column.clone(), value)
                        })
                        .collect::<HashMap<_, _>>();
                    let table_values = physical_join
                        .table_schema
                        .column_schemas()
                        .iter()
                        .map(|column| {
                            Arc::new(
                                projected_values
                                    .get(&column.name)
                                    .cloned()
                                    .unwrap_or(Value::Null),
                            )
                        })
                        .collect();
                    let table_keys = physical_join
                        .table_schema
                        .column_schemas()
                        .iter()
                        .map(|column| Arc::<str>::from(column.name.as_str()))
                        .collect();
                    messages.push(Arc::new(Message::new(
                        physical_join.table_name.clone(),
                        table_keys,
                        table_values,
                    )));
                    output_rows.push(Tuple::with_timestamp(Arc::from(messages), tuple.timestamp));
                    if output_rows.len() == OUTPUT_COLLECTION_ROWS {
                        let batch = RecordBatch::new_with_metadata_from(
                            std::mem::take(&mut output_rows),
                            input.as_ref(),
                        )
                        .map_err(|error| ProcessorError::ProcessingError(error.to_string()))?;
                        if !send_collection_with_cancellation(
                            output,
                            data_channel_capacity,
                            batch,
                            cancellation,
                            stats,
                        )
                        .await?
                        {
                            return Ok(());
                        }
                        emitted_output = true;
                    }
                }
            }

            lookup_task.finish().await?;
        }

        if !output_rows.is_empty() || !emitted_output {
            let batch = RecordBatch::new_with_metadata_from(output_rows, input.as_ref())
                .map_err(|error| ProcessorError::ProcessingError(error.to_string()))?;
            send_collection_with_cancellation(
                output,
                data_channel_capacity,
                batch,
                cancellation,
                stats,
            )
            .await?;
        }
        Ok(())
    }
}

impl Processor for LookupJoinProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let processor_id = self.id.clone();
        let physical_join = Arc::clone(&self.physical_join);
        let provider = Arc::clone(&self.provider);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let mut control_streams = fan_in_control_streams(std::mem::take(&mut self.control_inputs));
        let mut data_active = !input_streams.is_empty();
        let mut control_active = !control_streams.is_empty();
        let processor_span = stats.processor_span();

        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(
            async move {
                loop {
                    tokio::select! {
                        biased;
                        control_item = control_streams.next(), if control_active => {
                            match control_item {
                                Some(Ok(control_signal)) => {
                                    if forward_control_signal(
                                        control_signal,
                                        &task_cancellation,
                                        &control_output,
                                        channel_capacities.control,
                                    ).await? {
                                        return Ok(());
                                    }
                                }
                                Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                    log_broadcast_lagged(&processor_id, skipped, "lookup join control input");
                                }
                                None => control_active = false,
                            }
                        }
                        item = input_streams.next(), if data_active => {
                            match item {
                                Some(Ok(data)) => {
                                    log_received_data(&processor_id, &data);
                                    let is_terminal = data.is_terminal();
                                    match data {
                                        StreamData::Collection(collection) => {
                                            stats.record_collection_in(collection.num_rows() as u64);
                                            let process = Self::process_collection(
                                                collection,
                                                physical_join.as_ref(),
                                                Arc::clone(&provider),
                                                &cancellation,
                                                &output,
                                                channel_capacities.data,
                                                stats.as_ref(),
                                            );
                                            tokio::pin!(process);
                                            loop {
                                                tokio::select! {
                                                    biased;
                                                    control_item = control_streams.next(), if control_active => {
                                                        match control_item {
                                                            Some(Ok(control_signal)) => {
                                                                if forward_control_signal(
                                                                    control_signal,
                                                                    &task_cancellation,
                                                                    &control_output,
                                                                    channel_capacities.control,
                                                                ).await? {
                                                                    let _ = process.await;
                                                                    return Ok(());
                                                                }
                                                            }
                                                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                                                log_broadcast_lagged(&processor_id, skipped, "lookup join control input");
                                                            }
                                                            None => control_active = false,
                                                        }
                                                    }
                                                    result = &mut process => {
                                                        result?;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        other => {
                                            send_with_backpressure(
                                                &output,
                                                channel_capacities.data,
                                                other,
                                                Some(stats.as_ref()),
                                            ).await?;
                                        }
                                    }
                                    if is_terminal {
                                        return Ok(());
                                    }
                                }
                                Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                    log_broadcast_lagged(&processor_id, skipped, "lookup join data input");
                                }
                                None => data_active = false,
                            }
                        }
                    }

                    if !data_active && !control_active {
                        return Ok(());
                    }
                }
            },
            processor_span,
        )))
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }

    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>,
    {
        self.inputs.push(receiver.into());
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        self.control_inputs.push(receiver.into());
    }
}
