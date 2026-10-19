use super::{build_group_by_meta, AggregationWorker};
use crate::aggregation::AggregateFunctionRegistry;
use crate::model::Collection;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::window_metadata;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::processor::window_partition::{eval_partition_key, PartitionKey};

/// Tracks progress for a count-based window.
struct CountWindowState {
    target: u64,
    seen: u64,
    opened_at: Option<SystemTime>,
}

impl CountWindowState {
    fn new(target: u64) -> Self {
        Self {
            target,
            seen: 0,
            opened_at: None,
        }
    }

    fn register_row_and_check_finalize(&mut self, now: SystemTime) -> Option<SystemTime> {
        if self.seen == 0 {
            self.opened_at = Some(now);
        }
        self.seen += 1;
        if self.seen >= self.target {
            return Some(self.opened_at.take().unwrap_or(now));
        }
        None
    }

    fn reset(&mut self) {
        self.seen = 0;
        self.opened_at = None;
    }
}

/// Data-driven count window implementation.
pub struct StreamingCountAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    target: u64,
    stats: Arc<ProcessorStats>,
}

impl StreamingCountAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        target: u64,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            physical,
            aggregate_registry,
            target,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        target: u64,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            target,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

impl Processor for StreamingCountAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let physical = Arc::clone(&self.physical);
        let target = self.target;
        let stats = Arc::clone(&self.stats);

        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            let mut stage = StreamingCountStage::new(physical, aggregate_registry, target);

            loop {
                tokio::select! {
                    // Handle control signals first if present
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
                            if is_terminal {
                                break;
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                if let Some(rows) = data.row_count() { stats.record_collection_in(rows); }
                                let terminal = data.is_terminal();
                                let started = std::time::Instant::now();
                                let mut outputs = stage.process(data);
                                while let Some(data) = stage.next_output(&mut outputs, &stats)? {
                                    let rows = data.row_count();
                                    send_with_backpressure(&output, channel_capacities.data, data, Some(&stats)).await?;
                                    if let Some(rows) = rows { stats.record_collection_out(rows); }
                                }
                                stats.record_handle_duration(started.elapsed());
                                if terminal { break; }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                log_broadcast_lagged(&id, n, "data input");
                            }
                            None => {
                                tracing::info!(processor_id = %id, "all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
        }, processor_span)))
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

/// Synchronous row-at-a-time aggregation; downstream waits stay in the caller.
pub(crate) struct StreamingCountStage {
    template: AggregationWorker,
    global: CountWindowState,
    partitions: PartitionedCountAggregationState,
    partition_by: Vec<crate::expr::ScalarExpr>,
    target: u64,
}

pub(crate) enum StreamingCountOutput {
    Single(Option<StreamData>),
    Rows {
        collection: Box<dyn Collection>,
        next_row: usize,
    },
}

impl StreamingCountStage {
    pub(crate) fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        registry: Arc<AggregateFunctionRegistry>,
        target: u64,
    ) -> Self {
        let group_by = build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let partition_by = match &physical.window {
            StreamingWindowSpec::Count {
                partition_by_scalars,
                ..
            } => partition_by_scalars.clone(),
            _ => Vec::new(),
        };
        Self {
            template: AggregationWorker::new(physical, registry, group_by),
            global: CountWindowState::new(target),
            partitions: PartitionedCountAggregationState::new(),
            partition_by,
            target,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.template.groups.clear();
        self.global.reset();
        self.partitions.states.clear();
        self.partitions.state_ids.clear();
    }

    pub(crate) fn process(&mut self, data: StreamData) -> StreamingCountOutput {
        if data.is_terminal() {
            self.clear();
        }
        match data {
            StreamData::Collection(collection) => StreamingCountOutput::Rows {
                collection,
                next_row: 0,
            },
            other => StreamingCountOutput::Single(Some(other)),
        }
    }

    pub(crate) fn next_output(
        &mut self,
        output: &mut StreamingCountOutput,
        stats: &ProcessorStats,
    ) -> Result<Option<StreamData>, ProcessorError> {
        let (collection, next_row) = match output {
            StreamingCountOutput::Single(data) => return Ok(data.take()),
            StreamingCountOutput::Rows {
                collection,
                next_row,
            } => (collection, next_row),
        };
        while let Some(row) = collection.rows().get(*next_row) {
            *next_row += 1;
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(ProcessorError::ProcessingError(message)) => {
                    stats.record_error_logged(
                        "streaming count aggregation processor error",
                        message,
                    );
                    continue;
                }
                Err(error) => {
                    return Err(ProcessorError::ProcessingError(format!(
                        "Failed to process streaming count aggregation: {error}",
                    )))
                }
            }
            let (worker, window) = if self.partition_by.is_empty() {
                (&mut self.template, &mut self.global)
            } else {
                let key = match eval_partition_key(&self.partition_by, row, "countwindow") {
                    Ok(key) => key,
                    Err(error) => {
                        stats.record_error_logged(
                            "streaming count aggregation processor error",
                            error,
                        );
                        continue;
                    }
                };
                let state = self
                    .partitions
                    .get_or_insert(key, self.target, &self.template);
                (&mut state.worker, &mut state.window)
            };
            if let Err(error) = worker.update_groups(row) {
                stats.record_error_logged("streaming count aggregation processor error", error);
                continue;
            }
            if let Some(start) = window.register_row_and_check_finalize(row.timestamp) {
                let batch = worker.finalize_current_window().map_err(|error| {
                    ProcessorError::ProcessingError(format!(
                        "Failed to process streaming count aggregation: {error}"
                    ))
                })?;
                window.reset();
                if let Some(batch) = batch {
                    let batch =
                        window_metadata::attach_from_system_time(batch, start, row.timestamp)
                            .map_err(|error| {
                                ProcessorError::ProcessingError(format!(
                                    "Failed to process streaming count aggregation: {error}"
                                ))
                            })?;
                    return Ok(Some(StreamData::collection(batch)));
                }
            }
        }
        Ok(None)
    }
}

struct PartitionCountAggregationState {
    window: CountWindowState,
    worker: AggregationWorker,
}

struct PartitionedCountAggregationState {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<PartitionCountAggregationState>,
}

impl PartitionedCountAggregationState {
    fn new() -> Self {
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
        }
    }

    fn get_or_insert(
        &mut self,
        key: PartitionKey,
        target: u64,
        template: &AggregationWorker,
    ) -> &mut PartitionCountAggregationState {
        if let Some(id) = self.state_ids.get(&key).copied() {
            return &mut self.states[id];
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(PartitionCountAggregationState {
            window: CountWindowState::new(target),
            worker: AggregationWorker::new(
                Arc::clone(&template.physical),
                Arc::clone(&template.aggregate_registry),
                template.group_by_meta.clone(),
            ),
        });
        &mut self.states[id]
    }
}
