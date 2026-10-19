use super::{build_group_by_meta, AggregationWorker};
use crate::aggregation::AggregateFunctionRegistry;
use crate::model::Collection;
use crate::planner::physical::PhysicalStreamingAggregation;
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
use std::sync::Arc;
use std::time::SystemTime;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// EOS-driven streaming aggregation implementation.
pub struct StreamingEosAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl StreamingEosAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            physical,
            aggregate_registry,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
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
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    fn process_collection(
        worker: &mut AggregationWorker,
        collection: &dyn Collection,
        opened_at: &mut Option<SystemTime>,
        last_seen_at: &mut Option<SystemTime>,
        stats: &ProcessorStats,
    ) -> Result<(), String> {
        for row in collection.rows() {
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(ProcessorError::ProcessingError(message)) => {
                    stats.record_error_logged("streaming EOS aggregation processor error", message);
                    continue;
                }
                Err(err) => return Err(err.to_string()),
            }
            if let Err(message) = worker.update_groups(row) {
                stats.record_error_logged("streaming EOS aggregation processor error", message);
                continue;
            }
            if opened_at.is_none() {
                *opened_at = Some(row.timestamp);
            }
            *last_seen_at = Some(row.timestamp);
        }
        Ok(())
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

impl Processor for StreamingEosAggregationProcessor {
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
        let stats = Arc::clone(&self.stats);

        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            let mut stage = StreamingEosStage::new(physical, aggregate_registry);

            loop {
                tokio::select! {
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
                                let outputs = stage.process(data, &stats)?;
                                stats.record_handle_duration(started.elapsed());
                                for data in outputs.into_iter().flatten() {
                                    let rows = data.row_count();
                                    send_with_backpressure(&output, channel_capacities.data, data, Some(&stats)).await?;
                                    if let Some(rows) = rows { stats.record_collection_out(rows); }
                                }
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

/// Synchronous EOS aggregation shared by standalone and region placement.
pub(crate) struct StreamingEosStage {
    worker: AggregationWorker,
    opened_at: Option<SystemTime>,
    last_seen_at: Option<SystemTime>,
}

impl StreamingEosStage {
    pub(crate) fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let group_by = build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        Self {
            worker: AggregationWorker::new(physical, registry, group_by),
            opened_at: None,
            last_seen_at: None,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.worker.groups.clear();
        self.opened_at = None;
        self.last_seen_at = None;
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        match data {
            StreamData::Collection(collection) => {
                StreamingEosAggregationProcessor::process_collection(
                    &mut self.worker,
                    collection.as_ref(),
                    &mut self.opened_at,
                    &mut self.last_seen_at,
                    stats,
                )
                .map_err(|error| {
                    ProcessorError::ProcessingError(format!(
                        "Failed to process streaming EOS aggregation: {error}"
                    ))
                })?;
                Ok([None, None])
            }
            StreamData::Control(signal) if signal.is_terminal() => {
                let batch = if signal.is_graceful_end() {
                    self.worker.finalize_current_window().map_err(|error| {
                        ProcessorError::ProcessingError(format!(
                            "Failed to finalize EOS aggregation: {error}"
                        ))
                    })?
                } else {
                    None
                };
                let closed_at = self
                    .last_seen_at
                    .or(self.opened_at)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let output = batch
                    .map(|batch| {
                        window_metadata::attach_from_system_time(
                            batch,
                            self.opened_at.unwrap_or(closed_at),
                            closed_at,
                        )
                    })
                    .transpose()?
                    .map(StreamData::collection);
                self.clear();
                Ok([output, Some(StreamData::control(signal))])
            }
            other => Ok([Some(other), None]),
        }
    }
}
