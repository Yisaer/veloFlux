//! EosWindowProcessor - buffers all rows until data-path graceful end.

use crate::model::{Collection, Tuple};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::window_metadata;
use crate::processor::{
    ControlSignal, GaugeHandle, MetricKind, MetricSpec, Processor, ProcessorError, ProcessorStart,
    ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// EOS window implementation for non-incremental aggregation plans.
pub struct EosWindowProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl EosWindowProcessor {
    pub fn new(id: impl Into<String>) -> Self {
        Self::new_with_channel_capacities(id, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

impl Processor for EosWindowProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);

        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            let mut state = EosWindowStage::new(Arc::clone(&stats));

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
                            if is_terminal {
                                state.clear();
                                tracing::info!(processor_id = %id, "stopped");
                                return Ok(());
                            }
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                if let Some(rows) = data.row_count() { stats.record_collection_in(rows); }
                                let terminal = data.is_terminal();
                                let started = std::time::Instant::now();
                                let outputs = state.process(data)?;
                                stats.record_handle_duration(started.elapsed());
                                for data in outputs.into_iter().flatten() {
                                    let rows = data.row_count();
                                    send_with_backpressure(&output, channel_capacities.data, data, Some(&stats)).await?;
                                    if let Some(rows) = rows { stats.record_collection_out(rows); }
                                }
                                if terminal { return Ok(()); }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "eos window data input");
                            }
                            None => {
                                state.clear();
                                tracing::info!(processor_id = %id, "stopped");
                                return Ok(());
                            }
                        }
                    }
                }
            }
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

pub(crate) struct EosWindowStage {
    rows: Vec<Tuple>,
    opened_at: Option<SystemTime>,
    last_seen_at: Option<SystemTime>,
    stats: Arc<ProcessorStats>,
    rows_buffered: GaugeHandle,
}

impl EosWindowStage {
    pub(crate) fn new(stats: Arc<ProcessorStats>) -> Self {
        let rows_buffered = stats.register_gauge(MetricSpec {
            id: "window.rows_buffered",
            flat_name: "rows_buffered",
            kind: MetricKind::Gauge,
        });
        rows_buffered.set(0);
        Self {
            rows: Vec::new(),
            opened_at: None,
            last_seen_at: None,
            stats,
            rows_buffered,
        }
    }

    pub(crate) fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.rows_buffered = stats.register_gauge(MetricSpec {
            id: "window.rows_buffered",
            flat_name: "rows_buffered",
            kind: MetricKind::Gauge,
        });
        self.rows_buffered.set(self.rows.len() as u64);
        self.stats = stats;
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        match data {
            StreamData::Collection(collection) => {
                self.add_collection(collection)?;
                Ok([None, None])
            }
            StreamData::Control(signal) if signal.is_terminal() => {
                let output = if signal.is_graceful_end() {
                    self.flush()?.map(StreamData::collection)
                } else {
                    self.clear();
                    None
                };
                Ok([output, Some(StreamData::control(signal))])
            }
            other => Ok([Some(other), None]),
        }
    }

    fn add_collection(&mut self, collection: Box<dyn Collection>) -> Result<(), ProcessorError> {
        let rows = match collection.into_rows() {
            Ok(rows) => rows,
            Err(err) => {
                self.stats.record_error_logged(
                    "eos window processor error",
                    format!("failed to extract rows: {err}"),
                );
                return Ok(());
            }
        };
        for row in rows {
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(err) => {
                    self.stats
                        .record_error_logged("eos window processor error", err.to_string());
                    continue;
                }
            }
            if self.opened_at.is_none() {
                self.opened_at = Some(row.timestamp);
            }
            self.last_seen_at = Some(row.timestamp);
            self.rows.push(row);
        }
        self.rows_buffered.set(self.rows.len() as u64);
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<Box<dyn Collection>>, ProcessorError> {
        if self.rows.is_empty() {
            self.rows_buffered.set(0);
            return Ok(None);
        }
        let rows = std::mem::take(&mut self.rows);
        self.rows_buffered.set(0);
        let closed_at = self
            .last_seen_at
            .take()
            .or(self.opened_at)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let collection = match window_metadata::record_batch_from_system_time(
            rows,
            self.opened_at.take().unwrap_or(closed_at),
            closed_at,
        ) {
            Ok(collection) => collection,
            Err(err) => {
                self.stats
                    .record_error_logged("eos window processor error", err.to_string());
                return Ok(None);
            }
        };
        Ok(Some(Box::new(collection)))
    }

    pub(crate) fn clear(&mut self) {
        self.rows.clear();
        self.opened_at = None;
        self.last_seen_at = None;
        self.rows_buffered.set(0);
    }
}
