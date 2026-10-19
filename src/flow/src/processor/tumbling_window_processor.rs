//! TumblingWindowProcessor - buffers rows by tumbling windows and flushes on watermarks.
//!

use crate::expr::ScalarExpr;
use crate::planner::physical::{PhysicalPlan, PhysicalTumblingWindow};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::window_metadata;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};
use crate::processor::{
    ControlSignal, GaugeHandle, MetricKind, MetricSpec, Processor, ProcessorError, ProcessorStart,
    ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;

pub struct TumblingWindowProcessor {
    id: String,
    window_length: Duration,
    partition_by_scalars: Vec<ScalarExpr>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl TumblingWindowProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalTumblingWindow>) -> Self {
        Self::new_with_channel_capacities(id, physical, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalTumblingWindow>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        let length = physical.time_unit.duration(physical.length);
        let partition_by_scalars = physical.partition_by_scalars.clone();
        Self {
            id: id.into(),
            window_length: length,
            partition_by_scalars,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::TumblingWindow(window) => Some(Self::new(id, Arc::new(window.clone()))),
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

impl Processor for TumblingWindowProcessor {
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
        let partition_by_scalars = self.partition_by_scalars.clone();

        // Local state captured by the task.
        let window_length = self.window_length;
        let mut state =
            TumblingWindowStage::new(window_length, partition_by_scalars, Arc::clone(&stats));

        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
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
                                state.drop_all();
                                tracing::info!(processor_id = %id, "stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                if let Some(rows) = data.row_count() {
                                    stats.record_collection_in(rows);
                                }
                                let terminal = data.is_terminal();
                                let collection_input = matches!(data, StreamData::Collection(_));
                                let started = std::time::Instant::now();
                                let mut outputs = match state.process(data) {
                                    Ok(outputs) => outputs,
                                    Err(error) => {
                                        stats.record_error_logged("tumbling window processor error", error.to_string());
                                        stats.record_handle_duration(started.elapsed());
                                        continue;
                                    }
                                };
                                if collection_input { stats.record_handle_duration(started.elapsed()); }
                                while let Some(data) = state.next_output(&mut outputs)? {
                                    let rows = data.row_count();
                                    send_with_backpressure(
                                        &output, channel_capacities.data, data, Some(stats.as_ref()),
                                    ).await?;
                                    if let Some(rows) = rows { stats.record_collection_out(rows); }
                                }
                                if terminal { return Ok(()); }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "tumbling window data input");
                                continue;
                            }
                            None => {
                                // Upstream ended without control signal: drop buffered rows.
                                state.drop_all();
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

/// Synchronous window engine shared by channel and inline execution.
pub(crate) struct TumblingWindowStage {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<ProcessingState>,
    window_length: Duration,
    partition_by_scalars: Vec<ScalarExpr>,
    stats: Arc<ProcessorStats>,
    rows_buffered: GaugeHandle,
    buffered_rows: usize,
}

/// Incremental window output, with no task, Future, or transport ownership.
pub(crate) enum TumblingWindowOutput {
    Single(Option<StreamData>),
    Drain {
        partition: usize,
        watermark: Option<SystemTime>,
        terminal: Option<ControlSignal>,
    },
}

impl TumblingWindowStage {
    pub(crate) fn new(
        window_length: Duration,
        partition_by_scalars: Vec<ScalarExpr>,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        let rows_buffered = Self::buffer_gauge(&stats);
        rows_buffered.set(0);
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
            window_length,
            partition_by_scalars,
            stats,
            rows_buffered,
            buffered_rows: 0,
        }
    }

    fn buffer_gauge(stats: &ProcessorStats) -> GaugeHandle {
        stats.register_gauge(MetricSpec {
            id: "window.rows_buffered",
            flat_name: "rows_buffered",
            kind: MetricKind::Gauge,
        })
    }

    pub(crate) fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.rows_buffered = Self::buffer_gauge(&stats);
        self.rows_buffered.set(self.buffered_rows as u64);
        for state in &mut self.states {
            state.stats = Arc::clone(&stats);
        }
        self.stats = stats;
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
    ) -> Result<TumblingWindowOutput, ProcessorError> {
        match data {
            StreamData::Collection(collection) => {
                self.add_collection(collection)?;
                Ok(TumblingWindowOutput::Single(None))
            }
            StreamData::Watermark(watermark) => Ok(TumblingWindowOutput::Drain {
                partition: 0,
                watermark: Some(watermark),
                terminal: None,
            }),
            StreamData::Control(signal) if signal.is_terminal() => {
                if signal.is_graceful_end() {
                    Ok(TumblingWindowOutput::Drain {
                        partition: 0,
                        watermark: None,
                        terminal: Some(signal),
                    })
                } else {
                    self.drop_all();
                    Ok(TumblingWindowOutput::Single(Some(StreamData::control(
                        signal,
                    ))))
                }
            }
            other => Ok(TumblingWindowOutput::Single(Some(other))),
        }
    }

    pub(crate) fn next_output(
        &mut self,
        output: &mut TumblingWindowOutput,
    ) -> Result<Option<StreamData>, ProcessorError> {
        match output {
            TumblingWindowOutput::Single(data) => Ok(data.take()),
            TumblingWindowOutput::Drain {
                partition,
                watermark,
                terminal,
            } => {
                while let Some(state) = self.states.get_mut(*partition) {
                    let before = state.rows.len();
                    let result = state.next_window(*watermark);
                    self.buffered_rows -= before - state.rows.len();
                    self.rows_buffered.set(self.buffered_rows as u64);
                    if let Some(batch) = result? {
                        return Ok(Some(StreamData::collection(batch)));
                    }
                    *partition += 1;
                }
                Ok(terminal.take().map(StreamData::control))
            }
        }
    }

    pub(crate) fn drop_all(&mut self) {
        for state in &mut self.states {
            state.rows.clear();
        }
        self.buffered_rows = 0;
        self.rows_buffered.set(0);
    }

    fn state_for(&mut self, key: PartitionKey) -> &mut ProcessingState {
        if let Some(&id) = self.state_ids.get(&key) {
            return &mut self.states[id];
        }
        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(ProcessingState {
            rows: VecDeque::new(),
            window_length: self.window_length,
            stats: Arc::clone(&self.stats),
        });
        &mut self.states[id]
    }

    fn add_collection(
        &mut self,
        collection: Box<dyn crate::model::Collection>,
    ) -> Result<(), ProcessorError> {
        let rows = match collection.into_rows() {
            Ok(rows) => rows,
            Err(e) => {
                self.stats.record_error_logged(
                    "tumbling window processor error",
                    format!("failed to extract rows: {e}"),
                );
                return Ok(());
            }
        };
        for tuple in rows {
            let window_start = match window_metadata::floor_to_window_start(
                tuple.timestamp,
                self.window_length,
                "timestamp",
            ) {
                Ok(start) => start,
                Err(err) => {
                    self.stats
                        .record_error_logged("tumbling window processor error", err.to_string());
                    continue;
                }
            };
            let window_end = match window_start.checked_add(self.window_length) {
                Some(end) => end,
                None => {
                    self.stats.record_error_logged(
                        "tumbling window processor error",
                        "tumblingwindow end timestamp overflow".to_string(),
                    );
                    continue;
                }
            };
            if let Err(err) = window_metadata::validate_system_time(window_end) {
                self.stats
                    .record_error_logged("tumbling window processor error", err.to_string());
                continue;
            }
            let partition_key =
                match eval_partition_key(&self.partition_by_scalars, &tuple, "tumblingwindow") {
                    Ok(key) => key,
                    Err(message) => {
                        self.stats
                            .record_error_logged("tumbling window processor error", message);
                        continue;
                    }
                };
            self.state_for(partition_key).rows.push_back(tuple);
            self.buffered_rows += 1;
        }
        self.rows_buffered.set(self.buffered_rows as u64);
        Ok(())
    }
}

/// Per-partition ordered row buffer; transport is owned by the caller.
struct ProcessingState {
    rows: VecDeque<crate::model::Tuple>,
    window_length: Duration,
    stats: Arc<ProcessorStats>,
}

impl ProcessingState {
    fn next_window(
        &mut self,
        watermark: Option<SystemTime>,
    ) -> Result<Option<Box<dyn crate::model::Collection>>, ProcessorError> {
        while let Some(front) = self.rows.front() {
            let start = match window_metadata::floor_to_window_start(
                front.timestamp,
                self.window_length,
                "timestamp",
            ) {
                Ok(start) => start,
                Err(error) => {
                    self.stats
                        .record_error_logged("tumbling window processor error", error.to_string());
                    self.rows.pop_front();
                    continue;
                }
            };
            let end = match start.checked_add(self.window_length) {
                Some(end) => end,
                None => {
                    self.stats.record_error_logged(
                        "tumbling window processor error",
                        "tumblingwindow end timestamp overflow",
                    );
                    self.rows.pop_front();
                    continue;
                }
            };
            if watermark.is_some_and(|limit| end > limit) {
                return Ok(None);
            }
            let mut rows = Vec::new();
            while let Some(front) = self.rows.front() {
                let row_start = match window_metadata::floor_to_window_start(
                    front.timestamp,
                    self.window_length,
                    "timestamp",
                ) {
                    Ok(start) => start,
                    Err(error) => {
                        self.stats.record_error_logged(
                            "tumbling window processor error",
                            error.to_string(),
                        );
                        self.rows.pop_front();
                        continue;
                    }
                };
                if row_start != start {
                    break;
                }
                rows.push(self.rows.pop_front().ok_or_else(|| {
                    ProcessorError::ProcessingError(
                        "tumbling window row buffer corrupted during flush".into(),
                    )
                })?);
            }
            if rows.is_empty() {
                continue;
            }
            match window_metadata::record_batch_from_system_time(rows, start, end) {
                Ok(batch) => return Ok(Some(Box::new(batch))),
                Err(error) => self
                    .stats
                    .record_error_logged("tumbling window processor error", error.to_string()),
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::PhysicalTumblingWindow;
    use crate::processor::base::{Processor, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::processor::{BarrierControlSignal, ProcessorStats};
    use std::time::UNIX_EPOCH;
    use tokio::time::timeout;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        )
    }

    fn tuple_at_millis(millis: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(millis),
        )
    }

    #[tokio::test]
    async fn tumbling_window_graceful_end_flushes_buffer_before_terminal_control() {
        let spawner = test_spawner();
        let physical = PhysicalTumblingWindow::new(TimeUnit::Seconds, 10, Vec::new(), 0);
        let mut processor = TumblingWindowProcessor::new("tw", Arc::new(physical));
        let stats = Arc::new(ProcessorStats::collection_in_out());
        processor.set_stats(Arc::clone(&stats));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![tuple_at(1), tuple_at(2)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");

        let StreamData::Collection(collection) = first else {
            panic!("expected buffered collection before terminal control");
        };
        assert_eq!(collection.rows().len(), 2);
        assert!(matches!(
            second,
            StreamData::Control(ControlSignal::Barrier(
                BarrierControlSignal::StreamGracefulEnd { .. }
            ))
        ));
        assert_eq!(stats.snapshot().records_out, Some(2));
    }

    #[tokio::test]
    async fn tumbling_window_skips_invalid_timestamp_and_preserves_buffered_rows() {
        let spawner = test_spawner();
        let physical = PhysicalTumblingWindow::new(TimeUnit::Seconds, 10, Vec::new(), 0);
        let mut processor = TumblingWindowProcessor::new("tw", Arc::new(physical));
        let stats = Arc::new(ProcessorStats::collection_in_out());
        processor.set_stats(Arc::clone(&stats));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let invalid_timestamp = UNIX_EPOCH
            .checked_sub(Duration::from_secs(1))
            .expect("one second before epoch is representable");
        let invalid_tuple = crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            invalid_timestamp,
        );
        let batch = crate::model::RecordBatch::new(vec![tuple_at(1), invalid_tuple, tuple_at(2)])
            .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::watermark(UNIX_EPOCH + Duration::from_secs(10)))
            .is_ok());

        let item = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(collection) = item else {
            panic!("expected tumbling window collection");
        };
        assert_eq!(collection.rows().len(), 2);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.error_count, 1);
        assert!(snapshot
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("invalid timestamp")));
    }

    #[tokio::test]
    async fn millisecond_tumbling_window_uses_exact_boundaries_and_metadata() {
        let spawner = test_spawner();
        let physical = PhysicalTumblingWindow::new(TimeUnit::Milliseconds, 100, Vec::new(), 0);
        let mut processor = TumblingWindowProcessor::new("tw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_at_millis(1_050),
            tuple_at_millis(1_099),
            tuple_at_millis(1_100),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_099),
            ))
            .is_ok());
        assert!(
            timeout(Duration::from_millis(100), output_rx.recv())
                .await
                .is_err(),
            "window must not flush before its end boundary"
        );

        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_100),
            ))
            .is_ok());
        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(first) = first else {
            panic!("expected first millisecond tumbling window");
        };
        assert_eq!(first.num_rows(), 2);
        let metadata = first.metadata().window.as_deref().expect("window metadata");
        assert_eq!(metadata.start.epoch_micros(), 1_000_000);
        assert_eq!(metadata.end.epoch_micros(), 1_100_000);

        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_200),
            ))
            .is_ok());
        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(second) = second else {
            panic!("expected second millisecond tumbling window");
        };
        assert_eq!(second.num_rows(), 1);
    }
}
