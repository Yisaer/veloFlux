//! BatchProcessor - aggregates collections based on count/duration thresholds.

use crate::expr::ScalarExpr;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::batch_policy::BatchPolicy;
use crate::processor::count_window_stage::CountWindowStage;
use crate::processor::stage_clock::StageClock;
use crate::processor::timed_batch_stage::TimedBatchStage;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
#[cfg(test)]
use datatypes::Value;
use futures::{future::poll_fn, stream::StreamExt};
#[cfg(test)]
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::broadcast;
#[cfg(test)]
use tokio::time::Sleep;
use tokio::time::{Duration, Instant};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Processor that buffers collections before releasing them downstream.
pub struct BatchProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    batch_count: Option<usize>,
    batch_duration: Option<Duration>,
    partition_by_scalars: Vec<ScalarExpr>,
    stats: Arc<ProcessorStats>,
}

pub(crate) type BatchMode = BatchPolicy;

impl BatchProcessor {
    pub(crate) fn validate_batch_config(
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
    ) -> Result<(), ProcessorError> {
        Self::batch_mode(batch_count, batch_duration).map(|_| ())
    }

    pub(crate) fn batch_mode(
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
    ) -> Result<BatchMode, ProcessorError> {
        BatchPolicy::validate(batch_count, batch_duration, "batch processor")?;
        if batch_count.is_none() && batch_duration.is_none() {
            return Err(ProcessorError::InvalidConfiguration(
                "batch processor requires batch_count or batch_duration".into(),
            ));
        }
        Ok(BatchPolicy::new(batch_count, batch_duration))
    }

    pub fn new(
        id: impl Into<String>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            batch_count,
            batch_duration,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        Self::new_partitioned_with_channel_capacities(
            id,
            batch_count,
            batch_duration,
            Vec::new(),
            channel_capacities,
        )
    }

    pub(crate) fn new_partitioned_with_channel_capacities(
        id: impl Into<String>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
        partition_by_scalars: Vec<ScalarExpr>,
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
            batch_count,
            batch_duration,
            partition_by_scalars,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }

    #[cfg(test)]
    fn next_boundary(origin: Instant, duration: Duration, now: Instant) -> Instant {
        BatchPolicy::next_boundary(origin, duration, now)
    }
    #[cfg(test)]
    pub(crate) fn schedule_timer(
        timer: &mut Option<Pin<Box<Sleep>>>,
        origin: Instant,
        duration: Duration,
        has_data: bool,
    ) {
        BatchPolicy::schedule_timer(timer, origin, duration, has_data);
    }
}

impl Processor for BatchProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let processor_id = self.id.clone();
        let mode = match Self::batch_mode(self.batch_count, self.batch_duration) {
            Ok(mode) => mode,
            Err(err) => return ProcessorStart::failed(spawner, err),
        };
        let stats = Arc::clone(&self.stats);
        let partition_by_scalars = self.partition_by_scalars.clone();

        if !partition_by_scalars.is_empty() && !matches!(mode, BatchMode::CountOnly { .. }) {
            return ProcessorStart::failed(
                spawner,
                ProcessorError::InvalidConfiguration(
                    "partitioned batch processor only supports count mode".into(),
                ),
            );
        }
        if let BatchMode::CountOnly { count } = mode {
            let mut stage = match CountWindowStage::new(count, partition_by_scalars) {
                Ok(stage) => stage,
                Err(error) => return ProcessorStart::failed(spawner, error),
            };
            let processor_span = stats.processor_span();
            return ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
                loop {
                    let mut handle_started = None;
                    let (mut outputs, terminal) = tokio::select! {
                        biased;
                        control_item = control_streams.next(), if control_active => {
                            match control_item {
                                Some(Ok(signal)) => {
                                    let terminal = signal.is_terminal();
                                    send_control_with_backpressure(&control_output, channel_capacities.control, signal).await?;
                                    if terminal { (stage.finish(None), true) } else { continue; }
                                }
                                _ => { control_active = false; continue; }
                            }
                        }
                        item = input_streams.next() => {
                            match item {
                                Some(Ok(data)) => {
                                    log_received_data(&processor_id, &data);
                                    if let Some(rows) = data.row_count() { stats.record_collection_in(rows); }
                                    let terminal = data.is_terminal();
                                    handle_started = Some(std::time::Instant::now());
                                    (stage.process(data, &stats)?, terminal)
                                }
                                Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                    log_broadcast_lagged(&processor_id, skipped, "batch data input"); continue;
                                }
                                None => (stage.finish(None), true),
                            }
                        }
                    };
                    let started = handle_started.unwrap_or_else(std::time::Instant::now);
                    while let Some(data) = stage.next_output(&mut outputs)? {
                        let rows = data.row_count();
                        send_with_backpressure(&output, channel_capacities.data, data, Some(&stats)).await?;
                        if let Some(rows) = rows { stats.record_collection_out(rows); }
                    }
                    stats.record_handle_duration(started.elapsed());
                    if terminal { return Ok(()); }
                }
            }, processor_span)));
        }

        let Some(duration) = self.batch_duration else {
            return ProcessorStart::failed(
                spawner,
                ProcessorError::InvalidConfiguration("timed batching requires a duration".into()),
            );
        };
        let mut stage = match TimedBatchStage::new(self.batch_count, duration) {
            Ok(stage) => stage,
            Err(error) => return ProcessorStart::failed(spawner, error),
        };
        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            stage.start(Instant::now());
            let mut clock = StageClock::new(None);
            loop {
                clock.update(stage.deadline());
                let (mut outputs, terminal) = tokio::select! {
                    biased;
                    item = control_streams.next(), if control_active => {
                        match item {
                            Some(Ok(signal)) => {
                                let terminal = signal.is_terminal();
                                send_control_with_backpressure(&control_output, channel_capacities.control, signal).await?;
                                if terminal { (stage.finish(None), true) } else { continue; }
                            }
                            _ => { control_active = false; continue; }
                        }
                    }
                    _ = poll_fn(|cx| clock.poll_next(cx)) => (stage.finish(None), false),
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                if let Some(rows) = data.row_count() { stats.record_collection_in(rows); }
                                let terminal = data.is_terminal();
                                (stage.process(data), terminal)
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "batch data input"); continue;
                            }
                            None => (stage.finish(None), true),
                        }
                    }
                };
                let mut started = std::time::Instant::now();
                let mut elapsed = Duration::ZERO;
                while let Some(data) = stage.next_output(&mut outputs)? {
                    let rows = data.row_count();
                    elapsed += started.elapsed();
                    send_with_backpressure(&output, channel_capacities.data, data, Some(&stats)).await?;
                    started = std::time::Instant::now();
                    if let Some(rows) = rows { stats.record_collection_out(rows); }
                }
                stats.record_handle_duration(elapsed + started.elapsed());
                if terminal { return Ok(()); }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::batch_from_columns_simple;
    use crate::processor::base::{DEFAULT_CONTROL_CHANNEL_CAPACITY, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::runtime::TaskSpawner;
    use tokio::time::Duration;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    #[tokio::test]
    async fn test_batch_processor_count_only() {
        let spawner = test_spawner();
        let mut processor = BatchProcessor::new("batch_count", Some(2), None);
        let (tx, rx) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(rx);
        let (control_tx, control_rx) = broadcast::channel(DEFAULT_CONTROL_CHANNEL_CAPACITY);
        processor.add_control_input(control_rx);
        processor.output.subscribe(); // ensure there is at least one subscriber
        let mut output = processor
            .subscribe_output()
            .expect("output available to subscribe");
        processor.start(&spawner);

        let data = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "val".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )])
        .expect("batch");
        let _ = tx.send(StreamData::collection(Box::new(data)));

        let first = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("first batch timeout")
            .expect("first batch missing");
        assert_eq!(
            first.as_collection().unwrap().num_rows(),
            2,
            "count-only should flush first two rows"
        );
        drop(tx);
        let second = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("second batch timeout")
            .expect("second batch missing");
        assert_eq!(
            second.as_collection().unwrap().num_rows(),
            1,
            "remaining row should flush on close"
        );
        let _ = control_tx.send(ControlSignal::Instant(
            crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 0 },
        ));
        let _ = control_tx.send(ControlSignal::Instant(
            crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 0 },
        ));
    }

    #[tokio::test]
    async fn test_batch_processor_duration_only() {
        let spawner = test_spawner();
        let mut processor =
            BatchProcessor::new("batch_duration", None, Some(Duration::from_millis(50)));
        let (tx, rx) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(rx);
        let (control_tx, control_rx) = broadcast::channel(DEFAULT_CONTROL_CHANNEL_CAPACITY);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().expect("output");
        processor.start(&spawner);

        let data = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "val".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("batch");
        let _ = tx.send(StreamData::collection(Box::new(data)));

        let batch = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("duration batch timeout")
            .expect("duration batch missing");
        assert_eq!(
            batch.as_collection().unwrap().num_rows(),
            1,
            "duration-only should flush after timeout"
        );
        let _ = control_tx.send(ControlSignal::Instant(
            crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 0 },
        ));
    }

    #[test]
    fn batch_duration_must_fit_supported_millisecond_range() {
        let too_small =
            BatchProcessor::validate_batch_config(None, Some(Duration::from_nanos(999)))
                .expect_err("sub-millisecond duration should be rejected");
        assert!(matches!(too_small, ProcessorError::InvalidConfiguration(_)));

        let fractional_ms =
            BatchProcessor::validate_batch_config(None, Some(Duration::from_micros(1500)))
                .expect_err("sub-millisecond precision should be rejected");
        assert!(matches!(
            fractional_ms,
            ProcessorError::InvalidConfiguration(_)
        ));

        let too_large =
            BatchProcessor::validate_batch_config(None, Some(Duration::from_secs(u64::MAX)))
                .expect_err("duration beyond u64 milliseconds should be rejected");
        assert!(matches!(too_large, ProcessorError::InvalidConfiguration(_)));
    }

    #[test]
    fn next_boundary_aligns_to_fixed_grid_and_is_right_open() {
        let d = Duration::from_secs(10);
        let origin = Instant::now();

        // Tuples arriving at different offsets within the same cell [0, 10s) all
        // resolve to the SAME boundary: the grid never re-anchors to a batch's
        // first tuple.
        for offset_ms in [1u64, 3_000, 9_999] {
            assert_eq!(
                BatchProcessor::next_boundary(origin, d, origin + Duration::from_millis(offset_ms)),
                origin + d,
            );
        }
        assert_eq!(
            BatchProcessor::next_boundary(origin, d, origin + Duration::from_nanos(9_999_999_999)),
            origin + d,
        );

        // Right-open [t, t+D): a `now` exactly on a grid line opens the new cell at
        // that line, so its boundary is one full period later.
        assert_eq!(BatchProcessor::next_boundary(origin, d, origin), origin + d);
        assert_eq!(
            BatchProcessor::next_boundary(origin, d, origin + d),
            origin + Duration::from_secs(20),
        );

        // Catch-up across missed boundaries jumps to the next future boundary.
        assert_eq!(
            BatchProcessor::next_boundary(origin, d, origin + Duration::from_secs(35)),
            origin + Duration::from_secs(40),
        );
    }
}
