//! BatchProcessor - aggregates collections based on count/duration thresholds.

use crate::expr::ScalarExpr;
use crate::model::{Collection, Tuple};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::batch_policy::BatchPolicy;
use crate::processor::delivery_output::ChannelDeliveryOutput;
use crate::processor::native_batch_stage::NativeBatchStage;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
#[cfg(test)]
use datatypes::Value;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::broadcast;
use tokio::time::{Duration, Instant, Sleep};
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

struct CountPartitionBuffers {
    buffer_ids: HashMap<PartitionKey, usize>,
    buffers: Vec<Vec<Tuple>>,
}

impl CountPartitionBuffers {
    fn new() -> Self {
        Self {
            buffer_ids: HashMap::new(),
            buffers: Vec::new(),
        }
    }

    fn buffer_for(&mut self, key: PartitionKey) -> &mut Vec<Tuple> {
        if let Some(id) = self.buffer_ids.get(&key).copied() {
            return &mut self.buffers[id];
        }

        let id = self.buffers.len();
        self.buffer_ids.insert(key, id);
        self.buffers.push(Vec::new());
        &mut self.buffers[id]
    }

    fn buffers_mut(&mut self) -> impl Iterator<Item = &mut Vec<Tuple>> {
        self.buffers.iter_mut()
    }
}

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

    #[allow(clippy::too_many_arguments)]
    async fn flush_all(
        processor_id: &str,
        buffer: &mut Vec<Tuple>,
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        let mut stage_output = ChannelDeliveryOutput {
            output,
            capacity: data_channel_capacity,
            stats,
        };
        NativeBatchStage::flush_all(processor_id, buffer, &mut stage_output, stats).await
    }
    #[allow(clippy::too_many_arguments)]
    async fn drain_by_count(
        processor_id: &str,
        buffer: &mut Vec<Tuple>,
        output: &LinkOutput<StreamData>,
        count: usize,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        let mut stage_output = ChannelDeliveryOutput {
            output,
            capacity: data_channel_capacity,
            stats,
        };
        NativeBatchStage::drain_by_count(processor_id, buffer, &mut stage_output, count, stats)
            .await
    }
    fn append_partitioned_collection(
        partitions: &mut CountPartitionBuffers,
        partition_by_scalars: &[ScalarExpr],
        collection: Box<dyn Collection>,
        stats: &ProcessorStats,
    ) -> Result<(), ProcessorError> {
        let rows = collection.into_rows().map_err(|err| {
            ProcessorError::ProcessingError(format!("failed to extract rows: {err}"))
        })?;
        for tuple in rows {
            let key = match eval_partition_key(partition_by_scalars, &tuple, "countwindow") {
                Ok(key) => key,
                Err(message) => {
                    stats.record_error_logged("batch processor error", message);
                    continue;
                }
            };
            partitions.buffer_for(key).push(tuple);
        }
        Ok(())
    }

    async fn drain_partitioned_by_count(
        processor_id: &str,
        partitions: &mut CountPartitionBuffers,
        output: &LinkOutput<StreamData>,
        count: usize,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        for buffer in partitions.buffers_mut() {
            Self::drain_by_count(
                processor_id,
                buffer,
                output,
                count,
                data_channel_capacity,
                stats,
            )
            .await?;
        }
        Ok(())
    }

    async fn flush_partitioned_all(
        processor_id: &str,
        partitions: &mut CountPartitionBuffers,
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        for buffer in partitions.buffers_mut() {
            Self::flush_all(processor_id, buffer, output, data_channel_capacity, stats).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn next_boundary(origin: Instant, duration: Duration, now: Instant) -> Instant {
        BatchPolicy::next_boundary(origin, duration, now)
    }
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

        if !partition_by_scalars.is_empty() {
            let BatchMode::CountOnly { count } = mode else {
                return ProcessorStart::failed(
                    spawner,
                    ProcessorError::InvalidConfiguration(
                        "partitioned batch processor only supports count mode".to_string(),
                    ),
                );
            };
            let processor_span = stats.processor_span();
            return ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
                let mut partitions = CountPartitionBuffers::new();
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
                                    BatchProcessor::flush_partitioned_all(
                                        &processor_id,
                                        &mut partitions,
                                        &output,
                                        channel_capacities.data,
                                        &stats,
                                    )
                                    .await?;
                                    tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
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
                                    log_received_data(&processor_id, &data);
                                    match data {
                                        StreamData::Collection(collection) => {
                                            stats.record_collection_in(collection.num_rows() as u64);
                                            let handle_start = std::time::Instant::now();
                                            let res = async {
                                                BatchProcessor::append_partitioned_collection(
                                                    &mut partitions,
                                                    &partition_by_scalars,
                                                    collection,
                                                    stats.as_ref(),
                                                )?;
                                                BatchProcessor::drain_partitioned_by_count(
                                                    &processor_id,
                                                    &mut partitions,
                                                    &output,
                                                    count,
                                                    channel_capacities.data,
                                                    &stats,
                                                )
                                                .await
                                            }
                                            .await;
                                            stats.record_handle_duration(handle_start.elapsed());
                                            res?;
                                        }
                                        data => {
                                            let is_terminal = data.is_terminal();
                                            if is_terminal {
                                                BatchProcessor::flush_partitioned_all(
                                                    &processor_id,
                                                    &mut partitions,
                                                    &output,
                                                    channel_capacities.data,
                                                    &stats,
                                                )
                                                .await?;
                                            }
                                            send_with_backpressure(
                                                &output,
                                                channel_capacities.data,
                                                data,
                                                Some(stats.as_ref()),
                                            )
                                            .await?;
                                            if is_terminal {
                                                tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                                Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                    log_broadcast_lagged(&processor_id, skipped, "batch data input");
                                    continue;
                                }
                                None => {
                                    BatchProcessor::flush_partitioned_all(
                                        &processor_id,
                                        &mut partitions,
                                        &output,
                                        channel_capacities.data,
                                        &stats,
                                    )
                                    .await?;
                                    tracing::info!(processor_id = %processor_id, "input streams closed");
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }, processor_span)));
        }

        let policy = BatchPolicy::new(self.batch_count, self.batch_duration);
        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            let mut buffer: Vec<Tuple> = Vec::new();
            let mut timer: Option<Pin<Box<Sleep>>> = None;
            // Fixed grid origin, captured once before any tuple so duration windows
            // never re-anchor to a batch's first tuple. See `next_boundary`.
            let grid_origin = Instant::now();
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
                                    BatchProcessor::flush_all(
                                        &processor_id,
                                        &mut buffer,
                                        &output,
                                        channel_capacities.data,
                                        &stats,
                                    )
                                    .await?;
                                    tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                    return Ok(());
                                }
                                continue;
                            } else {
                                control_active = false;
                        }
                    }
                    _ = async {
                        if let Some(timer) = &mut timer {
                            timer.as_mut().await;
                        }
                    }, if timer.is_some() => {
                        BatchProcessor::flush_all(
                            &processor_id,
                            &mut buffer,
                            &output,
                            channel_capacities.data,
                            &stats,
                        )
                        .await?;
                        if let BatchMode::DurationOnly { duration } | BatchMode::Combined { duration, .. } = &mode {
                            BatchProcessor::schedule_timer(&mut timer, grid_origin, *duration, !buffer.is_empty());
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                match data {
                                    StreamData::Collection(collection) => {
                                        stats.record_collection_in(collection.num_rows() as u64);
                                        let handle_start = std::time::Instant::now();
                                        let mut stage_output = ChannelDeliveryOutput {
                                            output: &output, capacity: channel_capacities.data, stats: &stats,
                                        };
                                        let res = NativeBatchStage::feed(
                                            &processor_id, collection.rows().iter().cloned(),
                                            &mut buffer, &mut stage_output, &mut timer,
                                            grid_origin, &policy, &stats,
                                        ).await;
                                        // Handle duration measures per-collection processing wall time. If this handle
                                        // triggers one or more count-based flushes, the downstream send/backpressure
                                        // time is intentionally included.
                                        stats.record_handle_duration(handle_start.elapsed());
                                        res?;
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        if is_terminal {
                                            BatchProcessor::flush_all(
                                                &processor_id,
                                                &mut buffer,
                                                &output,
                                                channel_capacities.data,
                                                &stats,
                                            )
                                            .await?;
                                        }
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                            return Ok(());
                                        }
                                        if let BatchMode::DurationOnly { duration }
                                        | BatchMode::Combined { duration, .. } = &mode
                                        {
                                            BatchProcessor::schedule_timer(
                                                &mut timer,
                                                grid_origin,
                                                *duration,
                                                !buffer.is_empty(),
                                            );
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "batch data input");
                                continue;
                            }
                            None => {
                                BatchProcessor::flush_all(
                                    &processor_id,
                                    &mut buffer,
                                    &output,
                                    channel_capacities.data,
                                    &stats,
                                )
                                .await?;
                                tracing::info!(processor_id = %processor_id, "input streams closed");
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
