//! FusedTransformProcessor - a fused stateless transform chain in one task.
//!
//! Produced by the `fuse_stateless_transform` physical optimizer rule out of
//! `input -> [Order?] -> [Filter?] -> Project -> [EmptySuppress?]
//! -> [Batch?] -> [Encode?] -> [Compress?] -> [Encrypt?]`, sitting directly
//! below the (unfused) sink connector node.
//!
//! Collection transforms reuse the same stage functions as the standalone
//! tasks; bytes stay bytes downstream of encode. The task owns no delivery
//! machinery: completed payloads and collections are forwarded downstream,
//! and retry, capture, and checkpointing stay in the standalone sink task
//! (external I/O is never fused, like lookup joins).
use crate::model::{Collection, Tuple};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::batch_policy::BatchPolicy;
use crate::processor::batch_processor::{BatchMode, BatchProcessor};
use crate::processor::data_metrics::EncoderMetrics;
use crate::processor::delivery_output::ChannelDeliveryOutput;
use crate::processor::empty_suppress_processor::collection_is_effectively_empty;
use crate::processor::encoder_stage::EncoderStage;
use crate::processor::filter_processor::apply_filter;
use crate::processor::filter_processor::FilterStage;
use crate::processor::inline_delivery::ByteTransformChain;
use crate::processor::native_batch_stage::NativeBatchStage;
use crate::processor::order_processor::apply_order;
use crate::processor::order_processor::OrderStage;
use crate::processor::project_processor::apply_project;
use crate::processor::project_processor::ProjectStage;

use crate::processor::{
    ControlSignal, CounterHandle, MetricKind, MetricSpec, Processor, ProcessorError,
    ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{Instant, Sleep};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Single-task replacement for a fusible sink delivery chain.
///
/// Gates are enforced by the optimizer rule; this task assumes them:
/// no row-diff/column-filter in the span, single-child spine everywhere.
pub struct FusedTransformProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    /// Task configuration assembled once by the builder; moved into the
    /// task at start; runtime rebuilds construct a fresh processor.
    stages: FusedRowStages,
    encoder: Mutex<Option<Box<dyn crate::codec::encoder::SinkEncoder>>>,
    /// Ordered byte-transform stages (compress/encrypt) shared with the
    /// standalone sink processors; taken into the task on first start.
    pipeline: Mutex<Option<ByteTransformChain>>,
    batch_count: Option<usize>,
    batch_duration: Option<Duration>,
    stats: Arc<ProcessorStats>,
}

/// Stage configuration for the fused collection path, mirroring the
/// unfused processor chain:
/// `input -> [Order] -> [Filter] -> Project -> (EmptySuppress) -> ...`
///
/// Each stage bundles exactly what its shared `apply_*` function needs;
/// the raw SQL ASTs and plan topology stay in the plan nodes.
#[derive(Clone)]
pub(crate) struct FusedRowStages {
    pub(crate) filter: Option<FilterStage>,
    pub(crate) project: ProjectStage,
    pub(crate) order: Option<OrderStage>,
    pub(crate) filter_before_order: bool,
    pub(crate) empty_suppress_omit: bool,
    /// Holds the EmptySuppress counters when `omit_if_empty` inserted that
    /// stage: `(collections_forwarded, collections_suppressed)`.
    pub(crate) empty_suppress_counters: Option<(CounterHandle, CounterHandle)>,
}

/// Shared encoder state and inline byte-stage connections owned by the task.
struct FusedWriters {
    metrics: Option<EncoderMetrics>,
    encoder: Option<EncoderStage>,
    pipeline: ByteTransformChain,
}

impl FusedTransformProcessor {
    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        stages: FusedRowStages,
        encoder: Option<Box<dyn crate::codec::encoder::SinkEncoder>>,
        pipeline: ByteTransformChain,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
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
            stages,
            encoder: Mutex::new(encoder),
            pipeline: Mutex::new(Some(pipeline)),
            batch_count,
            batch_duration,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for FusedTransformProcessor {
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
        let stats = Arc::clone(&self.stats);
        let mut stages = self.stages.clone();
        if stages.empty_suppress_omit {
            stages.empty_suppress_counters = Some((
                stats.register_counter(MetricSpec {
                    id: "empty_suppress.collections_forwarded",
                    flat_name: "collections_forwarded",
                    kind: MetricKind::Counter,
                }),
                stats.register_counter(MetricSpec {
                    id: "empty_suppress.collections_suppressed",
                    flat_name: "collections_suppressed",
                    kind: MetricKind::Counter,
                }),
            ));
        }
        let encoder = match self.encoder.lock().map(|mut encoder| encoder.take()) {
            Ok(encoder) => encoder,
            Err(_) => {
                return ProcessorStart::failed(
                    spawner,
                    ProcessorError::ProcessingError(
                        "fused transform encoder mutex poisoned".to_string(),
                    ),
                );
            }
        };
        let pipeline = match self.pipeline.lock().map(|mut guard| guard.take()) {
            Ok(Some(pipeline)) => pipeline,
            Ok(None) => {
                return ProcessorStart::failed(
                    spawner,
                    ProcessorError::ProcessingError(
                        "fused transform processor already started".into(),
                    ),
                );
            }
            Err(_) => {
                return ProcessorStart::failed(
                    spawner,
                    ProcessorError::ProcessingError(
                        "fused transform pipeline mutex poisoned".to_string(),
                    ),
                );
            }
        };
        let processor_id = self.id.clone();
        if encoder.is_none() {
            tracing::info!(processor_id = %processor_id, "fused transform delivery mode: native");
        } else {
            tracing::info!(processor_id = %processor_id, "fused transform delivery mode: encoded");
        }
        let metrics = if encoder.is_some() {
            Some(EncoderMetrics::new(&stats))
        } else {
            stats.declare_collection_in_out();
            None
        };
        let mut writers = FusedWriters {
            metrics,
            encoder: encoder.map(|encoder| {
                EncoderStage::new(
                    encoder,
                    BatchPolicy::new(self.batch_count, self.batch_duration),
                )
            }),
            pipeline,
        };

        let batch_mode = if self.batch_count.is_some() || self.batch_duration.is_some() {
            match BatchProcessor::batch_mode(self.batch_count, self.batch_duration) {
                Ok(mode) => Some(mode),
                Err(err) => return ProcessorStart::failed(spawner, err),
            }
        } else {
            None
        };
        let native_policy = BatchPolicy::new(self.batch_count, self.batch_duration);
        let processor_span = stats.processor_span();
        let handle = spawner.spawn(tracing::Instrument::instrument(async move {
            let mut batch_buffer: Vec<Tuple> = Vec::new();
            let mut batch_timer: Option<Pin<Box<Sleep>>> = None;
            // Fixed grid origin, captured once before any tuple: duration
            // batch windows never re-anchor, mirroring BatchProcessor.
            let batch_grid_origin = Instant::now();

            loop {
                tokio::select! {
                    biased;

                    // Encoded delivery flushes before forwarding terminal
                    // control, like SinkEncoderProcessor. Native batching
                    // retains BatchProcessor's existing control ordering.
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            let encoded = writers.encoder.is_some();
                            if is_terminal && encoded {
                                if let Err(err) = Self::flush_batch_all(
                                    &mut writers, &mut batch_buffer, &stats,
                                    &output, channel_capacities.data,
                                ).await {
                                    stats.record_error_logged("fused flush error", err.to_string());
                                    if writers.pipeline.has_transforms() && matches!(err, ProcessorError::ChannelClosed) {
                                        return Err(err);
                                    }
                                }
                            }
                            send_control_with_backpressure(
                                &control_output, channel_capacities.control, control_signal,
                            ).await?;
                            if is_terminal {
                                if !encoded {
                                    Self::flush_batch_all(
                                        &mut writers, &mut batch_buffer, &stats,
                                        &output, channel_capacities.data,
                                    ).await?;
                                }
                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }

                    // ── ② batch timer — flushes buffered rows / in-progress
                    // delivery on duration boundaries ──
                    _ = async {
                        if let Some(timer) = &mut batch_timer {
                            timer.as_mut().await;
                        }
                    }, if batch_timer.is_some() => {
                        if batch_mode.is_some() {
                            let flush_result = Self::flush_batch_all(
                                &mut writers,
                                &mut batch_buffer,
                                &stats,
                                &output,
                                channel_capacities.data,
                            )
                            .await;
                            if writers.encoder.is_some() {
                                if let Err(err) = flush_result {
                                    stats.record_error_logged("fused flush error", err.to_string());
                                    if writers.pipeline.has_transforms() && matches!(err, ProcessorError::ChannelClosed) {
                                        return Err(err);
                                    }
                                }
                            } else {
                                flush_result?;
                            }
                            if let Some(
                                BatchMode::DurationOnly { duration }
                                | BatchMode::Combined { duration, .. },
                            ) = batch_mode.as_ref()
                            {
                                BatchProcessor::schedule_timer(
                                    &mut batch_timer,
                                    batch_grid_origin,
                                    *duration,
                                    !batch_buffer.is_empty()
                                        || writers.encoder.as_ref().is_some_and(EncoderStage::has_data),
                                );
                            }
                        }
                    }

                    // ── ③ data ──
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                if let Some(rows) = data.row_count() {
                                    stats.record_collection_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = Instant::now();
                                        match Self::stage_collection(
                                            &stages,
                                            collection,
                                            &stats,
                                        ) {
                                            Err(err) => {
                                                tracing::error!(
                                                    processor_id = %processor_id,
                                                    error = %err,
                                                    "fused stage error"
                                                );
                                                stats.record_handle_duration(
                                                    handle_start.elapsed(),
                                                );
                                                stats.record_error(err.to_string());
                                                continue;
                                            }
                                            Ok(None) => {
                                                stats.record_handle_duration(
                                                    handle_start.elapsed(),
                                                );
                                                continue;
                                            }
                                            Ok(Some(staged)) => {
                                                if let Some(encoder) = writers.encoder.as_mut() {
                                                    let channel = ChannelDeliveryOutput { output: &output, capacity: channel_capacities.data, stats: &stats };
                                                    let mut stage_output = writers.pipeline.output(channel, &stats, writers.metrics.as_ref().expect("encoded metrics"));
                                                    let send_result = encoder.feed(
                                                        &processor_id, staged, &mut stage_output,
                                                        &mut batch_timer, batch_grid_origin,
                                                        None,
                                                    ).await;
                                                    if let Err(err) = send_result {
                                                        stats.record_handle_duration(handle_start.elapsed());
                                                        stats.record_error_logged("fused encoder error", err.to_string());
                                                        if writers.pipeline.has_transforms() && matches!(err, ProcessorError::ChannelClosed) {
                                                            return Err(err);
                                                        }
                                                        continue;
                                                    }
                                                } else if batch_mode.is_some() {
                                                    let rows = match staged.into_rows() {
                                                        Ok(rows) => rows,
                                                        Err(err) => {
                                                            stats.record_handle_duration(handle_start.elapsed());
                                                            stats.record_error(err.to_string());
                                                            continue;
                                                        }
                                                    };
                                                    let mut stage_output = ChannelDeliveryOutput { output: &output, capacity: channel_capacities.data, stats: &stats };
                                                    NativeBatchStage::feed(
                                                        &processor_id, rows, &mut batch_buffer,
                                                        &mut stage_output, &mut batch_timer,
                                                        batch_grid_origin, &native_policy, &stats,
                                                    ).await?;
                                                } else {
                                                    let rows = staged.num_rows() as u64;
                                                    let send_res = send_with_backpressure(
                                                        &output,
                                                        channel_capacities.data,
                                                        StreamData::Collection(staged),
                                                        Some(stats.as_ref()),
                                                    )
                                                    .await;
                                                    send_res?;
                                                    stats.record_collection_out(rows);
                                                }
                                                stats.record_handle_duration(handle_start.elapsed());
                                            }
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        if is_terminal {
                                            let flush_result = Self::flush_batch_all(
                                                &mut writers,
                                                &mut batch_buffer,
                                                &stats,
                                                &output,
                                                channel_capacities.data,
                                            )
                                            .await;
                                            if writers.encoder.is_some() {
                                                if let Err(err) = flush_result {
                                                    stats.record_error_logged("fused flush error", err.to_string());
                                                    if writers.pipeline.has_transforms() && matches!(err, ProcessorError::ChannelClosed) {
                                                        return Err(err);
                                                    }
                                                }
                                            } else {
                                                flush_result?;
                                            }
                                            send_with_backpressure(
                                                &output,
                                                channel_capacities.data,
                                                data,
                                                Some(stats.as_ref()),
                                            )
                                            .await?;
                                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %processor_id, "stopped");
                                            return Ok(());
                                        }
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "fused transform data input");
                                continue;
                            }
                            None => {
                                let flush_result = Self::flush_batch_all(
                                    &mut writers,
                                    &mut batch_buffer,
                                    &stats,
                                    &output,
                                    channel_capacities.data,
                                )
                                .await;
                                if writers.encoder.is_some() {
                                    if let Err(err) = flush_result {
                                        stats.record_error_logged("fused flush error", err.to_string());
                                        if writers.pipeline.has_transforms() && matches!(err, ProcessorError::ChannelClosed) {
                                            return Err(err);
                                        }
                                    }
                                } else {
                                    flush_result?;
                                }
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }, processor_span));
        ProcessorStart::ready(handle)
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

impl FusedTransformProcessor {
    /// Filter stage of the collection path. A filter matching nothing emits
    /// nothing (`WHERE` semantics, like `FilterProcessor`): `Ok(None)` with
    /// no counter movement.
    fn apply_filter_stage(
        stages: &FusedRowStages,
        current: Box<dyn Collection>,
        stats: &ProcessorStats,
    ) -> Result<Option<Box<dyn Collection>>, ProcessorError> {
        let Some(filter) = stages.filter.as_ref() else {
            return Ok(Some(current));
        };
        let filtered = apply_filter(current.as_ref(), filter, Some(stats))?;
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(filtered))
    }

    /// Order/filter/project stages plus the `omit_if_empty` gate, in the
    /// recorded peel order. Returns `Ok(None)` for filter-empty (no counters)
    /// and suppressed (suppressed +1) inputs; otherwise the staged collection
    /// (forwarded +1 when the suppress stage exists).
    fn stage_collection(
        stages: &FusedRowStages,
        collection: Box<dyn Collection>,
        stats: &ProcessorStats,
    ) -> Result<Option<Box<dyn Collection>>, ProcessorError> {
        let mut current = collection;
        if stages.filter_before_order {
            current = match Self::apply_filter_stage(stages, current, stats)? {
                Some(filtered) => filtered,
                None => return Ok(None),
            };
            if let Some(order) = stages.order.as_ref() {
                current = apply_order(current, order, Some(stats))?;
            }
        } else {
            if let Some(order) = stages.order.as_ref() {
                current = apply_order(current, order, Some(stats))?;
            }
            current = match Self::apply_filter_stage(stages, current, stats)? {
                Some(filtered) => filtered,
                None => return Ok(None),
            };
        }
        let projected = apply_project(current.as_ref(), &stages.project, Some(stats))?;
        if stages.empty_suppress_omit && collection_is_effectively_empty(projected.as_ref()) {
            if let Some((_, suppressed)) = stages.empty_suppress_counters.as_ref() {
                suppressed.inc_by(1);
            }
            return Ok(None);
        }
        if let Some((forwarded, _)) = stages.empty_suppress_counters.as_ref() {
            forwarded.inc_by(1);
        }
        Ok(Some(projected))
    }

    async fn flush_batch_all(
        writers: &mut FusedWriters,
        batch_buffer: &mut Vec<Tuple>,
        stats: &Arc<ProcessorStats>,
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
    ) -> Result<(), ProcessorError> {
        let channel = ChannelDeliveryOutput {
            output,
            capacity: data_channel_capacity,
            stats,
        };
        if let Some(encoder) = writers.encoder.as_mut() {
            let mut stage_output = writers.pipeline.output(
                channel,
                stats,
                writers.metrics.as_ref().expect("encoded metrics"),
            );
            encoder.flush("fused", &mut stage_output, None).await
        } else {
            let mut stage_output = channel;
            NativeBatchStage::flush_all("fused", batch_buffer, &mut stage_output, stats).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encoder::{EncodeError, SinkEncoder};
    use crate::expr::ScalarExpr;
    use crate::model::{Message, RecordBatch, Tuple};
    use crate::planner::physical::PipelineStateUsage;
    use crate::processor::order_processor::OrderKey;
    use bytes::Bytes;
    use datatypes::Value;

    struct FixedEncoder {
        payload: Bytes,
    }

    impl crate::codec::encoder::SinkEncoder for FixedEncoder {
        fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
            Ok(None)
        }

        fn append(&mut self, _record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
            Ok(Some(self.payload.clone()))
        }

        fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
            Ok(None)
        }
    }

    fn sample_batch() -> RecordBatch {
        let message = Arc::new(Message::new(
            "test",
            vec![Arc::from("value")],
            vec![Value::Int64(1)],
        ));
        let tuple = Tuple::new(vec![message]);
        RecordBatch::new(vec![tuple]).expect("build batch")
    }

    fn sample_stats() -> ProcessorStats {
        let stats = ProcessorStats::default();
        stats.declare_collection_in_out();
        stats
    }

    fn sample_stages() -> FusedRowStages {
        FusedRowStages {
            filter: None,
            project: ProjectStage {
                fields: Arc::new([]),
                state: None,
                usage: Default::default(),
            },
            order: None,
            filter_before_order: true,
            empty_suppress_omit: false,
            empty_suppress_counters: None,
        }
    }

    fn order_key_asc_v() -> OrderKey {
        OrderKey {
            compiled: ScalarExpr::Column(crate::expr::scalar::ColumnRef::ByIndex {
                source_name: "stream".to_string(),
                column_index: 0,
            }),
            asc: true,
        }
    }

    fn batch_with_v(values: &[i64]) -> RecordBatch {
        let rows = values
            .iter()
            .map(|v| {
                let message = Arc::new(Message::new(
                    "stream",
                    vec![Arc::from("v")],
                    vec![Value::Int64(*v)],
                ));
                Tuple::new(vec![message])
            })
            .collect();
        RecordBatch::new(rows).expect("build batch")
    }

    fn stages_with_order(filter_before_order: bool) -> FusedRowStages {
        FusedRowStages {
            filter: None,
            project: ProjectStage {
                fields: Arc::new([]),
                state: None,
                usage: PipelineStateUsage::default(),
            },
            order: Some(OrderStage {
                keys: vec![order_key_asc_v()],
            }),
            filter_before_order,
            empty_suppress_omit: false,
            empty_suppress_counters: None,
        }
    }

    // coverage-covers: sink.fused_stateless_sink
    #[test]
    fn fused_order_sorts_collection_path_for_both_peel_orders() {
        for filter_before_order in [true, false] {
            let stats = sample_stats();
            let stages = stages_with_order(filter_before_order);
            let batch = batch_with_v(&[3, 1, 2]);
            let staged =
                FusedTransformProcessor::stage_collection(&stages, Box::new(batch), &stats)
                    .expect("order path succeeds")
                    .expect("rows survive");
            let got: Vec<i64> = staged
                .rows()
                .iter()
                .map(|tuple| match tuple.value_by_index("stream", 0) {
                    Some(Value::Int64(v)) => *v,
                    other => panic!("unexpected order value: {other:?}"),
                })
                .collect();
            assert_eq!(
                got,
                vec![1, 2, 3],
                "filter_before_order={filter_before_order}"
            );
        }
    }

    // coverage-covers: sink.fused_stateless_sink
    #[test]
    fn fused_stateful_filter_counts_through_collection_path() {
        use crate::expr::func::BinaryFunc;
        use crate::expr::ProcStateField;
        use datatypes::{ConcreteDatatype, Int64Type};
        use std::sync::atomic::Ordering;
        use std::time::{Duration, UNIX_EPOCH};

        fn tuple_at_ms(ms: u64) -> Tuple {
            Tuple::with_timestamp(
                Tuple::empty_messages(),
                UNIX_EPOCH + Duration::from_millis(ms),
            )
        }

        let state = Arc::new(crate::processor::processor_state::ProcessorState::new());
        let expr = ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            }),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(1500),
                ConcreteDatatype::Int64(Int64Type),
            )),
        };
        let stats = sample_stats();
        let stages = FusedRowStages {
            filter: Some(FilterStage {
                expr,
                state: Some(Arc::clone(&state)),
                usage: PipelineStateUsage {
                    last_hit_time_unix_ms: true,
                    ..PipelineStateUsage::default()
                },
            }),
            project: ProjectStage {
                fields: Arc::new([]),
                state: None,
                usage: PipelineStateUsage::default(),
            },
            order: None,
            filter_before_order: true,
            empty_suppress_omit: false,
            empty_suppress_counters: None,
        };
        let batch = RecordBatch::new(vec![
            tuple_at_ms(1000),
            tuple_at_ms(2000),
            tuple_at_ms(3000),
        ])
        .expect("build batch");
        let staged = FusedTransformProcessor::stage_collection(&stages, Box::new(batch), &stats)
            .expect("staged path succeeds")
            .expect("rows survive");
        assert_eq!(staged.num_rows(), 2);
        assert_eq!(
            state.last_hit_time_unix_ms.load(Ordering::Relaxed),
            2000,
            "row-scoped state must advance exactly like FilterProcessor"
        );
    }

    // coverage-covers: sink.fused_stateless_sink
    #[test]
    fn fused_empty_suppress_counts_forwarded_and_suppressed() {
        use crate::expr::func::BinaryFunc;
        use datatypes::{ConcreteDatatype, Int64Type};

        let stats = sample_stats();
        let forwarded = stats.register_counter(MetricSpec {
            id: "empty_suppress.collections_forwarded",
            flat_name: "collections_forwarded",
            kind: MetricKind::Counter,
        });
        let suppressed = stats.register_counter(MetricSpec {
            id: "empty_suppress.collections_suppressed",
            flat_name: "collections_suppressed",
            kind: MetricKind::Counter,
        });
        let stages = FusedRowStages {
            filter: None,
            project: ProjectStage {
                fields: Arc::new([]),
                state: None,
                usage: PipelineStateUsage::default(),
            },
            order: None,
            filter_before_order: true,
            empty_suppress_omit: true,
            empty_suppress_counters: Some((forwarded, suppressed)),
        };
        // Empty projection suppresses, like EmptySuppressProcessor. Masks
        // never reach this check: projection rebuilds tuples without them,
        // exactly like the standalone Project task.
        let batch = RecordBatch::empty();
        let staged = FusedTransformProcessor::stage_collection(&stages, Box::new(batch), &stats)
            .expect("stage succeeds");
        assert!(staged.is_none());
        // Plain collection forwards.
        let batch = sample_batch();
        let staged = FusedTransformProcessor::stage_collection(&stages, Box::new(batch), &stats)
            .expect("stage succeeds");
        assert!(staged.is_some());
        // Filter matching nothing moves no counters.
        let never = ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::Literal(
                Value::Int64(2),
                ConcreteDatatype::Int64(Int64Type),
            )),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(1),
                ConcreteDatatype::Int64(Int64Type),
            )),
        };
        let filtered_stages = FusedRowStages {
            filter: Some(FilterStage {
                expr: never,
                state: None,
                usage: PipelineStateUsage::default(),
            }),
            project: ProjectStage {
                fields: Arc::new([]),
                state: None,
                usage: PipelineStateUsage::default(),
            },
            order: None,
            filter_before_order: true,
            empty_suppress_omit: true,
            empty_suppress_counters: stages.empty_suppress_counters.clone(),
        };
        let batch = sample_batch();
        let staged =
            FusedTransformProcessor::stage_collection(&filtered_stages, Box::new(batch), &stats)
                .expect("filter path succeeds");
        assert!(staged.is_none());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.custom.get("collections_suppressed"), Some(&1));
        assert_eq!(snapshot.custom.get("collections_forwarded"), Some(&1));
    }

    // coverage-covers: sink.fused_stateless_sink, pipeline.runtime.stats
    #[tokio::test]
    async fn fused_native_immediate_counts_successful_collection_output() {
        use crate::processor::base::default_channel_capacities;
        let stats = Arc::new(ProcessorStats::default());
        let mut processor = FusedTransformProcessor::new_with_channel_capacities(
            "native",
            sample_stages(),
            None,
            ByteTransformChain::new(None, None),
            None,
            None,
            default_channel_capacities(),
        );
        processor.set_stats(Arc::clone(&stats));
        let (input, receiver) = tokio::sync::broadcast::channel(8);
        processor.add_input(receiver);
        let mut output = processor.subscribe_output().expect("output");
        let handle = processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
        input
            .send(StreamData::Collection(Box::new(sample_batch())))
            .unwrap_or_else(|_| panic!("send row"));
        input
            .send(StreamData::Collection(Box::new(RecordBatch::empty())))
            .unwrap_or_else(|_| panic!("send empty"));
        input
            .send(StreamData::stream_end())
            .unwrap_or_else(|_| panic!("send terminal"));
        for rows in [1, 0] {
            let data = tokio::time::timeout(Duration::from_secs(2), output.recv())
                .await
                .expect("output timeout")
                .expect("collection output");
            assert_eq!(data.as_collection().expect("collection").num_rows(), rows);
        }
        assert!(output.recv().await.expect("terminal").is_terminal());
        handle.await.expect("join").expect("processor result");
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.records_in, Some(1));
        assert_eq!(snapshot.records_out, Some(1));
        assert_eq!(snapshot.custom.get("collections_in"), Some(&2));
        assert_eq!(snapshot.custom.get("collections_out"), Some(&2));
        assert!(!snapshot.custom.contains_key("messages_out"));
    }

    // coverage-covers: sink.fused_stateless_sink, pipeline.runtime.stats
    #[tokio::test]
    async fn fused_immediate_empty_delivery_preserves_format_and_metric_domains() {
        use crate::codec::{JsonEncoder, SinkEncoderFactory};
        use crate::planner::physical::output_layout::OutputLayout;
        use crate::planner::sink::SinkEncoderConfig;
        use crate::processor::base::default_channel_capacities;
        for (format, expected) in [("array", b"[]".as_slice()), ("ndjson", b"".as_slice())] {
            let mut props = serde_json::Map::new();
            props.insert("format".into(), serde_json::Value::String(format.into()));
            let config = SinkEncoderConfig::new("json", props);
            let factory = Arc::new(JsonEncoder::new("json", &config).expect("factory"))
                .with_output_layout(Arc::new(OutputLayout::new(Vec::new())))
                .expect("output layout");
            let encoder = factory.start_encoder().expect("encoder");
            let stats = Arc::new(ProcessorStats::default());
            let mut processor = FusedTransformProcessor::new_with_channel_capacities(
                format,
                sample_stages(),
                Some(encoder),
                ByteTransformChain::new(None, None),
                None,
                None,
                default_channel_capacities(),
            );
            processor.set_stats(Arc::clone(&stats));
            let (input, receiver) = tokio::sync::broadcast::channel(8);
            processor.add_input(receiver);
            let mut output = processor.subscribe_output().expect("output");
            let handle =
                processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
            input
                .send(StreamData::Collection(Box::new(RecordBatch::empty())))
                .unwrap_or_else(|_| panic!("send empty"));
            input
                .send(StreamData::stream_end())
                .unwrap_or_else(|_| panic!("send terminal"));
            let data = tokio::time::timeout(Duration::from_secs(2), output.recv())
                .await
                .expect("output timeout")
                .expect("delivery output");
            let StreamData::EncodedDelivery { flags, bytes } = data else {
                panic!("expected encoded delivery for {format}");
            };
            assert!(flags.contains(crate::processor::EncodedDeliveryFlags::START));
            assert!(flags.contains(crate::processor::EncodedDeliveryFlags::END));
            assert_eq!(bytes.as_ref(), expected, "format={format}");
            assert!(output.recv().await.expect("terminal").is_terminal());
            handle.await.expect("join").expect("processor result");
            let snapshot = stats.snapshot();
            assert_eq!(snapshot.records_in, Some(0));
            assert_eq!(snapshot.records_out, None);
            assert_eq!(snapshot.custom.get("collections_in"), Some(&1));
            assert!(!snapshot.custom.contains_key("collections_out"));
            assert_eq!(snapshot.custom.get("messages_out"), Some(&1));
            assert_eq!(
                snapshot.custom.get("bytes_out"),
                Some(&(expected.len() as u64))
            );
        }
    }

    #[derive(Default)]
    struct EncoderObservation {
        appends: std::sync::atomic::AtomicUsize,
        appended: tokio::sync::Notify,
        finished: tokio::sync::Notify,
    }

    struct ObservedEncoder(Arc<EncoderObservation>);

    impl SinkEncoder for ObservedEncoder {
        fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
            Ok(None)
        }
        fn append(&mut self, _record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
            self.0
                .appends
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.appended.notify_one();
            Ok(Some(Bytes::from_static(b"AB")))
        }
        fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
            self.0.finished.notify_one();
            Ok(None)
        }
    }

    // coverage-covers: sink.fused_stateless_sink, pipeline.runtime.stats
    #[tokio::test]
    async fn fused_encoded_control_terminal_flushes_first_and_records_send_failure() {
        use crate::processor::base::LinkKind;
        use crate::processor::InstantControlSignal;
        use futures::FutureExt;
        for closed_output in [false, true] {
            let observed = Arc::new(EncoderObservation::default());
            let stats = Arc::new(ProcessorStats::default());
            let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
            let mut processor = FusedTransformProcessor::new_with_channel_capacities(
                "encoded",
                sample_stages(),
                Some(Box::new(ObservedEncoder(Arc::clone(&observed)))),
                ByteTransformChain::new(None, None),
                Some(2),
                None,
                capacities,
            );
            processor.set_stats(Arc::clone(&stats));
            let (input, input_rx) = tokio::sync::broadcast::channel(8);
            let (control, control_rx) = tokio::sync::broadcast::channel(8);
            processor.add_input(input_rx);
            processor.add_control_input(control_rx);
            let mut data_output = Some(processor.subscribe_output().expect("data output"));
            let mut control_output = processor
                .subscribe_control_output()
                .expect("control output");
            if closed_output {
                drop(data_output.take());
            }
            let handle =
                processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
            assert!(input
                .send(StreamData::Collection(Box::new(sample_batch())))
                .is_ok());
            tokio::time::timeout(Duration::from_secs(2), observed.appended.notified())
                .await
                .expect("partial batch encoded");
            assert!(control
                .send(ControlSignal::Instant(
                    InstantControlSignal::StreamQuickEnd { signal_id: 1 }
                ))
                .is_ok());
            tokio::time::timeout(Duration::from_secs(2), observed.finished.notified())
                .await
                .expect("terminal initiates flush");
            if let Some(data_output) = data_output.as_mut() {
                assert!(
                    control_output.recv().now_or_never().is_none(),
                    "terminal cannot overtake a blocked flush"
                );
                let StreamData::EncodedDelivery { bytes, .. } =
                    tokio::time::timeout(Duration::from_secs(2), data_output.recv())
                        .await
                        .expect("tail timeout")
                        .expect("tail delivery")
                else {
                    panic!("expected tail")
                };
                assert_eq!(bytes.as_ref(), b"AB");
                let StreamData::EncodedDelivery { flags, bytes } =
                    data_output.recv().await.expect("tail END")
                else {
                    panic!("expected END")
                };
                assert!(flags.contains(crate::processor::EncodedDeliveryFlags::END));
                assert!(bytes.is_empty());
            }
            let signal = tokio::time::timeout(Duration::from_secs(2), control_output.recv())
                .await
                .expect("control timeout")
                .expect("control terminal");
            assert!(signal.is_terminal());
            tokio::time::timeout(Duration::from_secs(2), async { handle.await })
                .await
                .expect("processor stop timeout")
                .expect("join")
                .expect("standalone encoder records flush errors and still stops normally");
            assert_eq!(stats.snapshot().error_count, 2 * u64::from(closed_output));
            assert_eq!(
                stats.snapshot().custom.get("messages_out"),
                Some(&u64::from(!closed_output))
            );
        }
    }

    // coverage-covers: sink.fused_stateless_sink, pipeline.runtime.stats
    #[tokio::test]
    async fn fused_finish_failure_drops_remaining_input_like_standalone_encoder() {
        use crate::processor::base::default_channel_capacities;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct FailFirstFinishEncoder {
            appends: Arc<AtomicUsize>,
            fail_finish: bool,
        }
        impl SinkEncoder for FailFirstFinishEncoder {
            fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                Ok(None)
            }
            fn append(&mut self, _record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
                self.appends.fetch_add(1, Ordering::SeqCst);
                Ok(Some(Bytes::from_static(b"AB")))
            }
            fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                if std::mem::take(&mut self.fail_finish) {
                    Err(EncodeError::Other("finish failure".into()))
                } else {
                    Ok(None)
                }
            }
        }
        let appends = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(ProcessorStats::default());
        let mut processor = FusedTransformProcessor::new_with_channel_capacities(
            "encoded",
            sample_stages(),
            Some(Box::new(FailFirstFinishEncoder {
                appends: Arc::clone(&appends),
                fail_finish: true,
            })),
            ByteTransformChain::new(None, None),
            Some(1),
            None,
            default_channel_capacities(),
        );
        processor.set_stats(Arc::clone(&stats));
        let (input, receiver) = tokio::sync::broadcast::channel(8);
        processor.add_input(receiver);
        let mut output = processor.subscribe_output().expect("output");
        let handle = processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
        let batch = RecordBatch::new((0..3).map(|_| sample_batch().rows()[0].clone()).collect())
            .expect("batch");
        assert!(input.send(StreamData::Collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::Collection(Box::new(sample_batch())))
            .is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());
        let StreamData::EncodedDelivery { bytes, .. } =
            tokio::time::timeout(Duration::from_secs(2), output.recv())
                .await
                .expect("output timeout")
                .expect("recovered delivery")
        else {
            panic!("expected delivery")
        };
        assert_eq!(bytes.as_ref(), b"AB");
        assert!(tokio::time::timeout(Duration::from_secs(2), output.recv())
            .await
            .expect("terminal timeout")
            .expect("terminal")
            .is_terminal());
        handle.await.expect("join").expect("processor result");
        assert_eq!(
            appends.load(Ordering::SeqCst),
            2,
            "failed input remainder is dropped; next input still runs"
        );
        assert_eq!(stats.snapshot().error_count, 1);
        assert_eq!(stats.snapshot().custom.get("messages_out"), Some(&1));
        assert_eq!(stats.snapshot().custom.get("messages_aborted"), Some(&1));
    }

    fn input_batch(rows: usize) -> RecordBatch {
        RecordBatch::new(
            (0..rows)
                .map(|_| sample_batch().rows()[0].clone())
                .collect(),
        )
        .expect("batch")
    }

    struct RecoveringCompressor {
        phase: &'static str,
        failed: bool,
    }
    impl crate::codec::compress::CompressWriter for RecoveringCompressor {
        fn begin_delivery(&mut self) -> Result<(), crate::codec::compress::CompressError> {
            self.check("begin")
        }
        fn write(
            &mut self,
            bytes: &[u8],
            out: &mut Vec<u8>,
        ) -> Result<(), crate::codec::compress::CompressError> {
            self.check("write")?;
            out.extend_from_slice(bytes);
            Ok(())
        }
        fn finish(
            &mut self,
            _out: &mut Vec<u8>,
        ) -> Result<(), crate::codec::compress::CompressError> {
            self.check("finish")
        }
        fn abort_delivery(&mut self) {}
    }
    impl RecoveringCompressor {
        fn check(&mut self, phase: &str) -> Result<(), crate::codec::compress::CompressError> {
            if phase == self.phase && !self.failed {
                self.failed = true;
                Err(crate::codec::compress::CompressError::Compress(
                    "injected stage failure".into(),
                ))
            } else {
                Ok(())
            }
        }
    }

    async fn collect_chain(
        fused: bool,
        encoder: Box<dyn SinkEncoder>,
        count: Option<usize>,
        duration: Option<Duration>,
        inputs: &[usize],
        compressor: Option<Box<dyn crate::codec::compress::CompressWriter>>,
        encryptor: Option<Box<dyn crate::codec::EncryptWriter>>,
    ) -> Vec<(crate::processor::EncodedDeliveryFlags, Bytes)> {
        use crate::processor::base::{default_channel_capacities, LinkKind};
        use crate::processor::byte_stage::{CompressDriver, EncryptDriver};
        use crate::processor::{SinkCompressProcessor, SinkEncoderProcessor, SinkEncryptProcessor};
        let capacities = default_channel_capacities().with_link_kind(LinkKind::Mpsc);
        let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
        let (input, receiver) = tokio::sync::broadcast::channel(16);
        let mut handles = Vec::new();
        let mut output = if fused {
            let mut processor = FusedTransformProcessor::new_with_channel_capacities(
                "paired",
                sample_stages(),
                Some(encoder),
                ByteTransformChain::new(
                    compressor.map(CompressDriver::new),
                    encryptor.map(EncryptDriver::new),
                ),
                count,
                duration,
                capacities,
            );
            processor.add_input(receiver);
            let output = processor.subscribe_output().expect("output");
            handles.push(processor.start(&spawner));
            output
        } else {
            let mut encoder = SinkEncoderProcessor::new_with_channel_capacities(
                "encoder", encoder, count, duration, capacities,
            );
            encoder.add_input(receiver);
            let mut output = encoder.subscribe_output().expect("encoder output");
            if let Some(writer) = compressor {
                let mut processor = SinkCompressProcessor::new_with_channel_capacities(
                    "compress", writer, capacities,
                );
                processor.add_input(output);
                output = processor.subscribe_output().expect("compress output");
                handles.push(processor.start(&spawner));
            }
            if let Some(writer) = encryptor {
                let mut processor = SinkEncryptProcessor::new_with_channel_capacities(
                    "encrypt", writer, capacities,
                );
                processor.add_input(output);
                output = processor.subscribe_output().expect("encrypt output");
                handles.push(processor.start(&spawner));
            }
            handles.push(encoder.start(&spawner));
            output
        };
        for rows in inputs {
            assert!(input
                .send(StreamData::Collection(Box::new(input_batch(*rows))))
                .is_ok());
        }
        assert!(input.send(StreamData::stream_end()).is_ok());
        let mut events = Vec::new();
        loop {
            let data = tokio::time::timeout(Duration::from_secs(2), output.recv())
                .await
                .expect("output timeout")
                .expect("output");
            match data {
                StreamData::EncodedDelivery { flags, bytes } => events.push((flags, bytes)),
                terminal if terminal.is_terminal() => break,
                _ => panic!("unexpected output"),
            }
        }
        for handle in handles {
            handle.await.expect("join").expect("chain result");
        }
        events
    }

    // coverage-covers: sink.fused_stateless_sink
    #[tokio::test]
    async fn shared_encoder_events_match_across_execution_topologies() {
        for (count, duration) in [
            (None, None),
            (Some(2), None),
            (None, Some(Duration::from_secs(60))),
            (Some(2), Some(Duration::from_secs(60))),
        ] {
            let mut outputs = Vec::new();
            for fused in [false, true] {
                outputs.push(
                    collect_chain(
                        fused,
                        Box::new(FixedEncoder {
                            payload: Bytes::from_static(b"AB"),
                        }),
                        count,
                        duration,
                        &[0, 3, 0, 2, 1],
                        None,
                        None,
                    )
                    .await,
                );
            }
            assert_eq!(
                outputs[0], outputs[1],
                "count={count:?}, duration={duration:?}"
            );
        }
    }

    struct RecoveringEncryptor {
        phase: &'static str,
        failed: bool,
    }
    impl RecoveringEncryptor {
        fn check(&mut self, phase: &str) -> Result<(), crate::codec::encrypt::EncryptError> {
            if self.phase == phase && !self.failed {
                self.failed = true;
                Err(crate::codec::encrypt::EncryptError::Encrypt(
                    "injected encrypt failure".into(),
                ))
            } else {
                Ok(())
            }
        }
    }
    impl crate::codec::EncryptWriter for RecoveringEncryptor {
        fn begin_delivery(
            &mut self,
            _out: &mut Vec<u8>,
        ) -> Result<(), crate::codec::encrypt::EncryptError> {
            self.check("begin")
        }
        fn write(
            &mut self,
            bytes: &[u8],
            out: &mut Vec<u8>,
        ) -> Result<(), crate::codec::encrypt::EncryptError> {
            self.check("write")?;
            out.extend_from_slice(bytes);
            Ok(())
        }
        fn finish(
            &mut self,
            bytes: &[u8],
            out: &mut Vec<u8>,
        ) -> Result<(), crate::codec::encrypt::EncryptError> {
            self.check("finish")?;
            out.extend_from_slice(bytes);
            Ok(())
        }
        fn abort_delivery(&mut self) {}
    }

    // coverage-covers: sink.fused_stateless_sink
    #[tokio::test]
    async fn shared_transform_failures_preserve_later_count_batches() {
        for transform in ["compress", "encrypt"] {
            for phase in ["begin", "write", "finish"] {
                let partial = transform == "encrypt" && phase == "write";
                let (count, inputs) = if partial {
                    (2, vec![1, 3])
                } else {
                    (1, vec![3])
                };
                let mut outputs = Vec::new();
                for fused in [false, true] {
                    let compressor = RecoveringCompressor {
                        phase: if transform == "compress" {
                            phase
                        } else {
                            "never"
                        },
                        failed: false,
                    };
                    let encryptor = (transform == "encrypt").then(|| {
                        Box::new(RecoveringEncryptor {
                            phase,
                            failed: false,
                        }) as Box<dyn crate::codec::EncryptWriter>
                    });
                    outputs.push(
                        collect_chain(
                            fused,
                            Box::new(FixedEncoder {
                                payload: Bytes::from_static(b"AB"),
                            }),
                            Some(count),
                            None,
                            &inputs,
                            Some(Box::new(compressor)),
                            encryptor,
                        )
                        .await,
                    );
                }
                assert_eq!(
                    outputs[0], outputs[1],
                    "transform={transform}, phase={phase}"
                );
                assert_eq!(
                    outputs[1].len(),
                    if partial { 1 } else { 2 },
                    "later batches survive transform={transform}, phase={phase}"
                );
                assert!(outputs[1].iter().all(|(_, bytes)| bytes.as_ref() == b"AB"));
            }
        }
    }

    // coverage-covers: sink.fused_stateless_sink
    #[tokio::test]
    async fn shared_partial_delivery_abort_and_recovery_match_across_topologies() {
        for phase in ["begin", "write", "finish"] {
            let mut outputs = Vec::new();
            for fused in [false, true] {
                outputs.push(
                    collect_chain(
                        fused,
                        Box::new(FixedEncoder {
                            payload: Bytes::from_static(b"AB"),
                        }),
                        Some(2),
                        None,
                        &[1, 3],
                        Some(Box::new(RecoveringCompressor {
                            phase,
                            failed: false,
                        })),
                        Some(Box::new(RecoveringEncryptor {
                            phase: "never",
                            failed: false,
                        })),
                    )
                    .await,
                );
            }
            assert_eq!(outputs[0], outputs[1], "partial phase={phase}");
            let completions: Vec<_> = outputs[1]
                .iter()
                .filter(|(flags, _)| flags.contains(crate::processor::EncodedDeliveryFlags::END))
                .collect();
            assert_eq!(completions.len(), 1, "next complete batch survives");
        }
    }

    // coverage-covers: sink.fused_stateless_sink, pipeline.runtime.stats
    #[tokio::test]
    async fn fused_count_batches_wait_for_successful_send_before_encoding_next_batch() {
        use crate::processor::base::LinkKind;
        use std::sync::atomic::Ordering;
        let observed = Arc::new(EncoderObservation::default());
        let stats = Arc::new(ProcessorStats::default());
        let mut processor = FusedTransformProcessor::new_with_channel_capacities(
            "backpressure",
            sample_stages(),
            Some(Box::new(ObservedEncoder(Arc::clone(&observed)))),
            ByteTransformChain::new(None, None),
            Some(1),
            None,
            ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc),
        );
        processor.set_stats(Arc::clone(&stats));
        let (input, receiver) = tokio::sync::broadcast::channel(8);
        processor.add_input(receiver);
        let mut output = processor.subscribe_output().expect("output");
        let handle = processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
        assert!(input
            .send(StreamData::Collection(Box::new(input_batch(5))))
            .is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());
        tokio::time::timeout(Duration::from_secs(2), async {
            while observed.appends.load(Ordering::SeqCst) < 2 {
                observed.appended.notified().await;
            }
        })
        .await
        .expect("second append");
        assert_eq!(observed.appends.load(Ordering::SeqCst), 2);
        assert_eq!(stats.snapshot().custom.get("messages_out"), Some(&1));
        assert_eq!(stats.snapshot().custom.get("bytes_out"), Some(&2));
        for _ in 0..5 {
            assert!(matches!(
                output.recv().await.expect("delivery"),
                StreamData::EncodedDelivery { .. }
            ));
        }
        assert!(output.recv().await.expect("terminal").is_terminal());
        handle.await.expect("join").expect("result");
        assert_eq!(stats.snapshot().custom.get("messages_out"), Some(&5));
        assert_eq!(stats.snapshot().custom.get("bytes_out"), Some(&10));
    }

    // coverage-covers: sink.fused_stateless_sink, pipeline.runtime.stats
    #[tokio::test(start_paused = true)]
    async fn fused_transforms_propagate_channel_closed_from_feed_timer_and_terminal() {
        use crate::processor::base::LinkKind;
        use crate::processor::byte_stage::CompressDriver;
        use crate::processor::InstantControlSignal;
        struct DeferredEncoder(Arc<tokio::sync::Notify>);
        impl SinkEncoder for DeferredEncoder {
            fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                Ok(None)
            }
            fn append(&mut self, _record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
                self.0.notify_one();
                Ok(None)
            }
            fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                Ok(Some(Bytes::from_static(b"AB")))
            }
        }
        for phase in ["feed", "timer", "terminal"] {
            let appended = Arc::new(tokio::sync::Notify::new());
            let stats = Arc::new(ProcessorStats::default());
            let mut processor = FusedTransformProcessor::new_with_channel_capacities(
                phase,
                sample_stages(),
                Some(Box::new(DeferredEncoder(Arc::clone(&appended)))),
                ByteTransformChain::new(
                    Some(CompressDriver::new(Box::new(RecoveringCompressor {
                        phase: "never",
                        failed: false,
                    }))),
                    None,
                ),
                Some(if phase == "feed" { 1 } else { 2 }),
                (phase == "timer").then_some(Duration::from_secs(1)),
                ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc),
            );
            processor.set_stats(Arc::clone(&stats));
            let (input, receiver) = tokio::sync::broadcast::channel(8);
            let (control, receiver_control) = tokio::sync::broadcast::channel(8);
            processor.add_input(receiver);
            processor.add_control_input(receiver_control);
            drop(processor.subscribe_output().expect("data output"));
            let _control_output = processor
                .subscribe_control_output()
                .expect("control output");
            let handle =
                processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
            assert!(input
                .send(StreamData::Collection(Box::new(sample_batch())))
                .is_ok());
            appended.notified().await;
            if phase == "timer" {
                tokio::time::advance(Duration::from_secs(1)).await;
            }
            if phase == "terminal" {
                assert!(control
                    .send(ControlSignal::Instant(
                        InstantControlSignal::StreamQuickEnd { signal_id: 1 }
                    ))
                    .is_ok());
            }
            let result = tokio::time::timeout(Duration::from_secs(2), async { handle.await })
                .await
                .expect("stop timeout")
                .expect("join");
            assert!(
                matches!(result, Err(ProcessorError::ChannelClosed)),
                "phase={phase}"
            );
            assert_eq!(stats.snapshot().custom.get("messages_out"), Some(&0));
            assert_eq!(stats.snapshot().custom.get("bytes_out"), Some(&0));
        }
    }

    // coverage-covers: sink.fused_stateless_sink
    #[tokio::test]
    async fn fused_repeated_start_fails_instead_of_changing_delivery_mode() {
        use crate::processor::base::default_channel_capacities;
        for encoded in [false, true] {
            let mut processor = FusedTransformProcessor::new_with_channel_capacities(
                "start",
                sample_stages(),
                encoded.then(|| {
                    Box::new(FixedEncoder {
                        payload: Bytes::from_static(b"AB"),
                    }) as Box<dyn SinkEncoder>
                }),
                ByteTransformChain::new(None, None),
                None,
                None,
                default_channel_capacities(),
            );
            let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
            processor
                .start(&spawner)
                .await
                .expect("first start")
                .expect("first result");
            let result = processor.start(&spawner).await.expect("second start task");
            assert!(
                matches!(result, Err(ProcessorError::ProcessingError(ref reason)) if reason.contains("already started")),
                "encoded={encoded}"
            );
        }
    }

    // coverage-covers: sink.fused_stateless_sink, pipeline.runtime.stats
    #[tokio::test(start_paused = true)]
    async fn encoder_only_closed_channel_matches_standalone_feed_timer_and_terminal() {
        use crate::codec::{JsonEncoder, SinkEncoderFactory};
        use crate::planner::physical::output_layout::OutputLayout;
        use crate::planner::sink::SinkEncoderConfig;
        use crate::processor::base::LinkKind;
        use crate::processor::{InstantControlSignal, SinkEncoderProcessor};
        for phase in ["feed", "timer", "terminal"] {
            let mut outcomes = Vec::new();
            for fused in [false, true] {
                let stats = Arc::new(ProcessorStats::default());
                let config = SinkEncoderConfig::json();
                let factory = Arc::new(JsonEncoder::new("json", &config).expect("factory"))
                    .with_output_layout(Arc::new(OutputLayout::new(Vec::new())))
                    .expect("layout");
                let encoder = factory.start_encoder().expect("encoder");
                let count = match phase {
                    "feed" => Some(1),
                    "terminal" => Some(2),
                    _ => None,
                };
                let duration = (phase == "timer").then_some(Duration::from_secs(1));
                let capacities =
                    ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                let (input, input_rx) = tokio::sync::broadcast::channel(8);
                let (control, control_rx) = tokio::sync::broadcast::channel(8);
                let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                let (handle, output, mut control_output) = if fused {
                    let mut processor = FusedTransformProcessor::new_with_channel_capacities(
                        "json",
                        sample_stages(),
                        Some(encoder),
                        ByteTransformChain::new(None, None),
                        count,
                        duration,
                        capacities,
                    );
                    processor.set_stats(Arc::clone(&stats));
                    processor.add_input(input_rx);
                    processor.add_control_input(control_rx);
                    let output = processor.subscribe_output().expect("data output");
                    let control_output = processor
                        .subscribe_control_output()
                        .expect("control output");
                    (processor.start(&spawner), output, control_output)
                } else {
                    let mut processor = SinkEncoderProcessor::new_with_channel_capacities(
                        "json", encoder, count, duration, capacities,
                    );
                    processor.set_stats(Arc::clone(&stats));
                    processor.add_input(input_rx);
                    processor.add_control_input(control_rx);
                    let output = processor.subscribe_output().expect("data output");
                    let control_output = processor
                        .subscribe_control_output()
                        .expect("control output");
                    (processor.start(&spawner), output, control_output)
                };
                let mut output = Some(output);
                if phase == "feed" {
                    drop(output.take());
                }
                assert!(input
                    .send(StreamData::Collection(Box::new(sample_batch())))
                    .is_ok());
                let accepted_bytes = if let Some(output) = output.as_mut() {
                    let StreamData::EncodedDelivery { flags, bytes } =
                        output.recv().await.expect("START")
                    else {
                        panic!("expected START")
                    };
                    assert!(flags.contains(crate::processor::EncodedDeliveryFlags::START));
                    assert!(!flags.contains(crate::processor::EncodedDeliveryFlags::END));
                    bytes.len() as u64
                } else {
                    0
                };
                drop(output);
                if phase == "timer" {
                    tokio::time::advance(Duration::from_secs(1)).await;
                }
                if phase != "terminal" {
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while stats.snapshot().error_count == 0 {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    })
                    .await
                    .expect("closed send error recorded");
                    assert!(
                        !handle.handle.is_finished(),
                        "encoder records and continues: fused={fused}, phase={phase}"
                    );
                }
                assert!(control
                    .send(ControlSignal::Instant(
                        InstantControlSignal::StreamQuickEnd { signal_id: 1 }
                    ))
                    .is_ok());
                assert!(
                    tokio::time::timeout(Duration::from_secs(2), control_output.recv())
                        .await
                        .expect("control timeout")
                        .expect("terminal control")
                        .is_terminal()
                );
                tokio::time::timeout(Duration::from_secs(2), async { handle.await })
                    .await
                    .expect("stop timeout")
                    .expect("join")
                    .expect("normal encoder control termination");
                let snapshot = stats.snapshot();
                assert_eq!(snapshot.error_count, 1, "fused={fused}, phase={phase}");
                assert_eq!(snapshot.custom.get("messages_out"), Some(&0));
                assert_eq!(snapshot.custom.get("bytes_out"), Some(&accepted_bytes));
                outcomes.push((
                    snapshot.error_count,
                    snapshot.custom["messages_out"],
                    snapshot.custom["bytes_out"],
                ));
            }
            assert_eq!(outcomes[0], outcomes[1], "phase={phase}");
        }
    }

    #[derive(Default)]
    struct CapturedOutput(Vec<StreamData>, Vec<ProcessorError>);
    impl crate::processor::delivery_output::DeliveryOutput for CapturedOutput {
        fn on_encoder_flush_error(&mut self, error: ProcessorError) -> Result<(), ProcessorError> {
            self.1.push(error);
            Ok(())
        }
        async fn send(&mut self, data: StreamData) -> Result<(), ProcessorError> {
            self.0.push(data);
            Ok(())
        }
    }
    fn completed_payloads(output: &CapturedOutput) -> Vec<Vec<u8>> {
        use crate::processor::EncodedDeliveryFlags;
        let mut deliveries = Vec::new();
        let mut pending = Vec::new();
        for data in &output.0 {
            if let StreamData::EncodedDelivery { flags, bytes } = data {
                if flags.contains(EncodedDeliveryFlags::ABORT) {
                    pending.clear();
                    continue;
                }
                if flags.contains(EncodedDeliveryFlags::START) {
                    pending.clear();
                }
                pending.extend_from_slice(bytes);
                if flags.contains(EncodedDeliveryFlags::END) {
                    deliveries.push(std::mem::take(&mut pending));
                }
            }
        }
        deliveries
    }

    // coverage-covers: sink.fused_stateless_sink
    #[tokio::test(start_paused = true)]
    async fn shared_encoder_input_checks_deadline_after_row_processing() {
        for count in [None, Some(3)] {
            for row_work_ms in [0, 5, 15] {
                let origin = Instant::now();
                let mut timer = None;
                let mut output = CapturedOutput::default();
                let mut stage = EncoderStage::new(
                    Box::new(FixedEncoder {
                        payload: Bytes::from_static(b"AB"),
                    }),
                    BatchPolicy::new(count, Some(Duration::from_secs(1))),
                );
                stage
                    .feed(
                        "deadline",
                        Box::new(sample_batch()),
                        &mut output,
                        &mut timer,
                        origin,
                        None,
                    )
                    .await
                    .expect("first feed");
                tokio::time::advance(Duration::from_millis(995)).await;
                // The row-stage input arrived before the boundary; work completes
                // before, exactly at, or after it without polling the batch timer.
                tokio::time::advance(Duration::from_millis(row_work_ms)).await;
                stage
                    .feed(
                        "deadline",
                        Box::new(sample_batch()),
                        &mut output,
                        &mut timer,
                        origin,
                        None,
                    )
                    .await
                    .expect("second feed");
                if row_work_ms >= 5 {
                    assert_eq!(completed_payloads(&output), vec![b"AB".to_vec()], "old batch must flush before new input: count={count:?}, row_work={row_work_ms}");
                    assert_eq!(
                        timer.as_ref().expect("new timer").deadline(),
                        origin + Duration::from_secs(2)
                    );
                }
                stage
                    .flush("deadline", &mut output, None)
                    .await
                    .expect("tail");
                let expected = if row_work_ms < 5 {
                    vec![b"ABAB".to_vec()]
                } else {
                    vec![b"AB".to_vec(), b"AB".to_vec()]
                };
                assert_eq!(completed_payloads(&output), expected);
            }
        }
    }

    // coverage-covers: sink.fused_stateless_sink, pipeline.runtime.stats
    #[tokio::test]
    async fn fused_multiple_stage_failures_count_one_aborted_delivery() {
        use crate::processor::base::default_channel_capacities;
        use crate::processor::byte_stage::{CompressDriver, EncryptDriver};
        struct FailSecondAppend {
            appends: usize,
        }
        impl SinkEncoder for FailSecondAppend {
            fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                Ok(None)
            }
            fn append(&mut self, _record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
                self.appends += 1;
                if matches!(self.appends, 2 | 5) {
                    Err(EncodeError::Other("selected append failed".into()))
                } else {
                    Ok(Some(Bytes::from_static(b"AB")))
                }
            }
            fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                Ok(None)
            }
        }
        for failed_transform in ["compress", "encrypt"] {
            let stats = Arc::new(ProcessorStats::default());
            let mut processor = FusedTransformProcessor::new_with_channel_capacities(
                "abort",
                sample_stages(),
                Some(Box::new(FailSecondAppend { appends: 0 })),
                ByteTransformChain::new(
                    (failed_transform == "compress").then(|| {
                        CompressDriver::new(Box::new(RecoveringCompressor {
                            phase: "write",
                            failed: false,
                        }))
                    }),
                    (failed_transform == "encrypt").then(|| {
                        EncryptDriver::new(Box::new(RecoveringEncryptor {
                            phase: "write",
                            failed: false,
                        }))
                    }),
                ),
                Some(2),
                None,
                default_channel_capacities(),
            );
            processor.set_stats(Arc::clone(&stats));
            let (input, receiver) = tokio::sync::broadcast::channel(8);
            processor.add_input(receiver);
            let mut output = processor.subscribe_output().expect("output");
            let handle =
                processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
            for rows in [1, 1, 2, 1, 1, 2] {
                assert!(input
                    .send(StreamData::Collection(Box::new(input_batch(rows))))
                    .is_ok());
            }
            assert!(input.send(StreamData::stream_end()).is_ok());
            let mut captured = CapturedOutput::default();
            loop {
                let data = tokio::time::timeout(Duration::from_secs(2), output.recv())
                    .await
                    .expect("output timeout")
                    .expect("output");
                if data.is_terminal() {
                    break;
                }
                captured.0.push(data);
            }
            assert_eq!(
                completed_payloads(&captured),
                vec![b"AB".to_vec(), b"AB".to_vec()]
            );
            handle.await.expect("join").expect("processor result");
            assert_eq!(stats.snapshot().error_count, 3);
            assert_eq!(
                stats.snapshot().custom.get("messages_aborted"),
                Some(&2),
                "two aborted deliveries: {failed_transform}"
            );
            assert_eq!(stats.snapshot().custom.get("messages_out"), Some(&2));
        }
    }

    // coverage-covers: sink.fused_stateless_sink
    #[tokio::test(start_paused = true)]
    async fn shared_native_input_checks_deadline_after_row_processing() {
        for count in [None, Some(3)] {
            for row_work_ms in [0, 5, 15] {
                let origin = Instant::now();
                let policy = BatchPolicy::new(count, Some(Duration::from_secs(1)));
                let stats = Arc::new(sample_stats());
                let mut timer = None;
                let mut buffer = Vec::new();
                let mut output = CapturedOutput::default();
                NativeBatchStage::feed(
                    "native",
                    input_batch(1).rows().to_vec(),
                    &mut buffer,
                    &mut output,
                    &mut timer,
                    origin,
                    &policy,
                    &stats,
                )
                .await
                .expect("first feed");
                tokio::time::advance(Duration::from_millis(995)).await;
                tokio::time::advance(Duration::from_millis(row_work_ms)).await;
                NativeBatchStage::feed(
                    "native",
                    input_batch(1).rows().to_vec(),
                    &mut buffer,
                    &mut output,
                    &mut timer,
                    origin,
                    &policy,
                    &stats,
                )
                .await
                .expect("second feed");
                if row_work_ms >= 5 {
                    assert_eq!(
                        output.0.len(),
                        1,
                        "old native batch flushed before new input"
                    );
                    assert_eq!(
                        timer.as_ref().expect("new timer").deadline(),
                        origin + Duration::from_secs(2)
                    );
                }
                NativeBatchStage::flush_all("native", &mut buffer, &mut output, &stats)
                    .await
                    .expect("tail");
                let batch_rows: Vec<_> = output
                    .0
                    .iter()
                    .map(|data| data.as_collection().expect("collection").num_rows())
                    .collect();
                assert_eq!(
                    batch_rows,
                    if row_work_ms < 5 { vec![2] } else { vec![1, 1] }
                );
            }
        }
    }

    // coverage-covers: sink.fused_stateless_sink
    #[tokio::test(start_paused = true)]
    async fn shared_due_flush_failure_does_not_drop_the_new_input() {
        struct FailFirstFinish(bool);
        impl SinkEncoder for FailFirstFinish {
            fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                Ok(None)
            }
            fn append(&mut self, _record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
                Ok(Some(Bytes::from_static(b"AB")))
            }
            fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                if std::mem::take(&mut self.0) {
                    Err(EncodeError::Other("old batch finish failed".into()))
                } else {
                    Ok(None)
                }
            }
        }
        let origin = Instant::now();
        let mut timer = None;
        let mut output = CapturedOutput::default();
        let mut stage = EncoderStage::new(
            Box::new(FailFirstFinish(true)),
            BatchPolicy::new(None, Some(Duration::from_secs(1))),
        );
        stage
            .feed(
                "late",
                Box::new(sample_batch()),
                &mut output,
                &mut timer,
                origin,
                None,
            )
            .await
            .expect("first feed");
        tokio::time::advance(Duration::from_secs(1)).await;
        stage
            .feed(
                "late",
                Box::new(sample_batch()),
                &mut output,
                &mut timer,
                origin,
                None,
            )
            .await
            .expect("new input survives old flush failure");
        assert_eq!(output.1.len(), 1);
        assert!(stage.has_data());
        assert_eq!(
            timer.as_ref().expect("new timer").deadline(),
            origin + Duration::from_secs(2)
        );
        stage
            .flush("late", &mut output, None)
            .await
            .expect("new tail");
        assert_eq!(completed_payloads(&output), vec![b"AB".to_vec()]);
    }
}
