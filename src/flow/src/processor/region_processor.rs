//! Synchronous collection stages sharing one channel-facing task.
//!
//! Algorithms stay in their existing stage engines. Only region input/output
//! waits are asynchronous; there are no internal tasks, futures, or queues.

use crate::aggregation::AggregateFunctionRegistry;
use crate::codec::encrypt::{AesGcmStreamWriter, SinkEncryptionConfig};
use crate::model::Collection;
use crate::planner::physical::DataDomain;
use crate::planner::physical::{PhysicalAggregation, PhysicalPlan, StreamingWindowSpec};
use crate::processor::aggregation_processor::AggregationProcessor;
use crate::processor::barrier_processor::{BarrierAction, BarrierStage};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, fan_in_streams_indexed, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, FanInStream, IndexedInput, LinkOutput,
    LinkReceiver, PausableFanInStream, ProcessorChannelCapacities,
};
use crate::processor::batch_function_compute_processor::{
    apply_compute as apply_batch_compute, bind_evaluations, BoundBatchEvaluation,
};
use crate::processor::byte_stage::{ByteTransformStage, CompressDriver, EncryptDriver};
use crate::processor::collection_layout_normalize_processor::normalize_collection;
use crate::processor::compute_processor::{apply_compute, compute_fields, ComputeExpr};
use crate::processor::count_window_stage::{CountWindowOutput, CountWindowStage};
use crate::processor::data_metrics::DataMetricDomains;
use crate::processor::decoder_processor::DecoderStage;
use crate::processor::delivery_transform::DeliveryStep;
use crate::processor::empty_suppress_processor::collection_is_effectively_empty;
use crate::processor::encoder_stage::{EncoderOutput, EncoderStage};
use crate::processor::eos_window_processor::EosWindowStage;
use crate::processor::filter_processor::{apply_filter, FilterStage};
use crate::processor::memory_collection_materialize_processor::materialize_collection;
use crate::processor::order_processor::{apply_order, OrderStage};
use crate::processor::output_row_accessor::OutputRowAccessor;
use crate::processor::project_processor::{apply_project, ProjectStage};
use crate::processor::row_diff_processor::{apply_row_diff, row_diff_fields, RowDiffState};
use crate::processor::sampler_processor::SamplerStage;
use crate::processor::sliding_window_stage::{SlidingWindowOutput, SlidingWindowStage};
use crate::processor::source_change_gate_processor::{gate_collection, SourceChangeGateState};
use crate::processor::stage_clock::{ClockDeadline, ClockEvent, StageClock};
use crate::processor::stage_metrics::StageMetrics;
use crate::processor::state_window_processor::{StateWindowOutput, StateWindowStage};
use crate::processor::stateful_function_processor::StatefulStage;
use crate::processor::streaming_aggregation_processor::{
    StreamingCountOutput, StreamingCountStage, StreamingEosStage, StreamingSlidingStage,
    StreamingStateStage, StreamingTumblingStage,
};
use crate::processor::timed_batch_stage::{TimedBatchOutput, TimedBatchStage};
use crate::processor::tumbling_window_processor::{TumblingWindowOutput, TumblingWindowStage};
use crate::processor::watermark_processor::WatermarkStage;
use crate::processor::{
    ControlSignal, CounterHandle, MetricKind, MetricSpec, Processor, ProcessorError,
    ProcessorStart, ProcessorStats, ProcessorStatsHandle, StreamData,
};
use crate::runtime::TaskSpawner;
use crate::stateful::StatefulFunctionRegistry;
use futures::{future::poll_fn, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

mod opaque;

enum CollectionStage {
    Encoder(EncoderStage),
    ByteTransform(ByteTransformStage),
    Sampler(SamplerStage),
    TimedBatch(TimedBatchStage),
    Decoder(DecoderStage),
    Barrier(BarrierStage),
    Watermark(WatermarkStage),
    Filter(FilterStage),
    Project(ProjectStage),
    Compute(Vec<ComputeExpr>),
    Order(OrderStage),
    EmptySuppress(bool),
    TumblingWindow(TumblingWindowStage),
    StreamingTumbling(StreamingTumblingStage),
    CountWindow(CountWindowStage),
    StreamingCount(StreamingCountStage),
    EosWindow(EosWindowStage),
    StreamingEos(StreamingEosStage),
    Stateful(StatefulStage),
    StateWindow(StateWindowStage),
    StreamingState(StreamingStateStage),
    Sliding(SlidingWindowStage),
    StreamingSliding(StreamingSlidingStage),
    Normalize {
        source: Arc<str>,
        keys: Arc<[Arc<str>]>,
    },
    Materialize {
        source: Arc<str>,
        keys: Arc<[Arc<str>]>,
        accessor: OutputRowAccessor,
    },
    RowDiff {
        accessor: OutputRowAccessor,
        keys: Arc<[Arc<str>]>,
        tracked: Arc<[bool]>,
        state: RowDiffState,
    },
    ChangeGate {
        source: Arc<str>,
        tracked: Arc<[usize]>,
        state: SourceChangeGateState,
    },
    BatchCompute(Vec<BoundBatchEvaluation>),
    Aggregation(PhysicalAggregation, Arc<AggregateFunctionRegistry>),
}

/// Synchronous incremental output descriptions, not asynchronous activations.
enum StageOutput {
    Encoder(EncoderEmission),
    Delivery(DeliveryStep),
    TimedBatch(TimedBatchOutput),
    Single(Option<StreamData>),
    Pair(Box<[Option<StreamData>; 2]>),
    StateWindow(StateWindowOutput),
    Sliding(SlidingWindowOutput),
    Tumbling(TumblingWindowOutput),
    Count(CountWindowOutput),
    StreamingCount(StreamingCountOutput),
}

/// Encoder-specific incremental state. The trailing terminal is allocated only
/// on termination, keeping ordinary input extraction free of output queues.
struct EncoderEmission {
    work: EncoderOutput,
    entered: bool,
    finishing: bool,
    error: Option<ProcessorError>,
    trailing: Option<Box<StreamData>>,
}
impl EncoderEmission {
    fn feed(collection: Box<dyn Collection>) -> Self {
        Self {
            work: EncoderStage::input(collection),
            entered: false,
            finishing: false,
            error: None,
            trailing: None,
        }
    }
    fn finish(trailing: Option<StreamData>) -> Self {
        Self {
            work: EncoderStage::finish(),
            entered: true,
            finishing: true,
            error: None,
            trailing: trailing.map(Box::new),
        }
    }
    fn next(
        &mut self,
        stage: &mut EncoderStage,
        stats: &ProcessorStats,
        metrics: Option<&crate::processor::data_metrics::EncoderMetrics>,
    ) -> Option<StreamData> {
        loop {
            if let Some(error) = self.error.take() {
                stats.record_error_logged("region encoder error", error.to_string());
            }
            let due_flush = !self.entered && stage.take_due_deadline();
            self.entered = true;
            let step = if due_flush {
                Some(stage.flush_step())
            } else {
                stage.next_step(&mut self.work)
            };
            let Some(step) = step else {
                if !self.finishing {
                    stage.schedule_clock();
                }
                return self.trailing.take().map(|data| *data);
            };
            if step.aborted {
                if let Some(metrics) = metrics {
                    metrics.record_message_aborted();
                }
            }
            if step.error.is_some() && !due_flush {
                self.finishing = true;
            }
            self.error = step.error;
            if let Some(frame) = step.frame {
                return Some(frame);
            }
        }
    }
    fn output_failed(&mut self) {
        self.work.stop();
        self.error = None;
        self.finishing = true;
    }
}

impl StageOutput {
    fn immediate([first, last]: [Option<StreamData>; 2]) -> Self {
        match (first, last) {
            (None, last) => Self::Single(last),
            (first, None) => Self::Single(first),
            (first, last) => Self::Pair(Box::new([first, last])),
        }
    }
}

/// Build-time dependencies; only engines needing a registry require it.
pub(crate) struct RegionResources {
    pub(crate) isolate_opaque: bool,
    pub(crate) encoders: BTreeMap<i64, EncoderStage>,
    pub(crate) decoders: BTreeMap<i64, DecoderStage>,
    pub(crate) samplers: BTreeMap<i64, SamplerStage>,
    pub(crate) stage_default_domains: DataMetricDomains,
    pub(crate) stage_domains: BTreeMap<i64, DataMetricDomains>,
    pub(crate) stage_stats: BTreeMap<i64, Arc<ProcessorStats>>,
    pub(crate) input_domain: DataDomain,
    pub(crate) aggregate_registry: Option<Arc<AggregateFunctionRegistry>>,
    pub(crate) stateful_registry: Option<Arc<StatefulFunctionRegistry>>,
}

impl Default for RegionResources {
    fn default() -> Self {
        Self {
            isolate_opaque: false,
            encoders: BTreeMap::new(),
            decoders: BTreeMap::new(),
            samplers: BTreeMap::new(),
            stage_domains: BTreeMap::new(),
            stage_stats: BTreeMap::new(),
            stage_default_domains: DataMetricDomains::NONE.passthrough(DataDomain::Collection),
            input_domain: DataDomain::Collection,
            aggregate_registry: Some(AggregateFunctionRegistry::with_builtins()),
            stateful_registry: Some(StatefulFunctionRegistry::with_builtins()),
        }
    }
}

impl RegionResources {
    fn aggregate_registry(&self) -> Result<Arc<AggregateFunctionRegistry>, ProcessorError> {
        self.aggregate_registry.clone().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "aggregate registry unavailable for region stage".into(),
            )
        })
    }
    fn stateful_registry(&self) -> Result<Arc<StatefulFunctionRegistry>, ProcessorError> {
        self.stateful_registry.clone().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "stateful registry unavailable for region stage".into(),
            )
        })
    }
}

/// Stage callbacks are synchronous; this guard can never include channel waits.
struct StageTimer<'a> {
    stats: &'a ProcessorStats,
    started: Option<std::time::Instant>,
}
impl<'a> StageTimer<'a> {
    fn new(stats: &'a ProcessorStats) -> Self {
        Self {
            stats,
            started: stats.handling_time_enabled().then(std::time::Instant::now),
        }
    }
}
impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            self.stats.record_handle_duration(started.elapsed());
        }
    }
}

struct Stage {
    stats: Arc<ProcessorStats>,
    metrics: StageMetrics,
    metric_domains: DataMetricDomains,
    passthrough_domain: Option<DataDomain>,
    suppression_counters: Option<(CounterHandle, CounterHandle)>,
    name: String,
    engine: CollectionStage,
    single_output: bool,
    strict_input_closure: bool,
    fatal_data_lag: bool,
}

impl Stage {
    #[cold]
    fn from_plan(
        plan: &PhysicalPlan,
        resources: &mut RegionResources,
    ) -> Result<Self, ProcessorError> {
        let engine = match plan {
            PhysicalPlan::SinkEncoder(_) | PhysicalPlan::IncSinkEncoder(_) => {
                CollectionStage::Encoder(
                    resources
                        .encoders
                        .remove(&plan.get_plan_index())
                        .ok_or_else(|| {
                            ProcessorError::InvalidConfiguration(
                                "missing bound encoder stage".into(),
                            )
                        })?,
                )
            }
            PhysicalPlan::SinkCompress(p) => {
                CollectionStage::ByteTransform(ByteTransformStage::Compress {
                    driver: CompressDriver::new(p.codec.build_writer().map_err(|error| {
                        ProcessorError::InvalidConfiguration(error.to_string())
                    })?),
                    delivery: Default::default(),
                })
            }
            PhysicalPlan::SinkEncrypt(p) => {
                let config = SinkEncryptionConfig {
                    algorithm: p.algorithm,
                    key_id: p.key_id.clone(),
                    key_bits: p.key_bits,
                    key: Arc::clone(&p.key),
                };
                let writer = AesGcmStreamWriter::from_config(&config)
                    .map_err(|error| ProcessorError::InvalidConfiguration(error.to_string()))?;
                if writer.key_bits() != p.key_bits {
                    return Err(ProcessorError::InvalidConfiguration(format!(
                        "sink encrypt key_bits mismatch for key_id `{}`",
                        p.key_id
                    )));
                }
                CollectionStage::ByteTransform(ByteTransformStage::Encrypt {
                    driver: EncryptDriver::new(Box::new(writer)),
                    delivery: Default::default(),
                })
            }
            PhysicalPlan::Watermark(p) => {
                CollectionStage::Watermark(WatermarkStage::from_processing(
                    &crate::planner::physical::PhysicalProcessTimeWatermark::new(
                        p.config.clone(),
                        vec![],
                        p.base.index(),
                    ),
                ))
            }
            PhysicalPlan::Sampler(_) => CollectionStage::Sampler(
                resources
                    .samplers
                    .remove(&plan.get_plan_index())
                    .ok_or_else(|| {
                        ProcessorError::InvalidConfiguration("missing bound sampler stage".into())
                    })?,
            ),
            PhysicalPlan::Batch(p) => {
                crate::processor::BatchProcessor::validate_batch_config(
                    p.common.batch_count,
                    p.common.batch_duration,
                )?;
                match (p.common.batch_count, p.common.batch_duration) {
                    (count, Some(duration)) => {
                        CollectionStage::TimedBatch(TimedBatchStage::new(count, duration)?)
                    }
                    (Some(count), None) => {
                        CollectionStage::CountWindow(CountWindowStage::new(count, vec![])?)
                    }
                    (None, None) => {
                        return Err(ProcessorError::InvalidConfiguration(
                            "batch stage requires count or duration".into(),
                        ))
                    }
                }
            }

            PhysicalPlan::Decoder(_) => CollectionStage::Decoder(
                resources
                    .decoders
                    .get(&plan.get_plan_index())
                    .cloned()
                    .ok_or_else(|| {
                        ProcessorError::InvalidConfiguration("missing bound decoder stage".into())
                    })?,
            ),
            PhysicalPlan::Barrier(p) => {
                CollectionStage::Barrier(BarrierStage::new(p.base.children().len()))
            }
            PhysicalPlan::ProcessTimeWatermark(p) => {
                CollectionStage::Watermark(WatermarkStage::from_processing(p))
            }
            PhysicalPlan::EventtimeWatermark(p) => {
                CollectionStage::Watermark(WatermarkStage::from_eventtime(p))
            }
            PhysicalPlan::Filter(p) => CollectionStage::Filter(FilterStage::from_plan(p)),
            PhysicalPlan::Project(p) => CollectionStage::Project(ProjectStage::from_plan(p)),
            PhysicalPlan::Compute(p) => CollectionStage::Compute(compute_fields(p)),
            PhysicalPlan::Order(p) => CollectionStage::Order(OrderStage::from_plan(p)),
            PhysicalPlan::EmptySuppress(p) => CollectionStage::EmptySuppress(p.omit_if_empty),
            PhysicalPlan::CollectionLayoutNormalize(p) => CollectionStage::Normalize {
                source: Arc::clone(&p.output_source_name),
                keys: p
                    .schema
                    .column_schemas()
                    .iter()
                    .map(|c| Arc::<str>::from(c.name.as_str()))
                    .collect::<Vec<_>>()
                    .into(),
            },
            PhysicalPlan::MemoryCollectionMaterialize(p) => {
                let accessor = OutputRowAccessor::from_output_layout(&p.output_layout);
                CollectionStage::Materialize {
                    source: Arc::from(""),
                    keys: accessor.column_names(),
                    accessor,
                }
            }
            PhysicalPlan::RowDiff(p) => {
                let (accessor, keys, tracked) = row_diff_fields(p)?;
                CollectionStage::RowDiff {
                    accessor,
                    keys,
                    tracked,
                    state: RowDiffState::default(),
                }
            }
            PhysicalPlan::SourceChangeGate(p) => CollectionStage::ChangeGate {
                source: Arc::from(p.source_name.as_str()),
                tracked: Arc::clone(&p.tracked_column_indexes),
                state: SourceChangeGateState::default(),
            },
            PhysicalPlan::BatchFunctionCompute(p) => {
                CollectionStage::BatchCompute(bind_evaluations(&p.evaluations)?)
            }
            PhysicalPlan::TumblingWindow(p) => {
                CollectionStage::TumblingWindow(TumblingWindowStage::new(
                    p.time_unit.duration(p.length),
                    p.partition_by_scalars.clone(),
                    Arc::new(ProcessorStats::collection_in_out()),
                ))
            }
            PhysicalPlan::CountWindow(p) => CollectionStage::CountWindow(CountWindowStage::new(
                usize::try_from(p.count).map_err(|_| {
                    ProcessorError::InvalidConfiguration(
                        "count window exceeds address space".into(),
                    )
                })?,
                p.partition_by_scalars.clone(),
            )?),
            PhysicalPlan::Aggregation(p) => {
                CollectionStage::Aggregation(p.clone(), resources.aggregate_registry()?)
            }
            PhysicalPlan::EosWindow(_) => CollectionStage::EosWindow(EosWindowStage::new(
                Arc::new(ProcessorStats::collection_in_out()),
            )),
            PhysicalPlan::StatefulFunction(p) => CollectionStage::Stateful(StatefulStage::new(
                Arc::new(p.clone()),
                resources.stateful_registry()?,
            )?),
            PhysicalPlan::StateWindow(p) => {
                CollectionStage::StateWindow(StateWindowStage::new(Arc::new(p.as_ref().clone())))
            }
            PhysicalPlan::SlidingWindow(p) => {
                CollectionStage::Sliding(SlidingWindowStage::from_plan(p))
            }
            PhysicalPlan::StreamingAggregation(p) => match &p.window {
                StreamingWindowSpec::Sliding { .. } => {
                    let stage = StreamingSlidingStage::new(
                        Arc::new(p.clone()),
                        resources.aggregate_registry()?,
                    )?;
                    stage.validate_supported_aggregates()?;
                    CollectionStage::StreamingSliding(stage)
                }
                StreamingWindowSpec::State { .. } => CollectionStage::StreamingState(
                    StreamingStateStage::new(Arc::new(p.clone()), resources.aggregate_registry()?)?,
                ),
                StreamingWindowSpec::Eos => CollectionStage::StreamingEos(StreamingEosStage::new(
                    Arc::new(p.clone()),
                    resources.aggregate_registry()?,
                )),
                StreamingWindowSpec::Count { count, .. } => {
                    CollectionStage::StreamingCount(StreamingCountStage::new(
                        Arc::new(p.clone()),
                        resources.aggregate_registry()?,
                        *count,
                    ))
                }
                StreamingWindowSpec::Tumbling { .. } => {
                    CollectionStage::StreamingTumbling(StreamingTumblingStage::new(
                        Arc::new(p.clone()),
                        resources.aggregate_registry()?,
                        Arc::new(ProcessorStats::collection_in_out()),
                    )?)
                }
            },
            _ => {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "no collection region adapter for {}",
                    plan.get_plan_name()
                )))
            }
        };
        let name = plan.get_plan_name();
        let stats = resources
            .stage_stats
            .remove(&plan.get_plan_index())
            .unwrap_or_else(|| {
                let mut stats = ProcessorStats::new("default", name.as_str(), "stage");
                stats.configure_stage_handling_time();
                Arc::new(stats)
            });
        let domains = resources
            .stage_domains
            .get(&plan.get_plan_index())
            .copied()
            .unwrap_or_else(|| {
                if matches!(
                    plan,
                    PhysicalPlan::Decoder(_) | PhysicalPlan::Barrier(_) | PhysicalPlan::Sampler(_)
                ) {
                    resources.stage_default_domains
                } else {
                    DataMetricDomains::NONE.passthrough(DataDomain::Collection)
                }
            });
        let passthrough =
            matches!(plan, PhysicalPlan::Barrier(_)).then_some(resources.input_domain);
        let metrics = Self::metrics_for(&engine, &stats, domains, passthrough);
        let mut stage = Self {
            stats: Arc::clone(&stats),
            metrics,
            metric_domains: domains,
            passthrough_domain: passthrough,
            suppression_counters: None,
            name,
            single_output: matches!(
                engine,
                CollectionStage::Decoder(_)
                    | CollectionStage::Barrier(_)
                    | CollectionStage::Filter(_)
                    | CollectionStage::Project(_)
                    | CollectionStage::Compute(_)
                    | CollectionStage::Order(_)
                    | CollectionStage::EmptySuppress(_)
                    | CollectionStage::Normalize { .. }
                    | CollectionStage::Materialize { .. }
                    | CollectionStage::RowDiff { .. }
                    | CollectionStage::ChangeGate { .. }
                    | CollectionStage::BatchCompute(_)
                    | CollectionStage::Aggregation(..)
            ),
            engine,
            fatal_data_lag: matches!(plan, PhysicalPlan::BatchFunctionCompute(_)),
            strict_input_closure: matches!(
                plan,
                PhysicalPlan::Barrier(_)
                    | PhysicalPlan::Filter(_)
                    | PhysicalPlan::CollectionLayoutNormalize(_)
                    | PhysicalPlan::MemoryCollectionMaterialize(_)
            ),
        };
        stage.set_stats(stats);
        Ok(stage)
    }

    fn metrics_for(
        engine: &CollectionStage,
        stats: &ProcessorStats,
        domains: DataMetricDomains,
        passthrough: Option<DataDomain>,
    ) -> StageMetrics {
        match engine {
            CollectionStage::Encoder(_) => StageMetrics::encoding(stats),
            CollectionStage::ByteTransform(engine) => {
                StageMetrics::encoded(stats, engine.metric_specs())
            }
            _ => StageMetrics::new(stats, domains, passthrough),
        }
    }
    fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.metrics = Self::metrics_for(
            &self.engine,
            &stats,
            self.metric_domains,
            self.passthrough_domain,
        );
        match &mut self.engine {
            CollectionStage::EosWindow(engine) => engine.set_stats(Arc::clone(&stats)),
            CollectionStage::TumblingWindow(engine) => engine.set_stats(Arc::clone(&stats)),
            CollectionStage::StreamingTumbling(engine) => engine.set_stats(Arc::clone(&stats)),
            _ => {}
        }
        if matches!(self.engine, CollectionStage::EmptySuppress(_)) {
            self.suppression_counters = Some((
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
        self.stats = stats;
    }
    fn record_immediate(&self, output: &StageOutput) -> Result<(), ProcessorError> {
        match output {
            StageOutput::Single(Some(data)) => self.metrics.record_output(&self.stats, data)?,
            StageOutput::Pair(pair) => {
                for data in pair.iter().flatten() {
                    self.metrics.record_output(&self.stats, data)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Direct handoff for engines that produce at most one immediate result.
    #[inline]
    fn process_single(&mut self, data: StreamData) -> Result<Option<StreamData>, ProcessorError> {
        let stats = self.stats.as_ref();
        let _timer =
            (!matches!(self.engine, CollectionStage::Barrier(_))).then(|| StageTimer::new(stats));
        self.metrics
            .record_input(stats, &data)
            .inspect_err(|error| {
                stats.record_error_logged("stage input domain error", error.to_string());
            })?;
        let result = match &mut self.engine {
            CollectionStage::Decoder(stage) => Ok(stage.process(data, stats)),
            CollectionStage::Barrier(_) => Ok(Some(data)),
            engine => match data {
                StreamData::Collection(collection) => Self::apply(
                    engine,
                    &self.name,
                    collection,
                    stats,
                    &self.suppression_counters,
                )
                .map(|data| data.map(StreamData::collection)),
                data => Ok(Some(data)),
            },
        };
        let result = result.or_else(|error| {
            self.process_error(error, stats)?;
            Ok(None)
        });
        if let Ok(Some(data)) = &result {
            self.metrics.record_output(stats, data)?;
        }
        result
    }

    #[cold]
    fn process_error(
        &self,
        error: ProcessorError,
        stats: &ProcessorStats,
    ) -> Result<(), ProcessorError> {
        if matches!(
            self.engine,
            CollectionStage::Filter(_)
                | CollectionStage::Project(_)
                | CollectionStage::Compute(_)
                | CollectionStage::Order(_)
                | CollectionStage::EmptySuppress(_)
                | CollectionStage::TumblingWindow(_)
                | CollectionStage::Normalize { .. }
                | CollectionStage::Materialize { .. }
                | CollectionStage::RowDiff { .. }
                | CollectionStage::ChangeGate { .. }
                | CollectionStage::BatchCompute(_)
        ) {
            tracing::warn!(stage_id = %self.name, %error, "region stage failed");
            stats.record_error_logged("region stage error", error.to_string());
            Ok(())
        } else {
            tracing::error!(stage_id = %self.name, %error, "region stage failed");
            stats.record_error_logged("region stage fatal error", error.to_string());
            Err(error)
        }
    }

    #[inline(never)]
    fn process(&mut self, data: StreamData) -> Result<StageOutput, ProcessorError> {
        if self.single_output {
            return self.process_single(data).map(StageOutput::Single);
        }
        let stats = self.stats.as_ref();
        let _timer = StageTimer::new(stats);
        self.metrics
            .record_input(stats, &data)
            .inspect_err(|error| {
                stats.record_error_logged("stage input domain error", error.to_string());
            })?;
        let result = match &mut self.engine {
            CollectionStage::Encoder(stage) => match data {
                StreamData::Collection(collection) => {
                    Ok(StageOutput::Encoder(EncoderEmission::feed(collection)))
                }
                data if data.is_terminal() => {
                    stage.clear_deadline();
                    Ok(StageOutput::Encoder(EncoderEmission::finish(Some(data))))
                }
                data => {
                    stage.schedule_clock();
                    Ok(StageOutput::Single(Some(data)))
                }
            },
            CollectionStage::ByteTransform(stage) => {
                let metrics = self.metrics.transform();
                match data {
                    StreamData::EncodedDelivery { flags, bytes } => {
                        Ok(StageOutput::Delivery(stage.process(flags, bytes, metrics)))
                    }
                    data => {
                        let aborted = data
                            .is_terminal()
                            .then(|| stage.finish(stats, metrics))
                            .flatten();
                        Ok(StageOutput::immediate([aborted, Some(data)]))
                    }
                }
            }
            CollectionStage::Sampler(stage) => {
                Ok(StageOutput::immediate(stage.process(data, stats)))
            }
            CollectionStage::TimedBatch(stage) => Ok(StageOutput::TimedBatch(stage.process(data))),
            CollectionStage::Decoder(stage) => Ok(StageOutput::Single(stage.process(data, stats))),
            // Input-port alignment is performed at the region input boundary.
            CollectionStage::Barrier(_) => Ok(StageOutput::Single(Some(data))),
            CollectionStage::Watermark(stage) => {
                stage.process(data, stats).map(StageOutput::immediate)
            }
            CollectionStage::Sliding(stage) => Ok(StageOutput::Sliding(stage.process(data, stats))),
            CollectionStage::StreamingSliding(stage) => {
                stage.process(data, stats).map(StageOutput::Sliding)
            }
            CollectionStage::StateWindow(stage) => {
                Ok(StageOutput::StateWindow(stage.process(data, stats)))
            }
            CollectionStage::StreamingState(stage) => {
                Ok(StageOutput::StateWindow(stage.process(data, stats)))
            }
            CollectionStage::EosWindow(stage) => stage.process(data).map(StageOutput::immediate),
            CollectionStage::StreamingEos(stage) => {
                stage.process(data, stats).map(StageOutput::immediate)
            }
            CollectionStage::Stateful(stage) => {
                stage.process(data, stats).map(StageOutput::immediate)
            }
            CollectionStage::TumblingWindow(window) => {
                window.process(data).map(StageOutput::Tumbling)
            }
            CollectionStage::StreamingTumbling(window) => {
                window.process(data).map(StageOutput::Tumbling)
            }
            CollectionStage::CountWindow(window) => {
                window.process(data, stats).map(StageOutput::Count)
            }
            CollectionStage::StreamingCount(window) => {
                Ok(StageOutput::StreamingCount(window.process(data)))
            }
            _ => match data {
                StreamData::Collection(collection) => Self::apply(
                    &mut self.engine,
                    &self.name,
                    collection,
                    stats,
                    &self.suppression_counters,
                )
                .map(|data| StageOutput::Single(data.map(StreamData::collection))),
                other => Ok(StageOutput::Single(Some(other))),
            },
        };
        let result = result.or_else(|error| {
            self.process_error(error, stats)?;
            Ok(StageOutput::Single(None))
        });
        if let Ok(output) = &result {
            self.record_immediate(output)?;
        }
        result
    }

    #[inline(never)]
    fn next_output(
        &mut self,
        output: &mut StageOutput,
    ) -> Result<Option<StreamData>, ProcessorError> {
        // Immediate outputs need no engine dispatch or computation.
        match output {
            StageOutput::Single(data) => return Ok(data.take()),
            StageOutput::Pair(data) => return Ok(data.iter_mut().find_map(Option::take)),
            _ => {}
        }
        let stats = self.stats.as_ref();
        let _timer = StageTimer::new(stats);
        let result = match output {
            StageOutput::Encoder(output) => match &mut self.engine {
                CollectionStage::Encoder(stage) => {
                    Ok(output.next(stage, stats, self.metrics.encoder()))
                }
                _ => Err(ProcessorError::ProcessingError(
                    "encoder output without owning stage".into(),
                )),
            },
            StageOutput::Delivery(step) => {
                if let Some(data) = step.output.take() {
                    Ok(Some(data))
                } else {
                    if let Some(error) = step.error.take() {
                        stats.record_error_logged("region byte transform error", error.to_string());
                    }
                    Ok(None)
                }
            }
            StageOutput::TimedBatch(output) => match &mut self.engine {
                CollectionStage::TimedBatch(stage) => stage.next_output(output),
                _ => Err(ProcessorError::ProcessingError(
                    "timed batch output without owning stage".into(),
                )),
            },
            StageOutput::StateWindow(output) => match &mut self.engine {
                CollectionStage::StateWindow(stage) => stage.next_output(output, stats),
                CollectionStage::StreamingState(stage) => stage.next_output(output, stats),
                _ => Err(ProcessorError::ProcessingError(
                    "state window output has no owning stage".into(),
                )),
            },
            StageOutput::Sliding(output) => match &mut self.engine {
                CollectionStage::Sliding(stage) => stage.next_output(output, stats),
                CollectionStage::StreamingSliding(stage) => stage.next_output(output, stats),
                _ => Err(ProcessorError::ProcessingError(
                    "sliding output has no owning stage".into(),
                )),
            },
            StageOutput::Single(data) => Ok(data.take()),
            StageOutput::Pair(data) => Ok(data.iter_mut().find_map(Option::take)),
            StageOutput::Count(output) => match &mut self.engine {
                CollectionStage::CountWindow(window) => window.next_output(output),
                _ => Err(ProcessorError::ProcessingError(
                    "count output has no owning stage".into(),
                )),
            },
            StageOutput::StreamingCount(output) => match &mut self.engine {
                CollectionStage::StreamingCount(window) => window.next_output(output, stats),
                _ => Err(ProcessorError::ProcessingError(
                    "count aggregation output has no owning stage".into(),
                )),
            },
            StageOutput::Tumbling(output) => match &mut self.engine {
                CollectionStage::TumblingWindow(window) => window.next_output(output),
                CollectionStage::StreamingTumbling(window) => window.next_output(output),
                _ => Err(ProcessorError::ProcessingError(
                    "window output has no owning stage".into(),
                )),
            },
        };
        if let Ok(Some(data)) = &result {
            self.metrics.record_output(stats, data)?;
        }
        if let Err(error) = &result {
            stats.record_error_logged("region stage output error", error.to_string());
        }
        result
    }

    fn start_clock(&mut self) -> Option<StageClock> {
        match &mut self.engine {
            CollectionStage::Encoder(stage) => stage.batch_duration().map(|_| {
                stage.start_clock(tokio::time::Instant::now());
                StageClock::new(None)
            }),
            CollectionStage::Sampler(stage) => Some(StageClock::new(Some((
                stage.period(),
                tokio::time::MissedTickBehavior::Skip,
            )))),
            CollectionStage::Watermark(stage) => {
                Some(StageClock::new(stage.period().map(|period| {
                    (period, tokio::time::MissedTickBehavior::Delay)
                })))
            }
            CollectionStage::TimedBatch(stage) => {
                stage.start(tokio::time::Instant::now());
                Some(StageClock::new(None))
            }
            _ => None,
        }
    }
    fn clock_deadline(&self) -> Result<Option<ClockDeadline>, ProcessorError> {
        match &self.engine {
            CollectionStage::Encoder(stage) => Ok(stage.deadline()),
            CollectionStage::Watermark(stage) => stage.deadline(),
            CollectionStage::TimedBatch(stage) => Ok(stage.deadline()),
            _ => Ok(None),
        }
    }
    fn on_timer(&mut self, event: ClockEvent) -> Result<StageOutput, ProcessorError> {
        let stats = self.stats.as_ref();
        let _timer = StageTimer::new(stats);
        let result = match &mut self.engine {
            CollectionStage::Encoder(stage) => {
                stage.clear_deadline();
                Ok(StageOutput::Encoder(EncoderEmission::finish(None)))
            }
            CollectionStage::Sampler(stage) => Ok(StageOutput::Single(stage.on_tick())),
            CollectionStage::Watermark(stage) => stage
                .on_timer(event, std::time::SystemTime::now())
                .map(StageOutput::Single),
            CollectionStage::TimedBatch(stage) => Ok(StageOutput::TimedBatch(stage.finish(None))),
            _ => Err(ProcessorError::ProcessingError(
                "clock without timed stage".into(),
            )),
        };
        if let Ok(output) = &result {
            self.record_immediate(output)?;
        }
        if let Err(error) = &result {
            stats.record_error_logged("region timer error", error.to_string());
        }
        result
    }

    fn channel_closed(&self, lane: &str) -> ProcessorError {
        let _timer = StageTimer::new(&self.stats);
        self.stats.record_error_logged(
            "region input closed",
            format!("unexpected {lane} input closure"),
        );
        ProcessorError::ChannelClosed
    }

    fn input_closed(&mut self) -> Result<Option<StageOutput>, ProcessorError> {
        if self.strict_input_closure {
            return Err(self.channel_closed("data"));
        }
        Ok(self.finish_input())
    }

    /// Synchronous lifecycle work; the region sends any buffered data on its data lane.
    fn finish_input(&mut self) -> Option<StageOutput> {
        let stats = self.stats.as_ref();
        let _timer = StageTimer::new(stats);
        let result = match &mut self.engine {
            CollectionStage::Encoder(stage) => {
                stage.clear_deadline();
                Some(StageOutput::Encoder(EncoderEmission::finish(None)))
            }
            CollectionStage::ByteTransform(stage) => Some(StageOutput::Single(
                stage.finish(stats, self.metrics.transform()),
            )),
            CollectionStage::Sampler(stage) => Some(StageOutput::Single(stage.finish())),
            CollectionStage::TimedBatch(stage) => Some(StageOutput::TimedBatch(stage.finish(None))),
            CollectionStage::Watermark(stage) => {
                stage.clear();
                None
            }
            CollectionStage::Sliding(stage) => {
                stage.clear();
                None
            }
            CollectionStage::StreamingSliding(stage) => {
                stage.clear();
                None
            }
            CollectionStage::CountWindow(window) => Some(StageOutput::Count(window.finish(None))),
            CollectionStage::EosWindow(stage) => {
                stage.clear();
                None
            }
            CollectionStage::StreamingEos(stage) => {
                stage.clear();
                None
            }
            CollectionStage::Stateful(stage) => {
                stage.clear();
                None
            }
            CollectionStage::StateWindow(stage) => {
                stage.clear();
                None
            }
            CollectionStage::StreamingState(stage) => {
                stage.clear();
                None
            }
            CollectionStage::TumblingWindow(window) => {
                window.drop_all();
                None
            }
            CollectionStage::StreamingTumbling(window) => {
                window.clear();
                None
            }
            CollectionStage::StreamingCount(window) => {
                window.clear();
                None
            }
            _ => None,
        };
        if let Some(output) = &result {
            if let Err(error) = self.record_immediate(output) {
                stats.record_error_logged("stage finish output error", error.to_string());
            }
        }
        result
    }

    fn apply(
        engine: &mut CollectionStage,
        name: &str,
        input: Box<dyn Collection>,
        stats: &ProcessorStats,
        suppression_counters: &Option<(CounterHandle, CounterHandle)>,
    ) -> Result<Option<Box<dyn Collection>>, ProcessorError> {
        match engine {
            CollectionStage::Normalize { source, keys } => {
                normalize_collection(keys.as_ref(), source, keys, input.as_ref(), name).map(Some)
            }
            CollectionStage::Materialize {
                source,
                keys,
                accessor,
            } => materialize_collection(source, keys, accessor, input.as_ref(), Some(stats))
                .map(Some),
            CollectionStage::RowDiff {
                accessor,
                keys,
                tracked,
                state,
            } => apply_row_diff(input, accessor, keys, tracked, state, Some(stats)).map(Some),
            CollectionStage::ChangeGate {
                source,
                tracked,
                state,
            } => gate_collection(input, source, tracked, state, Some(stats)),
            CollectionStage::BatchCompute(evaluations) => {
                let (output, errors) = apply_batch_compute(input, evaluations)?;
                for error in errors {
                    stats.record_error_logged("batch function compute error", error);
                }
                Ok(Some(output))
            }
            CollectionStage::Filter(stage) => {
                let out = apply_filter(input.as_ref(), stage, Some(stats))?;
                Ok((out.num_rows() != 0).then_some(out))
            }
            CollectionStage::Project(stage) => {
                apply_project(input.as_ref(), stage, Some(stats)).map(Some)
            }
            CollectionStage::Compute(fields) => {
                if fields.is_empty() {
                    Ok(Some(input))
                } else {
                    apply_compute(input.as_ref(), fields, Some(stats)).map(Some)
                }
            }
            CollectionStage::Order(stage) => apply_order(input, stage, Some(stats)).map(Some),
            CollectionStage::EmptySuppress(omit) => {
                let suppressed = *omit && collection_is_effectively_empty(input.as_ref());
                if let Some((forwarded, dropped)) = suppression_counters {
                    if suppressed {
                        dropped.inc_by(1);
                    } else {
                        forwarded.inc_by(1);
                    }
                }
                Ok((!suppressed).then_some(input))
            }
            CollectionStage::Aggregation(plan, registry) => {
                AggregationProcessor::process_batch_with_grouping(
                    plan,
                    registry,
                    input.as_ref(),
                    stats,
                )
                .map_err(|error| {
                    ProcessorError::ProcessingError(format!(
                        "Failed to process aggregation: {error}"
                    ))
                })
            }
            CollectionStage::Encoder(_)
            | CollectionStage::ByteTransform(_)
            | CollectionStage::Sampler(_)
            | CollectionStage::TimedBatch(_)
            | CollectionStage::Decoder(_)
            | CollectionStage::Barrier(_)
            | CollectionStage::Watermark(_)
            | CollectionStage::TumblingWindow(_)
            | CollectionStage::StreamingTumbling(_)
            | CollectionStage::CountWindow(_)
            | CollectionStage::StreamingCount(_)
            | CollectionStage::EosWindow(_)
            | CollectionStage::StreamingEos(_)
            | CollectionStage::Stateful(_)
            | CollectionStage::StateWindow(_)
            | CollectionStage::StreamingState(_)
            | CollectionStage::Sliding(_)
            | CollectionStage::StreamingSliding(_) => Err(ProcessorError::ProcessingError(
                "window requires event processing".into(),
            )),
        }
    }
}

impl Stage {
    fn control_preflush(
        &mut self,
        signal: &ControlSignal,
    ) -> Result<Option<StageOutput>, ProcessorError> {
        if matches!(self.engine, CollectionStage::Encoder(_)) {
            return Ok(self.finish_input());
        }
        let _timer = StageTimer::new(&self.stats);
        let result = match &mut self.engine {
            CollectionStage::Watermark(engine) => engine
                .control(signal, &self.stats)
                .map(|data| Some(StageOutput::immediate(data))),
            _ => Ok(None),
        };
        if let Ok(Some(output)) = &result {
            self.record_immediate(output)?;
        }
        result
    }
    fn align_data(
        &mut self,
        upstream: usize,
        data: StreamData,
    ) -> Result<BarrierAction, ProcessorError> {
        let _timer = StageTimer::new(&self.stats);
        let result = match &mut self.engine {
            CollectionStage::Barrier(engine) => engine.process(upstream, data),
            _ => Ok(BarrierAction::Forward(Some(data))),
        };
        if let Err(error) = &result {
            self.stats
                .record_error_logged("stage alignment error", error.to_string());
        }
        result
    }
    fn align_control(
        &mut self,
        signal: ControlSignal,
    ) -> Result<Option<ControlSignal>, ProcessorError> {
        let _timer = StageTimer::new(&self.stats);
        let result = match &mut self.engine {
            CollectionStage::Barrier(engine) => engine.control(signal),
            _ => Ok(Some(signal)),
        };
        if let Err(error) = &result {
            self.stats
                .record_error_logged("stage alignment error", error.to_string());
        }
        result
    }
    fn input_lagged(&self, skipped: u64) {
        self.stats.record_error_logged(
            "stage data input lag",
            format!("data input skipped {skipped} items"),
        );
    }
    fn output_failed(
        &mut self,
        flags: Option<crate::processor::EncodedDeliveryFlags>,
        pending: Option<&mut StageOutput>,
    ) -> bool {
        if let (
            CollectionStage::Encoder(engine),
            Some(flags),
            Some(StageOutput::Encoder(pending)),
        ) = (&mut self.engine, flags, pending)
        {
            let _timer = StageTimer::new(&self.stats);
            engine.output_failed(flags);
            pending.output_failed();
            true
        } else {
            false
        }
    }
}

/// Region-owned transport loop; stage callbacks own all processing metrics.
async fn drain_outputs(
    stages: &mut [Stage],
    emissions: &mut Vec<(usize, StageOutput)>,
    output: &LinkOutput<StreamData>,
    capacity: usize,
) -> Result<(), ProcessorError> {
    let mut steps = 0;
    macro_rules! work_step {
        () => {
            steps += 1;
            if steps == 64 {
                tokio::task::yield_now().await;
                steps = 0;
            }
        };
    }
    'drain: while !emissions.is_empty() {
        work_step!();
        let (mut index, data, consumed) = {
            let (index, pending) = emissions.last_mut().expect("pending stage output");
            if let StageOutput::Single(data) = pending {
                (*index, data.take(), true)
            } else {
                (*index, stages[*index].next_output(pending)?, false)
            }
        };
        if consumed || data.is_none() {
            emissions.pop();
        }
        let Some(mut data) = data else {
            continue;
        };
        while index + 1 < stages.len() {
            work_step!();
            index += 1;
            if stages[index].single_output {
                let Some(next) = stages[index].process_single(data)? else {
                    continue 'drain;
                };
                data = next;
                continue;
            }
            match stages[index].process(data)? {
                StageOutput::Single(Some(next)) => data = next,
                StageOutput::Single(None) => continue 'drain,
                pending => {
                    emissions.push((index, pending));
                    continue 'drain;
                }
            }
        }
        let flags = match &data {
            StreamData::EncodedDelivery { flags, .. } => Some(*flags),
            _ => None,
        };
        if let Err(error) = send_with_backpressure(output, capacity, data, None).await {
            if let Some((owner, pending)) = emissions.last_mut() {
                if *owner == index
                    && !stages
                        .iter()
                        .any(|stage| matches!(stage.engine, CollectionStage::ByteTransform(_)))
                    && stages[index].output_failed(flags, Some(pending))
                {
                    continue;
                }
            }
            return Err(error);
        }
    }
    Ok(())
}

/// Keep the ordinary stream fast path unless checkpoint alignment needs port identity.
enum RegionInput {
    Ordinary(FanInStream<StreamData>),
    Aligned(PausableFanInStream<StreamData>),
}
impl RegionInput {
    async fn next(&mut self) -> Option<Result<IndexedInput<StreamData>, BroadcastStreamRecvError>> {
        match self {
            Self::Ordinary(input) => input
                .next()
                .await
                .map(|item| item.map(|item| IndexedInput::Item { upstream: 0, item })),
            Self::Aligned(input) => input.next().await,
        }
    }
    fn pause(&mut self, upstream: usize) {
        if let Self::Aligned(input) = self {
            input.pause(upstream);
        }
    }
    fn resume_all(&mut self) {
        if let Self::Aligned(input) = self {
            input.resume_all();
        }
    }
}

pub(crate) struct RegionProcessor {
    isolate_opaque: bool,
    id: String,
    // Drivers only require Send. Move them into the task once without adding
    // synchronization to the synchronous stage hot path.
    stages: std::sync::Mutex<Vec<Stage>>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    pub(crate) output: LinkOutput<StreamData>,
    pub(crate) control_output: LinkOutput<ControlSignal>,
    capacities: ProcessorChannelCapacities,
}

impl RegionProcessor {
    pub(crate) fn new(
        id: String,
        plans: &[PhysicalPlan],
        mut resources: RegionResources,
        capacities: ProcessorChannelCapacities,
    ) -> Result<Self, ProcessorError> {
        if (resources.isolate_opaque && plans.len() != 1)
            || plans.is_empty()
            || plans
                .iter()
                .skip(1)
                .any(|p| matches!(p, PhysicalPlan::Barrier(_)))
        {
            return Err(ProcessorError::InvalidConfiguration(
                "region requires stages and Barrier must be its first stage".into(),
            ));
        }
        Ok(Self {
            isolate_opaque: resources.isolate_opaque,
            id,
            stages: std::sync::Mutex::new(
                plans
                    .iter()
                    .map(|plan| Stage::from_plan(plan, &mut resources))
                    .collect::<Result<_, _>>()?,
            ),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output: LinkOutput::new(capacities.data_link_kind, capacities.data),
            control_output: LinkOutput::new(capacities.control_link_kind, capacities.control),
            capacities,
        })
    }

    pub(crate) fn stage_stats(&self) -> Vec<ProcessorStatsHandle> {
        self.stages
            .lock()
            .expect("stage ownership mutex poisoned")
            .iter()
            .map(|stage| ProcessorStatsHandle {
                processor_id: self
                    .id
                    .rsplit_once('/')
                    .map(|(prefix, _)| format!("{prefix}/{}", stage.name))
                    .unwrap_or_else(|| stage.name.clone()),
                stats: Arc::clone(&stage.stats),
            })
            .collect()
    }
}

impl Processor for RegionProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut stages = match self.stages.get_mut() {
            Ok(stages) if !stages.is_empty() => std::mem::take(stages),
            _ => {
                return ProcessorStart::failed(
                    spawner,
                    ProcessorError::ProcessingError(
                        "region already started or stage ownership poisoned".into(),
                    ),
                )
            }
        };
        // External channel events belong to the entry stage. Data EOF then
        // propagates in stage order, after each predecessor's buffered output.
        let strict_input_closure = stages
            .first()
            .is_some_and(|stage| stage.strict_input_closure);
        let fatal_data_lag = stages.first().is_some_and(|stage| stage.fatal_data_lag);
        let mut emissions = Vec::with_capacity(stages.len());
        if let Some(Stage {
            engine: CollectionStage::Barrier(stage),
            ..
        }) = stages.first()
        {
            let expected = stage.expected_upstreams();
            if expected == 0
                || self.inputs.len() != expected
                || self.control_inputs.len() != expected
            {
                return ProcessorStart::failed(spawner, ProcessorError::InvalidConfiguration(format!(
                    "region Barrier upstream mismatch: expected={expected}, data={}, control={}", self.inputs.len(), self.control_inputs.len()
                )));
            }
        }
        let inputs = std::mem::take(&mut self.inputs);
        let mut input = if matches!(
            stages.first().map(|s| &s.engine),
            Some(CollectionStage::Barrier(_))
        ) {
            RegionInput::Aligned(fan_in_streams_indexed(inputs))
        } else {
            RegionInput::Ordinary(fan_in_streams(inputs))
        };
        let mut control = fan_in_control_streams(std::mem::take(&mut self.control_inputs));
        let mut control_active = !control.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let capacities = self.capacities;
        let id = self.id.clone();
        let span = tracing::info_span!("execution_region", region_id = %id);
        if self.isolate_opaque {
            let stage = stages.pop().expect("opaque region has one stage");
            let driver = opaque::OpaqueDriver {
                stage,
                input,
                control,
                output,
                control_output,
                capacities,
                spawner: spawner.clone(),
                id,
            };
            return ProcessorStart::ready(
                spawner.spawn(tracing::Instrument::instrument(driver.run(), span)),
            );
        }
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            let mut clocks: Vec<_> = stages.iter_mut().enumerate().filter_map(|(index, stage)| {
                stage.start_clock().map(|clock| (index, clock))
            }).collect();
            let mut clock_cursor = 0;
            let mut pending_terminal = None;
            loop {
                for (index, clock) in &mut clocks {
                    clock.update(stages[*index].clock_deadline()?);
                }
                tokio::select! {
                    biased;
                    item = control.next(), if control_active => {
                        match item {
                            Some(Ok(signal)) => {
                                let signal = if matches!(stages[0].engine, CollectionStage::Barrier(_)) {
                                    let Some(signal) = stages[0].align_control(signal)? else { continue; };
                                    signal
                                } else { signal };
                                let terminal = signal.is_terminal();
                                for index in 0..stages.len() {
                                    if !(matches!(stages[index].engine, CollectionStage::Watermark(_))
                                        || terminal && matches!(stages[index].engine, CollectionStage::Encoder(_))) {
                                        continue;
                                    }
                                    let pending = stages[index].control_preflush(&signal)?;
                                    if let Some(pending) = pending {
                                        emissions.push((index, pending));
                                    }
                                    drain_outputs(&mut stages, &mut emissions, &output, capacities.data).await?;
                                }
                                if terminal && stages.iter().any(|stage| matches!(&stage.engine, CollectionStage::ByteTransform(engine) if engine.active())) {
                                    pending_terminal = Some(signal);
                                    control_active = false;
                                    continue;
                                }
                                send_control_with_backpressure(&control_output, capacities.control, signal).await?;
                                if terminal {
                                    for index in 0..stages.len() {
                                        let pending = stages[index].finish_input();
                                        if let Some(pending) = pending {
                                            emissions.push((index, pending));
                                        }
                                        drain_outputs(&mut stages, &mut emissions, &output, capacities.data).await?;
                                    }
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "region control input");
                                if matches!(stages[0].engine, CollectionStage::ByteTransform(_) | CollectionStage::Encoder(_)) {
                                    control_active = false;
                                }
                            }
                            None => {
                                if strict_input_closure { return Err(stages[0].channel_closed("control")); }
                                control_active = false;
                            }
                        }
                    }
                    (index, event) = poll_fn(|cx| {
                        for offset in 0..clocks.len() {
                            let slot = (clock_cursor + offset) % clocks.len();
                            let (index, clock) = &mut clocks[slot];
                            if let std::task::Poll::Ready(event) = clock.poll_next(cx) {
                                let index = *index;
                                clock_cursor = (slot + 1) % clocks.len();
                                return std::task::Poll::Ready((index, event));
                            }
                        }
                        std::task::Poll::Pending
                    }), if !clocks.is_empty() => {
                        let pending = Some(stages[index].on_timer(event)?);
                        if let Some(pending) = pending {
                            emissions.push((index, pending));
                        }
                        drain_outputs(&mut stages, &mut emissions, &output, capacities.data).await?;
                    }
                    item = input.next() => {
                        let mut resume = false;
                        let data = match item {
                            Some(Ok(IndexedInput::Item { upstream, item: data })) => {
                                if matches!(stages[0].engine, CollectionStage::Barrier(_)) {
                                    match stages[0].align_data(upstream, data)? {
                                        BarrierAction::Pause => { input.pause(upstream); continue; }
                                        BarrierAction::Forward(None) => continue,
                                        BarrierAction::Forward(Some(data)) => data,
                                        BarrierAction::Resume(data) => { resume = true; data }
                                    }
                                } else { data }
                            },
                            Some(Ok(IndexedInput::Closed)) => {
                                if strict_input_closure { return Err(stages[0].channel_closed("data")); }
                                continue;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                if fatal_data_lag {
                                    stages[0].input_lagged(skipped);
                                    return Err(ProcessorError::ChannelClosed);
                                }
                                log_broadcast_lagged(&id, skipped, "region data input");
                                continue;
                            }
                            None => {
                                for index in 0..stages.len() {
                                    let pending = stages[index].input_closed()?;
                                    if let Some(pending) = pending {
                                        emissions.push((index, pending));
                                    }
                                    drain_outputs(&mut stages, &mut emissions, &output, capacities.data).await?;
                                }
                                if let Some(terminal) = pending_terminal.take() {
                                    send_control_with_backpressure(&control_output, capacities.control, terminal).await?;
                                }
                                return Ok(());
                            }
                        };
                        let terminal = data.is_terminal();
                        emissions.push((0usize, stages[0].process(data)?));
                        drain_outputs(&mut stages, &mut emissions, &output, capacities.data).await?;
                        if terminal { return Ok(()); }
                        if let Some(terminal) = pending_terminal.take_if(|_| !stages.iter().any(|stage| matches!(&stage.engine, CollectionStage::ByteTransform(engine) if engine.active()))) {
                            send_control_with_backpressure(&control_output, capacities.control, terminal).await?;
                            return Ok(());
                        }
                        if resume { input.resume_all(); }
                    }
                }
            }
        }, span)))
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }
    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }
    fn add_input<R: Into<LinkReceiver<StreamData>>>(&mut self, receiver: R) {
        self.inputs.push(receiver.into());
    }
    fn add_control_input<R: Into<LinkReceiver<ControlSignal>>>(&mut self, receiver: R) {
        self.control_inputs.push(receiver.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::ScalarExpr;
    use crate::model::{Message, RecordBatch, Tuple};
    use crate::planner::physical::{PhysicalCompute, PhysicalFilter};
    use crate::processor::base::{LinkKind, ProcessorChannelCapacities};
    use crate::processor::{BarrierControlSignalKind, ComputeProcessor, FilterProcessor};
    use datatypes::{BooleanType, ConcreteDatatype, Value};
    use sqlparser::ast::{Expr, Value as SqlValue};
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    fn input() -> StreamData {
        StreamData::collection(Box::new(
            RecordBatch::new(vec![Tuple::new(vec![Arc::new(Message::new(
                "input",
                vec![Arc::from("a")],
                vec![Value::Int64(7)],
            ))])])
            .unwrap(),
        ))
    }

    fn plans(accept: bool) -> Vec<PhysicalPlan> {
        vec![
            PhysicalPlan::Filter(PhysicalFilter::new(
                Expr::Value(SqlValue::Boolean(accept)),
                ScalarExpr::Literal(Value::Bool(accept), ConcreteDatatype::Bool(BooleanType)),
                vec![],
                1,
            )),
            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 2)),
        ]
    }

    fn json_encoder_runtime() -> Box<dyn crate::codec::SinkEncoder> {
        use crate::codec::{JsonEncoder, SinkEncoderFactory};
        use crate::planner::physical::output_layout::{
            OutputColumnLayout, OutputLayout, OutputValueRef,
        };
        let layout = Arc::new(OutputLayout::new(vec![OutputColumnLayout {
            name: Arc::from("a"),
            data_type: ConcreteDatatype::Int64(datatypes::Int64Type),
            value_ref: OutputValueRef::Message {
                message_index: 0,
                value_index: 0,
            },
        }]));
        Arc::new(
            JsonEncoder::new("json", &crate::planner::sink::SinkEncoderConfig::json()).unwrap(),
        )
        .with_output_layout(layout)
        .unwrap()
        .start_encoder()
        .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn encoder_regions_match_standalone_batching_clocks_framing_and_terminal_lanes() {
        use crate::planner::physical::PhysicalSinkEncoder;
        use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
        use crate::processor::processor_builder::PlanProcessor;
        use crate::processor::{EncodedDeliveryFlags, SinkCompressProcessor, SinkEncoderProcessor};
        let rows = |values: &[i64]| {
            StreamData::collection(Box::new(
                crate::model::batch_from_columns_simple(vec![(
                    "input".into(),
                    "a".into(),
                    values.iter().copied().map(Value::Int64).collect(),
                )])
                .unwrap(),
            ))
        };
        for common in [
            CommonSinkProps::default(),
            CommonSinkProps {
                batch_count: Some(2),
                batch_duration: None,
            },
            CommonSinkProps {
                batch_count: None,
                batch_duration: Some(Duration::from_millis(100)),
            },
            CommonSinkProps {
                batch_count: Some(2),
                batch_duration: Some(Duration::from_millis(100)),
            },
        ] {
            for compress in [false, true] {
                for mode in 0..5 {
                    let mut expected = None;
                    for executor in 0..3 {
                        let fused = executor != 0;
                        let isolated = executor == 2;
                        if isolated && compress {
                            continue;
                        }
                        let capacities =
                            ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                        let encoder = json_encoder_runtime();
                        let mut standalone = SinkEncoderProcessor::new_with_channel_capacities(
                            "encoder",
                            encoder,
                            common.batch_count,
                            common.batch_duration,
                            capacities,
                        );
                        let own = Arc::new(ProcessorStats::default());
                        let boundary = Arc::new(ProcessorStats::default());
                        standalone.set_stats(Arc::clone(&own));
                        let mut processors = if fused {
                            let prefix =
                                PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 1));
                            let encoder_plan = PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
                                vec![Arc::new(prefix.clone())],
                                2,
                                "test".into(),
                                SinkEncoderConfig::json(),
                                common.clone(),
                            ));
                            let mut specs = if isolated {
                                vec![encoder_plan.clone()]
                            } else {
                                vec![prefix, encoder_plan.clone()]
                            };
                            if compress {
                                specs.push(PhysicalPlan::SinkCompress(
                                    crate::planner::physical::PhysicalSinkCompress::new(
                                        Arc::new(encoder_plan),
                                        3,
                                        crate::codec::CompressionCodec::gzip(),
                                    ),
                                ));
                            }
                            let mut resources = RegionResources {
                                isolate_opaque: isolated,
                                ..Default::default()
                            };
                            resources
                                .encoders
                                .insert(2, standalone.into_stage().unwrap());
                            let mut region = RegionProcessor::new(
                                "encoder".into(),
                                &specs,
                                resources,
                                capacities,
                            )
                            .unwrap();

                            region
                                .stages
                                .get_mut()
                                .unwrap()
                                .iter_mut()
                                .find(|stage| matches!(stage.engine, CollectionStage::Encoder(_)))
                                .unwrap()
                                .set_stats(Arc::clone(&own));
                            region
                                .stages
                                .get_mut()
                                .unwrap()
                                .last_mut()
                                .unwrap()
                                .set_stats(Arc::clone(&boundary));
                            vec![PlanProcessor::Region(Box::new(region))]
                        } else {
                            let mut processors = vec![PlanProcessor::SinkEncoder(standalone)];
                            if compress {
                                processors.push(PlanProcessor::SinkCompress(
                                    SinkCompressProcessor::new_with_channel_capacities(
                                        "gzip",
                                        crate::codec::CompressionCodec::gzip()
                                            .build_writer()
                                            .unwrap(),
                                        capacities,
                                    ),
                                ));
                            }
                            processors
                        };
                        let (tx, rx) = mpsc::channel(1);
                        let (control_tx, control_rx) = mpsc::channel(1);
                        processors[0].add_input(rx.into());
                        processors[0].add_control_input(control_rx);
                        for index in 1..processors.len() {
                            let input = processors[index - 1].subscribe_output().unwrap();
                            let control = processors[index - 1].subscribe_control_output().unwrap();
                            processors[index].add_input(input);
                            processors[index].add_control_input(control);
                        }
                        let mut output = processors.last().unwrap().subscribe_output().unwrap();
                        let mut control_output = processors
                            .last()
                            .unwrap()
                            .subscribe_control_output()
                            .unwrap();
                        let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                        let handles: Vec<_> = processors
                            .iter_mut()
                            .map(|processor| processor.start(&spawner).handle)
                            .collect();
                        drop(processors);
                        let fence =
                            ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(1));
                        let mut frames = Vec::new();
                        tx.send(rows(&[1, 2, 3])).await.unwrap();
                        for round in 0..3 {
                            if round == 1 && common.batch_duration.is_some() {
                                tokio::time::advance(Duration::from_millis(100)).await;
                            }
                            if round == 2 {
                                tx.send(rows(&[])).await.unwrap();
                            }
                            tx.send(StreamData::control(fence.clone())).await.unwrap();
                            loop {
                                let data = output.recv().await.unwrap();
                                if data.as_control() == Some(&fence) {
                                    break;
                                }
                                let StreamData::EncodedDelivery { flags, bytes } = data else {
                                    panic!("unexpected encoder output");
                                };
                                frames.push((flags, bytes));
                            }
                        }
                        let terminal = if mode == 1 || mode == 3 {
                            ControlSignal::Instant(
                                crate::processor::InstantControlSignal::StreamQuickEnd {
                                    signal_id: 2,
                                },
                            )
                        } else {
                            ControlSignal::Barrier(
                                crate::processor::BarrierControlSignal::StreamGracefulEnd {
                                    barrier_id: 2,
                                },
                            )
                        };
                        if mode < 2 {
                            tx.send(StreamData::control(terminal.clone()))
                                .await
                                .unwrap();
                        } else if mode < 4 {
                            control_tx.send(terminal.clone()).await.unwrap();
                        }
                        drop(tx);
                        if mode == 2 || mode == 3 {
                            assert_eq!(control_output.recv().await.unwrap(), terminal);
                        }
                        loop {
                            match output.recv().await {
                                Ok(StreamData::EncodedDelivery { flags, bytes }) => {
                                    frames.push((flags, bytes))
                                }
                                Ok(data) => {
                                    assert!(mode < 2);
                                    assert_eq!(data.as_control(), Some(&terminal));
                                    break;
                                }
                                Err(ProcessorError::ChannelClosed) => {
                                    assert!(mode >= 2);
                                    break;
                                }
                                Err(error) => panic!("encoder receive failed: {error}"),
                            }
                        }
                        for handle in handles {
                            handle.await.unwrap().unwrap();
                        }
                        if !fused {
                            let stats = own.snapshot();
                            assert_eq!(stats.error_count, 0);
                            assert_eq!(stats.records_in, Some(3));
                        }
                        if fused {
                            let stats = boundary.snapshot();
                            assert_eq!(stats.records_in, if compress { None } else { Some(3) });
                            assert_eq!(stats.records_out, None);
                            assert_eq!(
                                stats.custom["messages_out"],
                                frames
                                    .iter()
                                    .filter(|(flags, _)| flags.contains(EncodedDeliveryFlags::END))
                                    .count() as u64
                            );
                            assert_eq!(
                                stats.custom["bytes_out"],
                                frames
                                    .iter()
                                    .map(|(_, bytes)| bytes.len() as u64)
                                    .sum::<u64>()
                            );
                        }
                        let actual = frames;
                        if let Some(expected) = &expected {
                            assert_eq!(
                                &actual, expected,
                                "count={:?}, duration={:?}, compress={compress}, mode={mode}",
                                common.batch_count, common.batch_duration
                            );
                        } else {
                            expected = Some(actual);
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn encoder_region_faults_abort_once_drop_failed_input_and_recover() {
        use crate::codec::{EncodeError, SinkEncoder};
        use crate::planner::physical::PhysicalSinkEncoder;
        use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
        use crate::processor::batch_policy::BatchPolicy;
        use crate::processor::EncodedDeliveryFlags;
        use bytes::Bytes;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct FaultEncoder {
            inner: Box<dyn SinkEncoder>,
            phase: u8,
            failed: bool,
            appends: usize,
            aborts: Arc<AtomicUsize>,
        }
        impl SinkEncoder for FaultEncoder {
            fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                let result = self.inner.begin_delivery()?;
                if self.phase == 0 && !self.failed {
                    self.failed = true;
                    return Err(EncodeError::Other("injected begin failure".into()));
                }
                Ok(result)
            }
            fn append(&mut self, input: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
                self.appends += 1;
                if self.phase == 1 && self.appends == 2 {
                    self.failed = true;
                    return Err(EncodeError::Other("injected append failure".into()));
                }
                self.inner.append(input)
            }
            fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                if self.phase == 2 && !self.failed {
                    self.failed = true;
                    return Err(EncodeError::Other("injected finish failure".into()));
                }
                self.inner.finish_delivery()
            }
            fn abort_delivery(&mut self) {
                self.aborts.fetch_add(1, Ordering::Relaxed);
                self.inner.abort_delivery();
            }
        }
        for phase in 0..3 {
            for compress in [false, true] {
                let encoder_plan = PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
                    vec![],
                    2,
                    "test".into(),
                    SinkEncoderConfig::json(),
                    CommonSinkProps {
                        batch_count: Some(2),
                        batch_duration: None,
                    },
                ));
                let mut specs = vec![encoder_plan.clone()];
                if compress {
                    specs.push(PhysicalPlan::SinkCompress(
                        crate::planner::physical::PhysicalSinkCompress::new(
                            Arc::new(encoder_plan),
                            3,
                            crate::codec::CompressionCodec::gzip(),
                        ),
                    ));
                }
                let aborts = Arc::new(AtomicUsize::new(0));
                let encoder = FaultEncoder {
                    inner: json_encoder_runtime(),
                    phase,
                    failed: false,
                    appends: 0,
                    aborts: Arc::clone(&aborts),
                };
                let mut resources = RegionResources::default();
                resources.encoders.insert(
                    2,
                    EncoderStage::new(Box::new(encoder), BatchPolicy::new(Some(2), None)),
                );
                let capacities =
                    ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                let mut region =
                    RegionProcessor::new("fault".into(), &specs, resources, capacities).unwrap();
                let boundary = Arc::new(ProcessorStats::default());

                region
                    .stages
                    .get_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|stage| matches!(stage.engine, CollectionStage::Encoder(_)))
                    .unwrap()
                    .set_stats(Arc::clone(&boundary));
                let (tx, rx) = mpsc::channel(1);
                region.add_input(rx);
                let mut output = region.subscribe_output().unwrap();
                let task = region
                    .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
                    .handle;
                drop(region);
                let fence = ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(1));
                let mut frames = Vec::new();
                for values in [&[1][..], &[2, 3, 4][..], &[5, 6][..]] {
                    tx.send(StreamData::collection(Box::new(
                        crate::model::batch_from_columns_simple(vec![(
                            "input".into(),
                            "a".into(),
                            values.iter().copied().map(Value::Int64).collect(),
                        )])
                        .unwrap(),
                    )))
                    .await
                    .unwrap();
                    tx.send(StreamData::control(fence.clone())).await.unwrap();
                    loop {
                        let data = timeout(Duration::from_secs(2), output.recv())
                            .await
                            .unwrap()
                            .unwrap();
                        if data.as_control() == Some(&fence) {
                            break;
                        }
                        let StreamData::EncodedDelivery { flags, bytes } = data else {
                            panic!("unexpected fault output");
                        };
                        frames.push((flags, bytes));
                    }
                }
                drop(tx);
                while let Ok(data) = timeout(Duration::from_secs(2), output.recv())
                    .await
                    .unwrap()
                {
                    let StreamData::EncodedDelivery { flags, bytes } = data else {
                        panic!("unexpected terminal output");
                    };
                    frames.push((flags, bytes));
                }
                task.await.unwrap().unwrap();
                assert_eq!(aborts.load(Ordering::Relaxed), 1);
                assert_eq!(boundary.snapshot().error_count, 1);
                assert_eq!(boundary.snapshot().custom["messages_aborted"], 1);
                assert_eq!(boundary.snapshot().custom["messages_aborted"], 1);
                let mut pending = Vec::new();
                let mut values = Vec::new();
                let mut abort_frames = 0;
                for (flags, bytes) in frames {
                    if flags.contains(EncodedDeliveryFlags::ABORT) {
                        pending.clear();
                        abort_frames += 1;
                        continue;
                    }
                    pending.extend_from_slice(&bytes);
                    if flags.contains(EncodedDeliveryFlags::END) {
                        let json = if compress {
                            use std::io::Read;
                            let mut decoded = Vec::new();
                            flate2::read::GzDecoder::new(pending.as_slice())
                                .read_to_end(&mut decoded)
                                .unwrap();
                            decoded
                        } else {
                            pending.clone()
                        };
                        let rows: serde_json::Value = serde_json::from_slice(&json).unwrap();
                        values.extend(
                            rows.as_array()
                                .unwrap()
                                .iter()
                                .map(|row| row["a"].as_i64().unwrap()),
                        );
                        pending.clear();
                    }
                }
                assert_eq!(abort_frames, usize::from(phase != 0));
                assert_eq!(
                    values,
                    if phase == 0 {
                        vec![2, 3, 4, 5, 6]
                    } else {
                        vec![5, 6]
                    },
                    "phase={phase}, compress={compress}"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn encoder_region_closed_output_preserves_feed_timer_and_control_error_scopes() {
        use crate::codec::{EncodeError, SinkEncoder};
        use crate::planner::physical::PhysicalSinkEncoder;
        use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
        use crate::processor::batch_policy::BatchPolicy;
        use bytes::Bytes;
        struct DeferredEncoder;
        impl SinkEncoder for DeferredEncoder {
            fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                Ok(None)
            }
            fn append(&mut self, _: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
                Ok(None)
            }
            fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                Ok(Some(Bytes::from_static(b"payload")))
            }
        }
        for mode in 0..5 {
            for (compress, isolated) in [(false, false), (true, false), (false, true)] {
                let common = match mode {
                    0 => CommonSinkProps::default(),
                    1 => CommonSinkProps {
                        batch_count: None,
                        batch_duration: Some(Duration::from_millis(10)),
                    },
                    _ => CommonSinkProps {
                        batch_count: Some(2),
                        batch_duration: None,
                    },
                };
                let encoder = PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
                    vec![],
                    2,
                    "test".into(),
                    SinkEncoderConfig::json(),
                    common.clone(),
                ));
                let mut specs = vec![encoder.clone()];
                if compress {
                    specs.push(PhysicalPlan::SinkCompress(
                        crate::planner::physical::PhysicalSinkCompress::new(
                            Arc::new(encoder),
                            3,
                            crate::codec::CompressionCodec::gzip(),
                        ),
                    ));
                }
                let mut resources = RegionResources {
                    isolate_opaque: isolated,
                    ..Default::default()
                };
                resources.encoders.insert(
                    2,
                    EncoderStage::new(
                        Box::new(DeferredEncoder),
                        BatchPolicy::new(common.batch_count, common.batch_duration),
                    ),
                );
                let capacities =
                    ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                let mut region =
                    RegionProcessor::new("closed".into(), &specs, resources, capacities).unwrap();
                let boundary = Arc::new(ProcessorStats::default());
                let transport = Arc::new(ProcessorStats::default());
                region.output.set_stats(Arc::clone(&transport));

                region
                    .stages
                    .get_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|stage| matches!(stage.engine, CollectionStage::Encoder(_)))
                    .unwrap()
                    .set_stats(Arc::clone(&boundary));
                let (tx, rx) = mpsc::channel(1);
                let (control_tx, control_rx) = mpsc::channel(1);
                region.add_input(rx);
                region.add_control_input(control_rx);
                drop(region.subscribe_output().unwrap());
                let mut control_output = region.subscribe_control_output().unwrap();
                let task = region
                    .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
                    .handle;
                tx.send(input()).await.unwrap();
                timeout(Duration::from_secs(2), async {
                    while boundary.snapshot().records_in != Some(1) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                if isolated {
                    // Input counters can become visible before a worker has finished
                    // lazy encoding. A control acknowledgement is handled only after
                    // the current data event has fully drained.
                    let fence =
                        ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(99));
                    control_tx.send(fence.clone()).await.unwrap();
                    assert_eq!(control_output.recv().await.unwrap(), fence);
                }
                if mode == 1 {
                    tokio::time::advance(Duration::from_millis(10)).await;
                }
                let terminal = ControlSignal::Instant(
                    crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 2 },
                );
                if !compress && mode < 2 {
                    timeout(Duration::from_secs(2), async {
                        while transport.snapshot().error_count != 1 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                }
                if mode == 2 || (!compress && mode < 2) {
                    control_tx.send(terminal.clone()).await.unwrap();
                }
                if mode == 3 {
                    tx.send(StreamData::stream_end()).await.unwrap();
                }
                drop(tx);
                let result = timeout(Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap();
                if compress {
                    assert!(matches!(result, Err(ProcessorError::ChannelClosed)));
                    let snapshot = transport.snapshot();
                    assert_eq!(snapshot.error_count, 1);
                    assert!(snapshot.last_error.is_some());
                } else {
                    if mode == 3 {
                        assert!(matches!(result, Err(ProcessorError::ChannelClosed)));
                    } else {
                        result.unwrap();
                    }
                    if mode < 3 {
                        assert_eq!(control_output.recv().await.unwrap(), terminal);
                    }
                    assert_eq!(
                        transport.snapshot().error_count,
                        if mode == 3 { 2 } else { 1 }
                    );
                }
                assert_eq!(boundary.snapshot().custom["messages_out"], 1);
                assert_eq!(boundary.snapshot().error_count, 0);
            }
        }
    }

    #[tokio::test]
    async fn encoder_region_count_backpressure_defers_remaining_input_encoding() {
        use crate::codec::{EncodeError, SinkEncoder};
        use crate::planner::physical::PhysicalSinkEncoder;
        use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
        use crate::processor::batch_policy::BatchPolicy;
        use bytes::Bytes;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct ObservedEncoder {
            inner: Box<dyn SinkEncoder>,
            appends: Arc<AtomicUsize>,
        }
        impl SinkEncoder for ObservedEncoder {
            fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                self.inner.begin_delivery()
            }
            fn append(&mut self, input: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
                self.appends.fetch_add(1, Ordering::Relaxed);
                self.inner.append(input)
            }
            fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
                self.inner.finish_delivery()
            }
        }
        let plan = PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
            vec![],
            2,
            "test".into(),
            SinkEncoderConfig::json(),
            CommonSinkProps {
                batch_count: Some(1),
                batch_duration: None,
            },
        ));
        let appends = Arc::new(AtomicUsize::new(0));
        let mut resources = RegionResources::default();
        resources.encoders.insert(
            2,
            EncoderStage::new(
                Box::new(ObservedEncoder {
                    inner: json_encoder_runtime(),
                    appends: Arc::clone(&appends),
                }),
                BatchPolicy::new(Some(1), None),
            ),
        );
        let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
        let mut region =
            RegionProcessor::new("backpressure".into(), &[plan], resources, capacities).unwrap();
        let stats = Arc::new(ProcessorStats::default());
        region
            .stages
            .get_mut()
            .unwrap()
            .first_mut()
            .unwrap()
            .set_stats(Arc::clone(&stats));
        let (tx, rx) = mpsc::channel(1);
        region.add_input(rx);
        let mut output = region.subscribe_output().unwrap();
        let task = region
            .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
            .handle;
        tx.send(StreamData::collection(Box::new(
            crate::model::batch_from_columns_simple(vec![(
                "input".into(),
                "a".into(),
                (1..=5).map(Value::Int64).collect(),
            )])
            .unwrap(),
        )))
        .await
        .unwrap();
        tx.send(StreamData::stream_end()).await.unwrap();
        timeout(Duration::from_secs(2), async {
            while appends.load(Ordering::Relaxed) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(appends.load(Ordering::Relaxed), 2);
        assert_eq!(stats.snapshot().custom["messages_out"], 2);
        for value in 1..=5 {
            let StreamData::EncodedDelivery { bytes, .. } =
                timeout(Duration::from_secs(2), output.recv())
                    .await
                    .unwrap()
                    .unwrap()
            else {
                panic!("missing delivery");
            };
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                serde_json::json!([{ "a": value }])
            );
        }
        assert!(output.recv().await.unwrap().is_terminal());
        task.await.unwrap().unwrap();
        assert_eq!(appends.load(Ordering::Relaxed), 5);
        assert_eq!(stats.snapshot().custom["messages_out"], 5);
    }

    #[tokio::test]
    async fn byte_regions_preserve_delivery_framing_metrics_and_exit_lanes() {
        use crate::codec::{CompressionCodec, EncryptionAlgorithm, SecretBytes};
        use crate::planner::physical::{PhysicalSinkCompress, PhysicalSinkEncrypt};
        use crate::processor::processor_builder::PlanProcessor;
        use crate::processor::{EncodedDeliveryFlags, SinkCompressProcessor, SinkEncryptProcessor};
        use std::io::Read;
        for encrypt in [false, true] {
            for mode in 0..8 {
                let mut expected = None;
                for fused in [false, true] {
                    let capacities =
                        ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                    let first = PhysicalPlan::SinkCompress(PhysicalSinkCompress::new(
                        Arc::new(plans(true).remove(0)),
                        10,
                        CompressionCodec::gzip(),
                    ));
                    let last = if encrypt {
                        PhysicalPlan::SinkEncrypt(PhysicalSinkEncrypt::new(
                            Arc::new(first.clone()),
                            11,
                            EncryptionAlgorithm::AesGcm,
                            "region-test",
                            128,
                            Arc::new(SecretBytes::new(vec![7; 16])),
                        ))
                    } else {
                        PhysicalPlan::SinkCompress(PhysicalSinkCompress::new(
                            Arc::new(first.clone()),
                            11,
                            CompressionCodec::zstd(),
                        ))
                    };
                    let specs = [first, last];
                    let (tx, rx) = mpsc::channel(1);
                    let (control_tx, control_rx) = mpsc::channel(1);
                    let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                    let mut stage_stats = Vec::new();
                    let mut processors = if fused {
                        let mut region = RegionProcessor::new(
                            "bytes".into(),
                            &specs,
                            RegionResources::default(),
                            capacities,
                        )
                        .unwrap();
                        for stage in region.stages.get_mut().unwrap().iter_mut() {
                            let stats = Arc::new(ProcessorStats::default());
                            stage_stats.push(Arc::clone(&stats));
                            stage.set_stats(stats);
                        }
                        vec![PlanProcessor::Region(Box::new(region))]
                    } else {
                        specs
                            .iter()
                            .map(|plan| {
                                let mut processor = match plan {
                                    PhysicalPlan::SinkCompress(p) => PlanProcessor::SinkCompress(
                                        SinkCompressProcessor::new_with_channel_capacities(
                                            "compress",
                                            p.codec.build_writer().unwrap(),
                                            capacities,
                                        ),
                                    ),
                                    PhysicalPlan::SinkEncrypt(p) => {
                                        let writer = AesGcmStreamWriter::from_config(
                                            &SinkEncryptionConfig {
                                                algorithm: p.algorithm,
                                                key_id: p.key_id.clone(),
                                                key_bits: p.key_bits,
                                                key: Arc::clone(&p.key),
                                            },
                                        )
                                        .unwrap();
                                        PlanProcessor::SinkEncrypt(
                                            SinkEncryptProcessor::new_with_channel_capacities(
                                                "encrypt",
                                                Box::new(writer),
                                                capacities,
                                            ),
                                        )
                                    }
                                    _ => unreachable!(),
                                };
                                let stats = Arc::new(ProcessorStats::default());
                                processor.set_stats(Arc::clone(&stats));
                                stage_stats.push(stats);
                                processor
                            })
                            .collect::<Vec<_>>()
                    };
                    processors[0].add_input(rx.into());
                    processors[0].add_control_input(control_rx);
                    for index in 1..processors.len() {
                        let input = processors[index - 1].subscribe_output().unwrap();
                        let control = processors[index - 1].subscribe_control_output().unwrap();
                        processors[index].add_input(input);
                        processors[index].add_control_input(control);
                    }
                    let mut output = processors.last().unwrap().subscribe_output().unwrap();
                    let mut control_output = processors
                        .last()
                        .unwrap()
                        .subscribe_control_output()
                        .unwrap();
                    let handles: Vec<_> = processors
                        .iter_mut()
                        .map(|processor| processor.start(&spawner).handle)
                        .collect();
                    drop(processors);
                    tx.send(StreamData::encoded_delivery_start(b"hello ".to_vec()))
                        .await
                        .unwrap();
                    let first = timeout(Duration::from_secs(2), output.recv())
                        .await
                        .unwrap()
                        .unwrap();
                    let StreamData::EncodedDelivery { flags, bytes } = first else {
                        panic!("missing START");
                    };
                    assert_eq!(flags, EncodedDeliveryFlags::START);
                    let mut frames = vec![(flags, bytes.len())];
                    let mut payload = bytes.to_vec();
                    if mode == 7 {
                        tx.send(StreamData::encoded_delivery_start(b"invalid".to_vec()))
                            .await
                            .unwrap();
                        let StreamData::EncodedDelivery { flags, bytes } =
                            timeout(Duration::from_secs(2), output.recv())
                                .await
                                .unwrap()
                                .unwrap()
                        else {
                            panic!("missing ABORT");
                        };
                        assert_eq!(flags, EncodedDeliveryFlags::ABORT);
                        assert!(bytes.is_empty());
                        frames.push((flags, bytes.len()));
                        tx.send(StreamData::encoded_delivery_start(b"hello ".to_vec()))
                            .await
                            .unwrap();
                        let StreamData::EncodedDelivery { flags, bytes } =
                            timeout(Duration::from_secs(2), output.recv())
                                .await
                                .unwrap()
                                .unwrap()
                        else {
                            panic!("missing recovery START");
                        };
                        assert_eq!(flags, EncodedDeliveryFlags::START);
                        frames.push((flags, bytes.len()));
                        payload = bytes.to_vec();
                    }
                    let terminal = if mode == 5 || mode == 6 {
                        ControlSignal::Instant(
                            crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 9 },
                        )
                    } else {
                        ControlSignal::Barrier(
                            crate::processor::BarrierControlSignal::StreamGracefulEnd {
                                barrier_id: 9,
                            },
                        )
                    };
                    let complete = mode < 2 || mode == 7;
                    if complete {
                        tx.send(StreamData::encoded_delivery_chunk(Vec::new()))
                            .await
                            .unwrap();
                        tx.send(StreamData::encoded_delivery_end(b"world".to_vec()))
                            .await
                            .unwrap();
                        if mode == 0 || mode == 7 {
                            tx.send(StreamData::control(terminal.clone()))
                                .await
                                .unwrap();
                        } else {
                            control_tx.send(terminal.clone()).await.unwrap();
                        }
                    } else if mode == 3 || mode == 5 {
                        tx.send(StreamData::control(terminal.clone()))
                            .await
                            .unwrap();
                    } else if mode == 4 || mode == 6 {
                        control_tx.send(terminal.clone()).await.unwrap();
                    }
                    drop(tx);
                    loop {
                        match timeout(Duration::from_secs(2), output.recv())
                            .await
                            .unwrap()
                        {
                            Ok(StreamData::EncodedDelivery { flags, bytes }) => {
                                frames.push((flags, bytes.len()));
                                if flags.contains(EncodedDeliveryFlags::ABORT) {
                                    assert!(bytes.is_empty());
                                    break;
                                }
                                payload.extend_from_slice(&bytes);
                                if flags.contains(EncodedDeliveryFlags::END) {
                                    break;
                                }
                            }
                            _ => panic!("unexpected non-delivery event"),
                        }
                    }
                    if mode == 0 || mode == 3 || mode == 5 || mode == 7 {
                        assert_eq!(
                            timeout(Duration::from_secs(2), output.recv())
                                .await
                                .unwrap()
                                .unwrap()
                                .as_control(),
                            Some(&terminal)
                        );
                    } else if mode == 1 || mode == 4 || mode == 6 {
                        assert_eq!(
                            timeout(Duration::from_secs(2), control_output.recv())
                                .await
                                .unwrap()
                                .unwrap(),
                            terminal
                        );
                    }
                    for handle in handles {
                        timeout(Duration::from_secs(2), handle)
                            .await
                            .unwrap()
                            .unwrap()
                            .unwrap();
                    }
                    if complete && !encrypt {
                        let gzip = zstd::stream::decode_all(payload.as_slice()).unwrap();
                        let mut decoded = Vec::new();
                        flate2::read::GzDecoder::new(gzip.as_slice())
                            .read_to_end(&mut decoded)
                            .unwrap();
                        assert_eq!(decoded, b"hello world");
                    }
                    let counters: Vec<_> = stage_stats
                        .iter()
                        .map(|stats| {
                            let snapshot = stats.snapshot();
                            (snapshot.error_count, snapshot.custom)
                        })
                        .collect();
                    let actual = (frames, counters);
                    if let Some(expected) = &expected {
                        assert_eq!(&actual, expected, "encrypt={encrypt}, mode={mode}");
                    } else {
                        expected = Some(actual);
                    }
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stage_timer_and_lifecycle_metrics_include_synchronous_callbacks() {
        use crate::codec::{CodecError, Merger, MergerOutputKind, MergerRegistry};
        use crate::processor::sampler_processor::{MergerConfig, PackerProps};
        use crate::processor::{SamplerConfig, SamplerProcessor, SamplingStrategy};

        struct SlowMerger {
            pending: bool,
            outcome: u8,
        }
        impl Merger for SlowMerger {
            fn merge(&mut self, _: &[u8]) -> Result<(), CodecError> {
                self.pending = true;
                Ok(())
            }
            fn trigger(&mut self) -> Result<Option<Vec<u8>>, CodecError> {
                if !std::mem::take(&mut self.pending) {
                    return Ok(None);
                }
                std::thread::sleep(Duration::from_millis(20));
                match self.outcome {
                    0 => Ok(Some(b"merged".to_vec())),
                    1 => Ok(None),
                    _ => Err(CodecError::Other("merger failure".into())),
                }
            }
        }
        for event in ["timer", "control", "eof"] {
            for outcome in 0..3 {
                let id = format!("timing_{event}_{outcome}");
                let stats = Arc::new(ProcessorStats::new("timing_test", &*id, "stage"));
                stats.set_pipeline_id("region_callback_timing");
                let histogram = veloflux_metrics::processor_handle_duration_seconds()
                    .with_label_values(&["timing_test", "region_callback_timing", &id]);
                let registry = Arc::new(MergerRegistry::new());
                registry.register("slow", MergerOutputKind::Bytes, move |_, _| {
                    Ok(Box::new(SlowMerger {
                        pending: false,
                        outcome,
                    }))
                });
                let config = SamplerConfig {
                    interval: Duration::from_secs(60),
                    strategy: SamplingStrategy::Packer {
                        props: PackerProps {
                            merger: MergerConfig {
                                merger_type: "slow".into(),
                                props: Default::default(),
                            },
                        },
                    },
                };
                let mut sampler = SamplerProcessor::new("sampler", config.clone());
                sampler.set_merger_registry(registry);
                sampler.set_merger_schema(Arc::new(datatypes::Schema::new(vec![])));
                let mut resources = RegionResources {
                    input_domain: DataDomain::Message,
                    stage_default_domains: DataMetricDomains::NONE.passthrough(DataDomain::Message),
                    ..Default::default()
                };
                resources.samplers.insert(1, sampler.into_stage().unwrap());
                let plan = PhysicalPlan::Sampler(crate::planner::physical::PhysicalSampler::new(
                    config.interval,
                    config.strategy,
                    None,
                    vec![],
                    1,
                ));
                let capacities =
                    ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                let mut region = RegionProcessor::new(id, &[plan], resources, capacities).unwrap();
                region
                    .stages
                    .get_mut()
                    .unwrap()
                    .first_mut()
                    .unwrap()
                    .set_stats(stats);
                let (tx, rx) = mpsc::channel(1);
                let (control_tx, control_rx) = mpsc::channel(1);
                region.add_input(rx);
                region.add_control_input(control_rx);
                let mut output = region.subscribe_output().unwrap();
                let mut control_output = region.subscribe_control_output().unwrap();
                let mut task = region
                    .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
                    .handle;
                // Allow the initial empty periodic tick before buffering input.
                tokio::task::yield_now().await;
                tx.send(StreamData::bytes(b"input".to_vec())).await.unwrap();
                let fence = ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(1));
                tx.send(StreamData::control(fence.clone())).await.unwrap();
                assert_eq!(output.recv().await.unwrap().as_control(), Some(&fence));
                let before = histogram.get_sample_sum();
                match event {
                    "timer" => {
                        tokio::time::advance(Duration::from_secs(60)).await;
                    }
                    "control" => {
                        control_tx
                            .send(ControlSignal::Barrier(
                                BarrierControlSignalKind::StreamGracefulEnd.with_id(2),
                            ))
                            .await
                            .unwrap();
                    }
                    _ => {
                        drop(tx);
                    }
                }
                if event == "timer" {
                    // No async wait inside the callback: one task turn completes it.
                    tokio::task::yield_now().await;
                } else {
                    if event == "control" {
                        control_output.recv().await.unwrap();
                    }
                    (&mut task).await.unwrap().unwrap();
                }
                assert!(
                    histogram.get_sample_sum() - before >= 0.020,
                    "synchronous {event} work missing for outcome {outcome}"
                );
                if outcome == 0 {
                    assert!(matches!(output.recv().await.unwrap(), StreamData::Bytes(_)));
                }
                if event == "timer" {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
    }

    #[tokio::test]
    async fn stage_timing_records_errors_and_excludes_transport_waits() {
        // The default stage keeps traffic/error metrics without clock reads or
        // binding a histogram, including when another metric binds its children.
        let mut disabled =
            Stage::from_plan(&plans(true)[0], &mut RegionResources::default()).unwrap();
        disabled.stats.set_pipeline_id("disabled_stage_timing");
        assert!(!disabled.stats.handling_time_enabled());
        assert!(StageTimer::new(&disabled.stats).started.is_none());
        assert!(disabled.on_timer(ClockEvent::Periodic).is_err());
        let _ = disabled.process(input()).unwrap();
        let snapshot = disabled.stats.snapshot();
        assert_eq!(snapshot.error_count, 1);
        assert_eq!(snapshot.records_in, Some(1));
        assert_eq!(snapshot.records_out, Some(1));
        assert!(veloflux_metrics::processor_handle_duration_seconds()
            .remove_label_values(&["default", "disabled_stage_timing", &disabled.name])
            .is_err());

        let mut shared_stats = ProcessorStats::new("timing_test", "disabled_shared", "stage");
        shared_stats.configure_stage_handling_time();
        shared_stats.set_shared_stream_name("disabled_stage_timing");
        let shared_stats = Arc::new(shared_stats);
        disabled.set_stats(Arc::clone(&shared_stats));
        assert!(disabled.on_timer(ClockEvent::Periodic).is_err());
        let _ = disabled.process(input()).unwrap();
        assert_eq!(shared_stats.snapshot().error_count, 1);
        assert_eq!(shared_stats.snapshot().records_out, Some(1));
        assert!(
            veloflux_metrics::shared_stream_processor_handle_duration_seconds()
                .remove_label_values(&["timing_test", "disabled_stage_timing", "disabled_shared"])
                .is_err()
        );

        let stats = Arc::new(ProcessorStats::new(
            "timing_test",
            "errors_and_waits",
            "stage",
        ));
        stats.set_pipeline_id("stage_callback_timing");
        let histogram = veloflux_metrics::processor_handle_duration_seconds().with_label_values(&[
            "timing_test",
            "stage_callback_timing",
            "errors_and_waits",
        ]);
        let mut resources = RegionResources::default();
        let mut stage = Stage::from_plan(&plans(true)[0], &mut resources).unwrap();
        stage.set_stats(Arc::clone(&stats));
        let before = histogram.get_sample_count();
        assert!(stage.on_timer(ClockEvent::Periodic).is_err());
        assert_eq!(histogram.get_sample_count(), before + 1);
        assert!(stage.process(StreamData::bytes(vec![1])).is_err());
        assert_eq!(histogram.get_sample_count(), before + 2);
        let mut output = LinkOutput::new(LinkKind::Mpsc, 1);
        let transport = Arc::new(ProcessorStats::new(
            "timing_test",
            "blocked_link",
            "transport",
        ));
        output.set_stats(Arc::clone(&transport));
        let mut receiver = output.subscribe().unwrap();
        send_with_backpressure(&output, 1, input(), None)
            .await
            .unwrap();
        let pending = stage.process(input()).unwrap();
        let before_sum = histogram.get_sample_sum();
        let mut stages = vec![stage];
        let mut emissions = vec![(0, pending)];
        let mut drain = Box::pin(drain_outputs(&mut stages, &mut emissions, &output, 1));
        assert!(timeout(Duration::from_millis(100), &mut drain)
            .await
            .is_err());
        assert_eq!(histogram.get_sample_sum(), before_sum);
        assert_eq!(transport.snapshot().custom["send_backpressure_waits"], 1);
        receiver.recv().await.unwrap();
        drain.await.unwrap();
        assert_eq!(histogram.get_sample_sum(), before_sum);
        receiver.recv().await.unwrap();
    }

    #[tokio::test]
    async fn byte_region_closed_output_records_transport_error_and_stage_production() {
        let plan = PhysicalPlan::SinkCompress(crate::planner::physical::PhysicalSinkCompress::new(
            Arc::new(plans(true).remove(0)),
            10,
            crate::codec::CompressionCodec::gzip(),
        ));
        let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
        let mut region = RegionProcessor::new(
            "bytes".into(),
            &[plan],
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        let stats = Arc::new(ProcessorStats::default());
        let transport = Arc::new(ProcessorStats::default());
        region.output.set_stats(Arc::clone(&transport));
        region
            .stages
            .get_mut()
            .unwrap()
            .first_mut()
            .unwrap()
            .set_stats(Arc::clone(&stats));
        let (tx, rx) = mpsc::channel(1);
        region.add_input(rx);
        drop(region.subscribe_output().unwrap());
        let task = region
            .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
            .handle;
        tx.send(StreamData::encoded_delivery_single(b"hello".to_vec()))
            .await
            .unwrap();
        let result = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(ProcessorError::ChannelClosed)));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.error_count, 0);
        assert_eq!(transport.snapshot().error_count, 1);
        assert_eq!(snapshot.custom["messages_in"], 1);
        assert_eq!(snapshot.custom["messages_out"], 1);
        assert_eq!(snapshot.custom["bytes_in"], 5);
        assert!(snapshot.custom["bytes_out"] > 0);
    }

    #[tokio::test]
    async fn region_external_control_closure_uses_entry_stage_policy() {
        let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
        let specs = [
            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 0)),
            plans(true).remove(0),
        ];
        let mut region = RegionProcessor::new(
            "control_close".into(),
            &specs,
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        let (tx, rx) = mpsc::channel(1);
        let (control_tx, control_rx) = mpsc::channel(1);
        region.add_input(rx);
        region.add_control_input(control_rx);
        let mut output = region.subscribe_output().unwrap();
        let task = region
            .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
            .handle;
        drop(control_tx);
        tx.send(input()).await.unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(2), output.recv())
                .await
                .unwrap()
                .unwrap(),
            StreamData::Collection(_)
        ));
        let end =
            ControlSignal::Barrier(crate::processor::BarrierControlSignal::StreamGracefulEnd {
                barrier_id: 1,
            });
        tx.send(StreamData::control(end.clone())).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), output.recv())
                .await
                .unwrap()
                .unwrap()
                .as_control(),
            Some(&end)
        );
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn region_data_eof_flushes_predecessors_before_strict_suffix_failure() {
        use crate::planner::physical::PhysicalBatch;
        let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
        let specs = [
            PhysicalPlan::Batch(PhysicalBatch::new(
                vec![],
                0,
                "batch".into(),
                crate::planner::sink::CommonSinkProps {
                    batch_count: Some(2),
                    ..Default::default()
                },
            )),
            plans(true).remove(0),
        ];
        let mut region = RegionProcessor::new(
            "data_close".into(),
            &specs,
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        let (tx, rx) = mpsc::channel(1);
        region.add_input(rx);
        let mut output = region.subscribe_output().unwrap();
        let task = region
            .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
            .handle;
        tx.send(input()).await.unwrap();
        drop(tx);
        assert_eq!(
            timeout(Duration::from_secs(2), output.recv())
                .await
                .unwrap()
                .unwrap()
                .as_collection()
                .unwrap()
                .num_rows(),
            1
        );
        let result = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(ProcessorError::ChannelClosed)));
    }

    #[tokio::test(start_paused = true)]
    async fn timed_batch_regions_match_standalone_timer_and_exit_lanes() {
        use crate::model::batch_from_columns_simple;
        use crate::planner::physical::PhysicalBatch;
        use crate::planner::sink::CommonSinkProps;
        use crate::processor::{BarrierControlSignal, BatchProcessor, InstantControlSignal};
        let rows = |values: &[i64]| {
            StreamData::collection(Box::new(
                batch_from_columns_simple(vec![(
                    "stream".into(),
                    "a".into(),
                    values.iter().copied().map(Value::Int64).collect(),
                )])
                .unwrap(),
            ))
        };
        for count in [None, Some(2)] {
            for mode in 0..5 {
                for fused in [false, true] {
                    let capacities =
                        ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                    let (tx, rx) = mpsc::channel(1);
                    let (control_tx, control_rx) = mpsc::channel(1);
                    let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                    let (mut output, mut control_output, task) = if fused {
                        let specs = [
                            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 1)),
                            PhysicalPlan::Batch(PhysicalBatch::new(
                                vec![],
                                2,
                                "batch".into(),
                                CommonSinkProps {
                                    batch_count: count,
                                    batch_duration: Some(Duration::from_millis(100)),
                                    ..Default::default()
                                },
                            )),
                            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 3)),
                        ];
                        let mut region = RegionProcessor::new(
                            "batch".into(),
                            &specs,
                            RegionResources::default(),
                            capacities,
                        )
                        .unwrap();
                        region.add_input(rx);
                        region.add_control_input(control_rx);
                        (
                            region.subscribe_output().unwrap(),
                            region.subscribe_control_output().unwrap(),
                            region.start(&spawner).handle,
                        )
                    } else {
                        let mut processor = BatchProcessor::new_with_channel_capacities(
                            "batch",
                            count,
                            Some(Duration::from_millis(100)),
                            capacities,
                        );
                        processor.add_input(rx);
                        processor.add_control_input(control_rx);
                        (
                            processor.subscribe_output().unwrap(),
                            processor.subscribe_control_output().unwrap(),
                            processor.start(&spawner).handle,
                        )
                    };
                    tx.send(rows(&[1, 2, 3])).await.unwrap();
                    let barrier =
                        ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(8));
                    tx.send(StreamData::control(barrier.clone())).await.unwrap();
                    if count.is_some() {
                        assert_eq!(
                            output
                                .recv()
                                .await
                                .unwrap()
                                .as_collection()
                                .unwrap()
                                .num_rows(),
                            2
                        );
                    }
                    assert_eq!(output.recv().await.unwrap().as_control(), Some(&barrier));
                    tokio::time::advance(Duration::from_millis(100)).await;
                    assert_eq!(
                        output
                            .recv()
                            .await
                            .unwrap()
                            .as_collection()
                            .unwrap()
                            .num_rows(),
                        if count.is_some() { 1 } else { 3 }
                    );
                    tx.send(rows(&[4])).await.unwrap();
                    tx.send(StreamData::control(barrier.clone())).await.unwrap();
                    assert_eq!(output.recv().await.unwrap().as_control(), Some(&barrier));
                    let end = if mode == 1 || mode == 3 {
                        ControlSignal::Instant(InstantControlSignal::StreamQuickEnd {
                            signal_id: 9,
                        })
                    } else {
                        ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd {
                            barrier_id: 9,
                        })
                    };
                    if mode < 2 {
                        tx.send(StreamData::control(end.clone())).await.unwrap();
                    } else if mode < 4 {
                        control_tx.send(end.clone()).await.unwrap();
                    } else {
                        drop(tx);
                    }
                    if mode == 2 || mode == 3 {
                        assert_eq!(control_output.recv().await.unwrap(), end);
                    }
                    assert_eq!(
                        output
                            .recv()
                            .await
                            .unwrap()
                            .as_collection()
                            .unwrap()
                            .num_rows(),
                        1
                    );
                    if mode < 2 {
                        assert_eq!(output.recv().await.unwrap().as_control(), Some(&end));
                    }
                    task.await.unwrap().unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn barrier_region_aligns_checkpoint_and_keeps_suffix_inline() {
        use crate::planner::execution::ExecutionPlan;
        use crate::planner::physical::PhysicalBarrier;
        use crate::CheckpointMode;
        let children: Vec<_> = plans(true).into_iter().map(Arc::new).collect();
        let barrier = PhysicalPlan::Barrier(PhysicalBarrier::new(children, 3));
        let mut suffix = plans(true).remove(0);
        let PhysicalPlan::Filter(filter) = &mut suffix else {
            unreachable!()
        };
        *filter = PhysicalFilter::new(
            filter.predicate.clone(),
            filter.scalar_predicate.clone(),
            vec![Arc::new(barrier.clone())],
            4,
        );
        let plan = ExecutionPlan::build(&Arc::new(suffix.clone())).unwrap();
        assert_eq!(plan.region_for(3).stages, vec![3, 4]);
        assert_eq!(
            plan.links
                .iter()
                .filter(|link| link.downstream == 3)
                .count(),
            2
        );
        let capacities = ProcessorChannelCapacities::new(2, 1).with_link_kind(LinkKind::Mpsc);
        let mut region = RegionProcessor::new(
            "aligned".into(),
            &[barrier, suffix],
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        let stats = Arc::new(ProcessorStats::new(
            "timing_test",
            "barrier_alignment",
            "region",
        ));
        stats.set_pipeline_id("region_callback_timing");
        let histogram = veloflux_metrics::processor_handle_duration_seconds().with_label_values(&[
            "timing_test",
            "region_callback_timing",
            "barrier_alignment",
        ]);
        let initial_count = histogram.get_sample_count();
        region
            .stages
            .get_mut()
            .unwrap()
            .first_mut()
            .unwrap()
            .set_stats(stats);
        let mut data = Vec::new();
        let mut control = Vec::new();
        for _ in 0..2 {
            let (tx, rx) = mpsc::channel(2);
            region.add_input(rx);
            data.push(tx);
            let (tx, rx) = mpsc::channel(1);
            region.add_control_input(rx);
            control.push(tx);
        }
        let mut output = region.subscribe_output().unwrap();
        let mut control_output = region.subscribe_control_output().unwrap();
        let task = region
            .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
            .handle;
        data[0]
            .send(StreamData::checkpoint(10, CheckpointMode::Continue))
            .await
            .unwrap();
        data[0].send(input()).await.unwrap();
        assert!(timeout(Duration::from_millis(20), output.recv())
            .await
            .is_err());
        assert_eq!(
            histogram.get_sample_count(),
            initial_count + 1,
            "checkpoint Pause must publish its synchronous alignment time"
        );
        data[1]
            .send(StreamData::checkpoint(10, CheckpointMode::Continue))
            .await
            .unwrap();
        let checkpoint = timeout(Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            checkpoint
                .as_control()
                .and_then(ControlSignal::checkpoint_mode),
            Some(CheckpointMode::Continue)
        );
        assert!(matches!(
            timeout(Duration::from_secs(2), output.recv())
                .await
                .unwrap()
                .unwrap(),
            StreamData::Collection(_)
        ));
        let before_sync = histogram.get_sample_count();
        let sync = ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(11));
        data[0]
            .send(StreamData::control(sync.clone()))
            .await
            .unwrap();
        assert!(timeout(Duration::from_millis(20), output.recv())
            .await
            .is_err());
        assert_eq!(
            histogram.get_sample_count(),
            before_sync + 1,
            "Forward(None) must publish alignment time even without downstream output"
        );
        data[1]
            .send(StreamData::control(sync.clone()))
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), output.recv())
                .await
                .unwrap()
                .unwrap()
                .as_control(),
            Some(&sync)
        );
        assert_eq!(
            histogram.get_sample_count(),
            before_sync + 2,
            "forwarded alignment and suffix work must share one drain observation"
        );
        let before_control = histogram.get_sample_count();
        let end =
            ControlSignal::Barrier(crate::processor::BarrierControlSignal::StreamGracefulEnd {
                barrier_id: 12,
            });
        control[0].send(end.clone()).await.unwrap();
        assert!(timeout(Duration::from_millis(20), control_output.recv())
            .await
            .is_err());
        assert_eq!(
            histogram.get_sample_count(),
            before_control + 1,
            "control alignment must publish time while waiting for another upstream"
        );
        control[1].send(end.clone()).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), control_output.recv())
                .await
                .unwrap()
                .unwrap(),
            end
        );
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            histogram.get_sample_count() > before_control + 1,
            "completed control alignment must contribute handling time"
        );
        let error_stats = Arc::new(ProcessorStats::new(
            "timing_test",
            "barrier_alignment_error",
            "region",
        ));
        error_stats.set_pipeline_id("region_callback_timing");
        let errors = veloflux_metrics::processor_handle_duration_seconds().with_label_values(&[
            "timing_test",
            "region_callback_timing",
            "barrier_alignment_error",
        ]);
        let before_error = errors.get_sample_count();
        let mut error_region = RegionProcessor::new(
            "alignment_error".into(),
            &[PhysicalPlan::Barrier(PhysicalBarrier::new(
                plans(true).into_iter().map(Arc::new).collect(),
                5,
            ))],
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        error_region
            .stages
            .get_mut()
            .unwrap()
            .first_mut()
            .unwrap()
            .set_stats(error_stats);
        let mut error_data = Vec::new();
        let mut error_control = Vec::new();
        for _ in 0..2 {
            let (tx, rx) = mpsc::channel(1);
            error_region.add_input(rx);
            error_data.push(tx);
            let (tx, rx) = mpsc::channel(1);
            error_region.add_control_input(rx);
            error_control.push(tx);
        }
        let mut error_output = error_region.subscribe_output().unwrap();
        let task = error_region
            .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
            .handle;
        error_data[0]
            .send(StreamData::checkpoint(13, CheckpointMode::Continue))
            .await
            .unwrap();
        assert!(timeout(Duration::from_millis(20), error_output.recv())
            .await
            .is_err());
        error_data[1]
            .send(StreamData::checkpoint(14, CheckpointMode::Continue))
            .await
            .unwrap();
        let error = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, ProcessorError::ProcessingError(_)));
        assert_eq!(
            errors.get_sample_count(),
            before_error + 2,
            "paused and failed alignments must both publish their synchronous time"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn region_polls_multiple_watermark_clocks() {
        use crate::planner::logical::TimeUnit;
        use crate::planner::physical::{PhysicalProcessTimeWatermark, WatermarkConfig};
        let capacities = ProcessorChannelCapacities::new(8, 1).with_link_kind(LinkKind::Mpsc);
        let specs: Vec<_> = [1, 2]
            .into_iter()
            .map(|seconds| {
                let config = WatermarkConfig::Tumbling {
                    time_unit: TimeUnit::Seconds,
                    length: seconds,
                };
                if seconds == 1 {
                    PhysicalPlan::ProcessTimeWatermark(PhysicalProcessTimeWatermark::new(
                        config,
                        vec![],
                        seconds as i64,
                    ))
                } else {
                    // The legacy node lowers to the same synchronous processing-time engine.
                    PhysicalPlan::Watermark(crate::planner::physical::PhysicalWatermark::new(
                        config,
                        vec![],
                        seconds as i64,
                    ))
                }
            })
            .collect();
        let mut region = RegionProcessor::new(
            "timers".into(),
            &specs,
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        let (tx, rx) = mpsc::channel(1);
        region.add_input(rx);
        let mut output = region.subscribe_output().unwrap();
        let task = region
            .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
            .handle;
        for _ in 0..2 {
            assert!(matches!(
                output.recv().await.unwrap(),
                StreamData::Watermark(_)
            ));
        }
        for (step, expected) in [(1, 1), (2, 2)] {
            tokio::time::advance(Duration::from_secs(1)).await;
            let barrier = ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(step));
            tx.send(StreamData::control(barrier.clone())).await.unwrap();
            let mut watermarks = 0;
            loop {
                match output.recv().await.unwrap() {
                    StreamData::Watermark(_) => watermarks += 1,
                    StreamData::Control(signal) => {
                        assert_eq!(signal, barrier);
                        break;
                    }
                    _ => panic!("unexpected timer output"),
                }
            }
            assert_eq!(watermarks, expected);
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn eventtime_region_control_graceful_flushes_data_before_control() {
        use crate::planner::physical::{PhysicalEmptySuppress, PhysicalEventtimeWatermark};
        use crate::processor::BarrierControlSignal;
        use std::time::UNIX_EPOCH;
        let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
        let specs = [
            PhysicalPlan::EventtimeWatermark(PhysicalEventtimeWatermark::new(
                Duration::from_secs(10),
                None,
                vec![],
                1,
            )),
            PhysicalPlan::EmptySuppress(PhysicalEmptySuppress::new(vec![], 2, "test".into(), true)),
        ];
        let mut region = RegionProcessor::new(
            "eventtime".into(),
            &specs,
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        let (tx, rx) = mpsc::channel(1);
        let (control_tx, control_rx) = mpsc::channel(1);
        region.add_input(rx);
        region.add_control_input(control_rx);
        let mut output = region.subscribe_output().unwrap();
        let mut control_output = region.subscribe_control_output().unwrap();
        let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
        let task = region.start(&spawner).handle;
        let tuple = |seconds| {
            let mut row = Tuple::new(vec![Arc::new(Message::new("stream", vec![], vec![]))]);
            row.timestamp = UNIX_EPOCH + Duration::from_secs(seconds);
            row
        };
        tx.send(StreamData::collection(Box::new(
            RecordBatch::new(vec![tuple(3), tuple(1), tuple(2)]).unwrap(),
        )))
        .await
        .unwrap();
        // A data barrier acknowledges ingestion without forcing the buffered rows out.
        let barrier = ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(10));
        tx.send(StreamData::control(barrier.clone())).await.unwrap();
        assert!(
            matches!(timeout(Duration::from_secs(2), output.recv()).await.unwrap().unwrap(), StreamData::Control(s) if s == barrier)
        );
        let end =
            ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd { barrier_id: 11 });
        control_tx.send(end.clone()).await.unwrap();
        let StreamData::Collection(rows) = timeout(Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected buffered data");
        };
        let times: Vec<_> = rows
            .into_rows()
            .unwrap()
            .into_iter()
            .map(|row| row.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs())
            .collect();
        assert_eq!(times, vec![1, 2, 3]);
        assert_eq!(
            timeout(Duration::from_secs(2), control_output.recv())
                .await
                .unwrap()
                .unwrap(),
            end
        );
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn region_matches_standalone_output_and_data_barriers() {
        for (accept, invalid) in [(true, false), (false, false), (false, true)] {
            for fused in [true, false] {
                let mut specs = plans(accept);
                if invalid {
                    let PhysicalPlan::Filter(filter) = &mut specs[0] else {
                        unreachable!()
                    };
                    filter.scalar_predicate = ScalarExpr::Literal(
                        Value::Int64(7),
                        ConcreteDatatype::Int64(datatypes::Int64Type),
                    );
                }
                let capacities =
                    ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                let (tx, rx) = mpsc::channel(1);
                let mut tasks = Vec::new();
                let filter_stats;
                let mut out;
                let mut control_out;
                if fused {
                    let mut region = RegionProcessor::new(
                        "region".into(),
                        &specs,
                        RegionResources::default(),
                        capacities,
                    )
                    .unwrap();
                    filter_stats = Arc::clone(&region.stages.get_mut().unwrap()[0].stats);
                    region.add_input(rx);
                    out = region.subscribe_output().unwrap();
                    control_out = region.subscribe_control_output().unwrap();
                    tasks.push(region.start(&spawner).handle);
                } else {
                    let PhysicalPlan::Filter(filter) = &specs[0] else {
                        unreachable!()
                    };
                    let PhysicalPlan::Compute(compute) = &specs[1] else {
                        unreachable!()
                    };
                    let mut filter = FilterProcessor::new_with_channel_capacities(
                        "filter",
                        Arc::new(filter.clone()),
                        capacities,
                    );
                    let mut compute = ComputeProcessor::new_with_channel_capacities(
                        "compute",
                        Arc::new(compute.clone()),
                        capacities,
                    );
                    filter_stats = Arc::new(ProcessorStats::collection_in_out());
                    filter.set_stats(Arc::clone(&filter_stats));
                    filter.add_input(rx);
                    compute.add_input(filter.subscribe_output().unwrap());
                    compute.add_control_input(filter.subscribe_control_output().unwrap());
                    out = compute.subscribe_output().unwrap();
                    control_out = compute.subscribe_control_output().unwrap();
                    tasks.push(compute.start(&spawner).handle);
                    tasks.push(filter.start(&spawner).handle);
                }
                tx.send(input()).await.unwrap();
                let barrier = BarrierControlSignalKind::SyncTest.with_id(17);
                tx.send(StreamData::control(ControlSignal::Barrier(barrier.clone())))
                    .await
                    .unwrap();
                if accept {
                    let value = timeout(Duration::from_secs(2), out.recv())
                        .await
                        .unwrap()
                        .unwrap();
                    let StreamData::Collection(batch) = value else {
                        panic!("expected collection")
                    };
                    assert_eq!(batch.num_rows(), 1);
                    assert_eq!(
                        batch.rows()[0].value_by_name("input", "a"),
                        Some(&Value::Int64(7))
                    );
                }
                let value = timeout(Duration::from_secs(2), out.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    matches!(value, StreamData::Control(ControlSignal::Barrier(b)) if b == barrier)
                );
                assert!(timeout(Duration::from_millis(10), control_out.recv())
                    .await
                    .is_err());
                let snapshot = filter_stats.snapshot();
                assert_eq!(snapshot.records_in, Some(1));
                assert_eq!(snapshot.records_out, Some(u64::from(accept)));
                assert_eq!(snapshot.error_count, u64::from(invalid));
                drop(tx);
                let mut saw_channel_closed = false;
                for task in tasks {
                    let result = timeout(Duration::from_secs(2), task)
                        .await
                        .unwrap()
                        .unwrap();
                    match result {
                        Ok(()) => {}
                        Err(ProcessorError::ChannelClosed) => saw_channel_closed = true,
                        other => panic!("unexpected processor exit: {other:?}"),
                    }
                }
                assert!(
                    saw_channel_closed,
                    "Filter retains unexpected input-close failure, fused={fused}"
                );
            }
        }
    }

    #[tokio::test]
    async fn region_preserves_control_lane_and_output_backpressure() {
        let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
        let mut region = RegionProcessor::new(
            "region".into(),
            &plans(true),
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        let stats = Arc::new(ProcessorStats::collection_in_out());
        region
            .stages
            .get_mut()
            .unwrap()
            .first_mut()
            .unwrap()
            .set_stats(Arc::clone(&stats));
        let (tx, rx) = mpsc::channel(1);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
        region.add_input(rx);
        region.add_control_input(ctrl_rx);
        let mut output = region.subscribe_output().unwrap();
        let mut control_output = region.subscribe_control_output().unwrap();
        let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
        let task = region.start(&spawner).handle;
        for _ in 0..3 {
            tx.send(input()).await.unwrap();
        }
        // In/out counters are published after successful sends. With capacity
        // one, the second output must wait for the receiver to make room.
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot().records_out, Some(2));
        for _ in 0..3 {
            let item = timeout(Duration::from_secs(2), output.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(item, StreamData::Collection(_)));
        }
        let signal = ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(23));
        ctrl_tx.send(signal.clone()).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), control_output.recv())
                .await
                .unwrap()
                .unwrap(),
            signal
        );
        assert!(timeout(Duration::from_millis(10), output.recv())
            .await
            .is_err());
        drop(tx);
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .expect_err("unexpected input closure must preserve Filter failure");
        assert_eq!(stats.snapshot().records_out, Some(3));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn region_budget_yields_between_stages_without_reordering_current_data() {
        use crate::planner::physical::PipelineStateUsage;
        use crate::processor::processor_state::ProcessorState;
        use std::sync::atomic::Ordering;
        for cancel in [false, true] {
            let state = Arc::new(ProcessorState::default());
            let specs: Vec<_> = (0..1024)
                .map(|index| {
                    let PhysicalPlan::Filter(mut filter) = plans(true).remove(0) else {
                        unreachable!()
                    };
                    filter.base.index = index;
                    filter.processor_state = Some(Arc::clone(&state));
                    filter.pipeline_state_usage = PipelineStateUsage {
                        last_hit_count: true,
                        ..Default::default()
                    };
                    PhysicalPlan::Filter(filter)
                })
                .collect();
            let mut region = RegionProcessor::new(
                "long".into(),
                &specs,
                RegionResources::default(),
                ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc),
            )
            .unwrap();
            let (tx, rx) = mpsc::channel(1);
            let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
            region.add_input(rx);
            region.add_control_input(ctrl_rx);
            let mut output = region.subscribe_output().unwrap();
            let mut control_output = region.subscribe_control_output().unwrap();
            let observer = {
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    loop {
                        let count = state.last_hit_count.load(Ordering::Relaxed);
                        if count != 0 {
                            return count;
                        }
                        tokio::task::yield_now().await;
                    }
                })
            };
            let task = region
                .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
                .handle;
            tx.send(input()).await.unwrap();
            let count = timeout(Duration::from_secs(2), observer)
                .await
                .unwrap()
                .unwrap();
            assert!(count < 1024, "the entire chain executed before yielding");
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                assert!(state.last_hit_count.load(Ordering::Relaxed) < 1024);
                drop(region);
                assert!(output.recv().await.is_err());
            } else {
                ctrl_tx
                    .send(ControlSignal::Instant(
                        crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 1 },
                    ))
                    .await
                    .unwrap();
                assert!(matches!(
                    output.recv().await.unwrap(),
                    StreamData::Collection(_)
                ));
                assert!(control_output.recv().await.unwrap().is_terminal());
                assert_eq!(state.last_hit_count.load(Ordering::Relaxed), 1024);
                task.await.unwrap().unwrap();
            }
        }
    }

    fn window_plan(aggregate: bool) -> PhysicalPlan {
        use crate::expr::scalar::ColumnRef;
        use crate::planner::logical::TimeUnit;
        use crate::planner::physical::{
            AggregateCall, PhysicalStreamingAggregation, PhysicalTumblingWindow,
        };
        let partition = ScalarExpr::Column(ColumnRef::ByName {
            column_name: "p".into(),
        });
        let expr = Expr::Identifier(sqlparser::ast::Ident::new("p"));
        if aggregate {
            PhysicalPlan::StreamingAggregation(PhysicalStreamingAggregation::new(
                StreamingWindowSpec::Tumbling {
                    time_unit: TimeUnit::Milliseconds,
                    length: 100,
                    partition_by_exprs: vec![expr],
                    partition_by_scalars: vec![partition],
                },
                Default::default(),
                vec![],
                vec![AggregateCall {
                    supports_inline_execution: true,
                    output_column: "total".into(),
                    func_name: "sum".into(),
                    args: vec![ScalarExpr::Column(ColumnRef::ByName {
                        column_name: "a".into(),
                    })],
                    distinct: false,
                }],
                vec![],
                vec![],
                2,
            ))
        } else {
            PhysicalPlan::TumblingWindow(PhysicalTumblingWindow::new_partitioned(
                TimeUnit::Milliseconds,
                100,
                vec![expr],
                vec![partition],
                vec![],
                2,
            ))
        }
    }

    fn window_input() -> StreamData {
        let rows = [(1001, 1, 2), (1002, 2, 4), (1101, 1, 8), (1201, 1, 16)]
            .into_iter()
            .map(|(ms, partition, value)| {
                let mut row = Tuple::with_timestamp(
                    Tuple::empty_messages(),
                    std::time::UNIX_EPOCH + Duration::from_millis(ms),
                );
                row.add_affiliate_column(Arc::new("p".into()), Value::Int64(partition));
                row.add_affiliate_column(Arc::new("a".into()), Value::Int64(value));
                row
            })
            .collect();
        StreamData::collection(Box::new(RecordBatch::new(rows).unwrap()))
    }

    fn connect_window<P: Processor>(
        mut processor: P,
        input: mpsc::Receiver<StreamData>,
        control: mpsc::Receiver<ControlSignal>,
        spawner: &TaskSpawner,
    ) -> (
        LinkReceiver<StreamData>,
        LinkReceiver<ControlSignal>,
        ProcessorStart,
    ) {
        processor.add_input(input);
        processor.add_control_input(control);
        let output = processor.subscribe_output().unwrap();
        let control_output = processor.subscribe_control_output().unwrap();
        (output, control_output, processor.start(spawner))
    }

    #[tokio::test]
    async fn tumbling_regions_match_standalone_partitions_metadata_and_terminal_lanes() {
        use crate::processor::streaming_aggregation_processor::StreamingTumblingAggregationProcessor;
        use crate::processor::tumbling_window_processor::TumblingWindowProcessor;
        use crate::processor::{BarrierControlSignal, InstantControlSignal};
        for aggregate in [false, true] {
            // Watermark + graceful data end, quick data end, graceful control end.
            for mode in 0..3 {
                let mut results = Vec::new();
                for fused in [false, true] {
                    let capacities =
                        ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                    let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                    let (tx, rx) = mpsc::channel(1);
                    let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
                    let plan = window_plan(aggregate);
                    let (mut output, mut control_output, task) = if fused {
                        let specs = vec![
                            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 1)),
                            plan,
                            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 3)),
                        ];
                        connect_window(
                            RegionProcessor::new(
                                "region".into(),
                                &specs,
                                RegionResources::default(),
                                capacities,
                            )
                            .unwrap(),
                            rx,
                            ctrl_rx,
                            &spawner,
                        )
                    } else {
                        match plan {
                            PhysicalPlan::TumblingWindow(p) => connect_window(
                                TumblingWindowProcessor::new_with_channel_capacities(
                                    "window",
                                    Arc::new(p),
                                    capacities,
                                ),
                                rx,
                                ctrl_rx,
                                &spawner,
                            ),
                            PhysicalPlan::StreamingAggregation(p) => connect_window(
                                StreamingTumblingAggregationProcessor::new_with_channel_capacities(
                                    "aggregation",
                                    Arc::new(p),
                                    AggregateFunctionRegistry::with_builtins(),
                                    capacities,
                                )
                                .unwrap(),
                                rx,
                                ctrl_rx,
                                &spawner,
                            ),
                            _ => unreachable!(),
                        }
                    };
                    tx.send(window_input()).await.unwrap();
                    let mut actual = Vec::new();
                    if mode == 0 {
                        tx.send(StreamData::Watermark(
                            std::time::UNIX_EPOCH + Duration::from_millis(1100),
                        ))
                        .await
                        .unwrap();
                        // Both partitions emit exactly at the millisecond end boundary.
                        for _ in 0..2 {
                            let StreamData::Collection(batch) =
                                timeout(Duration::from_secs(2), output.recv())
                                    .await
                                    .unwrap()
                                    .unwrap()
                            else {
                                panic!("expected window");
                            };
                            let meta = batch.metadata().window.as_deref().unwrap();
                            assert_eq!(
                                (meta.start.epoch_micros(), meta.end.epoch_micros()),
                                (1_000_000, 1_100_000)
                            );
                            let value = batch.rows()[0]
                                .value_by_name("", if aggregate { "total" } else { "a" })
                                .cloned();
                            actual.push((
                                meta.start.epoch_micros(),
                                meta.end.epoch_micros(),
                                value,
                            ));
                        }
                    }
                    let terminal = if mode == 1 {
                        ControlSignal::Instant(InstantControlSignal::StreamQuickEnd {
                            signal_id: 9,
                        })
                    } else {
                        ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd {
                            barrier_id: 9,
                        })
                    };
                    if mode == 2 {
                        // A data barrier acknowledges that the preceding collection was buffered.
                        let sync =
                            ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(8));
                        tx.send(StreamData::control(sync.clone())).await.unwrap();
                        assert!(
                            matches!(timeout(Duration::from_secs(2), output.recv()).await.unwrap().unwrap(), StreamData::Control(s) if s == sync)
                        );
                        ctrl_tx.send(terminal.clone()).await.unwrap();
                        assert_eq!(
                            timeout(Duration::from_secs(2), control_output.recv())
                                .await
                                .unwrap()
                                .unwrap(),
                            terminal
                        );
                    } else {
                        tx.send(StreamData::control(terminal.clone()))
                            .await
                            .unwrap();
                        loop {
                            match timeout(Duration::from_secs(2), output.recv())
                                .await
                                .unwrap()
                                .unwrap()
                            {
                                StreamData::Collection(batch) => {
                                    assert_eq!(mode, 0, "quick end must discard buffered windows");
                                    let meta = batch.metadata().window.as_deref().unwrap();
                                    let value = batch.rows()[0]
                                        .value_by_name("", if aggregate { "total" } else { "a" })
                                        .cloned();
                                    actual.push((
                                        meta.start.epoch_micros(),
                                        meta.end.epoch_micros(),
                                        value,
                                    ));
                                }
                                StreamData::Control(signal) => {
                                    assert_eq!(signal, terminal);
                                    break;
                                }
                                _ => panic!("unexpected event"),
                            }
                        }
                    }
                    timeout(Duration::from_secs(2), task.handle)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    assert!(matches!(
                        output.recv().await,
                        Err(ProcessorError::ChannelClosed)
                    ));
                    if mode == 0 {
                        assert_eq!(actual.len(), 4);
                        assert_eq!(
                            actual.iter().map(|x| x.2.clone()).collect::<Vec<_>>(),
                            vec![
                                Some(Value::Int64(2)),
                                Some(Value::Int64(4)),
                                Some(Value::Int64(8)),
                                Some(Value::Int64(16))
                            ]
                        );
                    } else {
                        assert!(actual.is_empty());
                    }
                    results.push(actual);
                }
                assert_eq!(results[0], results[1], "aggregate={aggregate}, mode={mode}");
            }
        }
    }

    #[tokio::test]
    async fn tumbling_region_waits_for_backpressure_before_draining_remaining_windows() {
        let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
        let specs = vec![
            window_plan(false),
            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 3)),
        ];
        let stats = Arc::new(ProcessorStats::collection_in_out());
        let mut resources = RegionResources::default();
        let mut stages = specs
            .iter()
            .map(|plan| Stage::from_plan(plan, &mut resources).unwrap())
            .collect::<Vec<_>>();
        stages[0].set_stats(Arc::clone(&stats));
        let output = LinkOutput::new(LinkKind::Mpsc, capacities.data);
        let mut receiver = output.subscribe().unwrap();
        let initial = stages[0].process(window_input()).unwrap();
        let mut emissions = vec![(0, initial)];
        drain_outputs(&mut stages, &mut emissions, &output, capacities.data)
            .await
            .unwrap();
        let pending = stages[0]
            .process(StreamData::Watermark(
                std::time::UNIX_EPOCH + Duration::from_millis(1400),
            ))
            .unwrap();
        emissions.push((0, pending));
        let drain = drain_outputs(&mut stages, &mut emissions, &output, capacities.data);
        tokio::pin!(drain);
        tokio::select! {
            result = &mut drain => panic!("drain completed before backpressure: {result:?}"),
            _ = async {
                while stats.snapshot().records_out != Some(2) { tokio::task::yield_now().await; }
            } => {}
        }
        // One window is in the channel, one is pending send, two remain buffered.
        assert_eq!(stats.snapshot().custom["rows_buffered"], 2);
        let consume = async {
            for _ in 0..4 {
                assert!(matches!(
                    receiver.recv().await.unwrap(),
                    StreamData::Collection(_)
                ));
            }
        };
        let (result, ()) = timeout(Duration::from_secs(2), async {
            tokio::join!(drain, consume)
        })
        .await
        .unwrap();
        result.unwrap();
        assert_eq!(stats.snapshot().custom["rows_buffered"], 0);
    }

    fn count_plan(aggregate: bool) -> PhysicalPlan {
        use crate::planner::physical::PhysicalCountWindow;
        match window_plan(aggregate) {
            PhysicalPlan::TumblingWindow(p) => {
                PhysicalPlan::CountWindow(PhysicalCountWindow::new_partitioned(
                    2,
                    p.partition_by_exprs,
                    p.partition_by_scalars,
                    vec![],
                    2,
                ))
            }
            PhysicalPlan::StreamingAggregation(mut p) => {
                let StreamingWindowSpec::Tumbling {
                    partition_by_exprs,
                    partition_by_scalars,
                    ..
                } = p.window
                else {
                    unreachable!()
                };
                p.window = StreamingWindowSpec::Count {
                    count: 2,
                    partition_by_exprs,
                    partition_by_scalars,
                };
                PhysicalPlan::StreamingAggregation(p)
            }
            _ => unreachable!(),
        }
    }

    fn count_input(rows: &[(u64, i64, i64)]) -> StreamData {
        StreamData::collection(Box::new(
            RecordBatch::new(
                rows.iter()
                    .map(|(ms, partition, value)| {
                        let mut row = Tuple::with_timestamp(
                            Tuple::empty_messages(),
                            std::time::UNIX_EPOCH + Duration::from_millis(*ms),
                        );
                        row.add_affiliate_column(Arc::new("p".into()), Value::Int64(*partition));
                        row.add_affiliate_column(Arc::new("a".into()), Value::Int64(*value));
                        row
                    })
                    .collect(),
            )
            .unwrap(),
        ))
    }

    #[tokio::test]
    async fn count_regions_match_standalone_partitions_and_lifecycle() {
        use crate::processor::streaming_aggregation_processor::StreamingCountAggregationProcessor;
        use crate::processor::{BarrierControlSignal, BatchProcessor, InstantControlSignal};
        for aggregate in [false, true] {
            for mode in 0..4 {
                let mut results = Vec::new();
                for fused in [false, true] {
                    let capacities =
                        ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                    let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                    let (tx, rx) = mpsc::channel(1);
                    let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
                    let plan = count_plan(aggregate);
                    let (mut output, mut control_output, task) = if fused {
                        let specs = vec![
                            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 1)),
                            plan,
                            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 3)),
                        ];
                        connect_window(
                            RegionProcessor::new(
                                "region".into(),
                                &specs,
                                RegionResources::default(),
                                capacities,
                            )
                            .unwrap(),
                            rx,
                            ctrl_rx,
                            &spawner,
                        )
                    } else {
                        match plan {
                            PhysicalPlan::CountWindow(p) => connect_window(
                                BatchProcessor::new_partitioned_with_channel_capacities(
                                    "count",
                                    Some(2),
                                    None,
                                    p.partition_by_scalars,
                                    capacities,
                                ),
                                rx,
                                ctrl_rx,
                                &spawner,
                            ),
                            PhysicalPlan::StreamingAggregation(p) => connect_window(
                                StreamingCountAggregationProcessor::new_with_channel_capacities(
                                    "count",
                                    Arc::new(p),
                                    AggregateFunctionRegistry::with_builtins(),
                                    2,
                                    capacities,
                                ),
                                rx,
                                ctrl_rx,
                                &spawner,
                            ),
                            _ => unreachable!(),
                        }
                    };
                    tx.send(count_input(&[(1000, 1, 1), (1001, 2, 10), (1002, 1, 2)]))
                        .await
                        .unwrap();
                    tx.send(count_input(&[
                        (1003, 2, 20),
                        (1004, 1, 3),
                        (1005, 1, 4),
                        (1006, 1, 5),
                    ]))
                    .await
                    .unwrap();
                    let sync =
                        ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(8));
                    tx.send(StreamData::control(sync.clone())).await.unwrap();
                    let mut actual = Vec::new();
                    loop {
                        match timeout(Duration::from_secs(2), output.recv())
                            .await
                            .unwrap()
                            .unwrap()
                        {
                            StreamData::Collection(batch) => {
                                let values = batch
                                    .rows()
                                    .iter()
                                    .map(|row| {
                                        row.value_by_name("", if aggregate { "total" } else { "a" })
                                            .cloned()
                                            .unwrap()
                                    })
                                    .collect::<Vec<_>>();
                                let meta = batch
                                    .metadata()
                                    .window
                                    .as_deref()
                                    .map(|m| (m.start.epoch_micros(), m.end.epoch_micros()));
                                actual.push((values, meta));
                            }
                            StreamData::Control(signal) => {
                                assert_eq!(signal, sync);
                                break;
                            }
                            _ => panic!("unexpected count event"),
                        }
                    }
                    assert_eq!(actual.len(), 3);
                    let terminal = if mode == 1 {
                        ControlSignal::Instant(InstantControlSignal::StreamQuickEnd {
                            signal_id: 9,
                        })
                    } else {
                        ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd {
                            barrier_id: 9,
                        })
                    };
                    match mode {
                        2 => {
                            ctrl_tx.send(terminal.clone()).await.unwrap();
                            assert_eq!(
                                timeout(Duration::from_secs(2), control_output.recv())
                                    .await
                                    .unwrap()
                                    .unwrap(),
                                terminal
                            );
                        }
                        3 => drop(tx),
                        _ => tx
                            .send(StreamData::control(terminal.clone()))
                            .await
                            .unwrap(),
                    }
                    // CountWindow flushes the partial batch on every exit; incremental count drops it.
                    if !aggregate {
                        let StreamData::Collection(batch) =
                            timeout(Duration::from_secs(2), output.recv())
                                .await
                                .unwrap()
                                .unwrap()
                        else {
                            panic!("expected partial batch");
                        };
                        assert_eq!(batch.num_rows(), 1);
                        assert_eq!(
                            batch.rows()[0].value_by_name("", "a"),
                            Some(&Value::Int64(5))
                        );
                        assert!(batch.metadata().window.is_none());
                    }
                    if mode < 2 {
                        assert!(
                            matches!(timeout(Duration::from_secs(2), output.recv()).await.unwrap().unwrap(), StreamData::Control(signal) if signal == terminal)
                        );
                    }
                    timeout(Duration::from_secs(2), task.handle)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    assert!(matches!(
                        output.recv().await,
                        Err(ProcessorError::ChannelClosed)
                    ));
                    if aggregate {
                        assert_eq!(
                            actual.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
                            vec![
                                vec![Value::Int64(3)],
                                vec![Value::Int64(30)],
                                vec![Value::Int64(7)]
                            ]
                        );
                        assert_eq!(
                            actual.iter().map(|x| x.1).collect::<Vec<_>>(),
                            vec![
                                Some((1_000_000, 1_002_000)),
                                Some((1_001_000, 1_003_000)),
                                Some((1_004_000, 1_005_000))
                            ]
                        );
                    } else {
                        assert_eq!(
                            actual.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
                            vec![
                                vec![Value::Int64(1), Value::Int64(2)],
                                vec![Value::Int64(3), Value::Int64(4)],
                                vec![Value::Int64(10), Value::Int64(20)]
                            ]
                        );
                        assert!(actual.iter().all(|x| x.1.is_none()));
                    }
                    results.push(actual);
                }
                assert_eq!(results[0], results[1]);
            }
        }
    }

    #[tokio::test]
    async fn streaming_count_backpressure_stops_remaining_row_evaluation() {
        let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
        let PhysicalPlan::StreamingAggregation(mut p) = count_plan(true) else {
            unreachable!()
        };
        p.window = StreamingWindowSpec::Count {
            count: 1,
            partition_by_exprs: vec![],
            partition_by_scalars: vec![],
        };
        let specs = vec![
            PhysicalPlan::StreamingAggregation(p),
            PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 3)),
        ];
        let stats = Arc::new(ProcessorStats::collection_in_out());
        let mut region = RegionProcessor::new(
            "region".into(),
            &specs,
            RegionResources::default(),
            capacities,
        )
        .unwrap();
        region
            .stages
            .get_mut()
            .unwrap()
            .first_mut()
            .unwrap()
            .set_stats(Arc::clone(&stats));
        let (tx, rx) = mpsc::channel(1);
        region.add_input(rx);
        let mut output = region.subscribe_output().unwrap();
        let task = region
            .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
            .handle;
        let StreamData::Collection(batch) = count_input(&[(1000, 1, 1), (1001, 1, 2)]) else {
            unreachable!()
        };
        let mut rows = batch.into_rows().unwrap();
        rows.push(Tuple::with_timestamp(
            Tuple::empty_messages(),
            std::time::UNIX_EPOCH - Duration::from_secs(1),
        ));
        tx.send(StreamData::collection(Box::new(
            RecordBatch::new(rows).unwrap(),
        )))
        .await
        .unwrap();
        timeout(Duration::from_secs(2), async {
            while stats.snapshot().records_out != Some(2) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            stats.snapshot().error_count,
            0,
            "invalid trailing row must remain unevaluated while send is blocked"
        );
        for _ in 0..2 {
            assert!(matches!(
                timeout(Duration::from_secs(2), output.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                StreamData::Collection(_)
            ));
        }
        drop(tx);
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(stats.snapshot().error_count, 1);
    }

    #[tokio::test]
    async fn batch_aggregation_regions_preserve_empty_grouped_results_and_fatal_errors() {
        use crate::expr::scalar::ColumnRef;
        use crate::planner::physical::{AggregateCall, BasePhysicalPlan};
        for missing_function in [false, true] {
            let mut results = Vec::new();
            for fused in [false, true] {
                let capacities =
                    ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                let (tx, rx) = mpsc::channel(1);
                let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
                let p = PhysicalAggregation {
                    base: BasePhysicalPlan::new(vec![], 2),
                    aggregate_mappings: Default::default(),
                    group_by_exprs: vec![Expr::Identifier(sqlparser::ast::Ident::new("p"))],
                    group_by_scalars: vec![ScalarExpr::Column(ColumnRef::ByName {
                        column_name: "p".into(),
                    })],
                    aggregate_calls: vec![AggregateCall {
                        supports_inline_execution: !missing_function,
                        output_column: "total".into(),
                        func_name: if missing_function { "missing" } else { "sum" }.into(),
                        args: vec![ScalarExpr::Column(ColumnRef::ByName {
                            column_name: "a".into(),
                        })],
                        distinct: false,
                    }],
                };
                let (mut output, _, task) = if fused {
                    let specs = vec![
                        PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 1)),
                        PhysicalPlan::Aggregation(p),
                        PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 3)),
                    ];
                    connect_window(
                        RegionProcessor::new(
                            "region".into(),
                            &specs,
                            RegionResources::default(),
                            capacities,
                        )
                        .unwrap(),
                        rx,
                        ctrl_rx,
                        &spawner,
                    )
                } else {
                    connect_window(
                        AggregationProcessor::new_with_channel_capacities(
                            "aggregation",
                            Arc::new(p),
                            AggregateFunctionRegistry::with_builtins(),
                            capacities,
                        ),
                        rx,
                        ctrl_rx,
                        &spawner,
                    )
                };
                let mut actual = Vec::new();
                for rows in [&[][..], &[(1000, 1, 1), (1001, 2, 10), (1002, 1, 2)][..]] {
                    tx.send(count_input(rows)).await.unwrap();
                    if missing_function {
                        break;
                    }
                    let StreamData::Collection(batch) =
                        timeout(Duration::from_secs(2), output.recv())
                            .await
                            .unwrap()
                            .unwrap()
                    else {
                        panic!("expected aggregate output");
                    };
                    let mut values = batch
                        .rows()
                        .iter()
                        .map(|r| r.value_by_name("", "total").cloned().unwrap())
                        .collect::<Vec<_>>();
                    values.sort_by_key(|v| match v {
                        Value::Int64(n) => *n,
                        _ => i64::MIN,
                    });
                    actual.push(values);
                }
                drop(tx);
                let result = timeout(Duration::from_secs(2), task.handle)
                    .await
                    .unwrap()
                    .unwrap();
                if missing_function {
                    assert!(
                        matches!(result, Err(ProcessorError::ProcessingError(message)) if message.starts_with("Failed to process aggregation:"))
                    );
                } else {
                    result.unwrap();
                    assert_eq!(actual[1], vec![Value::Int64(3), Value::Int64(10)]);
                }
                results.push(actual);
            }
            assert_eq!(results[0], results[1]);
        }
    }

    #[tokio::test]
    async fn buffered_window_regions_match_standalone_lifecycle_and_metadata() {
        use crate::planner::physical::{PhysicalEosWindow, PhysicalStateWindow};
        use crate::processor::eos_window_processor::EosWindowProcessor;
        use crate::processor::state_window_processor::StateWindowProcessor;
        use crate::processor::streaming_aggregation_processor::{
            StreamingEosAggregationProcessor, StreamingStateAggregationProcessor,
        };
        use crate::processor::{BarrierControlSignal, InstantControlSignal};
        for state_window in [false, true] {
            for aggregate in [false, true] {
                for mode in 0..4 {
                    for fused in [false, true] {
                        let capacities =
                            ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
                        let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
                        let (tx, rx) = mpsc::channel(1);
                        let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
                        let plan = if aggregate {
                            let PhysicalPlan::StreamingAggregation(mut p) = window_plan(true)
                            else {
                                unreachable!()
                            };
                            p.window = if state_window {
                                let StreamingWindowSpec::Tumbling {
                                    partition_by_exprs,
                                    partition_by_scalars,
                                    ..
                                } = p.window
                                else {
                                    unreachable!()
                                };
                                StreamingWindowSpec::State {
                                    open_expr: Expr::Value(SqlValue::Boolean(true)),
                                    emit_expr: Expr::Value(SqlValue::Boolean(true)),
                                    partition_by_exprs,
                                    open_scalar: ScalarExpr::Literal(
                                        Value::Bool(true),
                                        ConcreteDatatype::Bool(BooleanType),
                                    ),
                                    emit_scalar: ScalarExpr::Literal(
                                        Value::Bool(true),
                                        ConcreteDatatype::Bool(BooleanType),
                                    ),
                                    partition_by_scalars,
                                }
                            } else {
                                StreamingWindowSpec::Eos
                            };
                            PhysicalPlan::StreamingAggregation(p)
                        } else if state_window {
                            let PhysicalPlan::TumblingWindow(p) = window_plan(false) else {
                                unreachable!()
                            };
                            PhysicalPlan::StateWindow(Box::new(PhysicalStateWindow::new(
                                Expr::Value(SqlValue::Boolean(true)),
                                Expr::Value(SqlValue::Boolean(true)),
                                p.partition_by_exprs,
                                ScalarExpr::Literal(
                                    Value::Bool(true),
                                    ConcreteDatatype::Bool(BooleanType),
                                ),
                                ScalarExpr::Literal(
                                    Value::Bool(true),
                                    ConcreteDatatype::Bool(BooleanType),
                                ),
                                p.partition_by_scalars,
                                vec![],
                                2,
                            )))
                        } else {
                            PhysicalPlan::EosWindow(PhysicalEosWindow::new(vec![], 2))
                        };
                        let (mut output, mut control_output, task) = if fused {
                            connect_window(
                                RegionProcessor::new(
                                    "region".into(),
                                    &[
                                        PhysicalPlan::Compute(PhysicalCompute::new(
                                            vec![],
                                            vec![],
                                            1,
                                        )),
                                        plan,
                                        PhysicalPlan::Compute(PhysicalCompute::new(
                                            vec![],
                                            vec![],
                                            3,
                                        )),
                                    ],
                                    RegionResources::default(),
                                    capacities,
                                )
                                .unwrap(),
                                rx,
                                ctrl_rx,
                                &spawner,
                            )
                        } else {
                            match plan {
                                PhysicalPlan::EosWindow(_) => connect_window(EosWindowProcessor::new_with_channel_capacities("window", capacities), rx, ctrl_rx, &spawner),
                                PhysicalPlan::StateWindow(p) => connect_window(StateWindowProcessor::new_with_channel_capacities("window", Arc::new(*p), capacities), rx, ctrl_rx, &spawner),
                                PhysicalPlan::StreamingAggregation(p) if state_window => connect_window(StreamingStateAggregationProcessor::new_with_channel_capacities("window", Arc::new(p), AggregateFunctionRegistry::with_builtins(), capacities).unwrap(), rx, ctrl_rx, &spawner),
                                PhysicalPlan::StreamingAggregation(p) => connect_window(StreamingEosAggregationProcessor::new_with_channel_capacities("window", Arc::new(p), AggregateFunctionRegistry::with_builtins(), capacities), rx, ctrl_rx, &spawner),
                                _ => unreachable!(),
                            }
                        };
                        let snapshot = |batch: &dyn Collection| {
                            let metadata = batch.metadata().window.as_deref().unwrap();
                            (
                                batch
                                    .rows()
                                    .iter()
                                    .map(|row| {
                                        row.value_by_name("", if aggregate { "total" } else { "a" })
                                            .cloned()
                                            .unwrap()
                                    })
                                    .collect::<Vec<_>>(),
                                metadata.start.epoch_micros(),
                                metadata.end.epoch_micros(),
                            )
                        };
                        let mut actual = Vec::new();
                        tx.send(window_input()).await.unwrap();
                        let sync =
                            ControlSignal::Barrier(BarrierControlSignalKind::SyncTest.with_id(8));
                        tx.send(StreamData::control(sync.clone())).await.unwrap();
                        loop {
                            match timeout(Duration::from_secs(2), output.recv())
                                .await
                                .unwrap()
                                .unwrap()
                            {
                                StreamData::Collection(batch) => {
                                    actual.push(snapshot(batch.as_ref()))
                                }
                                StreamData::Control(signal) => {
                                    assert_eq!(signal, sync);
                                    break;
                                }
                                _ => panic!("unexpected window event"),
                            }
                        }
                        let signal = if mode == 1 {
                            ControlSignal::Instant(InstantControlSignal::StreamQuickEnd {
                                signal_id: 9,
                            })
                        } else {
                            ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd {
                                barrier_id: 9,
                            })
                        };
                        match mode {
                            2 => {
                                ctrl_tx.send(signal.clone()).await.unwrap();
                                assert_eq!(
                                    timeout(Duration::from_secs(2), control_output.recv())
                                        .await
                                        .unwrap()
                                        .unwrap(),
                                    signal
                                );
                            }
                            3 => drop(tx),
                            _ => tx.send(StreamData::control(signal.clone())).await.unwrap(),
                        }
                        loop {
                            match timeout(Duration::from_secs(2), output.recv())
                                .await
                                .unwrap()
                            {
                                Ok(StreamData::Collection(batch)) => {
                                    actual.push(snapshot(batch.as_ref()))
                                }
                                Ok(StreamData::Control(s)) => {
                                    assert!(mode < 2);
                                    assert_eq!(s, signal);
                                    break;
                                }
                                Err(ProcessorError::ChannelClosed) => {
                                    assert!(mode >= 2);
                                    break;
                                }
                                _ => panic!("unexpected window output"),
                            }
                        }
                        timeout(Duration::from_secs(2), task.handle)
                            .await
                            .unwrap()
                            .unwrap()
                            .unwrap();
                        assert!(matches!(
                            output.recv().await,
                            Err(ProcessorError::ChannelClosed)
                        ));
                        let mut expected = Vec::new();
                        if state_window {
                            expected.push((
                                if aggregate {
                                    vec![Value::Int64(10)]
                                } else {
                                    vec![Value::Int64(2), Value::Int64(8)]
                                },
                                1_001_000,
                                1_101_000,
                            ));
                            if mode == 0 {
                                expected.extend([
                                    (vec![Value::Int64(16)], 1_201_000, 1_201_000),
                                    (vec![Value::Int64(4)], 1_002_000, 1_002_000),
                                ]);
                            }
                        } else if mode == 0 {
                            expected.push((
                                if aggregate {
                                    vec![Value::Int64(30)]
                                } else {
                                    vec![
                                        Value::Int64(2),
                                        Value::Int64(4),
                                        Value::Int64(8),
                                        Value::Int64(16),
                                    ]
                                },
                                1_001_000,
                                1_201_000,
                            ));
                        }
                        actual.sort_by_key(|row| (row.1, row.2));
                        expected.sort_by_key(|row| (row.1, row.2));
                        assert_eq!(actual, expected, "state={state_window}, aggregate={aggregate}, mode={mode}, fused={fused}");
                    }
                }
            }
        }
    }

    fn sliding_plan(aggregate: bool, lookahead: Option<u64>, triggered: bool) -> PhysicalPlan {
        use crate::expr::func::BinaryFunc;
        use crate::expr::scalar::ColumnRef;
        use crate::planner::logical::TimeUnit;
        use crate::planner::physical::{PhysicalSlidingWindow, PipelineStateUsage};
        let PhysicalPlan::TumblingWindow(partition) = window_plan(false) else {
            unreachable!()
        };
        let trigger = triggered.then(|| ScalarExpr::CallBinary {
            func: BinaryFunc::Gt,
            expr1: Box::new(ScalarExpr::Column(ColumnRef::ByName {
                column_name: "a".into(),
            })),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(5),
                ConcreteDatatype::Int64(datatypes::Int64Type),
            )),
        });
        if aggregate {
            let PhysicalPlan::StreamingAggregation(mut p) = window_plan(true) else {
                unreachable!()
            };
            p.window = StreamingWindowSpec::Sliding {
                time_unit: TimeUnit::Milliseconds,
                lookback: 100,
                lookahead,
                partition_by_exprs: partition.partition_by_exprs,
                partition_by_scalars: partition.partition_by_scalars,
                trigger_condition_expr: None,
                trigger_condition_scalar: trigger,
                trigger_processor_state: None,
                trigger_state_usage: PipelineStateUsage::default(),
            };
            PhysicalPlan::StreamingAggregation(p)
        } else {
            let mut p = PhysicalSlidingWindow::new_partitioned(
                TimeUnit::Milliseconds,
                100,
                lookahead,
                partition.partition_by_exprs,
                partition.partition_by_scalars,
                vec![],
                2,
            );
            p.trigger_condition_scalar = trigger;
            PhysicalPlan::SlidingWindow(p)
        }
    }

    #[tokio::test]
    async fn sliding_regions_match_standalone_partition_bounds_triggers_and_lifecycle() {
        use crate::processor::sliding_window_processor::SlidingWindowProcessor;
        use crate::processor::streaming_aggregation_processor::StreamingSlidingAggregationProcessor;
        use crate::processor::{BarrierControlSignal, InstantControlSignal};
        for aggregate in [false, true] {
            for lookahead in [None, Some(50)] {
                for triggered in [false, true] {
                    for mode in 0..4 {
                        for fused in [false, true] {
                            let capacities = ProcessorChannelCapacities::new(1, 1)
                                .with_link_kind(LinkKind::Mpsc);
                            let spawner =
                                TaskSpawner::from_handle(tokio::runtime::Handle::current());
                            let (tx, rx) = mpsc::channel(1);
                            let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
                            let plan = sliding_plan(aggregate, lookahead, triggered);
                            let (mut output, mut control_output, task) = if fused {
                                connect_window(
                                    RegionProcessor::new(
                                        "region".into(),
                                        &[
                                            PhysicalPlan::Compute(PhysicalCompute::new(
                                                vec![],
                                                vec![],
                                                1,
                                            )),
                                            plan,
                                            PhysicalPlan::Compute(PhysicalCompute::new(
                                                vec![],
                                                vec![],
                                                3,
                                            )),
                                        ],
                                        RegionResources::default(),
                                        capacities,
                                    )
                                    .unwrap(),
                                    rx,
                                    ctrl_rx,
                                    &spawner,
                                )
                            } else {
                                match plan {
                                    PhysicalPlan::SlidingWindow(p) => connect_window(SlidingWindowProcessor::new_with_channel_capacities("sliding", Arc::new(p), capacities), rx, ctrl_rx, &spawner),
                                    PhysicalPlan::StreamingAggregation(p) => connect_window(StreamingSlidingAggregationProcessor::new_with_channel_capacities("sliding", Arc::new(p), AggregateFunctionRegistry::with_builtins(), capacities).unwrap(), rx, ctrl_rx, &spawner),
                                    _ => unreachable!(),
                                }
                            };
                            // One capacity slot must remain available for the watermark; the
                            // input queue is bounded independently of the output channel.
                            tx.send(window_input()).await.unwrap();
                            let snapshot = |batch: &dyn Collection| {
                                let meta = batch.metadata().window.as_deref().unwrap();
                                (
                                    batch
                                        .rows()
                                        .iter()
                                        .map(|row| {
                                            row.value_by_name(
                                                "",
                                                if aggregate { "total" } else { "a" },
                                            )
                                            .cloned()
                                            .unwrap()
                                        })
                                        .collect::<Vec<_>>(),
                                    meta.start.epoch_micros(),
                                    meta.end.epoch_micros(),
                                )
                            };
                            let mut actual = Vec::new();
                            // Drain immediate outputs before sending the next data event.
                            if lookahead.is_none() {
                                for _ in 0..if triggered { 2 } else { 4 } {
                                    let StreamData::Collection(batch) =
                                        timeout(Duration::from_secs(2), output.recv())
                                            .await
                                            .unwrap()
                                            .unwrap()
                                    else {
                                        panic!("expected immediate sliding window");
                                    };
                                    actual.push(snapshot(batch.as_ref()));
                                }
                            }
                            tx.send(StreamData::Watermark(
                                std::time::UNIX_EPOCH + Duration::from_millis(1151),
                            ))
                            .await
                            .unwrap();
                            let sync = ControlSignal::Barrier(
                                BarrierControlSignalKind::SyncTest.with_id(8),
                            );
                            tx.send(StreamData::control(sync.clone())).await.unwrap();
                            loop {
                                match timeout(Duration::from_secs(2), output.recv())
                                    .await
                                    .unwrap()
                                    .unwrap()
                                {
                                    StreamData::Collection(batch) => {
                                        actual.push(snapshot(batch.as_ref()))
                                    }
                                    StreamData::Control(signal) => {
                                        assert_eq!(signal, sync);
                                        break;
                                    }
                                    _ => panic!("sliding window must consume the watermark"),
                                }
                            }
                            let signal = if mode == 1 {
                                ControlSignal::Instant(InstantControlSignal::StreamQuickEnd {
                                    signal_id: 9,
                                })
                            } else {
                                ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd {
                                    barrier_id: 9,
                                })
                            };
                            match mode {
                                2 => {
                                    ctrl_tx.send(signal.clone()).await.unwrap();
                                    assert_eq!(
                                        timeout(Duration::from_secs(2), control_output.recv())
                                            .await
                                            .unwrap()
                                            .unwrap(),
                                        signal
                                    );
                                }
                                3 => drop(tx),
                                _ => tx.send(StreamData::control(signal.clone())).await.unwrap(),
                            }
                            loop {
                                match timeout(Duration::from_secs(2), output.recv())
                                    .await
                                    .unwrap()
                                {
                                    Ok(StreamData::Collection(batch)) => {
                                        actual.push(snapshot(batch.as_ref()))
                                    }
                                    Ok(StreamData::Control(s)) => {
                                        assert!(mode < 2);
                                        assert_eq!(s, signal);
                                        break;
                                    }
                                    Err(ProcessorError::ChannelClosed) => {
                                        assert!(mode >= 2);
                                        break;
                                    }
                                    _ => panic!("unexpected sliding output"),
                                }
                            }
                            timeout(Duration::from_secs(2), task.handle)
                                .await
                                .unwrap()
                                .unwrap()
                                .unwrap();
                            let mut expected = Vec::new();
                            let mut add = |values: &[i64], trigger: i64| {
                                expected.push((
                                    if aggregate {
                                        vec![Value::Int64(values.iter().sum())]
                                    } else {
                                        values.iter().map(|v| Value::Int64(*v)).collect()
                                    },
                                    (trigger - 100) * 1000,
                                    (trigger + lookahead.unwrap_or(0) as i64) * 1000,
                                ));
                            };
                            if lookahead.is_none() {
                                if !triggered {
                                    add(&[2], 1001);
                                    add(&[4], 1002);
                                }
                                add(&[2, 8], 1101);
                                add(&[8, 16], 1201);
                            } else {
                                if !triggered {
                                    add(&[2], 1001);
                                }
                                add(&[2, 8], 1101);
                                if !triggered {
                                    add(&[4], 1002);
                                }
                                if mode == 0 {
                                    add(&[8, 16], 1201);
                                }
                            }
                            assert_eq!(actual, expected, "aggregate={aggregate}, lookahead={lookahead:?}, triggered={triggered}, mode={mode}, fused={fused}");
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn sliding_region_backpressure_defers_trailing_row_errors() {
        for aggregate in [false, true] {
            let capacities = ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc);
            let plan = sliding_plan(aggregate, None, false);
            let mut region = RegionProcessor::new(
                "region".into(),
                &[
                    plan,
                    PhysicalPlan::Compute(PhysicalCompute::new(vec![], vec![], 3)),
                ],
                RegionResources::default(),
                capacities,
            )
            .unwrap();
            let stats = Arc::new(ProcessorStats::collection_in_out());
            region
                .stages
                .get_mut()
                .unwrap()
                .first_mut()
                .unwrap()
                .set_stats(Arc::clone(&stats));
            let (tx, rx) = mpsc::channel(1);
            region.add_input(rx);
            let mut output = region.subscribe_output().unwrap();
            let task = region
                .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
                .handle;
            let StreamData::Collection(batch) = count_input(&[(1000, 1, 1), (1001, 1, 2)]) else {
                unreachable!()
            };
            let mut rows = batch.into_rows().unwrap();
            rows.push(Tuple::with_timestamp(
                Tuple::empty_messages(),
                std::time::UNIX_EPOCH - Duration::from_secs(1),
            ));
            tx.send(StreamData::collection(Box::new(
                RecordBatch::new(rows).unwrap(),
            )))
            .await
            .unwrap();
            timeout(Duration::from_secs(2), async {
                while stats
                    .snapshot()
                    .custom
                    .get("collections_out")
                    .copied()
                    .unwrap_or(0)
                    != 2
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(stats.snapshot().error_count, 0);
            for _ in 0..2 {
                assert!(matches!(
                    timeout(Duration::from_secs(2), output.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                    StreamData::Collection(_)
                ));
            }
            drop(tx);
            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(stats.snapshot().error_count, 1);
        }
    }
}
