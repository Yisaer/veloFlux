use super::{
    apply_aggregate_updates, build_group_by_meta, create_accumulators_static, GroupByMeta,
};
use crate::aggregation::AggregateFunctionRegistry;
use crate::planner::physical::{
    PhysicalStreamingAggregation, PipelineStateUsage, StreamingWindowSpec,
};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::pipeline_state_runtime::update_row_hit_state;
use crate::processor::processor_state::ProcessorState;
use crate::processor::sliding_window_stage::{sliding_window_output, SlidingWindowOutput};
use crate::processor::window_metadata;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::processor::sliding_window_runtime::evaluate_trigger_condition;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};

/// Incremental sliding aggregation over inclusive logical bounds
/// `[trigger_timestamp - lookback, trigger_timestamp + lookahead]`.
/// Each triggering row opens a window containing retained history. With no
/// lookahead, emit immediately; otherwise upstream watermarks close windows.
pub struct StreamingSlidingAggregationProcessor {
    id: String,
    stage: Option<StreamingSlidingStage>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

struct WindowGroupState {
    accumulators: Vec<Box<dyn crate::aggregation::AggregateAccumulator>>,
    last_tuple: crate::model::Tuple,
}

struct IncAggWindow {
    start: SystemTime,
    end: SystemTime,
    groups: HashMap<Vec<Value>, WindowGroupState>,
}

#[derive(Clone, Copy)]
struct SlidingWindowBounds {
    length: Duration,
    delay: Duration,
}

impl SlidingWindowBounds {
    fn for_trigger(self, trigger: SystemTime) -> Option<(SystemTime, SystemTime)> {
        let start = window_metadata::saturating_sub_to_epoch(trigger, self.length);
        let end = trigger.checked_add(self.delay)?;
        Some((start, end))
    }
}

impl StreamingSlidingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
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
    ) -> Result<Self, ProcessorError> {
        let stage = StreamingSlidingStage::new(physical, aggregate_registry)?;
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );

        Ok(Self {
            id: id.into(),
            stage: Some(stage),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        })
    }

    fn extract_window_spec(
        physical: &PhysicalStreamingAggregation,
    ) -> Result<(Duration, Duration), ProcessorError> {
        match &physical.window {
            StreamingWindowSpec::Sliding {
                time_unit,
                lookback,
                lookahead,
                ..
            } => Ok((
                time_unit.duration(*lookback),
                lookahead.map(|l| time_unit.duration(l)).unwrap_or_default(),
            )),
            other => Err(ProcessorError::InvalidConfiguration(format!(
                "streaming sliding aggregation requires sliding window spec, got {other:?}",
            ))),
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

impl Processor for StreamingSlidingAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let Some(mut stage) = self.stage.take() else {
            return ProcessorStart::failed(
                spawner,
                ProcessorError::InvalidConfiguration(
                    "sliding aggregation processor was already started".into(),
                ),
            );
        };
        if let Err(error) = stage.validate_supported_aggregates() {
            return ProcessorStart::failed(spawner, error);
        }
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);

        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
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
                                if let Some(rows) = data.row_count() { stats.record_collection_in(rows); }
                                let terminal = data.is_terminal();
                                let started = std::time::Instant::now();
                                let mut outputs = stage.process(data, &stats)?;
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

struct TriggeredSlidingPartition {
    rows: VecDeque<crate::model::Tuple>,
    windows: VecDeque<IncAggWindow>,
}

struct PartitionedTriggeredSlidingWindows {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<TriggeredSlidingPartition>,
    partition_by_scalars: Vec<crate::expr::ScalarExpr>,
}

impl PartitionedTriggeredSlidingWindows {
    fn new(partition_by_scalars: Vec<crate::expr::ScalarExpr>) -> Self {
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
            partition_by_scalars,
        }
    }

    fn key_for_tuple(&self, tuple: &crate::model::Tuple) -> Result<PartitionKey, ProcessorError> {
        eval_partition_key(&self.partition_by_scalars, tuple, "slidingwindow")
            .map_err(ProcessorError::ProcessingError)
    }

    fn state_for_key(&mut self, key: PartitionKey) -> &mut TriggeredSlidingPartition {
        if let Some(id) = self.state_ids.get(&key).copied() {
            return &mut self.states[id];
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(TriggeredSlidingPartition {
            rows: VecDeque::new(),
            windows: VecDeque::new(),
        });
        &mut self.states[id]
    }
}

/// Synchronous incremental sliding aggregation, with one output per downstream wait.
pub(crate) struct StreamingSlidingStage {
    physical: Arc<PhysicalStreamingAggregation>,
    registry: Arc<AggregateFunctionRegistry>,
    group_by: Vec<GroupByMeta>,
    partitions: PartitionedTriggeredSlidingWindows,
    trigger: Option<crate::expr::ScalarExpr>,
    trigger_state: Option<Arc<ProcessorState>>,
    trigger_usage: PipelineStateUsage,
    bounds: SlidingWindowBounds,
}

impl StreamingSlidingStage {
    pub(crate) fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
        let group_by_meta =
            build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let partition_by_scalars = match &physical.window {
            StreamingWindowSpec::Sliding {
                partition_by_scalars,
                ..
            } => partition_by_scalars.clone(),
            _ => Vec::new(),
        };
        let trigger_condition_scalar = match &physical.window {
            StreamingWindowSpec::Sliding {
                trigger_condition_scalar,
                ..
            } => trigger_condition_scalar.clone(),
            _ => None,
        };
        let (trigger_processor_state, trigger_state_usage) = match &physical.window {
            StreamingWindowSpec::Sliding {
                trigger_processor_state,
                trigger_state_usage,
                ..
            } => (trigger_processor_state.clone(), *trigger_state_usage),
            _ => (None, PipelineStateUsage::default()),
        };
        let (length, delay) = StreamingSlidingAggregationProcessor::extract_window_spec(&physical)?;
        Ok(Self {
            physical,
            registry: aggregate_registry,
            group_by: group_by_meta,
            partitions: PartitionedTriggeredSlidingWindows::new(partition_by_scalars),
            trigger: trigger_condition_scalar,
            trigger_state: trigger_processor_state,
            trigger_usage: trigger_state_usage,
            bounds: SlidingWindowBounds { length, delay },
        })
    }
    pub(crate) fn validate_supported_aggregates(&self) -> Result<(), ProcessorError> {
        for call in &self.physical.aggregate_calls {
            if call.distinct {
                return Err(ProcessorError::InvalidConfiguration(
                    "DISTINCT aggregates are not supported in streaming sliding aggregation"
                        .to_string(),
                ));
            }
            if !self.registry.supports_incremental(&call.func_name) {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "Aggregate function '{}' does not support incremental updates",
                    call.func_name
                )));
            }
        }
        Ok(())
    }
    pub(crate) fn clear(&mut self) {
        self.partitions.state_ids.clear();
        self.partitions.states.clear();
    }
    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> Result<SlidingWindowOutput, ProcessorError> {
        if let StreamData::Watermark(watermark) = &data {
            if self.bounds.delay.is_zero() {
                return Ok(SlidingWindowOutput::Single(None));
            }
            match window_metadata::validate_system_time(*watermark) {
                Ok(()) => {}
                Err(ProcessorError::ProcessingError(error)) => {
                    stats.record_error_logged(
                        "streaming sliding aggregation processor error",
                        error,
                    );
                    return Ok(SlidingWindowOutput::Single(None));
                }
                Err(error) => return Err(error),
            }
        }
        if data.is_terminal() && !matches!(&data, StreamData::Control(s) if s.is_graceful_end()) {
            self.clear();
        }
        Ok(sliding_window_output(
            data,
            stats,
            "streaming sliding aggregation processor error",
        ))
    }
    pub(crate) fn next_output(
        &mut self,
        output: &mut SlidingWindowOutput,
        stats: &ProcessorStats,
    ) -> Result<Option<StreamData>, ProcessorError> {
        match output {
            SlidingWindowOutput::Single(data) => Ok(data.take()),
            SlidingWindowOutput::Drain {
                partition,
                watermark,
                terminal,
            } => {
                while let Some(state) = self.partitions.states.get_mut(*partition) {
                    while let Some(window) = state.windows.front() {
                        if watermark.is_some_and(|limit| window.end > limit) {
                            break;
                        }
                        let window = state.windows.pop_front().ok_or_else(|| {
                            ProcessorError::ProcessingError("sliding window disappeared".into())
                        })?;
                        if let Some(data) = render_window(&self.physical, &self.group_by, &window)?
                        {
                            return Ok(Some(data));
                        }
                    }
                    if let Some(watermark) = watermark {
                        trim_trigger_rows(&mut state.rows, *watermark, self.bounds.length);
                    }
                    *partition += 1;
                }
                Ok(terminal.take().map(StreamData::control))
            }
            SlidingWindowOutput::Rows(rows) => {
                for tuple in rows.by_ref() {
                    if let Err(error) = window_metadata::validate_system_time(tuple.timestamp) {
                        stats.record_error_logged(
                            "streaming sliding aggregation processor error",
                            error.to_string(),
                        );
                        continue;
                    }
                    let now = tuple.timestamp;
                    let key = match self.partitions.key_for_tuple(&tuple) {
                        Ok(key) => key,
                        Err(error) => {
                            stats.record_error_logged(
                                "streaming sliding aggregation processor error",
                                error.to_string(),
                            );
                            continue;
                        }
                    };
                    let should_trigger = match evaluate_trigger_condition(
                        self.trigger.as_ref(),
                        &tuple,
                        "slidingwindow trigger condition",
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            stats.record_error_logged(
                                "streaming sliding aggregation error",
                                error.to_string(),
                            );
                            false
                        }
                    };
                    let logical_bounds = if should_trigger {
                        let Some((start, end)) = self.bounds.for_trigger(now) else {
                            stats.record_error_logged(
                                "streaming sliding aggregation processor error",
                                "slidingwindow end timestamp overflow",
                            );
                            continue;
                        };
                        if let Err(error) = window_metadata::validate_system_time(end) {
                            stats.record_error_logged(
                                "streaming sliding aggregation processor error",
                                error.to_string(),
                            );
                            continue;
                        }
                        Some((start, end))
                    } else {
                        None
                    };
                    let state = self.partitions.state_for_key(key);
                    state.rows.push_back(tuple);
                    let tuple = state.rows.back().ok_or_else(|| {
                        ProcessorError::ProcessingError("sliding input row disappeared".into())
                    })?;
                    if let Err(error) = update_triggered_windows_with_tuple(
                        &self.physical,
                        &self.registry,
                        &self.group_by,
                        &mut state.windows,
                        tuple,
                        now,
                    ) {
                        state.rows.pop_back();
                        stats.record_error_logged(
                            "streaming sliding aggregation processor error",
                            error.to_string(),
                        );
                        continue;
                    }
                    let mut output = None;
                    if let Some((start, end)) = logical_bounds {
                        if let Some(trigger_state) = self.trigger_state.as_deref() {
                            if let Err(error) = update_row_hit_state(
                                trigger_state,
                                self.trigger_usage,
                                tuple.timestamp,
                            ) {
                                stats.record_error_logged(
                                    "streaming sliding aggregation processor error",
                                    error.to_string(),
                                );
                                continue;
                            }
                        }
                        let window = match build_triggered_window_from_rows(
                            &self.physical,
                            &self.registry,
                            &self.group_by,
                            &state.rows,
                            start,
                            end,
                        ) {
                            Ok(window) => window,
                            Err(error) => {
                                stats.record_error_logged(
                                    "streaming sliding aggregation processor error",
                                    error.to_string(),
                                );
                                continue;
                            }
                        };
                        if self.bounds.delay.is_zero() {
                            match render_window(&self.physical, &self.group_by, &window) {
                                Ok(data) => output = data,
                                Err(error) => stats.record_error_logged(
                                    "streaming sliding aggregation processor error",
                                    error.to_string(),
                                ),
                            }
                        } else {
                            state.windows.push_back(window);
                        }
                    }
                    trim_trigger_rows(&mut state.rows, now, self.bounds.length);
                    if output.is_some() {
                        return Ok(output);
                    }
                }
                Ok(None)
            }
        }
    }
}

fn render_window(
    physical: &PhysicalStreamingAggregation,
    group_by_meta: &[GroupByMeta],
    window: &IncAggWindow,
) -> Result<Option<StreamData>, ProcessorError> {
    if window.groups.is_empty() {
        return Ok(None);
    }
    let mut out_rows = Vec::with_capacity(window.groups.len());
    for (key_values, state) in &window.groups {
        let mut entries = Vec::new();
        for (call, accumulator) in physical.aggregate_calls.iter().zip(&state.accumulators) {
            entries.push((Arc::new(call.output_column.clone()), accumulator.finalize()));
        }
        for (index, value) in key_values.iter().enumerate() {
            if let Some(meta) = group_by_meta.get(index).filter(|m| !m.is_simple) {
                entries.push((Arc::new(meta.output_name.clone()), value.clone()));
            }
        }
        let mut tuple = crate::model::Tuple::with_timestamp(
            state.last_tuple.messages.clone(),
            state.last_tuple.timestamp,
        );
        tuple.add_affiliate_columns(entries);
        out_rows.push(tuple);
    }
    window_metadata::record_batch_from_system_time(out_rows, window.start, window.end)
        .map(|batch| Some(StreamData::collection(Box::new(batch))))
}

fn update_window_with_tuple(
    physical: &PhysicalStreamingAggregation,
    aggregate_registry: &AggregateFunctionRegistry,
    group_by_meta: &[GroupByMeta],
    window: &mut IncAggWindow,
    tuple: &crate::model::Tuple,
) -> Result<(), ProcessorError> {
    let mut key_values = Vec::with_capacity(group_by_meta.len());
    for meta in group_by_meta {
        key_values.push(meta.scalar.eval_with_tuple(tuple).map_err(|e| {
            ProcessorError::ProcessingError(format!("failed to evaluate group-by expression: {e}"))
        })?);
    }
    let mut row_call_args = Vec::with_capacity(physical.aggregate_calls.len());
    for (idx, call) in physical.aggregate_calls.iter().enumerate() {
        let mut args = Vec::with_capacity(call.args.len());
        for arg_expr in &call.args {
            args.push(arg_expr.eval_with_tuple(tuple).map_err(|e| {
                ProcessorError::ProcessingError(format!(
                    "failed to evaluate aggregate argument: {e}"
                ))
            })?);
        }
        row_call_args.push((idx, args));
    }

    match window.groups.entry(key_values) {
        Entry::Occupied(mut o) => {
            let entry = o.get_mut();
            apply_aggregate_updates(&mut entry.accumulators, &row_call_args)
                .map_err(ProcessorError::ProcessingError)?;
            entry.last_tuple = tuple.clone();
        }
        Entry::Vacant(v) => {
            let accumulators =
                create_accumulators_static(&physical.aggregate_calls, aggregate_registry)
                    .map_err(ProcessorError::ProcessingError)?;
            let mut state = WindowGroupState {
                accumulators,
                last_tuple: tuple.clone(),
            };
            apply_aggregate_updates(&mut state.accumulators, &row_call_args)
                .map_err(ProcessorError::ProcessingError)?;
            v.insert(state);
        }
    }

    Ok(())
}

fn trim_trigger_rows(
    rows: &mut VecDeque<crate::model::Tuple>,
    now: SystemTime,
    lookback: Duration,
) {
    let min_start = window_metadata::saturating_sub_to_epoch(now, lookback);
    while let Some(front) = rows.front() {
        if front.timestamp >= min_start {
            break;
        }
        rows.pop_front();
    }
}

fn update_triggered_windows_with_tuple(
    physical: &PhysicalStreamingAggregation,
    aggregate_registry: &AggregateFunctionRegistry,
    group_by_meta: &[GroupByMeta],
    windows: &mut VecDeque<IncAggWindow>,
    tuple: &crate::model::Tuple,
    now: SystemTime,
) -> Result<(), ProcessorError> {
    for window in windows.iter_mut() {
        if window.start <= now && window.end >= now {
            update_window_with_tuple(physical, aggregate_registry, group_by_meta, window, tuple)?;
        }
    }
    Ok(())
}

fn build_triggered_window_from_rows(
    physical: &PhysicalStreamingAggregation,
    aggregate_registry: &AggregateFunctionRegistry,
    group_by_meta: &[GroupByMeta],
    rows: &VecDeque<crate::model::Tuple>,
    start: SystemTime,
    end: SystemTime,
) -> Result<IncAggWindow, ProcessorError> {
    let mut window = IncAggWindow {
        start,
        end,
        groups: HashMap::new(),
    };
    for row in rows {
        if row.timestamp < start {
            continue;
        }
        if row.timestamp > end {
            break;
        }
        update_window_with_tuple(
            physical,
            aggregate_registry,
            group_by_meta,
            &mut window,
            row,
        )?;
    }
    Ok(window)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::AggregateFunctionRegistry;
    use crate::expr::func::BinaryFunc;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ProcStateField;
    use crate::expr::ScalarExpr;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::AggregateCall;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::processor::processor_state::ProcessorState;
    use crate::runtime::TaskSpawner;
    use datatypes::{ConcreteDatatype, Int64Type, Value};
    use sqlparser::ast::{Expr, Ident};
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use std::time::UNIX_EPOCH;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn col(name: &str) -> ScalarExpr {
        ScalarExpr::Column(ColumnRef::ByName {
            column_name: name.to_string(),
        })
    }

    fn tuple_at(sec: u64, a: i64) -> crate::model::Tuple {
        tuple_with_a_at(UNIX_EPOCH + Duration::from_secs(sec), a)
    }

    fn tuple_at_millis(millis: u64, a: i64) -> crate::model::Tuple {
        tuple_with_a_at(UNIX_EPOCH + Duration::from_millis(millis), a)
    }

    fn tuple_with_a_at(timestamp: SystemTime, a: i64) -> crate::model::Tuple {
        let mut tuple =
            crate::model::Tuple::with_timestamp(crate::model::Tuple::empty_messages(), timestamp);
        tuple.add_affiliate_column(Arc::new("a".to_string()), Value::Int64(a));
        tuple
    }

    fn a_greater_than_five() -> ScalarExpr {
        col("a").call_binary(
            ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type)),
            BinaryFunc::Gt,
        )
    }

    fn make_physical() -> Arc<PhysicalStreamingAggregation> {
        make_physical_with_lookahead(None)
    }

    fn make_physical_with_lookahead(lookahead: Option<u64>) -> Arc<PhysicalStreamingAggregation> {
        make_physical_with_lookahead_and_trigger(lookahead, None, None)
    }

    fn make_physical_with_lookahead_and_trigger(
        lookahead: Option<u64>,
        trigger_condition_scalar: Option<ScalarExpr>,
        trigger_processor_state: Option<Arc<ProcessorState>>,
    ) -> Arc<PhysicalStreamingAggregation> {
        make_physical_with_window(
            TimeUnit::Seconds,
            2,
            lookahead,
            trigger_condition_scalar,
            trigger_processor_state,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn make_physical_with_window(
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        trigger_condition_scalar: Option<ScalarExpr>,
        trigger_processor_state: Option<Arc<ProcessorState>>,
    ) -> Arc<PhysicalStreamingAggregation> {
        let call = AggregateCall {
            supports_inline_execution: true,
            output_column: "sum_a".to_string(),
            func_name: "sum".to_string(),
            args: vec![col("a")],
            distinct: false,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "sum_a".to_string(),
            Expr::Function(sqlparser::ast::Function {
                name: sqlparser::ast::ObjectName(vec![Ident::new("sum")]),
                args: vec![sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(Ident::new("a"))),
                )],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            }),
        );
        let trigger_state_usage = if trigger_processor_state.is_some() {
            PipelineStateUsage {
                last_hit_time_unix_ms: true,
                ..PipelineStateUsage::default()
            }
        } else {
            PipelineStateUsage::default()
        };

        Arc::new(PhysicalStreamingAggregation::new(
            StreamingWindowSpec::Sliding {
                time_unit,
                lookback,
                lookahead,
                partition_by_exprs: Vec::new(),
                partition_by_scalars: Vec::new(),
                trigger_condition_expr: None,
                trigger_condition_scalar,
                trigger_processor_state,
                trigger_state_usage,
            },
            mappings,
            Vec::new(),
            vec![call],
            Vec::new(),
            Vec::new(),
            0,
        ))
    }

    fn extract_sum(collection: &dyn crate::model::Collection) -> i64 {
        assert_eq!(
            collection.rows().len(),
            1,
            "expected one row per sliding emit"
        );
        let value = collection.rows()[0]
            .value_by_name("", "sum_a")
            .cloned()
            .expect("aggregate value");
        let Value::Int64(sum) = value else {
            panic!("expected Int64 aggregate value, got {value:?}");
        };
        sum
    }

    #[tokio::test]
    async fn sliding_aggregation_emits_trigger_relative_window_per_tuple_when_delay_is_zero() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical();

        let mut processor = StreamingSlidingAggregationProcessor::new(
            "sliding",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("sliding processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_at(1, 1),
            tuple_at(2, 2),
            tuple_at(4, 4),
            tuple_at(5, 8),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut sums = Vec::new();
        for _ in 0..4 {
            let item = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            let StreamData::Collection(collection) = item else {
                panic!("expected sliding aggregation collection");
            };
            sums.push(extract_sum(collection.as_ref()));
        }

        assert_eq!(sums, vec![1, 3, 6, 12]);
    }

    #[tokio::test]
    async fn sliding_aggregation_updates_last_hit_time_after_trigger_hit() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let state = Arc::new(ProcessorState::new());
        let trigger = ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            }),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(1_500),
                ConcreteDatatype::Int64(Int64Type),
            )),
        };
        let physical =
            make_physical_with_lookahead_and_trigger(None, Some(trigger), Some(Arc::clone(&state)));

        let mut processor = StreamingSlidingAggregationProcessor::new(
            "sliding",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("sliding processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(1, 1), tuple_at(2, 2), tuple_at(4, 4)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut sums = Vec::new();
        for _ in 0..2 {
            let item = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            let StreamData::Collection(collection) = item else {
                panic!("expected sliding aggregation collection");
            };
            sums.push(extract_sum(collection.as_ref()));
        }

        assert_eq!(sums, vec![1, 3]);
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 2_000);
        assert!(output_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn sliding_aggregation_graceful_end_flushes_all_pending_windows_before_terminal_control()
    {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical_with_lookahead(Some(2));

        let mut processor = StreamingSlidingAggregationProcessor::new(
            "sliding",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("sliding processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(1, 1), tuple_at(2, 2)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let mut sums = Vec::new();
        loop {
            let item = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match item {
                StreamData::Collection(collection) => sums.push(extract_sum(collection.as_ref())),
                StreamData::Control(ControlSignal::Barrier(
                    crate::processor::BarrierControlSignal::StreamGracefulEnd { .. },
                )) => break,
                other => panic!("unexpected output: {}", other.description()),
            }
        }

        assert_eq!(sums, vec![3, 3]);
    }

    #[tokio::test]
    async fn sliding_aggregation_validates_end_only_for_trigger_tuple() {
        let max_timestamp_secs = u64::try_from(i64::MAX).expect("i64 max fits u64") / 1_000_000;
        let lookahead_secs = 10;
        let first_timestamp =
            UNIX_EPOCH + Duration::from_secs(max_timestamp_secs.saturating_sub(lookahead_secs));
        let second_timestamp =
            UNIX_EPOCH + Duration::from_secs(max_timestamp_secs.saturating_sub(1));
        let watermark = UNIX_EPOCH + Duration::from_secs(max_timestamp_secs);

        for (second_value, expected_sum, expected_errors) in [(1, 11, 0), (10, 10, 1)] {
            let spawner = test_spawner();
            let aggregate_registry = AggregateFunctionRegistry::with_builtins();
            let physical = make_physical_with_lookahead_and_trigger(
                Some(lookahead_secs),
                Some(a_greater_than_five()),
                None,
            );
            let mut processor = StreamingSlidingAggregationProcessor::new(
                "sliding",
                Arc::clone(&physical),
                Arc::clone(&aggregate_registry),
            )
            .expect("sliding processor");
            let stats = Arc::new(ProcessorStats::collection_in_out());
            processor.set_stats(Arc::clone(&stats));
            let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
            processor.add_input(input.subscribe());
            let mut output_rx = processor.subscribe_output().unwrap();
            let _handle = processor.start(&spawner);

            let batch = crate::model::RecordBatch::new(vec![
                tuple_with_a_at(first_timestamp, 10),
                tuple_with_a_at(second_timestamp, second_value),
            ])
            .expect("batch");
            assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
            assert!(input.send(StreamData::watermark(watermark)).is_ok());

            let item = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            let StreamData::Collection(collection) = item else {
                panic!("expected streaming sliding aggregation collection");
            };
            assert_eq!(extract_sum(collection.as_ref()), expected_sum);

            let snapshot = stats.snapshot();
            assert_eq!(snapshot.error_count, expected_errors);
            assert_eq!(snapshot.last_error.is_some(), expected_errors > 0);
        }
    }

    #[tokio::test]
    async fn millisecond_sliding_aggregation_uses_exact_boundaries_and_metadata() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical_with_window(TimeUnit::Milliseconds, 100, Some(50), None, None);

        let mut processor = StreamingSlidingAggregationProcessor::new(
            "sliding",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("sliding processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // Every row triggers a logical [t - lookback, t + lookahead] window. The window
        // triggered at 150ms must include the buffered row at 100ms.
        let batch =
            crate::model::RecordBatch::new(vec![tuple_at_millis(100, 1), tuple_at_millis(150, 2)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        // The 100ms-triggered window ends at 150ms and may flush, but the 150ms-triggered
        // window must remain pending until its 200ms deadline.
        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(199),
            ))
            .is_ok());
        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(first) = first else {
            panic!("expected first sliding aggregation collection");
        };
        let first_metadata = first.metadata().window.as_deref().expect("window metadata");
        assert_eq!(first_metadata.start.epoch_micros(), 0);
        assert_eq!(first_metadata.end.epoch_micros(), 150_000);
        assert!(output_rx.try_recv().is_err());

        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(200),
            ))
            .is_ok());
        let item = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(collection) = item else {
            panic!("expected sliding aggregation collection");
        };
        assert_eq!(extract_sum(collection.as_ref()), 3);
        let metadata = collection
            .metadata()
            .window
            .as_deref()
            .expect("window metadata");
        assert_eq!(metadata.start.epoch_micros(), 50_000);
        assert_eq!(metadata.end.epoch_micros(), 200_000);
    }
}
