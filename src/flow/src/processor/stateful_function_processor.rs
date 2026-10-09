//! StatefulFunctionProcessor - evaluates stateful scalar functions per row.

use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{
    PartitionGroupKey, PhysicalPlan, PhysicalStatefulFunction, StatefulCall,
};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use crate::stateful::{
    DeferredEvalInput, DeferredProbe, ResolvedStatefulRow, StatefulEvalInput, StatefulEvalOutput,
    StatefulFunction, StatefulFunctionInstance, StatefulFunctionRegistry,
};
use datatypes::Value;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

struct StatefulPartitionState {
    instance: Box<dyn StatefulFunctionInstance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PartitionKey {
    Global,
    Values(Vec<Value>),
}

struct StatefulProcessorCall {
    output_column: Arc<String>,
    function: Arc<dyn StatefulFunction>,
    arg_scalars: Vec<ScalarExpr>,
    when_scalar: Option<ScalarExpr>,
    until_scalar: Option<ScalarExpr>,
    partition_group_id: usize,
    states: HashMap<PartitionKey, StatefulPartitionState>,
    pending: HashMap<PartitionKey, VecDeque<PendingOrigin>>,
}

#[derive(Clone)]
struct PendingOrigin {
    origin_id: u64,
}

struct BufferedRow {
    tuple: crate::model::Tuple,
    unresolved: usize,
}

fn resolve_row(
    pending_rows: &mut HashMap<u64, BufferedRow>,
    _call_index: usize,
    output_column: &Arc<String>,
    resolution: ResolvedStatefulRow,
) -> Result<(), ProcessorError> {
    let row = pending_rows.get_mut(&resolution.origin_id).ok_or_else(|| {
        ProcessorError::ProcessingError(format!(
            "stateful function resolved unknown origin {}",
            resolution.origin_id
        ))
    })?;
    row.tuple
        .add_affiliate_column(Arc::clone(output_column), resolution.value);
    row.unresolved = row.unresolved.saturating_sub(1);
    Ok(())
}

struct PartitionGroup {
    scalars: Vec<ScalarExpr>,
}

pub struct StatefulFunctionProcessor {
    id: String,
    physical_stateful: Arc<PhysicalStatefulFunction>,
    partition_groups: Vec<PartitionGroup>,
    calls: Vec<StatefulProcessorCall>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl StatefulFunctionProcessor {
    pub fn new(
        id: impl Into<String>,
        physical_stateful: Arc<PhysicalStatefulFunction>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
        Self::new_with_channel_capacities(
            id,
            physical_stateful,
            stateful_registry,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_stateful: Arc<PhysicalStatefulFunction>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Result<Self, ProcessorError> {
        let id = id.into();
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );

        let mut partition_group_ids: HashMap<PartitionGroupKey, usize> = HashMap::new();
        let mut partition_groups = vec![PartitionGroup {
            scalars: Vec::new(),
        }];
        let mut calls = Vec::with_capacity(physical_stateful.calls.len());
        for StatefulCall {
            output_column,
            func_name,
            arg_scalars,
            when_scalar,
            until_scalar,
            partition_group_key,
            partition_by_scalars,
            ..
        } in &physical_stateful.calls
        {
            let function = stateful_registry.get(func_name).ok_or_else(|| {
                ProcessorError::InvalidConfiguration(format!(
                    "unknown stateful function '{}'",
                    func_name
                ))
            })?;
            let partition_group_id = if partition_by_scalars.is_empty() {
                0
            } else if let Some(existing) = partition_group_ids.get(partition_group_key) {
                *existing
            } else {
                let id = partition_groups.len();
                partition_groups.push(PartitionGroup {
                    scalars: partition_by_scalars.clone(),
                });
                partition_group_ids.insert(partition_group_key.clone(), id);
                id
            };
            calls.push(StatefulProcessorCall {
                output_column: Arc::new(output_column.clone()),
                function,
                arg_scalars: arg_scalars.clone(),
                when_scalar: when_scalar.clone(),
                until_scalar: until_scalar.clone(),
                partition_group_id,
                states: HashMap::new(),
                pending: HashMap::new(),
            });
        }
        Ok(Self {
            id,
            physical_stateful,
            partition_groups,
            calls,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        })
    }

    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
    ) -> Result<Option<Self>, ProcessorError> {
        match plan.as_ref() {
            PhysicalPlan::StatefulFunction(stateful) => Ok(Some(Self::new(
                id,
                Arc::new(stateful.clone()),
                stateful_registry,
            )?)),
            _ => Ok(None),
        }
    }

    fn apply_stateful(
        collection: Box<dyn Collection>,
        partition_groups: &[PartitionGroup],
        calls: &mut [StatefulProcessorCall],
        pending_rows: &mut HashMap<u64, BufferedRow>,
        pending_order: &mut VecDeque<u64>,
        next_origin_id: &mut u64,
        stats: Option<&ProcessorStats>,
    ) -> Result<Box<dyn Collection>, ProcessorError> {
        let metadata = collection.metadata().clone();
        let rows = collection.into_rows().map_err(|e| {
            ProcessorError::ProcessingError(format!("Failed to materialize rows: {}", e))
        })?;
        let mut out_rows = Vec::with_capacity(rows.len());

        'rows: for mut tuple in rows {
            let origin_id = *next_origin_id;
            *next_origin_id = next_origin_id.saturating_add(1);
            let mut partition_keys = Vec::with_capacity(partition_groups.len());
            for group in partition_groups {
                let key = if group.scalars.is_empty() {
                    PartitionKey::Global
                } else {
                    let mut key_values = Vec::with_capacity(group.scalars.len());
                    for scalar in &group.scalars {
                        let value = match scalar.eval_with_tuple(&tuple) {
                            Ok(value) => value,
                            Err(error) => {
                                if let Some(stats) = stats {
                                    stats.record_error_logged(
                                        "stateful function processor error",
                                        format!(
                                            "failed to evaluate stateful function partition key: {error}"
                                        ),
                                    );
                                }
                                continue 'rows;
                            }
                        };
                        key_values.push(value);
                    }
                    PartitionKey::Values(key_values)
                };
                partition_keys.push(key);
            }

            let mut prepared_updates = Vec::with_capacity(calls.len());
            let mut deferred_origins = Vec::new();
            for (idx, call) in calls.iter_mut().enumerate() {
                let partition_key = &partition_keys[call.partition_group_id];
                let should_apply = match call.when_scalar.as_ref() {
                    Some(scalar) => match scalar.eval_with_tuple(&tuple) {
                        Ok(Value::Bool(v)) => v,
                        Ok(Value::Null) => false,
                        Ok(other) => {
                            if let Some(stats) = stats {
                                stats.record_error_logged(
                                    "stateful function processor error",
                                    format!("stateful function filter must be bool, got {other:?}"),
                                );
                            }
                            continue 'rows;
                        }
                        Err(error) => {
                            if let Some(stats) = stats {
                                stats.record_error_logged(
                                    "stateful function processor error",
                                    format!("failed to evaluate stateful function filter: {error}"),
                                );
                            }
                            continue 'rows;
                        }
                    },
                    None => true,
                };

                let mut args = Vec::with_capacity(call.arg_scalars.len());
                for scalar in &call.arg_scalars {
                    let value = match scalar.eval_with_tuple(&tuple) {
                        Ok(value) => value,
                        Err(error) => {
                            if let Some(stats) = stats {
                                stats.record_error_logged(
                                    "stateful function processor error",
                                    error.to_string(),
                                );
                            }
                            continue 'rows;
                        }
                    };
                    args.push(value);
                }

                let state = call.states.entry(partition_key.clone()).or_insert_with(|| {
                    StatefulPartitionState {
                        instance: call.function.create_instance(),
                    }
                });
                let prepared_result = if call.function.supports_deferred_eval() {
                    let pending = call.pending.get(partition_key);
                    let mut probes = Vec::with_capacity(pending.map_or(0, VecDeque::len));
                    for origin in pending.into_iter().flatten() {
                        let origin_tuple = match pending_rows.get(&origin.origin_id) {
                            Some(row) => &row.tuple,
                            None => {
                                if let Some(stats) = stats {
                                    stats.record_error_logged(
                                        "stateful function processor error",
                                        format!("missing pending origin row {}", origin.origin_id),
                                    );
                                }
                                continue 'rows;
                            }
                        };
                        let until = match call.until_scalar.as_mut() {
                            Some(scalar) => {
                                if let Err(error) = scalar.bind_origin_row(origin_tuple) {
                                    if let Some(stats) = stats {
                                        stats.record_error_logged(
                                            "stateful function processor error",
                                            format!(
                                                "failed to bind stateful function UNTIL origin row: {error}"
                                            ),
                                        );
                                    }
                                    continue 'rows;
                                }
                                match scalar.eval_with_tuple(&tuple) {
                                    Ok(Value::Bool(value)) => value,
                                    Ok(Value::Null) => false,
                                    Ok(other) => {
                                        if let Some(stats) = stats {
                                            stats.record_error_logged(
                                                "stateful function processor error",
                                                format!(
                                                    "stateful function UNTIL must be bool, got {other:?}"
                                                ),
                                            );
                                        }
                                        continue 'rows;
                                    }
                                    Err(error) => {
                                        if let Some(stats) = stats {
                                            stats.record_error_logged(
                                                "stateful function processor error",
                                                format!(
                                                    "failed to evaluate stateful function UNTIL: {error}"
                                                ),
                                            );
                                        }
                                        continue 'rows;
                                    }
                                }
                            }
                            None => false,
                        };
                        probes.push(DeferredProbe {
                            origin_id: origin.origin_id,
                            args: &args,
                            until,
                            should_apply,
                        });
                    }
                    deferred_origins.push((idx, partition_key.clone(), origin_id));
                    state.instance.prepare_deferred_eval(DeferredEvalInput {
                        origin_id,
                        origin_args: &args,
                        probes: &probes,
                    })
                } else {
                    state.instance.prepare_eval(StatefulEvalInput {
                        args: &args,
                        should_apply,
                    })
                };
                let prepared = match prepared_result {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        if let Some(stats) = stats {
                            stats.record_error_logged(
                                "stateful function processor error",
                                format!("failed to evaluate stateful function: {error}"),
                            );
                        }
                        continue 'rows;
                    }
                };
                let output = match prepared.output {
                    StatefulEvalOutput::Immediate(value) => {
                        tuple.add_affiliate_column(Arc::clone(&call.output_column), value);
                        (None, Vec::new())
                    }
                    StatefulEvalOutput::Deferred(resolved) => (None, resolved),
                };
                prepared_updates.push((
                    idx,
                    partition_key.clone(),
                    output.0,
                    output.1,
                    prepared.update,
                ));
            }

            for (idx, partition_key, output, resolved, update) in prepared_updates {
                let call = &mut calls[idx];
                let state = call.states.entry(partition_key.clone()).or_insert_with(|| {
                    StatefulPartitionState {
                        instance: call.function.create_instance(),
                    }
                });
                state.instance.commit_eval(update);
                if let Some(output) = output {
                    tuple.add_affiliate_column(Arc::clone(&call.output_column), output);
                }
                let resolved_ids: HashSet<u64> = resolved.iter().map(|row| row.origin_id).collect();
                for resolution in resolved {
                    resolve_row(pending_rows, idx, &call.output_column, resolution)?;
                }
                if call.function.supports_deferred_eval() && !resolved_ids.is_empty() {
                    call.pending
                        .entry(partition_key.clone())
                        .or_default()
                        .retain(|origin| !resolved_ids.contains(&origin.origin_id));
                }
            }
            for (idx, partition_key, id) in deferred_origins {
                calls[idx]
                    .pending
                    .entry(partition_key)
                    .or_default()
                    .push_back(PendingOrigin { origin_id: id });
            }

            let deferred_count = calls
                .iter()
                .filter(|call| call.function.supports_deferred_eval())
                .count();
            if deferred_count == 0 {
                out_rows.push(tuple);
            } else {
                pending_order.push_back(origin_id);
                pending_rows.insert(
                    origin_id,
                    BufferedRow {
                        tuple,
                        unresolved: deferred_count,
                    },
                );
            }
        }

        while let Some(origin_id) = pending_order.front().copied() {
            let Some(row) = pending_rows.get(&origin_id) else {
                pending_order.pop_front();
                continue;
            };
            if row.unresolved != 0 {
                break;
            }
            let row = pending_rows
                .remove(&origin_id)
                .expect("buffered row disappeared");
            pending_order.pop_front();
            out_rows.push(row.tuple);
        }

        let batch = RecordBatch::new_with_metadata(out_rows, metadata)
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        Ok(Box::new(batch))
    }

    fn finish_stateful(
        calls: &mut [StatefulProcessorCall],
        pending_rows: &mut HashMap<u64, BufferedRow>,
        pending_order: &mut VecDeque<u64>,
    ) -> Result<Vec<crate::model::Tuple>, ProcessorError> {
        for (idx, call) in calls.iter_mut().enumerate() {
            let output_column = Arc::clone(&call.output_column);
            for state in call.states.values_mut() {
                for resolution in state.instance.finish() {
                    resolve_row(pending_rows, idx, &output_column, resolution)?;
                }
            }
        }

        let mut rows = Vec::with_capacity(pending_order.len());
        while let Some(origin_id) = pending_order.pop_front() {
            if let Some(row) = pending_rows.remove(&origin_id) {
                rows.push(row.tuple);
            }
        }
        Ok(rows)
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

impl Processor for StatefulFunctionProcessor {
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
        let partition_groups = std::mem::take(&mut self.partition_groups);
        let mut calls = std::mem::take(&mut self.calls);
        let mut pending_rows = HashMap::new();
        let mut pending_order = VecDeque::new();
        let mut next_origin_id = 0;
        let _physical_stateful = Arc::clone(&self.physical_stateful);
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "stateful function processor starting");
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
                                tracing::info!(processor_id = %id, "received StreamEnd (control)");
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
                                log_received_data(&id, &data);
                                if let Some(rows) = data.row_count() {
                                    stats.record_collection_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        match Self::apply_stateful(
                                            collection,
                                            &partition_groups,
                                            &mut calls,
                                            &mut pending_rows,
                                            &mut pending_order,
                                            &mut next_origin_id,
                                            Some(stats.as_ref()),
                                        ) {
                                            Ok(out_collection) => {
                                                let out = StreamData::collection(out_collection);
                                                let out_rows = out.row_count();
                                                let send_res = if out_rows != Some(0) {
                                                    send_with_backpressure(
                                                        &output,
                                                        channel_capacities.data,
                                                        out,
                                                        Some(stats.as_ref()),
                                                    )
                                                    .await
                                                } else {
                                                    Ok(())
                                                };
                                                // For synchronous processors, handle duration includes downstream send/backpressure time.
                                                stats.record_handle_duration(handle_start.elapsed());
                                                send_res?;
                                                if let Some(rows) = out_rows {
                                                    stats.record_collection_out(rows);
                                                }
                                            }
                                            Err(e) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                stats.record_error_logged("stateful function processor error", e.to_string());
                                            }
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        if is_terminal {
                                            let rows = Self::finish_stateful(
                                                &mut calls,
                                                &mut pending_rows,
                                                &mut pending_order,
                                            )?;
                                            if !rows.is_empty() {
                                                let batch = RecordBatch::new(rows)
                                                    .map_err(|error| ProcessorError::ProcessingError(error.to_string()))?;
                                                send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    StreamData::collection(Box::new(batch)),
                                                    Some(stats.as_ref()),
                                                )
                                                .await?;
                                            }
                                        }
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(
                                    &id,
                                    skipped,
                                    "stateful function data input",
                                );
                                continue;
                            }
                            None => {
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
