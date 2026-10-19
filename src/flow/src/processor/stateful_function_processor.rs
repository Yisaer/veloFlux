//! Synchronous stateful-function evaluation.

use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{PartitionGroupKey, PhysicalStatefulFunction, StatefulCall};
use crate::processor::{ProcessorError, ProcessorStats, StreamData};
use crate::stateful::{
    DeferredEvalInput, DeferredProbe, ResolvedStatefulRow, StatefulEvalInput, StatefulEvalOutput,
    StatefulFunction, StatefulFunctionInstance, StatefulFunctionRegistry,
};
use datatypes::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

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

impl StatefulStage {
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
}

/// Stateful evaluation owns only synchronous function and pending-row state.
pub(crate) struct StatefulStage {
    _physical: Arc<PhysicalStatefulFunction>,
    partition_groups: Vec<PartitionGroup>,
    calls: Vec<StatefulProcessorCall>,
    pending_rows: HashMap<u64, BufferedRow>,
    pending_order: VecDeque<u64>,
    next_origin_id: u64,
}

impl StatefulStage {
    pub(crate) fn new(
        physical_stateful: Arc<PhysicalStatefulFunction>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
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
            _physical: physical_stateful,
            partition_groups,
            calls,
            pending_rows: HashMap::new(),
            pending_order: VecDeque::new(),
            next_origin_id: 0,
        })
    }

    pub(crate) fn clear(&mut self) {
        for call in &mut self.calls {
            call.states.clear();
            call.pending.clear();
        }
        self.pending_rows.clear();
        self.pending_order.clear();
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        match data {
            StreamData::Collection(collection) => {
                match StatefulStage::apply_stateful(
                    collection,
                    &self.partition_groups,
                    &mut self.calls,
                    &mut self.pending_rows,
                    &mut self.pending_order,
                    &mut self.next_origin_id,
                    Some(stats),
                ) {
                    Ok(batch) => Ok([
                        (batch.num_rows() != 0).then(|| StreamData::collection(batch)),
                        None,
                    ]),
                    Err(error) => {
                        stats.record_error_logged(
                            "stateful function processor error",
                            error.to_string(),
                        );
                        Ok([None, None])
                    }
                }
            }
            data if data.is_terminal() => {
                let rows = StatefulStage::finish_stateful(
                    &mut self.calls,
                    &mut self.pending_rows,
                    &mut self.pending_order,
                )?;
                let output = if rows.is_empty() {
                    None
                } else {
                    Some(StreamData::collection(Box::new(
                        RecordBatch::new(rows)
                            .map_err(|error| ProcessorError::ProcessingError(error.to_string()))?,
                    )))
                };
                Ok([output, Some(data)])
            }
            other => Ok([Some(other), None]),
        }
    }
}
