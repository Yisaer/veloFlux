//! Synchronous incremental aggregation engines.

use crate::aggregation::{
    AccumulatorFactory, AccumulatorState, AccumulatorUpdate, AggregateAccumulator,
    AggregateFunctionRegistry, AggregateUpdate,
};
use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{AggregateCall, PhysicalStreamingAggregation};
use datatypes::Value;
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;

#[path = "streaming_count_aggregation_processor.rs"]
mod streaming_count_aggregation_processor;
#[path = "streaming_eos_aggregation_processor.rs"]
mod streaming_eos_aggregation_processor;
#[path = "streaming_tumbling_aggregation_processor.rs"]
mod streaming_tumbling_aggregation_processor;

pub(crate) use streaming_eos_aggregation_processor::StreamingEosStage;
#[path = "streaming_sliding_aggregation_processor.rs"]
mod streaming_sliding_aggregation_processor;
pub(crate) use streaming_sliding_aggregation_processor::StreamingSlidingStage;
#[path = "streaming_state_aggregation_processor.rs"]
mod streaming_state_aggregation_processor;
pub(crate) use streaming_state_aggregation_processor::StreamingStateStage;

pub(crate) use streaming_count_aggregation_processor::{StreamingCountOutput, StreamingCountStage};
pub(crate) use streaming_tumbling_aggregation_processor::StreamingTumblingStage;

#[cfg(test)]
#[path = "streaming_aggregation_tests.rs"]
mod tests;

/// Group-by metadata bundled for evaluation and output.
#[derive(Clone)]
struct GroupByMeta {
    scalar: ScalarExpr,
    is_simple: bool,
    output_name: String,
}

/// All calls share the same row transaction and group lifecycle.
struct GroupState {
    accumulators: Vec<AccumulatorState>,
    last_tuple: LastRow,
    key_values: Vec<Value>,
}

/// Aggregation output only inherits source messages and the last timestamp.
/// Do not retain discarded affiliate columns or output masks in every group.
struct LastRow {
    messages: Arc<[Arc<crate::model::Message>]>,
    timestamp: std::time::SystemTime,
}

impl LastRow {
    fn capture(tuple: &crate::model::Tuple) -> Self {
        Self {
            messages: Arc::clone(&tuple.messages),
            timestamp: tuple.timestamp,
        }
    }
}

/// Binding is per argument/call, not a whole-statement fast-path decision.
enum BoundArguments {
    Column { source: String, index: usize },
    CountWildcard(Option<String>),
    Expressions(Vec<ScalarExpr>),
}

struct BoundCall {
    arguments: BoundArguments,
    factory: AccumulatorFactory,
}

impl BoundCall {
    fn new(call: &AggregateCall, registry: &AggregateFunctionRegistry) -> Result<Self, String> {
        use crate::expr::scalar::ColumnRef;
        if call.distinct {
            return Err("DISTINCT aggregates are not supported yet".to_string());
        }
        let factory = registry
            .accumulator_factory(&call.func_name)
            .ok_or_else(|| format!("Aggregate function '{}' not found", call.func_name))?;
        let arguments = match call.args.as_slice() {
            [ScalarExpr::Column(ColumnRef::ByIndex {
                source_name,
                column_index,
            })] => BoundArguments::Column {
                source: source_name.clone(),
                index: *column_index,
            },
            [ScalarExpr::Wildcard { source_name }] if factory.uses_non_null_wildcard_sentinel() => {
                BoundArguments::CountWildcard(source_name.clone())
            }
            _ => BoundArguments::Expressions(call.args.clone()),
        };
        Ok(Self { arguments, factory })
    }

    fn prepare(
        &self,
        tuple: &crate::model::Tuple,
        accumulator: &AccumulatorState,
        arguments: &mut Vec<Value>,
    ) -> Result<AccumulatorUpdate, String> {
        use crate::expr::scalar::ColumnRef;
        static COUNT_ROW: [Value; 1] = [Value::Bool(true)];
        let eval_error = |error| format!("Failed to evaluate aggregate argument: {error}");
        let values = match &self.arguments {
            BoundArguments::Column { source, index } => {
                if let Some(value) = tuple.value_by_index(source, *index) {
                    std::slice::from_ref(value)
                } else {
                    // Reuse the evaluator's diagnostic on the cold error path.
                    arguments.clear();
                    arguments.push(
                        ScalarExpr::Column(ColumnRef::ByIndex {
                            source_name: source.clone(),
                            column_index: *index,
                        })
                        .eval_with_tuple(tuple)
                        .map_err(eval_error)?,
                    );
                    arguments.as_slice()
                }
            }
            BoundArguments::CountWildcard(source) => {
                if let Some(source) = source {
                    let has_column = tuple.messages.iter().any(|message| {
                        message.source() == source && message.entries().next().is_some()
                    }) || (source.is_empty()
                        && tuple
                            .affiliate()
                            .is_some_and(|affiliate| affiliate.entries().next().is_some()));
                    if !has_column {
                        arguments.clear();
                        arguments.push(
                            ScalarExpr::Wildcard {
                                source_name: Some(source.clone()),
                            }
                            .eval_with_tuple(tuple)
                            .map_err(eval_error)?,
                        );
                    }
                }
                // Only a registry-bound builtin COUNT uses a non-null sentinel.
                &COUNT_ROW
            }
            BoundArguments::Expressions(expressions) => {
                arguments.clear();
                for expression in expressions {
                    arguments.push(expression.eval_with_tuple(tuple).map_err(eval_error)?);
                }
                arguments.as_slice()
            }
        };
        accumulator.prepare(values)
    }
}

struct BoundAggregation {
    calls: Result<Vec<BoundCall>, String>,
}

#[derive(Default)]
struct RowScratch {
    arguments: Vec<Value>,
    updates: Vec<AccumulatorUpdate>,
}

impl RowScratch {
    fn clear(&mut self) {
        self.arguments.clear();
        self.updates.clear();
    }

    fn apply(
        &mut self,
        calls: &[BoundCall],
        tuple: &crate::model::Tuple,
        accumulators: &mut [AccumulatorState],
    ) -> Result<(), String> {
        debug_assert_eq!(calls.len(), accumulators.len());
        self.clear();
        for (call, accumulator) in calls.iter().zip(accumulators.iter()) {
            match call.prepare(tuple, accumulator, &mut self.arguments) {
                Ok(update) => self.updates.push(update),
                Err(error) => {
                    self.clear();
                    return Err(error);
                }
            }
        }
        self.arguments.clear();
        // No fallible work remains after the first mutation.
        for (accumulator, update) in accumulators.iter_mut().zip(self.updates.drain(..)) {
            accumulator.commit(update);
        }
        Ok(())
    }
}

/// Shared execution for count, tumbling, state and EOS aggregation.
struct AggregationWorker {
    physical: Arc<PhysicalStreamingAggregation>,
    group_by_meta: Arc<[GroupByMeta]>,
    bound: Arc<BoundAggregation>,
    groups: HashMap<Vec<Value>, GroupState>,
    scratch: RowScratch,
}

impl AggregationWorker {
    fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
    ) -> Self {
        let calls = physical
            .aggregate_calls
            .iter()
            .map(|call| BoundCall::new(call, &aggregate_registry))
            .collect();
        Self {
            physical,
            group_by_meta: Arc::from(group_by_meta),
            bound: Arc::new(BoundAggregation { calls }),
            groups: HashMap::new(),
            scratch: RowScratch::default(),
        }
    }

    /// New windows/partitions share bindings, never mutable accumulator state.
    fn empty(&self) -> Self {
        Self {
            physical: Arc::clone(&self.physical),
            group_by_meta: Arc::clone(&self.group_by_meta),
            bound: Arc::clone(&self.bound),
            groups: HashMap::new(),
            scratch: RowScratch::default(),
        }
    }

    fn clear(&mut self) {
        self.groups.clear();
        self.scratch.clear();
    }

    fn update_groups(&mut self, tuple: &crate::model::Tuple) -> Result<(), String> {
        let mut key_values = Vec::with_capacity(self.group_by_meta.len());
        for meta in self.group_by_meta.iter() {
            key_values.push(
                meta.scalar
                    .eval_with_tuple(tuple)
                    .map_err(|error| format!("Failed to evaluate group-by expression: {error}"))?,
            );
        }
        let calls = self.bound.calls.as_ref().map_err(Clone::clone)?;
        if let Some(state) = self.groups.get_mut(&key_values) {
            self.scratch.apply(calls, tuple, &mut state.accumulators)?;
            state.last_tuple = LastRow::capture(tuple);
            state.key_values = key_values;
        } else {
            let mut accumulators = calls
                .iter()
                .map(|call| call.factory.create())
                .collect::<Vec<_>>();
            self.scratch.apply(calls, tuple, &mut accumulators)?;
            self.groups.insert(
                key_values.clone(),
                GroupState {
                    accumulators,
                    last_tuple: LastRow::capture(tuple),
                    key_values,
                },
            );
        }
        Ok(())
    }

    fn finalize_current_window(&mut self) -> Result<Option<Box<dyn Collection>>, String> {
        if self.groups.is_empty() {
            return Ok(None);
        }
        let mut output_tuples = Vec::with_capacity(self.groups.len());
        for (_key, mut state) in self.groups.drain() {
            output_tuples.push(finalize_group(
                &self.physical.aggregate_calls,
                &self.group_by_meta,
                &mut state.accumulators,
                &state.last_tuple,
                &state.key_values,
            )?);
        }
        let collection = RecordBatch::new(output_tuples)
            .map_err(|error| format!("Failed to build RecordBatch: {error}"))?;
        Ok(Some(Box::new(collection)))
    }
}

fn apply_aggregate_updates(
    accumulators: &mut [Box<dyn AggregateAccumulator>],
    row_call_args: &[(usize, Vec<Value>)],
) -> Result<(), String> {
    let mut prepared = Vec::<(usize, Box<dyn AggregateUpdate>)>::with_capacity(row_call_args.len());
    for (idx, args) in row_call_args {
        let update = accumulators
            .get(*idx)
            .ok_or_else(|| "accumulator missing".to_string())?
            .prepare_update(args)?;
        prepared.push((*idx, update));
    }

    for (idx, update) in prepared {
        accumulators
            .get_mut(idx)
            .ok_or_else(|| "accumulator missing".to_string())?
            .commit_update(update);
    }

    Ok(())
}

fn finalize_group(
    aggregate_calls: &[AggregateCall],
    group_by_meta: &[GroupByMeta],
    accumulators: &mut [AccumulatorState],
    last_tuple: &LastRow,
    key_values: &[Value],
) -> Result<crate::model::Tuple, String> {
    use std::sync::Arc;

    let mut affiliate_entries = Vec::new();
    for (call, accumulator) in aggregate_calls.iter().zip(accumulators.iter_mut()) {
        affiliate_entries.push((Arc::new(call.output_column.clone()), accumulator.finalize()));
    }

    for (idx, value) in key_values.iter().enumerate() {
        if let Some(meta) = group_by_meta.get(idx) {
            if !meta.is_simple {
                affiliate_entries.push((Arc::new(meta.output_name.clone()), value.clone()));
            }
        }
    }

    let mut tuple =
        crate::model::Tuple::with_timestamp(last_tuple.messages.clone(), last_tuple.timestamp);
    tuple.add_affiliate_columns(affiliate_entries);
    Ok(tuple)
}

fn create_accumulators_static(
    aggregate_calls: &[AggregateCall],
    registry: &AggregateFunctionRegistry,
) -> Result<Vec<Box<dyn AggregateAccumulator>>, String> {
    let mut accumulators = Vec::new();
    for call in aggregate_calls {
        if call.distinct {
            return Err("DISTINCT aggregates are not supported yet".to_string());
        }
        let function = registry
            .get(&call.func_name)
            .ok_or_else(|| format!("Aggregate function '{}' not found", call.func_name))?;
        accumulators.push(function.create_accumulator());
    }
    Ok(accumulators)
}

fn build_group_by_meta(exprs: &[Expr], scalars: &[ScalarExpr]) -> Vec<GroupByMeta> {
    assert_eq!(
        exprs.len(),
        scalars.len(),
        "group-by exprs and scalars length mismatch"
    );
    exprs
        .iter()
        .zip(scalars.iter())
        .map(|(expr, scalar)| GroupByMeta {
            scalar: scalar.clone(),
            is_simple: is_simple_column_expr(expr),
            output_name: expr.to_string(),
        })
        .collect()
}

fn is_simple_column_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_))
}
