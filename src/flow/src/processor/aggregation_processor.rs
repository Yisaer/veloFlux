//! Synchronous grouped aggregation evaluation.

use crate::aggregation::{AggregateAccumulator, AggregateFunctionRegistry, AggregateUpdate};
use crate::model::Collection;
use crate::planner::physical::PhysicalAggregation;
use crate::processor::ProcessorStats;
use datatypes::Value;
use sqlparser::ast::Expr;
use std::sync::Arc;

/// Group-by metadata bundled for evaluation and output decoration.
struct GroupByMeta {
    is_simple: bool,
    output_name: String,
}

pub(crate) struct AggregationStage;

impl AggregationStage {
    pub(crate) fn process_batch_with_grouping(
        physical_aggregation: &PhysicalAggregation,
        aggregate_registry: &Arc<AggregateFunctionRegistry>,
        collection: &dyn Collection,
        stats: &ProcessorStats,
    ) -> Result<Option<Box<dyn Collection>>, String> {
        use crate::model::RecordBatch;
        use std::collections::hash_map::Entry;
        use std::collections::HashMap;

        let num_rows = collection.num_rows();
        let group_by_meta = build_group_by_meta(&physical_aggregation.group_by_exprs);

        // Group states keyed by evaluated group-by values.
        struct GroupState {
            accumulators: Vec<Box<dyn AggregateAccumulator>>,
            last_row_idx: usize,
            key_values: Vec<Value>,
        }

        let rows = collection.rows();
        let mut groups: HashMap<String, GroupState> = HashMap::new();

        for (row_idx, row) in rows.iter().enumerate() {
            let key_values = match Self::evaluate_group_by_for_tuple(physical_aggregation, row) {
                Ok(values) => values,
                Err(message) => {
                    stats.record_error_logged("aggregation processor error", message);
                    continue;
                }
            };
            let row_call_args =
                match Self::evaluate_aggregate_args_for_tuple(physical_aggregation, row) {
                    Ok(args) => args,
                    Err(message) => {
                        stats.record_error_logged("aggregation processor error", message);
                        continue;
                    }
                };

            let key_repr = format!("{:?}", key_values);
            match groups.entry(key_repr) {
                Entry::Occupied(mut o) => {
                    let entry = o.get_mut();
                    if let Err(message) =
                        apply_aggregate_updates(&mut entry.accumulators, &row_call_args)
                    {
                        stats.record_error_logged("aggregation processor error", message);
                        continue;
                    }
                    entry.last_row_idx = row_idx;
                    entry.key_values = key_values;
                }
                Entry::Vacant(v) => {
                    let accumulators =
                        Self::create_accumulators_static(physical_aggregation, aggregate_registry)?;
                    let mut state = GroupState {
                        accumulators,
                        last_row_idx: row_idx,
                        key_values: key_values.clone(),
                    };
                    if let Err(message) =
                        apply_aggregate_updates(&mut state.accumulators, &row_call_args)
                    {
                        stats.record_error_logged("aggregation processor error", message);
                        continue;
                    }
                    v.insert(state);
                }
            }
        }

        if num_rows == 0 {
            // No rows: fall back to single-group finalize (existing behavior).
            let mut accumulators =
                Self::create_accumulators_static(physical_aggregation, aggregate_registry)?;
            // No updates needed as there are no rows.
            let tuple = Self::finalize_group(
                physical_aggregation,
                &mut accumulators,
                None,
                rows,
                &[],
                &group_by_meta,
            )?;
            let collection = RecordBatch::new_with_metadata_from(vec![tuple], collection)
                .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;
            return Ok(Some(Box::new(collection)));
        }

        if groups.is_empty() {
            return Ok(None);
        }

        // Finalize each group into output tuples.
        let mut output_tuples = Vec::with_capacity(groups.len());
        for (_key, mut state) in groups.into_iter() {
            let tuple = Self::finalize_group(
                physical_aggregation,
                &mut state.accumulators,
                Some(state.last_row_idx),
                rows,
                &state.key_values,
                &group_by_meta,
            )?;
            output_tuples.push(tuple);
        }

        let collection = RecordBatch::new_with_metadata_from(output_tuples, collection)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;
        Ok(Some(Box::new(collection)))
    }
}

impl AggregationStage {
    fn create_accumulators_static(
        physical_aggregation: &PhysicalAggregation,
        aggregate_registry: &Arc<AggregateFunctionRegistry>,
    ) -> Result<Vec<Box<dyn AggregateAccumulator>>, String> {
        let mut accumulators = Vec::new();

        for call in &physical_aggregation.aggregate_calls {
            if call.distinct {
                return Err("DISTINCT aggregates are not supported yet".to_string());
            }

            let function = aggregate_registry.get(&call.func_name).ok_or_else(|| {
                format!(
                    "Aggregate function '{}' not found in registry",
                    call.func_name
                )
            })?;

            let accumulator = function.create_accumulator();
            accumulators.push(accumulator);
        }

        Ok(accumulators)
    }
    fn evaluate_aggregate_args_for_tuple(
        physical_aggregation: &PhysicalAggregation,
        tuple: &crate::model::Tuple,
    ) -> Result<Vec<(usize, Vec<Value>)>, String> {
        let mut all_args = Vec::with_capacity(physical_aggregation.aggregate_calls.len());
        for (idx, call) in physical_aggregation.aggregate_calls.iter().enumerate() {
            let mut args = Vec::with_capacity(call.args.len());
            for arg_expr in &call.args {
                args.push(
                    arg_expr
                        .eval_with_tuple(tuple)
                        .map_err(|e| format!("Failed to evaluate aggregate argument: {}", e))?,
                );
            }
            all_args.push((idx, args));
        }
        Ok(all_args)
    }
    fn finalize_group(
        physical_aggregation: &PhysicalAggregation,
        accumulators: &mut [Box<dyn AggregateAccumulator>],
        last_row_idx: Option<usize>,
        all_rows: &[crate::model::Tuple],
        key_values: &[Value],
        group_by_meta: &[GroupByMeta],
    ) -> Result<crate::model::Tuple, String> {
        use std::sync::Arc;

        // Finalize aggregate values.
        let mut affiliate_entries = Vec::new();
        for (call, accumulator) in physical_aggregation
            .aggregate_calls
            .iter()
            .zip(accumulators.iter_mut())
        {
            affiliate_entries.push((Arc::new(call.output_column.clone()), accumulator.finalize()));
        }

        // Add computed group-by keys (non-simple column refs) to affiliate so downstream can access them.
        for (idx, value) in key_values.iter().enumerate() {
            if let Some(meta) = group_by_meta.get(idx) {
                if !meta.is_simple {
                    affiliate_entries.push((Arc::new(meta.output_name.clone()), value.clone()));
                }
            }
        }

        let mut tuple = match last_row_idx {
            Some(idx) => {
                let anchor = all_rows
                    .get(idx)
                    .ok_or_else(|| format!("row index {} out of bounds", idx))?;
                crate::model::Tuple::with_timestamp(anchor.messages.clone(), anchor.timestamp)
            }
            None => crate::model::Tuple::new(vec![]),
        };
        tuple.add_affiliate_columns(affiliate_entries);
        Ok(tuple)
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

fn is_simple_column_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_))
}

fn build_group_by_meta(exprs: &[Expr]) -> Vec<GroupByMeta> {
    exprs
        .iter()
        .map(|expr| GroupByMeta {
            is_simple: is_simple_column_expr(expr),
            output_name: expr.to_string(),
        })
        .collect()
}

impl AggregationStage {
    fn evaluate_group_by_for_tuple(
        physical_aggregation: &PhysicalAggregation,
        tuple: &crate::model::Tuple,
    ) -> Result<Vec<Value>, String> {
        let mut values = Vec::with_capacity(physical_aggregation.group_by_scalars.len());
        for scalar in &physical_aggregation.group_by_scalars {
            values.push(
                scalar
                    .eval_with_tuple(tuple)
                    .map_err(|e| format!("Failed to evaluate group-by expression: {}", e))?,
            );
        }
        Ok(values)
    }
}
