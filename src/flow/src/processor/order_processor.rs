//! Synchronous collection sorting by ORDER BY keys.
//!
//! Semantics: sorting is applied within each incoming Collection (no global ordering across batches).

use crate::expr::value_compare;
use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch, Tuple};
use crate::planner::physical::PhysicalOrder;
use crate::processor::{ProcessorError, ProcessorStats};
use datatypes::Value;
use std::cmp::Ordering;

impl OrderStage {
    fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
        value_compare::compare_values(left, right)
    }
    fn compare_key_values(
        idx: usize,
        key: &OrderKey,
        left: &Value,
        right: &Value,
    ) -> Result<Ordering, String> {
        // Enforce NULLS LAST, regardless of ASC/DESC.
        if left.is_null() && right.is_null() {
            return Ok(Ordering::Equal);
        }
        if left.is_null() {
            return Ok(Ordering::Greater);
        }
        if right.is_null() {
            return Ok(Ordering::Less);
        }

        let ord = OrderStage::compare_values(left, right).ok_or_else(|| {
            format!(
                "ORDER BY key #{} is not comparable (left={:?}, right={:?})",
                idx, left, right
            )
        })?;

        Ok(if key.asc { ord } else { ord.reverse() })
    }
}

/// Task-facing ORDER BY key: the compiled expression plus direction.
/// The original SQL text and plan topology stay in the plan node.
#[derive(Clone, Debug)]
pub(crate) struct OrderKey {
    pub compiled: ScalarExpr,
    pub asc: bool,
}

/// Task-facing ORDER BY configuration, converted once from the plan node.
#[derive(Clone, Debug, Default)]
pub(crate) struct OrderStage {
    pub keys: Vec<OrderKey>,
}

impl OrderStage {
    pub(crate) fn from_plan(order: &PhysicalOrder) -> Self {
        Self {
            keys: order
                .keys
                .iter()
                .map(|key| OrderKey {
                    compiled: key.compiled_expr.clone(),
                    asc: key.asc,
                })
                .collect(),
        }
    }
}

pub(crate) fn apply_order(
    collection: Box<dyn Collection>,
    order: &OrderStage,
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let metadata = collection.metadata().clone();
    let rows = collection.into_rows().map_err(|e| {
        ProcessorError::ProcessingError(format!("Failed to materialize rows: {}", e))
    })?;

    if rows.is_empty() || order.keys.is_empty() {
        return Ok(Box::new(
            RecordBatch::new_with_metadata(rows, metadata).map_err(|e| {
                ProcessorError::ProcessingError(format!("Failed to build record batch: {}", e))
            })?,
        ));
    }

    let mut sortable_rows: Vec<SortableRow> = Vec::with_capacity(rows.len());
    let mut key_representatives: Vec<Option<Value>> = vec![None; order.keys.len()];
    'rows: for row in rows {
        let mut key_values = Vec::with_capacity(order.keys.len());
        for (idx, key) in order.keys.iter().enumerate() {
            let value = match key.compiled.eval_with_tuple(&row) {
                Ok(value) => value,
                Err(error) => {
                    if let Some(stats) = stats {
                        stats.record_error_logged(
                            "order processor error",
                            format!("Failed to evaluate ORDER BY key #{}: {}", idx, error),
                        );
                    }
                    continue 'rows;
                }
            };
            key_values.push(value);
        }

        for (idx, (key, value)) in order.keys.iter().zip(key_values.iter()).enumerate() {
            if value.is_null() {
                continue;
            }
            if OrderStage::compare_values(value, value).is_none() {
                if let Some(stats) = stats {
                    stats.record_error_logged(
                        "order processor error",
                        format!(
                            "ORDER BY key #{} is not comparable (value={:?})",
                            idx, value
                        ),
                    );
                }
                continue 'rows;
            }
            if let Some(representative) = &key_representatives[idx] {
                if OrderStage::compare_key_values(idx, key, value, representative).is_ok() {
                    continue;
                }
                if let Some(stats) = stats {
                    stats.record_error_logged(
                        "order processor error",
                        format!(
                            "ORDER BY key #{} is not comparable (left={:?}, right={:?})",
                            idx, value, representative
                        ),
                    );
                }
                continue 'rows;
            }
            key_representatives[idx] = Some(value.clone());
        }

        sortable_rows.push(SortableRow { row, key_values });
    }

    sortable_rows.sort_unstable_by(|left, right| {
        for (idx, (key, (left_value, right_value))) in order
            .keys
            .iter()
            .zip(left.key_values.iter().zip(right.key_values.iter()))
            .enumerate()
        {
            let ord = OrderStage::compare_key_values(idx, key, left_value, right_value)
                .unwrap_or(Ordering::Equal);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    let rows = sortable_rows.into_iter().map(|row| row.row).collect();

    let batch = RecordBatch::new_with_metadata(rows, metadata)
        .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
    Ok(Box::new(batch))
}

struct SortableRow {
    row: Tuple,
    key_values: Vec<Value>,
}
