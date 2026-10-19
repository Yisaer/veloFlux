use super::{build_group_by_meta, AggregationWorker};
use crate::aggregation::AggregateFunctionRegistry;
use crate::model::Collection;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::window_metadata;
use crate::processor::{ProcessorError, ProcessorStats, StreamData};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use crate::processor::window_partition::{eval_partition_key, PartitionKey};

/// Tracks progress for a count-based window.
struct CountWindowState {
    target: u64,
    seen: u64,
    opened_at: Option<SystemTime>,
}

impl CountWindowState {
    fn new(target: u64) -> Self {
        Self {
            target,
            seen: 0,
            opened_at: None,
        }
    }

    fn register_row_and_check_finalize(&mut self, now: SystemTime) -> Option<SystemTime> {
        if self.seen == 0 {
            self.opened_at = Some(now);
        }
        self.seen += 1;
        if self.seen >= self.target {
            return Some(self.opened_at.take().unwrap_or(now));
        }
        None
    }

    fn reset(&mut self) {
        self.seen = 0;
        self.opened_at = None;
    }
}

/// Synchronous row-at-a-time aggregation; downstream waits stay in the caller.
pub(crate) struct StreamingCountStage {
    template: AggregationWorker,
    global: CountWindowState,
    partitions: PartitionedCountAggregationState,
    partition_by: Vec<crate::expr::ScalarExpr>,
    target: u64,
}

pub(crate) enum StreamingCountOutput {
    Single(Option<StreamData>),
    Rows {
        collection: Box<dyn Collection>,
        next_row: usize,
    },
}

impl StreamingCountStage {
    pub(crate) fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        registry: Arc<AggregateFunctionRegistry>,
        target: u64,
    ) -> Self {
        let group_by = build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let partition_by = match &physical.window {
            StreamingWindowSpec::Count {
                partition_by_scalars,
                ..
            } => partition_by_scalars.clone(),
            _ => Vec::new(),
        };
        Self {
            template: AggregationWorker::new(physical, registry, group_by),
            global: CountWindowState::new(target),
            partitions: PartitionedCountAggregationState::new(),
            partition_by,
            target,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.template.groups.clear();
        self.global.reset();
        self.partitions.states.clear();
        self.partitions.state_ids.clear();
    }

    pub(crate) fn process(&mut self, data: StreamData) -> StreamingCountOutput {
        if data.is_terminal() {
            self.clear();
        }
        match data {
            StreamData::Collection(collection) => StreamingCountOutput::Rows {
                collection,
                next_row: 0,
            },
            other => StreamingCountOutput::Single(Some(other)),
        }
    }

    pub(crate) fn next_output(
        &mut self,
        output: &mut StreamingCountOutput,
        stats: &ProcessorStats,
    ) -> Result<Option<StreamData>, ProcessorError> {
        let (collection, next_row) = match output {
            StreamingCountOutput::Single(data) => return Ok(data.take()),
            StreamingCountOutput::Rows {
                collection,
                next_row,
            } => (collection, next_row),
        };
        while let Some(row) = collection.rows().get(*next_row) {
            *next_row += 1;
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(ProcessorError::ProcessingError(message)) => {
                    stats.record_error_logged(
                        "streaming count aggregation processor error",
                        message,
                    );
                    continue;
                }
                Err(error) => {
                    return Err(ProcessorError::ProcessingError(format!(
                        "Failed to process streaming count aggregation: {error}",
                    )))
                }
            }
            let (worker, window) = if self.partition_by.is_empty() {
                (&mut self.template, &mut self.global)
            } else {
                let key = match eval_partition_key(&self.partition_by, row, "countwindow") {
                    Ok(key) => key,
                    Err(error) => {
                        stats.record_error_logged(
                            "streaming count aggregation processor error",
                            error,
                        );
                        continue;
                    }
                };
                let state = self
                    .partitions
                    .get_or_insert(key, self.target, &self.template);
                (&mut state.worker, &mut state.window)
            };
            if let Err(error) = worker.update_groups(row) {
                stats.record_error_logged("streaming count aggregation processor error", error);
                continue;
            }
            if let Some(start) = window.register_row_and_check_finalize(row.timestamp) {
                let batch = worker.finalize_current_window().map_err(|error| {
                    ProcessorError::ProcessingError(format!(
                        "Failed to process streaming count aggregation: {error}"
                    ))
                })?;
                window.reset();
                if let Some(batch) = batch {
                    let batch =
                        window_metadata::attach_from_system_time(batch, start, row.timestamp)
                            .map_err(|error| {
                                ProcessorError::ProcessingError(format!(
                                    "Failed to process streaming count aggregation: {error}"
                                ))
                            })?;
                    return Ok(Some(StreamData::collection(batch)));
                }
            }
        }
        Ok(None)
    }
}

struct PartitionCountAggregationState {
    window: CountWindowState,
    worker: AggregationWorker,
}

struct PartitionedCountAggregationState {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<PartitionCountAggregationState>,
}

impl PartitionedCountAggregationState {
    fn new() -> Self {
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
        }
    }

    fn get_or_insert(
        &mut self,
        key: PartitionKey,
        target: u64,
        template: &AggregationWorker,
    ) -> &mut PartitionCountAggregationState {
        if let Some(id) = self.state_ids.get(&key).copied() {
            return &mut self.states[id];
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(PartitionCountAggregationState {
            window: CountWindowState::new(target),
            worker: AggregationWorker::new(
                Arc::clone(&template.physical),
                Arc::clone(&template.aggregate_registry),
                template.group_by_meta.clone(),
            ),
        });
        &mut self.states[id]
    }
}
