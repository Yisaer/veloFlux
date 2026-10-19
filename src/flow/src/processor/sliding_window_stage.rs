//! Synchronous sliding-window state and incremental output shared by both placements.
use crate::expr::ScalarExpr;
use crate::planner::physical::{PhysicalSlidingWindow, PipelineStateUsage};
use crate::processor::pipeline_state_runtime::update_row_hit_state;
use crate::processor::processor_state::ProcessorState;
use crate::processor::sliding_window_runtime::evaluate_trigger_condition;
use crate::processor::window_metadata;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};
use crate::processor::{ControlSignal, ProcessorError, ProcessorStats, StreamData};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub(crate) enum SlidingWindowOutput {
    Single(Option<StreamData>),
    Rows(std::vec::IntoIter<crate::model::Tuple>),
    Drain {
        partition: usize,
        watermark: Option<SystemTime>,
        terminal: Option<ControlSignal>,
    },
}

pub(crate) fn sliding_window_output(
    data: StreamData,
    stats: &ProcessorStats,
    context: &'static str,
) -> SlidingWindowOutput {
    match data {
        StreamData::Collection(collection) => match collection.into_rows() {
            Ok(rows) => SlidingWindowOutput::Rows(rows.into_iter()),
            Err(error) => {
                stats.record_error_logged(context, format!("failed to extract rows: {error}"));
                SlidingWindowOutput::Single(None)
            }
        },
        StreamData::Watermark(watermark) => SlidingWindowOutput::Drain {
            partition: 0,
            watermark: Some(watermark),
            terminal: None,
        },
        StreamData::Control(signal) if signal.is_graceful_end() => SlidingWindowOutput::Drain {
            partition: 0,
            watermark: None,
            terminal: Some(signal),
        },
        other => SlidingWindowOutput::Single(Some(other)),
    }
}

struct SlidingTriggerRuntime {
    condition: Option<ScalarExpr>,
    state: Option<Arc<ProcessorState>>,
    state_usage: PipelineStateUsage,
}
struct WindowRequest {
    start: SystemTime,
    end: SystemTime,
}
struct SlidingPartition {
    rows: VecDeque<crate::model::Tuple>,
    pending: VecDeque<WindowRequest>,
}

pub(crate) struct SlidingWindowStage {
    ids: HashMap<PartitionKey, usize>,
    states: Vec<SlidingPartition>,
    lookback: Duration,
    lookahead: Option<Duration>,
    partition_by: Vec<ScalarExpr>,
    trigger: SlidingTriggerRuntime,
}

impl SlidingWindowStage {
    pub(crate) fn from_plan(plan: &PhysicalSlidingWindow) -> Self {
        Self {
            ids: HashMap::new(),
            states: Vec::new(),
            lookback: plan.time_unit.duration(plan.lookback),
            lookahead: plan.lookahead.map(|n| plan.time_unit.duration(n)),
            partition_by: plan.partition_by_scalars.clone(),
            trigger: SlidingTriggerRuntime {
                condition: plan.trigger_condition_scalar.clone(),
                state: plan.trigger_processor_state.clone(),
                state_usage: plan.trigger_state_usage,
            },
        }
    }
    pub(crate) fn clear(&mut self) {
        self.ids.clear();
        self.states.clear();
    }
    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> SlidingWindowOutput {
        if data.is_terminal() && !matches!(&data, StreamData::Control(s) if s.is_graceful_end()) {
            self.clear();
        }
        sliding_window_output(data, stats, "sliding window processor error")
    }
    fn emit_window(
        state: &SlidingPartition,
        request: WindowRequest,
        stats: &ProcessorStats,
    ) -> Option<StreamData> {
        let mut rows = Vec::new();
        for row in &state.rows {
            if row.timestamp < request.start {
                continue;
            }
            if row.timestamp > request.end {
                break;
            }
            rows.push(row.clone());
        }
        match window_metadata::record_batch_from_system_time(rows, request.start, request.end) {
            Ok(batch) => Some(StreamData::collection(Box::new(batch))),
            Err(error) => {
                stats.record_error_logged("sliding window processor error", error.to_string());
                None
            }
        }
    }
    fn trim(state: &mut SlidingPartition, watermark: SystemTime, lookback: Duration) {
        let min_start = state
            .pending
            .front()
            .map(|p| p.start)
            .unwrap_or_else(|| window_metadata::saturating_sub_to_epoch(watermark, lookback));
        while state
            .rows
            .front()
            .is_some_and(|row| row.timestamp < min_start)
        {
            state.rows.pop_front();
        }
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
                while let Some(state) = self.states.get_mut(*partition) {
                    while let Some(request) = state.pending.front() {
                        if watermark.is_some_and(|limit| request.end > limit) {
                            break;
                        }
                        let request = state.pending.pop_front().ok_or_else(|| {
                            ProcessorError::ProcessingError("sliding request disappeared".into())
                        })?;
                        if let Some(data) = Self::emit_window(state, request, stats) {
                            return Ok(Some(data));
                        }
                    }
                    if let Some(watermark) = watermark {
                        Self::trim(state, *watermark, self.lookback);
                    }
                    *partition += 1;
                }
                Ok(terminal.take().map(StreamData::control))
            }
            SlidingWindowOutput::Rows(rows) => {
                for tuple in rows.by_ref() {
                    let key = match eval_partition_key(&self.partition_by, &tuple, "slidingwindow")
                    {
                        Ok(key) => key,
                        Err(error) => {
                            stats.record_error_logged("sliding window processor error", error);
                            continue;
                        }
                    };
                    let should_trigger = match evaluate_trigger_condition(
                        self.trigger.condition.as_ref(),
                        &tuple,
                        "slidingwindow trigger condition",
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            stats.record_error_logged(
                                "sliding window processor error",
                                error.to_string(),
                            );
                            false
                        }
                    };
                    let request = if should_trigger {
                        let end = match self.lookahead {
                            Some(delay) => tuple.timestamp.checked_add(delay),
                            None => Some(tuple.timestamp),
                        };
                        let Some(end) = end else {
                            stats.record_error_logged(
                                "sliding window processor error",
                                "slidingwindow end timestamp overflow",
                            );
                            continue;
                        };
                        if let Err(error) = window_metadata::validate_system_time(end) {
                            stats.record_error_logged(
                                "sliding window processor error",
                                error.to_string(),
                            );
                            continue;
                        }
                        if let Some(state) = self.trigger.state.as_deref() {
                            if let Err(error) = update_row_hit_state(
                                state,
                                self.trigger.state_usage,
                                tuple.timestamp,
                            ) {
                                stats.record_error_logged(
                                    "sliding window processor error",
                                    error.to_string(),
                                );
                                continue;
                            }
                        }
                        Some(WindowRequest {
                            start: window_metadata::saturating_sub_to_epoch(
                                tuple.timestamp,
                                self.lookback,
                            ),
                            end,
                        })
                    } else {
                        None
                    };
                    let next = self.states.len();
                    let index = *self.ids.entry(key).or_insert_with(|| {
                        self.states.push(SlidingPartition {
                            rows: VecDeque::new(),
                            pending: VecDeque::new(),
                        });
                        next
                    });
                    let state = &mut self.states[index];
                    state.rows.push_back(tuple);
                    if let Some(request) = request {
                        if self.lookahead.is_some() {
                            state.pending.push_back(request);
                        } else if let Some(data) = Self::emit_window(state, request, stats) {
                            return Ok(Some(data));
                        }
                    }
                }
                Ok(None)
            }
        }
    }
}
