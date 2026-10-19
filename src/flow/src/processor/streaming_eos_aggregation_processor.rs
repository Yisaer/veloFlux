use super::{build_group_by_meta, AggregationWorker};
use crate::aggregation::AggregateFunctionRegistry;
use crate::model::Collection;
use crate::planner::physical::PhysicalStreamingAggregation;
use crate::processor::window_metadata;
use crate::processor::{ProcessorError, ProcessorStats, StreamData};
use std::sync::Arc;
use std::time::SystemTime;

impl StreamingEosStage {
    fn process_collection(
        worker: &mut AggregationWorker,
        collection: &dyn Collection,
        opened_at: &mut Option<SystemTime>,
        last_seen_at: &mut Option<SystemTime>,
        stats: &ProcessorStats,
    ) -> Result<(), String> {
        for row in collection.rows() {
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(ProcessorError::ProcessingError(message)) => {
                    stats.record_error_logged("streaming EOS aggregation processor error", message);
                    continue;
                }
                Err(err) => return Err(err.to_string()),
            }
            if let Err(message) = worker.update_groups(row) {
                stats.record_error_logged("streaming EOS aggregation processor error", message);
                continue;
            }
            if opened_at.is_none() {
                *opened_at = Some(row.timestamp);
            }
            *last_seen_at = Some(row.timestamp);
        }
        Ok(())
    }
}

/// Synchronous EOS aggregation shared by standalone and region placement.
pub(crate) struct StreamingEosStage {
    worker: AggregationWorker,
    opened_at: Option<SystemTime>,
    last_seen_at: Option<SystemTime>,
}

impl StreamingEosStage {
    pub(crate) fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let group_by = build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        Self {
            worker: AggregationWorker::new(physical, registry, group_by),
            opened_at: None,
            last_seen_at: None,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.worker.groups.clear();
        self.opened_at = None;
        self.last_seen_at = None;
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        match data {
            StreamData::Collection(collection) => {
                StreamingEosStage::process_collection(
                    &mut self.worker,
                    collection.as_ref(),
                    &mut self.opened_at,
                    &mut self.last_seen_at,
                    stats,
                )
                .map_err(|error| {
                    ProcessorError::ProcessingError(format!(
                        "Failed to process streaming EOS aggregation: {error}"
                    ))
                })?;
                Ok([None, None])
            }
            StreamData::Control(signal) if signal.is_terminal() => {
                let batch = if signal.is_graceful_end() {
                    self.worker.finalize_current_window().map_err(|error| {
                        ProcessorError::ProcessingError(format!(
                            "Failed to finalize EOS aggregation: {error}"
                        ))
                    })?
                } else {
                    None
                };
                let closed_at = self
                    .last_seen_at
                    .or(self.opened_at)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let output = batch
                    .map(|batch| {
                        window_metadata::attach_from_system_time(
                            batch,
                            self.opened_at.unwrap_or(closed_at),
                            closed_at,
                        )
                    })
                    .transpose()?
                    .map(StreamData::collection);
                self.clear();
                Ok([output, Some(StreamData::control(signal))])
            }
            other => Ok([Some(other), None]),
        }
    }
}
