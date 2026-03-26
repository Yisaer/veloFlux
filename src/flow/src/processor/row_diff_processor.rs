//! RowDiffProcessor - computes sink-side row diffs while preserving stable row schema.

use crate::model::{Collection, RecordBatch};
use crate::planner::physical::PhysicalRowDiff;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure,
    ProcessorChannelCapacities,
};
use crate::processor::output_row_accessor::OutputRowAccessor;
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Default)]
struct RowDiffState {
    previous_full_row: Option<Vec<Arc<Value>>>,
}

pub struct RowDiffProcessor {
    id: String,
    row_accessor: OutputRowAccessor,
    tracked_flags: Arc<[bool]>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl RowDiffProcessor {
    pub fn new(
        id: impl Into<String>,
        physical_row_diff: Arc<PhysicalRowDiff>,
    ) -> Result<Self, ProcessorError> {
        Self::new_with_channel_capacities(id, physical_row_diff, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_row_diff: Arc<PhysicalRowDiff>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Result<Self, ProcessorError> {
        let input_child = physical_row_diff.base.children().first().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "row diff processor requires exactly one input child".to_string(),
            )
        })?;
        let output_schema = input_child.output_schema().map_err(|err| {
            ProcessorError::InvalidConfiguration(format!(
                "row diff processor failed to resolve output schema: {err}"
            ))
        })?;
        let row_accessor = OutputRowAccessor::from_output_schema(&output_schema);
        let tracked_flags = build_tracked_flags(
            row_accessor.width(),
            physical_row_diff.tracked_column_indexes.as_ref(),
        )?;
        let (output, _) = broadcast::channel(channel_capacities.data);
        let (control_output, _) = broadcast::channel(channel_capacities.control);
        Ok(Self {
            id: id.into(),
            row_accessor,
            tracked_flags,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::default()),
        })
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

fn build_tracked_flags(
    column_count: usize,
    tracked_column_indexes: &[usize],
) -> Result<Arc<[bool]>, ProcessorError> {
    let mut flags = vec![false; column_count];
    for &index in tracked_column_indexes {
        if index >= column_count {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "row diff tracked column index {index} out of bounds for output width {column_count}"
            )));
        }
        flags[index] = true;
    }
    Ok(flags.into())
}

fn build_diff_row(
    current_values: &[Arc<Value>],
    previous_full_row: Option<&[Arc<Value>]>,
    tracked_flags: &[bool],
) -> (Vec<Arc<Value>>, Arc<[bool]>) {
    let mut diff_values = Vec::with_capacity(current_values.len());
    let mut output_mask = Vec::with_capacity(current_values.len());

    for (idx, current_value) in current_values.iter().enumerate() {
        if !tracked_flags.get(idx).copied().unwrap_or(false) {
            diff_values.push(Arc::clone(current_value));
            output_mask.push(true);
            continue;
        }

        let changed = previous_full_row
            .and_then(|previous| previous.get(idx))
            .is_none_or(|previous| previous.as_ref() != current_value.as_ref());

        if changed {
            diff_values.push(Arc::clone(current_value));
            output_mask.push(true);
        } else {
            diff_values.push(Arc::new(Value::Null));
            output_mask.push(false);
        }
    }

    (diff_values, output_mask.into())
}

fn apply_row_diff(
    input_collection: Box<dyn Collection>,
    row_accessor: &mut OutputRowAccessor,
    tracked_flags: &[bool],
    state: &mut RowDiffState,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let input_rows = input_collection.into_rows().map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to materialize row diff input: {err}"))
    })?;
    let mut output_rows = Vec::with_capacity(input_rows.len());

    for tuple in input_rows {
        let current_values = row_accessor
            .extract_row(&tuple)?
            .into_required_values("row diff processor")?;
        let (diff_values, output_mask) = build_diff_row(
            current_values.as_slice(),
            state.previous_full_row.as_deref(),
            tracked_flags,
        );
        let output_tuple =
            row_accessor.materialize_tuple(tuple.timestamp, &diff_values, Some(output_mask));
        state.previous_full_row = Some(current_values);
        output_rows.push(output_tuple);
    }

    let output = RecordBatch::new(output_rows).map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to build row diff output: {err}"))
    })?;
    Ok(Box::new(output))
}

impl Processor for RowDiffProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(
        &mut self,
        spawner: &TaskSpawner,
    ) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let mut row_accessor = self.row_accessor.clone();
        let tracked_flags = Arc::clone(&self.tracked_flags);
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, "row diff processor starting");

        spawner.spawn(async move {
            let mut state = RowDiffState::default();
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
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        match apply_row_diff(
                                            collection,
                                            &mut row_accessor,
                                            tracked_flags.as_ref(),
                                            &mut state,
                                        ) {
                                            Ok(out_collection) => {
                                                let out_data = StreamData::collection(out_collection);
                                                let out_rows = out_data.num_rows_hint();
                                                let send_res = send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    out_data,
                                                    Some(stats.as_ref()),
                                                )
                                                .await;
                                                stats.record_handle_duration(handle_start.elapsed());
                                                send_res?;
                                                if let Some(rows) = out_rows {
                                                    stats.record_out(rows);
                                                }
                                            }
                                            Err(err) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                stats.record_error_logged("row diff processor error", err.to_string());
                                            }
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
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
                                log_broadcast_lagged(&id, skipped, "row diff data input");
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
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}
