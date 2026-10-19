//! Synchronous count buffering shared by BatchProcessor and execution regions.
use crate::expr::ScalarExpr;
use crate::model::Tuple;
use crate::processor::native_batch_stage::NativeBatchStage;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};
use crate::processor::{ControlSignal, ProcessorError, ProcessorStats, StreamData};
use std::collections::{HashMap, VecDeque};

pub(crate) struct CountWindowStage {
    count: usize,
    partition_by: Vec<ScalarExpr>,
    ids: HashMap<PartitionKey, usize>,
    buffers: Vec<VecDeque<Tuple>>,
}

pub(crate) enum CountWindowOutput {
    Single(Option<StreamData>),
    Drain {
        partition: usize,
        all: bool,
        terminal: Option<ControlSignal>,
    },
}

impl CountWindowStage {
    pub(crate) fn new(count: usize, partition_by: Vec<ScalarExpr>) -> Result<Self, ProcessorError> {
        if count == 0 {
            return Err(ProcessorError::InvalidConfiguration(
                "count window requires a positive count".into(),
            ));
        }
        let buffers = if partition_by.is_empty() {
            vec![VecDeque::new()]
        } else {
            Vec::new()
        };
        Ok(Self {
            count,
            partition_by,
            ids: HashMap::new(),
            buffers,
        })
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> Result<CountWindowOutput, ProcessorError> {
        match data {
            StreamData::Collection(collection) => {
                // Preserve partition insertion/output order across the entire input,
                // but construct only one completed batch at each downstream send.
                let rows = collection.into_rows().map_err(|err| {
                    ProcessorError::ProcessingError(format!("failed to extract rows: {err}"))
                })?;
                if self.partition_by.is_empty() {
                    self.buffers[0].extend(rows);
                } else {
                    for row in rows {
                        let key = match eval_partition_key(&self.partition_by, &row, "countwindow")
                        {
                            Ok(key) => key,
                            Err(error) => {
                                stats.record_error_logged("batch processor error", error);
                                continue;
                            }
                        };
                        let next = self.buffers.len();
                        let index = *self.ids.entry(key).or_insert_with(|| {
                            self.buffers.push(VecDeque::new());
                            next
                        });
                        self.buffers[index].push_back(row);
                    }
                }
                Ok(CountWindowOutput::Drain {
                    partition: 0,
                    all: false,
                    terminal: None,
                })
            }
            StreamData::Control(signal) if signal.is_terminal() => Ok(self.finish(Some(signal))),
            other => Ok(CountWindowOutput::Single(Some(other))),
        }
    }

    pub(crate) fn finish(&self, terminal: Option<ControlSignal>) -> CountWindowOutput {
        CountWindowOutput::Drain {
            partition: 0,
            all: true,
            terminal,
        }
    }

    pub(crate) fn next_output(
        &mut self,
        output: &mut CountWindowOutput,
    ) -> Result<Option<StreamData>, ProcessorError> {
        match output {
            CountWindowOutput::Single(data) => Ok(data.take()),
            CountWindowOutput::Drain {
                partition,
                all,
                terminal,
            } => {
                while let Some(buffer) = self.buffers.get_mut(*partition) {
                    let count = if *all { buffer.len() } else { self.count };
                    if count != 0 && buffer.len() >= count {
                        let rows = buffer.drain(..count).collect();
                        return NativeBatchStage::collection(rows)
                            .map(|batch| Some(StreamData::collection(batch)));
                    }
                    *partition += 1;
                }
                Ok(terminal.take().map(StreamData::control))
            }
        }
    }
}
