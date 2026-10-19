//! Synchronous fixed-grid batching. The driver owns all timer and output waits.
use super::batch_policy::BatchPolicy;
use super::native_batch_stage::NativeBatchStage;
use super::stage_clock::ClockDeadline;
use super::{ProcessorError, StreamData};
use crate::model::{Collection, Tuple};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::Instant;

pub(crate) struct TimedBatchStage {
    policy: BatchPolicy,
    origin: Option<Instant>,
    deadline: Option<Instant>,
    generation: u64,
    rows: VecDeque<Tuple>,
}

pub(crate) enum TimedBatchOutput {
    Feed {
        collection: Box<dyn Collection>,
        offset: usize,
        entered: bool,
    },
    Forward(Option<StreamData>),
    Drain(Option<StreamData>),
}

impl TimedBatchStage {
    pub(crate) fn new(count: Option<usize>, duration: Duration) -> Result<Self, ProcessorError> {
        BatchPolicy::validate(count, Some(duration), "timed batch stage")?;
        Ok(Self {
            policy: BatchPolicy::new(count, Some(duration)),
            origin: None,
            deadline: None,
            generation: 0,
            rows: VecDeque::new(),
        })
    }

    pub(crate) fn start(&mut self, now: Instant) {
        self.origin = Some(now);
    }

    pub(crate) fn deadline(&self) -> Option<ClockDeadline> {
        self.deadline.map(|at| ClockDeadline {
            generation: self.generation,
            at,
        })
    }

    fn clear_deadline(&mut self) {
        if self.deadline.take().is_some() {
            self.generation = self.generation.wrapping_add(1);
        }
    }

    fn schedule(&mut self) {
        if self.rows.is_empty() {
            self.clear_deadline();
        } else if self.deadline.is_none() {
            if let (Some(origin), Some(duration)) = (self.origin, self.policy.duration()) {
                self.deadline = Some(BatchPolicy::next_boundary(origin, duration, Instant::now()));
                self.generation = self.generation.wrapping_add(1);
            }
        }
    }

    pub(crate) fn process(&mut self, data: StreamData) -> TimedBatchOutput {
        match data {
            StreamData::Collection(collection) => {
                // Match the existing collection preparation contract, then recheck
                // the fixed deadline before appending the prepared input.
                let _ = collection.rows();
                TimedBatchOutput::Feed {
                    collection,
                    offset: 0,
                    entered: false,
                }
            }
            data if data.is_terminal() => self.finish(Some(data)),
            data => TimedBatchOutput::Forward(Some(data)),
        }
    }

    pub(crate) fn finish(&mut self, trailing: Option<StreamData>) -> TimedBatchOutput {
        self.clear_deadline();
        TimedBatchOutput::Drain(trailing)
    }

    pub(crate) fn next_output(
        &mut self,
        output: &mut TimedBatchOutput,
    ) -> Result<Option<StreamData>, ProcessorError> {
        match output {
            TimedBatchOutput::Feed {
                collection,
                offset,
                entered,
            } => {
                let first = !*entered;
                *entered = true;
                if first
                    && self
                        .deadline
                        .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    self.clear_deadline();
                    if !self.rows.is_empty() {
                        return self.flush(self.rows.len());
                    }
                }
                let rows = collection.rows();
                while let Some(row) = rows.get(*offset) {
                    self.rows.push_back(row.clone());
                    *offset += 1;
                    if let Some(count) = self.policy.count_threshold() {
                        if self.rows.len() >= count {
                            return self.flush(count);
                        }
                    }
                }
                self.schedule();
                Ok(None)
            }
            TimedBatchOutput::Forward(data) => {
                if data.is_some() {
                    Ok(data.take())
                } else {
                    self.schedule();
                    Ok(None)
                }
            }
            TimedBatchOutput::Drain(trailing) => {
                if !self.rows.is_empty() {
                    self.flush(self.rows.len())
                } else {
                    Ok(trailing.take())
                }
            }
        }
    }

    fn flush(&mut self, count: usize) -> Result<Option<StreamData>, ProcessorError> {
        let rows = self.rows.drain(..count).collect();
        NativeBatchStage::collection(rows).map(|batch| Some(StreamData::collection(batch)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::batch_from_columns_simple;
    use datatypes::Value;

    fn input(values: &[i64]) -> StreamData {
        StreamData::collection(Box::new(
            batch_from_columns_simple(vec![(
                "stream".into(),
                "a".into(),
                values.iter().copied().map(Value::Int64).collect(),
            )])
            .unwrap(),
        ))
    }
    fn values(data: StreamData) -> Vec<i64> {
        let StreamData::Collection(collection) = data else {
            panic!("expected batch");
        };
        collection
            .rows()
            .iter()
            .map(|row| {
                let Value::Int64(value) = row.value_by_index("stream", 0).unwrap() else {
                    panic!("expected integer");
                };
                *value
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_grid_rechecks_prepared_input_and_drains_one_batch_at_a_time() {
        let origin = Instant::now();
        let mut stage = TimedBatchStage::new(Some(2), Duration::from_millis(100)).unwrap();
        stage.start(origin);
        tokio::time::advance(Duration::from_millis(20)).await;
        let mut first = stage.process(input(&[1]));
        assert!(stage.next_output(&mut first).unwrap().is_none());
        assert_eq!(
            stage.deadline().unwrap().at,
            origin + Duration::from_millis(100)
        );
        tokio::time::advance(Duration::from_millis(79)).await;
        let mut prepared = stage.process(input(&[2]));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(
            values(stage.next_output(&mut prepared).unwrap().unwrap()),
            vec![1]
        );
        assert!(
            stage.rows.is_empty(),
            "new rows wait behind the expired batch output"
        );
        assert!(stage.next_output(&mut prepared).unwrap().is_none());
        assert_eq!(
            stage.deadline().unwrap().at,
            origin + Duration::from_millis(200)
        );
        let mut many = stage.process(input(&[3, 4, 5, 6]));
        assert_eq!(
            values(stage.next_output(&mut many).unwrap().unwrap()),
            vec![2, 3]
        );
        assert!(
            stage.rows.is_empty(),
            "remaining input must not be appended before the send completes"
        );
        // Crossing the deadline during an in-flight input does not split that
        // input into a second duration batch. The driver services timers afterwards.
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(
            values(stage.next_output(&mut many).unwrap().unwrap()),
            vec![4, 5]
        );
        assert!(stage.next_output(&mut many).unwrap().is_none());
        assert_eq!(
            stage.deadline().unwrap().at,
            origin + Duration::from_millis(200)
        );
        let mut timer = stage.finish(None);
        assert_eq!(
            values(stage.next_output(&mut timer).unwrap().unwrap()),
            vec![6]
        );
        assert!(stage.next_output(&mut timer).unwrap().is_none());
        assert!(stage.deadline().is_none());
    }
}
