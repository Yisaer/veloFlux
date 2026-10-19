//! Shared synchronous batching and framing for sink encoding.
//! The outer driver owns every timer and transport wait.
use crate::codec::SinkEncoder;
use crate::model::Collection;
use crate::processor::batch_policy::BatchPolicy;
use crate::processor::{EncodedDeliveryFlags, ProcessorError, StreamData};
use tokio::time::Instant;

#[derive(Default)]
struct DeliveryEncoding {
    row_count: usize,
    active: bool,
    emitted_chunk: bool,
    pending_start_chunk: Option<bytes::Bytes>,
}

/// A concrete input description retaining only the unconsumed collection.
/// No encoder work for the next segment occurs until its driver asks again.
pub(crate) struct EncoderOutput {
    collection: Option<Box<dyn Collection>>,
    offset: usize,
    total: usize,
    done: bool,
}

/// Lifecycle notifications precede the frame; an error follows its successful send.
pub(crate) struct EncodingStep {
    pub(crate) aborted: bool,
    pub(crate) frame: Option<StreamData>,
    pub(crate) error: Option<ProcessorError>,
}

impl EncoderOutput {
    pub(crate) fn stop(&mut self) {
        self.done = true;
    }
}

impl EncodingStep {
    fn frame(frame: Option<StreamData>) -> Self {
        Self {
            aborted: false,
            frame,
            error: None,
        }
    }
}

pub(crate) struct EncoderStage {
    encoder: Box<dyn SinkEncoder>,
    delivery: DeliveryEncoding,
    policy: BatchPolicy,
    origin: Option<Instant>,
    deadline: Option<Instant>,
    generation: u64,
}

impl EncoderStage {
    pub(crate) fn new(encoder: Box<dyn SinkEncoder>, policy: BatchPolicy) -> Self {
        Self {
            encoder,
            delivery: DeliveryEncoding::default(),
            policy,
            origin: None,
            deadline: None,
            generation: 0,
        }
    }
    pub(crate) fn start_clock(&mut self, now: Instant) {
        self.origin = Some(now);
    }
    pub(crate) fn deadline(&self) -> Option<super::stage_clock::ClockDeadline> {
        self.deadline.map(|at| super::stage_clock::ClockDeadline {
            generation: self.generation,
            at,
        })
    }
    pub(crate) fn clear_deadline(&mut self) {
        if self.deadline.take().is_some() {
            self.generation = self.generation.wrapping_add(1);
        }
    }
    pub(crate) fn take_due_deadline(&mut self) -> bool {
        if self.deadline.is_some_and(|at| Instant::now() >= at) {
            self.clear_deadline();
            true
        } else {
            false
        }
    }
    pub(crate) fn schedule_clock(&mut self) {
        if !self.has_data() {
            self.clear_deadline();
        } else if self.deadline.is_none() {
            if let (Some(origin), Some(duration)) = (self.origin, self.policy.duration()) {
                self.deadline = Some(BatchPolicy::next_boundary(origin, duration, Instant::now()));
                self.generation = self.generation.wrapping_add(1);
            }
        }
    }
    pub(crate) fn flush_step(&mut self) -> EncodingStep {
        self.finish_delivery(None)
    }
    pub(crate) fn output_failed(&mut self, flags: EncodedDeliveryFlags) {
        if flags.contains(EncodedDeliveryFlags::START) && !flags.contains(EncodedDeliveryFlags::END)
        {
            self.delivery.emitted_chunk = false;
        }
    }
    pub(crate) fn has_data(&self) -> bool {
        self.delivery.row_count > 0
    }
    pub(crate) fn batch_duration(&self) -> Option<std::time::Duration> {
        self.policy.duration()
    }
    pub(crate) fn input(collection: Box<dyn Collection>) -> EncoderOutput {
        let total = collection.num_rows();
        EncoderOutput {
            collection: Some(collection),
            offset: 0,
            total,
            done: false,
        }
    }
    pub(crate) fn finish() -> EncoderOutput {
        EncoderOutput {
            collection: None,
            offset: 0,
            total: 0,
            done: false,
        }
    }

    pub(crate) fn next_step(&mut self, output: &mut EncoderOutput) -> Option<EncodingStep> {
        if output.done {
            return None;
        }
        let Some(collection) = output.collection.as_ref() else {
            output.done = true;
            return Some(self.finish_delivery(None));
        };
        let total = output.total;
        if total == 0 {
            output.done = true;
            if !matches!(self.policy, BatchPolicy::Immediate) {
                return None;
            }
            return Some(match self.start_delivery() {
                Ok(()) => self.finish_delivery(None),
                Err(error) => self.fail(error),
            });
        }
        if output.offset == total {
            output.done = true;
            return None;
        }
        let end = self.policy.count_threshold().map_or(total, |count| {
            output.offset + (count - self.delivery.row_count).min(total - output.offset)
        });
        let segment;
        let input = if output.offset == 0 && end == total {
            collection.as_ref()
        } else {
            segment = match collection.slice(output.offset, end) {
                Ok(segment) => segment,
                Err(error) => {
                    output.done = true;
                    // A slice error precedes append; preserve the existing active
                    // delivery just as the standalone processor does.
                    return Some(EncodingStep {
                        aborted: false,
                        frame: None,
                        error: Some(ProcessorError::ProcessingError(format!(
                            "collection slice error: {error}"
                        ))),
                    });
                }
            };
            segment.as_ref()
        };
        if !self.delivery.active {
            if let Err(error) = self.start_delivery() {
                output.done = true;
                return Some(self.fail(error));
            }
        }
        let chunk = match self.encoder.append(input) {
            Ok(chunk) => chunk,
            Err(error) => {
                output.done = true;
                return Some(self.fail(ProcessorError::ProcessingError(format!(
                    "encoder append error: {error}"
                ))));
            }
        };
        self.delivery.row_count += input.num_rows();
        output.offset = end;
        let step = if matches!(self.policy, BatchPolicy::Immediate)
            || self
                .policy
                .count_threshold()
                .is_some_and(|count| self.delivery.row_count >= count)
        {
            self.finish_delivery(chunk)
        } else {
            let chunk = if self.delivery.emitted_chunk {
                chunk
            } else {
                Self::combine_chunks([self.delivery.pending_start_chunk.take(), chunk, None])
            };
            let frame = chunk.map(|chunk| {
                if self.delivery.emitted_chunk {
                    StreamData::encoded_delivery_chunk(chunk)
                } else {
                    self.delivery.emitted_chunk = true;
                    StreamData::encoded_delivery_start(chunk)
                }
            });
            EncodingStep::frame(frame)
        };
        if step.error.is_some() {
            output.done = true;
        }
        Some(step)
    }

    fn start_delivery(&mut self) -> Result<(), ProcessorError> {
        self.delivery.active = true;
        let chunk = self.encoder.begin_delivery().map_err(|error| {
            ProcessorError::ProcessingError(format!("encoder begin error: {error}"))
        })?;
        self.delivery = DeliveryEncoding {
            active: true,
            pending_start_chunk: chunk.filter(|chunk| !chunk.is_empty()),
            ..Default::default()
        };
        Ok(())
    }

    fn fail(&mut self, error: ProcessorError) -> EncodingStep {
        let aborted = self.delivery.active;
        let emit_abort = self.delivery.emitted_chunk;
        if aborted {
            self.encoder.abort_delivery();
            self.delivery = DeliveryEncoding::default();
        }
        EncodingStep {
            aborted,
            frame: emit_abort.then(StreamData::encoded_delivery_abort),
            error: Some(error),
        }
    }

    fn finish_delivery(&mut self, prefix: Option<bytes::Bytes>) -> EncodingStep {
        if !self.delivery.active {
            return EncodingStep::frame(None);
        }
        let end = match self.encoder.finish_delivery() {
            Ok(chunk) => chunk,
            Err(error) => {
                return self.fail(ProcessorError::ProcessingError(format!(
                    "encoder finish error: {error}"
                )))
            }
        };
        let chunk = Self::combine_chunks([
            if self.delivery.emitted_chunk {
                None
            } else {
                self.delivery.pending_start_chunk.take()
            },
            prefix,
            end,
        ])
        .unwrap_or_default();
        let frame = if self.delivery.emitted_chunk {
            StreamData::encoded_delivery_end(chunk)
        } else {
            StreamData::encoded_delivery_single(chunk)
        };
        self.delivery = DeliveryEncoding::default();
        EncodingStep::frame(Some(frame))
    }

    fn combine_chunks(chunks: [Option<bytes::Bytes>; 3]) -> Option<bytes::Bytes> {
        let mut count = 0;
        let mut total_len = 0;
        for chunk in chunks.iter().flatten().filter(|chunk| !chunk.is_empty()) {
            count += 1;
            total_len += chunk.len();
        }
        match count {
            0 => None,
            1 => chunks.into_iter().flatten().find(|chunk| !chunk.is_empty()),
            _ => {
                let mut bytes = Vec::with_capacity(total_len);
                for chunk in chunks.into_iter().flatten() {
                    bytes.extend_from_slice(&chunk);
                }
                Some(bytes.into())
            }
        }
    }
}
