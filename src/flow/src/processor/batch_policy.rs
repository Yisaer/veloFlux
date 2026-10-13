//! Shared count/duration policy; buffering belongs to individual stages.
use crate::processor::ProcessorError;
use std::pin::Pin;
use tokio::time::{sleep_until, Duration, Instant, Sleep};
pub(crate) enum BatchPolicy {
    Immediate,
    CountOnly { count: usize },
    DurationOnly { duration: Duration },
    Combined { count: usize, duration: Duration },
}

impl BatchPolicy {
    pub(crate) fn new(batch_count: Option<usize>, batch_duration: Option<Duration>) -> Self {
        match (batch_count, batch_duration) {
            (Some(count), Some(duration)) => Self::Combined { count, duration },
            (Some(count), None) => Self::CountOnly { count },
            (None, Some(duration)) => Self::DurationOnly { duration },
            (None, None) => Self::Immediate,
        }
    }

    pub(crate) fn count_threshold(&self) -> Option<usize> {
        match self {
            BatchPolicy::CountOnly { count } | BatchPolicy::Combined { count, .. } => Some(*count),
            BatchPolicy::Immediate | BatchPolicy::DurationOnly { .. } => None,
        }
    }

    pub(crate) fn duration(&self) -> Option<Duration> {
        match self {
            BatchPolicy::DurationOnly { duration } | BatchPolicy::Combined { duration, .. } => {
                Some(*duration)
            }
            BatchPolicy::Immediate | BatchPolicy::CountOnly { .. } => None,
        }
    }
    pub(crate) fn validate(
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
        owner: &str,
    ) -> Result<(), ProcessorError> {
        if matches!(batch_count, Some(0)) {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "{owner} requires batch_count > 0 when configured"
            )));
        }
        if let Some(duration) = batch_duration {
            let millis = duration.as_millis();
            if millis == 0 {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "{owner} requires batch_duration >= 1ms when configured"
                )));
            }
            if !duration.subsec_nanos().is_multiple_of(1_000_000) {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "{owner} requires batch_duration to use millisecond precision"
                )));
            }
            if u64::try_from(millis).is_err() {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "{owner} requires batch_duration to fit in u64 milliseconds"
                )));
            }
        }
        Ok(())
    }
    /// Fixed-grid boundary handling.
    ///
    /// The batch flush deadline is aligned to a fixed processing-time grid
    /// `grid_origin + k * duration` (captured once at processor start), NOT to the
    /// arrival of each batch's first sample. Anchoring to a fixed grid keeps every
    /// window the same fixed partition of the timeline, so a periodic source of the
    /// same period yields exactly `duration / period` rows per window instead of an
    /// extra boundary sample per batch.
    ///
    /// The window is left-closed / right-open `[t, t+duration)`: a sample landing on
    /// a boundary belongs to the next window. That is realized by the `biased`
    /// select and the input-entry deadline check, so the boundary flush happens
    /// before a collection prepared at or after the boundary is appended.
    ///
    /// The grid origin is data-independent (processor start), so the first window is
    /// naturally a partial one — it is emitted as-is; every subsequent window is a
    /// full-length grid cell.
    pub(crate) fn next_boundary(grid_origin: Instant, duration: Duration, now: Instant) -> Instant {
        let elapsed = now.saturating_duration_since(grid_origin);
        let period_ms = duration.as_millis();
        let next_boundary_ms = (elapsed.as_millis() / period_ms + 1)
            .checked_mul(period_ms)
            .expect("validated batch duration keeps boundary offset representable");
        let boundary_offset = Duration::from_millis(
            u64::try_from(next_boundary_ms)
                .expect("validated batch duration keeps boundary offset within u64 milliseconds"),
        );
        grid_origin + boundary_offset
    }

    /// Input preparation can cross a boundary after the task selected data.
    /// Compare the deadline with the clock, even if the sleep was not polled.
    pub(crate) fn take_due_timer(timer: &mut Option<Pin<Box<Sleep>>>, now: Instant) -> bool {
        if timer.as_ref().is_some_and(|timer| now >= timer.deadline()) {
            *timer = None;
            true
        } else {
            false
        }
    }

    pub(crate) fn schedule_timer(
        timer: &mut Option<Pin<Box<Sleep>>>,
        grid_origin: Instant,
        duration: Duration,
        has_data: bool,
    ) {
        if has_data {
            if timer.is_none() {
                let boundary = Self::next_boundary(grid_origin, duration, Instant::now());
                *timer = Some(Box::pin(sleep_until(boundary)));
            }
        } else if timer.is_some() {
            *timer = None;
        }
    }
}
