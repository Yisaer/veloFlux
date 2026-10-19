//! Region-owned timer waits. Semantic stages expose only periods and deadlines.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::time::{interval, sleep_until, Instant, Interval, MissedTickBehavior, Sleep};

#[derive(Clone, Copy)]
pub(crate) enum ClockEvent {
    Periodic,
    Deadline,
}

#[derive(Clone, Copy)]
pub(crate) struct ClockDeadline {
    pub(crate) generation: u64,
    pub(crate) at: Instant,
}

impl ClockDeadline {
    pub(crate) fn wall(generation: u64, at: SystemTime) -> Self {
        let delay = at.duration_since(SystemTime::now()).unwrap_or_default();
        Self {
            generation,
            at: Instant::now() + delay,
        }
    }
}

pub(crate) struct StageClock {
    periodic: Option<Interval>,
    deadline: Option<Pin<Box<Sleep>>>,
    generation: Option<u64>,
}

impl StageClock {
    pub(crate) fn new(period: Option<(Duration, MissedTickBehavior)>) -> Self {
        let periodic = period.map(|(period, behavior)| {
            let mut ticker = interval(period);
            ticker.set_missed_tick_behavior(behavior);
            ticker
        });
        Self {
            periodic,
            deadline: None,
            generation: None,
        }
    }

    pub(crate) fn update(&mut self, deadline: Option<ClockDeadline>) {
        if self.generation == deadline.map(|d| d.generation) {
            return;
        }
        self.generation = deadline.map(|d| d.generation);
        self.deadline = deadline.map(|d| Box::pin(sleep_until(d.at)));
    }

    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<ClockEvent> {
        // Retain the watermark driver's periodic-before-deadline priority.
        if let Some(periodic) = &mut self.periodic {
            if periodic.poll_tick(cx).is_ready() {
                return Poll::Ready(ClockEvent::Periodic);
            }
        }
        if let Some(deadline) = &mut self.deadline {
            if deadline.as_mut().poll(cx).is_ready() {
                return Poll::Ready(ClockEvent::Deadline);
            }
        }
        Poll::Pending
    }
}
