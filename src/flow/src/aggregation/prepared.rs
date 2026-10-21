//! Concrete and registered implementations share one accumulator/update representation.

use super::avg::{AvgAccumulator, AvgUpdate};
use super::count::{CountAccumulator, CountUpdate};
use super::extrema::{ExtremaUpdate, MaxAccumulator, MinAccumulator};
use super::sum::{SumAccumulator, SumUpdate};
use super::{AggregateAccumulator, AggregateUpdate};
use datatypes::Value;

/// Internal factory for builtin accumulators with non-boxed updates.
#[derive(Clone, Copy)]
pub(crate) struct PreparedAccumulatorFactory(Kind);

#[derive(Clone, Copy)]
enum Kind {
    Count,
    Avg,
    Sum,
    Max,
    Min,
}

pub(crate) struct AccumulatorState(State);

enum State {
    Count(CountAccumulator),
    Avg(AvgAccumulator),
    Sum(SumAccumulator),
    Max(MaxAccumulator),
    Min(MinAccumulator),
    Registered(Box<dyn AggregateAccumulator>),
}

pub(crate) struct AccumulatorUpdate(Update);

enum Update {
    Count(CountUpdate),
    Avg(AvgUpdate),
    Sum(SumUpdate),
    Max(ExtremaUpdate),
    Min(ExtremaUpdate),
    Registered(Box<dyn AggregateUpdate>),
}

impl PreparedAccumulatorFactory {
    pub(super) fn count() -> Self {
        Self(Kind::Count)
    }
    pub(super) fn avg() -> Self {
        Self(Kind::Avg)
    }
    pub(super) fn sum() -> Self {
        Self(Kind::Sum)
    }
    pub(super) fn max() -> Self {
        Self(Kind::Max)
    }
    pub(super) fn min() -> Self {
        Self(Kind::Min)
    }

    pub(crate) fn is_count(&self) -> bool {
        matches!(self.0, Kind::Count)
    }

    pub(crate) fn create(&self) -> AccumulatorState {
        AccumulatorState(match self.0 {
            Kind::Count => State::Count(CountAccumulator::default()),
            Kind::Avg => State::Avg(AvgAccumulator::default()),
            Kind::Sum => State::Sum(SumAccumulator::default()),
            Kind::Max => State::Max(MaxAccumulator::default()),
            Kind::Min => State::Min(MinAccumulator::default()),
        })
    }
}

impl AccumulatorState {
    pub(crate) fn registered(accumulator: Box<dyn AggregateAccumulator>) -> Self {
        Self(State::Registered(accumulator))
    }

    // Keep concrete dispatch in the row loop; an out-of-line Result return
    // otherwise copies the whole update enum for every aggregate.
    #[inline(always)]
    pub(crate) fn prepare(&self, args: &[Value]) -> Result<AccumulatorUpdate, String> {
        let update = match &self.0 {
            State::Count(acc) => acc.prepare(args).map(Update::Count),
            State::Avg(acc) => acc.prepare(args).map(Update::Avg),
            State::Sum(acc) => acc.prepare(args).map(Update::Sum),
            State::Max(acc) => acc.prepare(args).map(Update::Max),
            State::Min(acc) => acc.prepare(args).map(Update::Min),
            State::Registered(acc) => acc.prepare_update(args).map(Update::Registered),
        }?;
        Ok(AccumulatorUpdate(update))
    }

    #[inline]
    pub(crate) fn commit(&mut self, update: AccumulatorUpdate) {
        let matched = match (&mut self.0, update.0) {
            (State::Count(acc), Update::Count(update)) => {
                acc.commit(update);
                true
            }
            (State::Avg(acc), Update::Avg(update)) => {
                acc.commit(update);
                true
            }
            (State::Sum(acc), Update::Sum(update)) => {
                acc.commit(update);
                true
            }
            (State::Max(acc), Update::Max(update)) => {
                acc.commit(update);
                true
            }
            (State::Min(acc), Update::Min(update)) => {
                acc.commit(update);
                true
            }
            (State::Registered(acc), Update::Registered(update)) => {
                acc.commit_update(update);
                true
            }
            _ => false,
        };
        assert!(matched, "update must belong to its accumulator");
    }

    #[inline]
    pub(crate) fn finalize(&self) -> Value {
        match &self.0 {
            State::Count(acc) => acc.finalize(),
            State::Avg(acc) => acc.finalize(),
            State::Sum(acc) => acc.finalize(),
            State::Max(acc) => acc.finalize(),
            State::Min(acc) => acc.finalize(),
            State::Registered(acc) => acc.finalize(),
        }
    }
}
