//! Synchronous watermark generation; region owns timer waits.

use crate::planner::physical::{
    PhysicalEventtimeWatermark, PhysicalProcessTimeWatermark, WatermarkConfig,
};
use crate::processor::stage_clock::{ClockDeadline, ClockEvent};
use crate::processor::{ControlSignal, ProcessorError, ProcessorStats, StreamData};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use tokio::sync::broadcast;

fn watermark_to_nanos(ts: SystemTime) -> Result<u128, ProcessorError> {
    ts.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .map_err(|error| ProcessorError::ProcessingError(format!("invalid timestamp: {error}")))
}
fn watermark_from_nanos(nanos: u128) -> Result<SystemTime, ProcessorError> {
    let nanos = u64::try_from(nanos).map_err(|_| {
        ProcessorError::ProcessingError("timestamp out of range for u64 nanos".into())
    })?;
    UNIX_EPOCH
        .checked_add(Duration::from_nanos(nanos))
        .ok_or_else(|| ProcessorError::ProcessingError("timestamp overflow".into()))
}

pub(crate) struct WatermarkStage {
    engine: WatermarkEngine,
}
enum WatermarkEngine {
    Processing(ProcessingTimeState),
    Eventtime(EventtimeWatermarkState),
}
struct ProcessingTimeState {
    period: Duration,
    lookahead: Option<Duration>,
    monotonic: bool,
    pending: BinaryHeap<Reverse<u128>>,
    generation: u64,
    last_emitted: Option<u128>,
}
impl WatermarkStage {
    fn periodic(period: Duration) -> Self {
        Self {
            engine: WatermarkEngine::Processing(ProcessingTimeState {
                period,
                lookahead: None,
                monotonic: false,
                pending: BinaryHeap::new(),
                generation: 0,
                last_emitted: None,
            }),
        }
    }
    pub(crate) fn from_processing(plan: &PhysicalProcessTimeWatermark) -> Self {
        let mut stage = Self::periodic(plan.config.interval_duration());
        if let WatermarkConfig::Sliding {
            time_unit,
            lookahead,
            ..
        } = &plan.config
        {
            if let WatermarkEngine::Processing(state) = &mut stage.engine {
                state.lookahead = lookahead.map(|n| time_unit.duration(n));
                state.monotonic = true;
            }
        }
        stage
    }
    pub(crate) fn from_eventtime(plan: &PhysicalEventtimeWatermark) -> Self {
        Self {
            engine: WatermarkEngine::Eventtime(EventtimeWatermarkState::new(plan.late_tolerance)),
        }
    }
    pub(crate) fn period(&self) -> Option<Duration> {
        match &self.engine {
            WatermarkEngine::Processing(state) => Some(state.period),
            _ => None,
        }
    }
    pub(crate) fn deadline(&self) -> Result<Option<ClockDeadline>, ProcessorError> {
        match &self.engine {
            WatermarkEngine::Processing(state) => state
                .pending
                .peek()
                .map(|Reverse(nanos)| {
                    watermark_from_nanos(*nanos).map(|at| ClockDeadline::wall(state.generation, at))
                })
                .transpose(),
            _ => Ok(None),
        }
    }
    pub(crate) fn clear(&mut self) {
        match &mut self.engine {
            WatermarkEngine::Processing(state) => {
                state.pending.clear();
                state.generation = state.generation.wrapping_add(1);
            }
            WatermarkEngine::Eventtime(state) => {
                state.buffer.clear();
            }
        }
    }
    pub(crate) fn on_timer(
        &mut self,
        event: ClockEvent,
        now: SystemTime,
    ) -> Result<Option<StreamData>, ProcessorError> {
        let WatermarkEngine::Processing(state) = &mut self.engine else {
            return Ok(None);
        };
        let timestamp = match event {
            ClockEvent::Periodic => now,
            ClockEvent::Deadline => {
                let Some(Reverse(nanos)) = state.pending.pop() else {
                    return Ok(None);
                };
                state.generation = state.generation.wrapping_add(1);
                watermark_from_nanos(nanos)?
            }
        };
        if state.monotonic {
            let nanos = watermark_to_nanos(timestamp)?;
            if state.last_emitted.is_some_and(|last| nanos <= last) {
                return Ok(None);
            }
            state.last_emitted = Some(nanos);
        }
        Ok(Some(StreamData::watermark(timestamp)))
    }
    fn event_outputs(
        step: EventtimeStep,
        stats: &ProcessorStats,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        for error in step.errors {
            stats.record_error_logged("watermark processor error", error);
        }
        let mut output = step.outputs.into_iter();
        let pair = [output.next(), output.next()];
        if output.next().is_some() {
            return Err(ProcessorError::ProcessingError(
                "event-time step exceeded its two-output contract".into(),
            ));
        }
        Ok(pair)
    }
    fn recover_event_step(
        result: Result<EventtimeStep, ProcessorError>,
        stats: &ProcessorStats,
        action: &str,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        match result {
            Ok(step) => Self::event_outputs(step, stats),
            Err(error) => {
                stats.record_error_logged(
                    "watermark processor error",
                    format!("eventtime {action} error: {error}"),
                );
                Ok([None, None])
            }
        }
    }
    pub(crate) fn control(
        &mut self,
        signal: &ControlSignal,
        stats: &ProcessorStats,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        match &mut self.engine {
            WatermarkEngine::Eventtime(state) if signal.is_graceful_end() => {
                Self::recover_event_step(state.on_graceful_end(), stats, "flush")
            }
            _ => Ok([None, None]),
        }
    }
    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        match (&mut self.engine, data) {
            (WatermarkEngine::Processing(state), StreamData::Collection(collection)) => {
                if let Some(lookahead) = state.lookahead {
                    for row in collection.rows() {
                        let at = row.timestamp.checked_add(lookahead).ok_or_else(|| {
                            ProcessorError::ProcessingError(
                                "sliding watermark deadline overflow".into(),
                            )
                        })?;
                        state.pending.push(Reverse(watermark_to_nanos(at)?));
                    }
                    state.generation = state.generation.wrapping_add(1);
                    if let Some(Reverse(nanos)) = state.pending.peek() {
                        watermark_from_nanos(*nanos)?;
                    }
                }
                Ok([Some(StreamData::collection(collection)), None])
            }
            (WatermarkEngine::Eventtime(state), StreamData::Collection(collection)) => {
                let rows = match collection.into_rows() {
                    Ok(rows) => rows,
                    Err(error) => {
                        stats.record_error_logged(
                            "watermark processor error",
                            format!("eventtime collection error: {error}"),
                        );
                        return Ok([None, None]);
                    }
                };
                Self::recover_event_step(state.on_rows(rows), stats, "ingest")
            }
            (WatermarkEngine::Eventtime(_), StreamData::Watermark(_)) => {
                stats.record_error_logged(
                    "watermark processor error",
                    "unexpected StreamData::Watermark input in eventtime watermark processor",
                );
                Ok([None, None])
            }
            (WatermarkEngine::Eventtime(state), StreamData::Control(signal))
                if signal.is_graceful_end() =>
            {
                let mut outputs =
                    Self::recover_event_step(state.on_graceful_end(), stats, "flush")?;
                outputs[1] = Some(StreamData::control(signal));
                Ok(outputs)
            }
            (_, other) => Ok([Some(other), None]),
        }
    }
}

#[derive(Debug, Clone)]
struct HeapItem {
    ts_nanos: u128,
    seq: u64,
    tuple: crate::model::Tuple,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.ts_nanos == other.ts_nanos && self.seq == other.seq
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts_nanos
            .cmp(&other.ts_nanos)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

struct EventtimeStep {
    outputs: Vec<StreamData>,
    errors: Vec<String>,
}

#[derive(Debug)]
struct EventtimeWatermarkState {
    current_watermark_nanos: u128,
    max_timestamp_seen_nanos: u128,
    late_tolerance_nanos: u128,
    buffer: BinaryHeap<Reverse<HeapItem>>,
    seq: u64,
}

impl Default for EventtimeWatermarkState {
    fn default() -> Self {
        Self::new(Duration::ZERO)
    }
}

impl EventtimeWatermarkState {
    fn new(late_tolerance: Duration) -> Self {
        Self {
            current_watermark_nanos: 0,
            max_timestamp_seen_nanos: 0,
            late_tolerance_nanos: late_tolerance.as_nanos(),
            buffer: BinaryHeap::new(),
            seq: 0,
        }
    }

    fn compute_target(&self) -> u128 {
        let candidate = self
            .max_timestamp_seen_nanos
            .saturating_sub(self.late_tolerance_nanos);
        std::cmp::max(self.current_watermark_nanos, candidate)
    }

    fn on_rows(&mut self, rows: Vec<crate::model::Tuple>) -> Result<EventtimeStep, ProcessorError> {
        let mut errors = Vec::new();

        for tuple in rows {
            let ts_nanos = match watermark_to_nanos(tuple.timestamp) {
                Ok(ts) => ts,
                Err(err) => {
                    errors.push(err.to_string());
                    continue;
                }
            };
            if ts_nanos <= self.current_watermark_nanos {
                continue;
            }
            self.max_timestamp_seen_nanos = std::cmp::max(self.max_timestamp_seen_nanos, ts_nanos);
            self.buffer.push(Reverse(HeapItem {
                ts_nanos,
                seq: self.seq,
                tuple,
            }));
            self.seq = self.seq.wrapping_add(1);
        }

        let target = self.compute_target();
        let outputs = self.flush_up_to(target)?;
        Ok(EventtimeStep { outputs, errors })
    }

    fn on_graceful_end(&mut self) -> Result<EventtimeStep, ProcessorError> {
        let outputs = self.flush_all()?;
        Ok(EventtimeStep {
            outputs,
            errors: Vec::new(),
        })
    }

    fn flush_up_to(&mut self, target: u128) -> Result<Vec<StreamData>, ProcessorError> {
        if target <= self.current_watermark_nanos {
            return Ok(Vec::new());
        }

        let mut out_rows = Vec::new();
        while let Some(Reverse(head)) = self.buffer.peek() {
            if head.ts_nanos > target {
                break;
            }
            if let Some(Reverse(item)) = self.buffer.pop() {
                out_rows.push(item.tuple);
            } else {
                break;
            }
        }

        let mut out = Vec::new();
        if !out_rows.is_empty() {
            let batch = crate::model::RecordBatch::new(out_rows)
                .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
            out.push(StreamData::collection(Box::new(batch)));
        }

        let watermark_ts = watermark_from_nanos(target)?;
        out.push(StreamData::watermark(watermark_ts));
        self.current_watermark_nanos = target;
        Ok(out)
    }

    fn flush_all(&mut self) -> Result<Vec<StreamData>, ProcessorError> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }
        let mut out_rows = Vec::with_capacity(self.buffer.len());
        while let Some(Reverse(item)) = self.buffer.pop() {
            out_rows.push(item.tuple);
        }
        if out_rows.is_empty() {
            return Ok(Vec::new());
        }
        let batch = crate::model::RecordBatch::new(out_rows)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
        Ok(vec![StreamData::collection(Box::new(batch))])
    }
}

#[cfg(test)]
mod tests {
    use crate::processor::base::*;
    use crate::processor::*;

    use crate::planner::physical::*;

    use tokio::time::*;

    use super::*;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::WatermarkConfig;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::runtime::TaskSpawner;

    use tokio::time::timeout;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        )
    }

    fn tuple_at_millis(millis: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(millis),
        )
    }

    #[tokio::test]
    async fn sliding_watermark_emits_millisecond_deadline_per_tuple() {
        let spawner = test_spawner();
        let physical = PhysicalProcessTimeWatermark::new(
            WatermarkConfig::Sliding {
                time_unit: TimeUnit::Milliseconds,
                lookback: 100,
                lookahead: Some(100),
            },
            Vec::new(),
            0,
        );
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "wm".into(),
            &[crate::planner::physical::PhysicalPlan::ProcessTimeWatermark(physical)],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .expect("valid sliding config");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let base = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("now")
            .as_millis() as u64;
        let batch = crate::model::RecordBatch::new(vec![tuple_at_millis(base)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut saw_collection = false;
        let mut saw_deadline_or_later = false;
        let expected_deadline = UNIX_EPOCH + Duration::from_millis(base + 100);
        for _ in 0..8 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(_) => saw_collection = true,
                StreamData::Watermark(ts) => {
                    if ts >= expected_deadline {
                        saw_deadline_or_later = true;
                    }
                }
                _ => {}
            }
            if saw_collection && saw_deadline_or_later {
                break;
            }
        }

        assert!(saw_collection);
        assert!(saw_deadline_or_later);
    }

    #[tokio::test]
    async fn sliding_watermark_emits_deadline_per_tuple_in_processing_time() {
        let spawner = test_spawner();
        let physical = PhysicalProcessTimeWatermark::new(
            WatermarkConfig::Sliding {
                time_unit: TimeUnit::Seconds,
                lookback: 10,
                lookahead: Some(1),
            },
            Vec::new(),
            0,
        );
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "wm".into(),
            &[crate::planner::physical::PhysicalPlan::ProcessTimeWatermark(physical)],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .expect("valid sliding config");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // Use "now-ish" timestamps to match processing-time assumptions.
        let base = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("now")
            .as_secs();
        let batch = crate::model::RecordBatch::new(vec![tuple_at(base)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut saw_collection = false;
        let mut saw_deadline_or_later = false;
        let expected_deadline = UNIX_EPOCH + Duration::from_secs(base + 1);
        for _ in 0..8 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(_) => saw_collection = true,
                StreamData::Watermark(ts) => {
                    if ts >= expected_deadline {
                        saw_deadline_or_later = true;
                    }
                }
                _ => {}
            }
            if saw_collection && saw_deadline_or_later {
                break;
            }
        }

        assert!(saw_collection);
        assert!(saw_deadline_or_later);
    }

    #[tokio::test]
    async fn sliding_watermark_without_lookahead_still_emits_tick_watermarks() {
        let spawner = test_spawner();
        let physical = PhysicalProcessTimeWatermark::new(
            WatermarkConfig::Sliding {
                time_unit: TimeUnit::Seconds,
                lookback: 10,
                lookahead: None,
            },
            Vec::new(),
            0,
        );
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "wm".into(),
            &[crate::planner::physical::PhysicalPlan::ProcessTimeWatermark(physical)],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .expect("valid sliding config");
        let (_input, _) = broadcast::channel::<StreamData>(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(_input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // The ticker may fire immediately; just assert we see a watermark promptly.
        loop {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            if matches!(msg, StreamData::Watermark(_)) {
                break;
            }
        }
    }

    // coverage-covers: processor.eventtime.watermark_progress
    #[tokio::test]
    async fn eventtime_watermark_sorts_and_emits_monotonic_watermarks() {
        let spawner = test_spawner();
        let physical = PhysicalEventtimeWatermark::new(Duration::from_secs(1), None, Vec::new(), 0);

        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "ewm".into(),
            &[crate::planner::physical::PhysicalPlan::EventtimeWatermark(
                physical,
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // Out-of-order input: 3, 1, 2 (late_tolerance=1 => candidate=2)
        let batch = crate::model::RecordBatch::new(vec![tuple_at(3), tuple_at(1), tuple_at(2)])
            .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        // Expect ordered output up to watermark=2, then watermark.
        let mut saw_rows = Vec::new();
        let mut saw_watermark = None;
        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(collection) => {
                    let rows = collection.into_rows().expect("rows");
                    for t in rows {
                        saw_rows.push(t.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs());
                    }
                }
                StreamData::Watermark(ts) => {
                    saw_watermark = Some(ts.duration_since(UNIX_EPOCH).unwrap().as_secs());
                    break;
                }
                _ => {}
            }
        }

        assert_eq!(saw_rows, vec![1, 2]);
        assert_eq!(saw_watermark, Some(2));

        // Next batch advances max to 4 => target becomes 3, flushing 3 and watermark 3.
        let batch = crate::model::RecordBatch::new(vec![tuple_at(4)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut saw_rows = Vec::new();
        let mut saw_watermark = None;
        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(collection) => {
                    let rows = collection.into_rows().expect("rows");
                    for t in rows {
                        saw_rows.push(t.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs());
                    }
                }
                StreamData::Watermark(ts) => {
                    saw_watermark = Some(ts.duration_since(UNIX_EPOCH).unwrap().as_secs());
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(saw_rows, vec![3]);
        assert_eq!(saw_watermark, Some(3));
    }

    // coverage-covers: processor.eventtime.watermark_progress
    #[test]
    fn eventtime_state_computes_target_and_emits_watermarks() {
        /*
        Summary:
        - Validates state transitions for event-time watermark computation:
          - `max_seen` tracks the max tuple timestamp observed so far.
          - `target = max(current_watermark, max_seen - late_tolerance)` advances monotonically.
        - Validates output transitions when `target` advances:
          - First emits a `Collection` containing all buffered tuples with `ts <= target`,
            in non-decreasing `(ts, seq)` order.
          - Then emits `Watermark(target)`.
        */
        let mut state = EventtimeWatermarkState::new(Duration::from_secs(1));

        let step = state
            .on_rows(vec![tuple_at(3), tuple_at(1), tuple_at(2)])
            .expect("on_rows");
        assert_eq!(
            state.current_watermark_nanos,
            Duration::from_secs(2).as_nanos()
        );
        assert!(step.errors.is_empty());
        assert_eq!(step.outputs.len(), 2);
        match &step.outputs[0] {
            StreamData::Collection(collection) => {
                let rows = collection.clone().into_rows().expect("rows");
                let secs: Vec<u64> = rows
                    .into_iter()
                    .map(|t| t.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs())
                    .collect();
                assert_eq!(secs, vec![1, 2]);
            }
            _ => panic!("expected collection"),
        }
        match &step.outputs[1] {
            StreamData::Watermark(ts) => {
                assert_eq!(ts.duration_since(UNIX_EPOCH).unwrap().as_secs(), 2);
            }
            _ => panic!("expected watermark"),
        }

        let step = state.on_rows(vec![tuple_at(4)]).expect("on_rows");
        assert_eq!(
            state.current_watermark_nanos,
            Duration::from_secs(3).as_nanos()
        );
        assert!(step.errors.is_empty());
        assert_eq!(step.outputs.len(), 2);
        match &step.outputs[0] {
            StreamData::Collection(collection) => {
                let rows = collection.clone().into_rows().expect("rows");
                let secs: Vec<u64> = rows
                    .into_iter()
                    .map(|t| t.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs())
                    .collect();
                assert_eq!(secs, vec![3]);
            }
            _ => panic!("expected collection"),
        }
        match &step.outputs[1] {
            StreamData::Watermark(ts) => {
                assert_eq!(ts.duration_since(UNIX_EPOCH).unwrap().as_secs(), 3);
            }
            _ => panic!("expected watermark"),
        }
    }

    // coverage-covers: processor.eventtime.watermark_progress
    #[test]
    fn eventtime_state_drops_late_events() {
        /*
        Summary:
        - Establishes a watermark (late_tolerance = 0) and then verifies late-drop semantics:
          - Any tuple with `ts <= current_watermark` is considered late and dropped.
          - Late drops do not advance watermark and do not emit outputs.
        */
        let mut state = EventtimeWatermarkState::new(Duration::from_secs(0));
        let step = state.on_rows(vec![tuple_at(2)]).expect("on_rows");
        assert_eq!(step.outputs.len(), 2);
        assert_eq!(
            state.current_watermark_nanos,
            Duration::from_secs(2).as_nanos()
        );

        let step = state
            .on_rows(vec![tuple_at(2), tuple_at(1)])
            .expect("on_rows");
        assert!(step.outputs.is_empty());
    }
}
