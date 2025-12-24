//! WatermarkProcessor - emits or forwards watermarks to drive time-related operators.

use crate::planner::physical::{
    PhysicalEventtimeWatermark, PhysicalPlan, PhysicalProcessTimeWatermark, WatermarkConfig,
    WatermarkStrategy,
};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::{interval, sleep, Interval, MissedTickBehavior, Sleep};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Watermark processor variants by window/operator kind.
pub enum WatermarkProcessor {
    ProcessTime(ProcessTimeWatermarkProcessor),
    Eventtime(EventtimeWatermarkProcessor),
}

/// Processing-time watermark processor variants by window/operator kind.
pub enum ProcessTimeWatermarkProcessor {
    Tumbling(TumblingWatermarkProcessor),
    Sliding(SlidingWatermarkProcessor),
}

impl WatermarkProcessor {
    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::ProcessTimeWatermark(watermark) => match &watermark.config {
                WatermarkConfig::Tumbling { .. } => Some(WatermarkProcessor::ProcessTime(
                    ProcessTimeWatermarkProcessor::Tumbling(TumblingWatermarkProcessor::new(
                        id,
                        Arc::new(watermark.clone()),
                    )),
                )),
                WatermarkConfig::Sliding { .. } => Some(WatermarkProcessor::ProcessTime(
                    ProcessTimeWatermarkProcessor::Sliding(SlidingWatermarkProcessor::new(
                        id,
                        Arc::new(watermark.clone()),
                    )),
                )),
            },
            PhysicalPlan::EventtimeWatermark(watermark) => Some(WatermarkProcessor::Eventtime(
                EventtimeWatermarkProcessor::new(id, Arc::new(watermark.clone())),
            )),
            _ => None,
        }
    }
}

impl Processor for WatermarkProcessor {
    fn id(&self) -> &str {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.id(),
            WatermarkProcessor::Eventtime(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.start(),
            WatermarkProcessor::Eventtime(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.subscribe_output(),
            WatermarkProcessor::Eventtime(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.subscribe_control_output(),
            WatermarkProcessor::Eventtime(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.add_input(receiver),
            WatermarkProcessor::Eventtime(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.add_control_input(receiver),
            WatermarkProcessor::Eventtime(p) => p.add_control_input(receiver),
        }
    }
}

impl Processor for ProcessTimeWatermarkProcessor {
    fn id(&self) -> &str {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.id(),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.start(),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.subscribe_output(),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.subscribe_control_output(),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.add_input(receiver),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.add_control_input(receiver),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.add_control_input(receiver),
        }
    }
}

/// Watermark operator processor.
pub struct TumblingWatermarkProcessor {
    id: String,
    physical: Arc<PhysicalProcessTimeWatermark>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl TumblingWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalProcessTimeWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn build_interval(strategy: &WatermarkStrategy) -> Option<Interval> {
        strategy.interval_duration().map(|duration| {
            let mut ticker = interval(duration);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker
        })
    }
}

impl Processor for TumblingWatermarkProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let mut ticker = Self::build_interval(self.physical.config.strategy());
        tracing::info!(processor_id = %id, "watermark processor starting");

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
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
                    _tick = async {
                        if let Some(ticker) = ticker.as_mut() {
                            ticker.tick().await;
                            Some(())
                        } else {
                            None
                        }
                    }, if ticker.is_some() => {
                        let ts = SystemTime::now();
                        send_with_backpressure(&output, StreamData::watermark(ts)).await?;
                        continue;
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                send_with_backpressure(&output, StreamData::control(signal)).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "WatermarkProcessor input lagged by {} messages",
                                    skipped
                                );
                                tracing::warn!(processor_id = %id, skipped = skipped, "input lagged");
                                forward_error(&output, &id, message).await?;
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

/// Sliding window watermark processor (processing-time only).
///
/// When `WatermarkConfig::Sliding { lookahead: Some(L), .. }`, this processor schedules and emits
/// a deadline watermark per trigger tuple at `deadline = tuple.timestamp + L`.
///
/// Event-time watermark semantics are not implemented yet.
///
/// Implementation note (why there is a heap + a single `Sleep`):
/// - Each incoming tuple yields a deadline time `t + lookahead`; we push all deadlines into a
///   min-heap (`BinaryHeap<Reverse<_>>`) so we can always find the earliest pending deadline.
/// - We do NOT spawn one task per tuple. Instead, the processor's main loop keeps at most one
///   active timer (`next_sleep`) that sleeps until the current earliest deadline.
/// - When `next_sleep` fires, we pop that earliest deadline and emit a `StreamData::Watermark`
///   for it, then rebuild `next_sleep` for the next earliest deadline (if any).
///   This avoids unbounded numbers of concurrent timers while still emitting per-tuple deadlines
///   at precise times.
pub struct SlidingWatermarkProcessor {
    id: String,
    lookahead: Duration,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl SlidingWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalProcessTimeWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let lookahead_secs = match &physical.config {
            WatermarkConfig::Sliding {
                lookahead: Some(lookahead),
                ..
            } => *lookahead,
            WatermarkConfig::Sliding {
                lookahead: None, ..
            } => {
                panic!("SlidingWatermarkProcessor requires sliding watermark lookahead")
            }
            _ => panic!("SlidingWatermarkProcessor requires WatermarkConfig::Sliding"),
        };
        let lookahead = Duration::from_secs(lookahead_secs);
        Self {
            id: id.into(),
            lookahead,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn to_nanos(ts: SystemTime) -> Result<u128, ProcessorError> {
        Ok(ts
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ProcessorError::ProcessingError(format!("invalid timestamp: {e}")))?
            .as_nanos())
    }

    fn from_nanos(nanos: u128) -> Result<SystemTime, ProcessorError> {
        let nanos_u64 = u64::try_from(nanos).map_err(|_| {
            ProcessorError::ProcessingError("timestamp out of range for u64 nanos".to_string())
        })?;
        Ok(UNIX_EPOCH + Duration::from_nanos(nanos_u64))
    }

    fn build_sleep(deadline_nanos: u128) -> Result<Pin<Box<Sleep>>, ProcessorError> {
        let deadline_ts = Self::from_nanos(deadline_nanos)?;
        let delay = match deadline_ts.duration_since(SystemTime::now()) {
            Ok(d) => d,
            Err(_) => Duration::from_secs(0),
        };
        Ok(Box::pin(sleep(delay)))
    }
}

impl Processor for SlidingWatermarkProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let lookahead = self.lookahead;
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();

        tokio::spawn(async move {
            let mut pending_deadlines: BinaryHeap<Reverse<u128>> = BinaryHeap::new();
            let mut next_sleep: Option<Pin<Box<Sleep>>> = None;
            let mut last_emitted_nanos: Option<u128> = None;
            tracing::info!(processor_id = %id, "watermark processor starting");

            async fn emit_deadline_watermark(
                output: &broadcast::Sender<StreamData>,
                last_emitted_nanos: &mut Option<u128>,
                ts: SystemTime,
            ) -> Result<(), ProcessorError> {
                let nanos = SlidingWatermarkProcessor::to_nanos(ts)?;
                if last_emitted_nanos.is_some_and(|last| nanos <= last) {
                    return Ok(());
                }
                *last_emitted_nanos = Some(nanos);
                send_with_backpressure(output, StreamData::watermark(ts)).await?;
                Ok(())
            }

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
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
                    _deadline = async {
                        if let Some(sleep) = next_sleep.as_mut() {
                            sleep.as_mut().await;
                            Some(())
                        } else {
                            None
                        }
                    }, if next_sleep.is_some() => {
                        if let Some(Reverse(deadline_nanos)) = pending_deadlines.pop() {
                            let deadline_ts = SlidingWatermarkProcessor::from_nanos(deadline_nanos)?;
                            emit_deadline_watermark(&output, &mut last_emitted_nanos, deadline_ts)
                                .await?;
                        }
                        next_sleep = if let Some(Reverse(nanos)) = pending_deadlines.peek() {
                            Some(SlidingWatermarkProcessor::build_sleep(*nanos)?)
                        } else {
                            None
                        };
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                match data {
                                    StreamData::Collection(collection) => {
                                        for row in collection.rows() {
                                            let deadline = row.timestamp + lookahead;
                                            let deadline_nanos =
                                                SlidingWatermarkProcessor::to_nanos(deadline)?;
                                            pending_deadlines.push(Reverse(deadline_nanos));
                                        }
                                        next_sleep = if let Some(Reverse(nanos)) = pending_deadlines.peek() {
                                            Some(SlidingWatermarkProcessor::build_sleep(*nanos)?)
                                        } else {
                                            None
                                        };
                                        send_with_backpressure(&output, StreamData::collection(collection))
                                            .await?;
                                    }
                                    other => {
                                        let is_terminal = other.is_terminal();
                                        send_with_backpressure(&output, other).await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "WatermarkProcessor input lagged by {} messages",
                                    skipped
                                );
                                tracing::warn!(processor_id = %id, skipped = skipped, "input lagged");
                                forward_error(&output, &id, message).await?;
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

/// Event-time watermark processor (data-driven, no ticker).
///
/// This processor buffers out-of-order tuples and emits:
/// - ordered `StreamData::Collection(RecordBatch)` (non-decreasing tuple.timestamp)
/// - monotonic `StreamData::Watermark`
/// Late events (`ts <= current_watermark`) are dropped.
pub struct EventtimeWatermarkProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    late_tolerance_nanos: u128,
}

impl EventtimeWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalEventtimeWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);

        let late_tolerance = match physical.config.strategy() {
            WatermarkStrategy::EventTime { late_tolerance } => *late_tolerance,
            WatermarkStrategy::ProcessingTime { .. } => Duration::ZERO,
        };

        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            late_tolerance_nanos: late_tolerance.as_nanos(),
        }
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn to_nanos(ts: SystemTime) -> Result<u128, ProcessorError> {
        Ok(ts
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ProcessorError::ProcessingError(format!("invalid timestamp: {e}")))?
            .as_nanos())
    }

    fn from_nanos(nanos: u128) -> Result<SystemTime, ProcessorError> {
        let nanos_u64 = u64::try_from(nanos).map_err(|_| {
            ProcessorError::ProcessingError("timestamp out of range for u64 nanos".to_string())
        })?;
        Ok(UNIX_EPOCH + Duration::from_nanos(nanos_u64))
    }
}

impl Processor for EventtimeWatermarkProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let late_tolerance_nanos = self.late_tolerance_nanos;
        let mut current_watermark_nanos: u128 = 0;
        let mut max_timestamp_seen_nanos: u128 = 0;
        let mut buffer: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();
        let mut seq: u64 = 0;

        tracing::info!(processor_id = %id, "eventtime watermark processor starting");
        tokio::spawn(async move {
            async fn flush_up_to(
                id: &str,
                output: &broadcast::Sender<StreamData>,
                target: u128,
                current_watermark_nanos: &mut u128,
                buffer: &mut BinaryHeap<Reverse<HeapItem>>,
            ) -> Result<(), ProcessorError> {
                if target <= *current_watermark_nanos {
                    return Ok(());
                }

                let mut out_rows = Vec::new();
                while let Some(Reverse(head)) = buffer.peek() {
                    if head.ts_nanos > target {
                        break;
                    }
                    let Reverse(item) = buffer
                        .pop()
                        .expect("peek returned Some, pop must succeed");
                    out_rows.push(item.tuple);
                }

                if !out_rows.is_empty() {
                    match crate::model::RecordBatch::new(out_rows) {
                        Ok(batch) => {
                            send_with_backpressure(output, StreamData::collection(Box::new(batch)))
                                .await?;
                        }
                        Err(err) => {
                            forward_error(output, id, format!("eventtime batch build error: {err}"))
                                .await?;
                        }
                    }
                }

                let watermark_ts = EventtimeWatermarkProcessor::from_nanos(target)?;
                send_with_backpressure(output, StreamData::watermark(watermark_ts)).await?;
                *current_watermark_nanos = target;
                Ok(())
            }

            async fn flush_all(
                id: &str,
                output: &broadcast::Sender<StreamData>,
                buffer: &mut BinaryHeap<Reverse<HeapItem>>,
            ) -> Result<(), ProcessorError> {
                if buffer.is_empty() {
                    return Ok(());
                }
                let mut out_rows = Vec::with_capacity(buffer.len());
                while let Some(Reverse(item)) = buffer.pop() {
                    out_rows.push(item.tuple);
                }
                if out_rows.is_empty() {
                    return Ok(());
                }
                match crate::model::RecordBatch::new(out_rows) {
                    Ok(batch) => {
                        send_with_backpressure(output, StreamData::collection(Box::new(batch))).await
                    }
                    Err(err) => {
                        forward_error(output, id, format!("eventtime batch build error: {err}"))
                            .await?;
                        Ok(())
                    }
                }
            }

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            if matches!(control_signal, ControlSignal::StreamGracefulEnd) {
                                flush_all(&id, &output, &mut buffer).await?;
                            }
                            send_control_with_backpressure(&control_output, control_signal).await?;
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
                            Some(Ok(StreamData::Collection(collection))) => {
                                let rows = match collection.into_rows() {
                                    Ok(rows) => rows,
                                    Err(err) => {
                                        forward_error(&output, &id, format!("eventtime collection error: {err}")).await?;
                                        continue;
                                    }
                                };
                                for tuple in rows {
                                    let ts_nanos = match EventtimeWatermarkProcessor::to_nanos(tuple.timestamp) {
                                        Ok(ts) => ts,
                                        Err(err) => {
                                            forward_error(&output, &id, format!("eventtime ingest error: {err}")).await?;
                                            continue;
                                        }
                                    };
                                    if ts_nanos <= current_watermark_nanos {
                                        continue;
                                    }
                                    max_timestamp_seen_nanos = std::cmp::max(max_timestamp_seen_nanos, ts_nanos);
                                    buffer.push(Reverse(HeapItem {
                                        ts_nanos,
                                        seq,
                                        tuple,
                                    }));
                                    seq = seq.wrapping_add(1);
                                }
                                let candidate = max_timestamp_seen_nanos.saturating_sub(late_tolerance_nanos);
                                let target = std::cmp::max(current_watermark_nanos, candidate);
                                flush_up_to(
                                    &id,
                                    &output,
                                    target,
                                    &mut current_watermark_nanos,
                                    &mut buffer,
                                )
                                .await?;
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                let target = EventtimeWatermarkProcessor::to_nanos(ts)?;
                                flush_up_to(
                                    &id,
                                    &output,
                                    target,
                                    &mut current_watermark_nanos,
                                    &mut buffer,
                                )
                                .await?;
                            }
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                if matches!(signal, ControlSignal::StreamGracefulEnd) {
                                    flush_all(&id, &output, &mut buffer).await?;
                                }
                                send_with_backpressure(&output, StreamData::control(signal)).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Ok(other)) => {
                                let is_terminal = other.is_terminal();
                                send_with_backpressure(&output, other).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "EventtimeWatermarkProcessor input lagged by {} messages",
                                    skipped
                                );
                                tracing::warn!(processor_id = %id, skipped = skipped, "input lagged");
                                forward_error(&output, &id, message).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::WatermarkConfig;
    use tokio::time::timeout;

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(Vec::new(), UNIX_EPOCH + Duration::from_secs(sec))
    }

    #[tokio::test]
    async fn sliding_watermark_emits_deadline_per_tuple_in_processing_time() {
        let physical = PhysicalProcessTimeWatermark::new(
            WatermarkConfig::Sliding {
                time_unit: TimeUnit::Seconds,
                lookback: 10,
                lookahead: Some(15),
                strategy: WatermarkStrategy::ProcessingTime {
                    time_unit: TimeUnit::Seconds,
                    interval: 1,
                },
            },
            Vec::new(),
            0,
        );
        let mut processor = SlidingWatermarkProcessor::new("wm", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

        // Use an old timestamp so deadline is already in the past and should be emitted immediately.
        let batch = crate::model::RecordBatch::new(vec![tuple_at(1)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut saw_collection = false;
        let mut saw_deadline = false;
        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(_) => saw_collection = true,
                StreamData::Watermark(ts) => {
                    if ts == UNIX_EPOCH + Duration::from_secs(16) {
                        saw_deadline = true;
                    }
                }
                _ => {}
            }
            if saw_collection && saw_deadline {
                break;
            }
        }

        assert!(saw_collection);
        assert!(saw_deadline);
    }

    #[tokio::test]
    async fn eventtime_watermark_sorts_and_emits_monotonic_watermarks() {
        let physical = PhysicalEventtimeWatermark::new(
            WatermarkConfig::Tumbling {
                time_unit: TimeUnit::Seconds,
                length: 10,
                strategy: WatermarkStrategy::EventTime {
                    late_tolerance: Duration::from_secs(1),
                },
            },
            Vec::new(),
            0,
        );

        let mut processor = EventtimeWatermarkProcessor::new("ewm", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

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
                        saw_rows.push(
                            t.timestamp
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        );
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
                        saw_rows.push(
                            t.timestamp
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        );
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
}
