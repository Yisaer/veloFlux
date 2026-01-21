//! OrderProcessor - sorts each incoming Collection by ORDER BY keys.
//!
//! Semantics: sorting is applied within each incoming Collection (no global ordering across batches).

use crate::expr::value_compare;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{PhysicalOrder, PhysicalOrderKey, PhysicalPlan};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use datatypes::Value;
use futures::stream::StreamExt;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct OrderProcessor {
    id: String,
    physical_order: Arc<PhysicalOrder>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    stats: Arc<ProcessorStats>,
}

impl OrderProcessor {
    pub fn new(id: impl Into<String>, physical_order: Arc<PhysicalOrder>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical_order,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Order(order) => Some(Self::new(id, Arc::new(order.clone()))),
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
        value_compare::compare_values(left, right)
    }

    fn compare_key_values(
        key: &PhysicalOrderKey,
        left: &Value,
        right: &Value,
    ) -> Result<Ordering, String> {
        // Enforce NULLS LAST, regardless of ASC/DESC.
        if left.is_null() && right.is_null() {
            return Ok(Ordering::Equal);
        }
        if left.is_null() {
            return Ok(Ordering::Greater);
        }
        if right.is_null() {
            return Ok(Ordering::Less);
        }

        let ord = Self::compare_values(left, right).ok_or_else(|| {
            format!(
                "ORDER BY key '{}' is not comparable (left={:?}, right={:?})",
                key.original_expr, left, right
            )
        })?;

        Ok(if key.asc { ord } else { ord.reverse() })
    }

    fn compare_row_keys(
        keys: &[PhysicalOrderKey],
        left: &[Value],
        right: &[Value],
    ) -> Result<Ordering, String> {
        for (idx, key) in keys.iter().enumerate() {
            let lv = left
                .get(idx)
                .ok_or_else(|| "ORDER BY key arity mismatch".to_string())?;
            let rv = right
                .get(idx)
                .ok_or_else(|| "ORDER BY key arity mismatch".to_string())?;
            let ord = Self::compare_key_values(key, lv, rv)?;
            if ord != Ordering::Equal {
                return Ok(ord);
            }
        }
        Ok(Ordering::Equal)
    }

    fn apply_order(
        physical_order: &PhysicalOrder,
        collection: Box<dyn Collection>,
    ) -> Result<Box<dyn Collection>, ProcessorError> {
        let rows = collection.into_rows().map_err(|e| {
            ProcessorError::ProcessingError(format!("Failed to materialize rows: {}", e))
        })?;

        if rows.is_empty() || physical_order.keys.is_empty() {
            return Ok(Box::new(RecordBatch::new(rows).map_err(|e| {
                ProcessorError::ProcessingError(format!("Failed to build record batch: {}", e))
            })?));
        }

        let mut keyed = Vec::with_capacity(rows.len());
        for tuple in rows {
            let mut key_values = Vec::with_capacity(physical_order.keys.len());
            for key in &physical_order.keys {
                let value = key.compiled_expr.eval_with_tuple(&tuple).map_err(|e| {
                    ProcessorError::ProcessingError(format!(
                        "Failed to evaluate ORDER BY key '{}': {}",
                        key.original_expr, e
                    ))
                })?;
                key_values.push(value);
            }
            keyed.push((key_values, tuple));
        }

        let err: RefCell<Option<String>> = RefCell::new(None);
        keyed.sort_by(|(left_keys, _), (right_keys, _)| {
            if err.borrow().is_some() {
                return Ordering::Equal;
            }
            match Self::compare_row_keys(&physical_order.keys, left_keys, right_keys) {
                Ok(ord) => ord,
                Err(e) => {
                    *err.borrow_mut() = Some(e);
                    Ordering::Equal
                }
            }
        });

        if let Some(e) = err.into_inner() {
            return Err(ProcessorError::ProcessingError(e));
        }

        let sorted_rows = keyed.into_iter().map(|(_, tuple)| tuple).collect();
        let batch = RecordBatch::new(sorted_rows)
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        Ok(Box::new(batch))
    }
}

impl Processor for OrderProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let physical_order = Arc::clone(&self.physical_order);
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "order processor starting");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
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
                                        match Self::apply_order(physical_order.as_ref(), collection) {
                                            Ok(out_collection) => {
                                                let out = StreamData::collection(out_collection);
                                                let out_rows = out.num_rows_hint();
                                                send_with_backpressure(&output, out).await?;
                                                if let Some(rows) = out_rows {
                                                    stats.record_out(rows);
                                                }
                                            }
                                            Err(e) => {
                                                stats.record_error(e.to_string());
                                            }
                                        }
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
                                log_broadcast_lagged(&id, skipped, "order data input");
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
