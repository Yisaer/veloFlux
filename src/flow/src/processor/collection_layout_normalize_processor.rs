//! CollectionLayoutNormalizeProcessor - normalizes tuple layout for collection sources.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::planner::physical::{PhysicalCollectionLayoutNormalize, PhysicalPlan};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use datatypes::Value;
use futures::stream::StreamExt;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct CollectionLayoutNormalizeProcessor {
    id: String,
    output_source_name: Arc<str>,
    keys: Arc<[Arc<str>]>,
    resolved: Option<Vec<ResolvedGetter>>,

    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    stats: Arc<ProcessorStats>,
}

#[derive(Debug, Clone)]
enum ResolvedGetter {
    MessageByIndex { msg_idx: usize, key_idx: usize },
    Affiliate { column_name: Arc<str> },
    Missing,
}

impl CollectionLayoutNormalizeProcessor {
    pub fn new(id: impl Into<String>, spec: Arc<PhysicalCollectionLayoutNormalize>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);

        let schema = spec.schema();
        let keys: Vec<Arc<str>> = schema
            .column_schemas()
            .iter()
            .map(|col| Arc::<str>::from(col.name.as_str()))
            .collect();

        Self {
            id: id.into(),
            output_source_name: Arc::<str>::from(spec.output_source_name()),
            keys: Arc::from(keys),
            resolved: None,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::CollectionLayoutNormalize(spec) => {
                Some(Self::new(id, Arc::new(spec.clone())))
            }
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

fn resolve_getters(keys: &[Arc<str>], sample: &Tuple) -> Vec<ResolvedGetter> {
    let mut by_name = std::collections::HashMap::<&str, (usize, usize)>::new();
    for (msg_idx, msg) in sample.messages().iter().enumerate() {
        for (key_idx, (key, _)) in msg.entries().enumerate() {
            by_name.entry(key).or_insert((msg_idx, key_idx));
        }
    }

    keys.iter()
        .map(|col| {
            let name = col.as_ref();
            if let Some((msg_idx, key_idx)) = by_name.get(name).copied() {
                return ResolvedGetter::MessageByIndex { msg_idx, key_idx };
            }
            if sample.affiliate().and_then(|aff| aff.value(name)).is_some() {
                return ResolvedGetter::Affiliate {
                    column_name: Arc::clone(col),
                };
            }
            ResolvedGetter::Missing
        })
        .collect()
}

fn resolved_value(tuple: &Tuple, getter: &ResolvedGetter, expected: &str) -> Option<Arc<Value>> {
    match getter {
        ResolvedGetter::MessageByIndex { msg_idx, key_idx } => tuple
            .messages()
            .get(*msg_idx)
            .and_then(|msg| msg.entry_by_index(*key_idx))
            .and_then(|(key, value)| {
                if key.as_ref() == expected {
                    Some(Arc::clone(value))
                } else {
                    None
                }
            }),
        ResolvedGetter::Affiliate { column_name } => tuple
            .affiliate()
            .and_then(|aff| aff.value(column_name.as_ref()))
            .map(|v| Arc::new(v.clone())),
        ResolvedGetter::Missing => None,
    }
}

fn fallback_value(tuple: &Tuple, expected: &str) -> Option<(Arc<Value>, ResolvedGetter)> {
    for (msg_idx, msg) in tuple.messages().iter().enumerate() {
        if let Some((_, value)) = msg.entry_by_name(expected) {
            if let Some((key_idx, _)) = msg.entries().enumerate().find(|(_, (k, _))| *k == expected)
            {
                return Some((
                    Arc::clone(value),
                    ResolvedGetter::MessageByIndex { msg_idx, key_idx },
                ));
            }
            return Some((Arc::clone(value), ResolvedGetter::Missing));
        }
    }
    if let Some(value) = tuple
        .affiliate()
        .and_then(|aff| aff.value(expected))
        .cloned()
    {
        return Some((
            Arc::new(value),
            ResolvedGetter::Affiliate {
                column_name: Arc::<str>::from(expected),
            },
        ));
    }
    None
}

fn normalize_collection(
    schema_keys: &[Arc<str>],
    output_source_name: &Arc<str>,
    shared_keys: &Arc<[Arc<str>]>,
    resolved: &mut Option<Vec<ResolvedGetter>>,
    input: &dyn Collection,
    processor_id: &str,
) -> Result<Box<dyn Collection>, ProcessorError> {
    if resolved.is_none() {
        if let Some(sample) = input.rows().first() {
            *resolved = Some(resolve_getters(schema_keys, sample));
        }
    }

    let mut missing = BTreeSet::<String>::new();
    let mut rows = Vec::with_capacity(input.num_rows());
    for tuple in input.rows() {
        let mut values = Vec::with_capacity(schema_keys.len());
        for (idx, col_name) in schema_keys.iter().enumerate() {
            let expected = col_name.as_ref();
            let resolved_item = resolved.as_mut().and_then(|r| r.get(idx).cloned());

            let mut value = resolved_item
                .as_ref()
                .and_then(|g| resolved_value(tuple, g, expected));

            if value.is_none() {
                if let Some((found, new_getter)) = fallback_value(tuple, expected) {
                    value = Some(found);
                    if let Some(resolved_vec) = resolved.as_mut() {
                        if let Some(slot) = resolved_vec.get_mut(idx) {
                            *slot = new_getter;
                        }
                    }
                }
            }

            if value.is_none() {
                missing.insert(expected.to_string());
            }
            values.push(value.unwrap_or_else(|| Arc::new(Value::Null)));
        }

        let msg = Arc::new(Message::new_shared_keys(
            Arc::clone(output_source_name),
            Arc::clone(shared_keys),
            values,
        ));
        rows.push(Tuple::with_timestamp(Arc::from(vec![msg]), tuple.timestamp));
    }

    if !missing.is_empty() {
        tracing::warn!(
            processor_id = %processor_id,
            missing_columns = ?missing,
            "collection layout normalize filled NULL for missing columns"
        );
    }

    let batch = RecordBatch::new(rows)
        .map_err(|err| ProcessorError::ProcessingError(format!("invalid record batch: {err}")))?;
    Ok(Box::new(batch))
}

impl Processor for CollectionLayoutNormalizeProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let data_receivers = std::mem::take(&mut self.inputs);
        let mut input_streams = fan_in_streams(data_receivers);

        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();

        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let schema_keys: Vec<Arc<str>> = self.keys.iter().cloned().collect();
        let output_source_name = Arc::clone(&self.output_source_name);
        let shared_keys = Arc::clone(&self.keys);
        let mut resolved = self.resolved.take();
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "collection layout normalize starting");

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match control_item {
                            Some(Ok(control_signal)) => {
                                let is_terminal = control_signal.is_terminal();
                                send_control_with_backpressure(&control_output, control_signal).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (control)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "collection normalize control input");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
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
                                        match normalize_collection(
                                            &schema_keys,
                                            &output_source_name,
                                            &shared_keys,
                                            &mut resolved,
                                            collection.as_ref(),
                                            &id,
                                        ) {
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
                                    StreamData::Control(control_signal) => {
                                        let is_terminal = control_signal.is_terminal();
                                        let out = StreamData::control(control_signal);
                                        send_with_backpressure(&output, out).await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
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
                                log_broadcast_lagged(&id, skipped, "collection normalize data input");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
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
