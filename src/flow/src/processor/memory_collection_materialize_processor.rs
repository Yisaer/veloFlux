//! MemoryCollectionMaterializeProcessor - reshapes tuples for memory collection sinks.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::planner::physical::output_schema::{OutputSchema, OutputValueGetter};
use crate::planner::physical::{PhysicalMemoryCollectionMaterialize, PhysicalPlan};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use datatypes::Value;
use futures::stream::StreamExt;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct MemoryCollectionMaterializeProcessor {
    id: String,
    output_source_name: Arc<str>,
    output_schema: OutputSchema,
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
    MessageByIndex {
        msg_idx: usize,
        key_idx: usize,
        expected_source: Arc<str>,
        expected_key: Arc<str>,
    },
    Affiliate {
        column_name: Arc<str>,
    },
    /// Permanently missing for the lifetime of this processor instance.
    Missing {
        column_name: Arc<str>,
    },
}

impl MemoryCollectionMaterializeProcessor {
    pub fn new(id: impl Into<String>, spec: Arc<PhysicalMemoryCollectionMaterialize>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);

        let output_schema = spec.output_schema.clone();
        let keys: Vec<Arc<str>> = output_schema
            .columns
            .iter()
            .map(|col| Arc::clone(&col.name))
            .collect();

        Self {
            id: id.into(),
            output_source_name: Arc::<str>::from(""),
            output_schema,
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
            PhysicalPlan::MemoryCollectionMaterialize(spec) => {
                Some(Self::new(id, Arc::new(spec.clone())))
            }
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

fn resolve_getters(output_schema: &OutputSchema, sample: &Tuple) -> Vec<ResolvedGetter> {
    let mut by_source = HashMap::<&str, HashMap<&str, (usize, usize)>>::new();
    for (msg_idx, msg) in sample.messages().iter().enumerate() {
        let entry = by_source.entry(msg.source()).or_default();
        for (key_idx, (key, _)) in msg.entries().enumerate() {
            entry.entry(key).or_insert((msg_idx, key_idx));
        }
    }

    output_schema
        .columns
        .iter()
        .map(|col| match &col.getter {
            OutputValueGetter::Affiliate { column_name } => ResolvedGetter::Affiliate {
                column_name: Arc::clone(column_name),
            },
            OutputValueGetter::MessageByName {
                source_name,
                column_name,
            } => by_source
                .get(source_name.as_ref())
                .and_then(|m| m.get(column_name.as_ref()).copied())
                .map(|(msg_idx, key_idx)| ResolvedGetter::MessageByIndex {
                    msg_idx,
                    key_idx,
                    expected_source: Arc::clone(source_name),
                    expected_key: Arc::clone(column_name),
                })
                .unwrap_or_else(|| ResolvedGetter::Missing {
                    column_name: Arc::clone(&col.name),
                }),
        })
        .collect()
}

fn resolved_value(tuple: &Tuple, getter: &ResolvedGetter) -> Option<Arc<Value>> {
    match getter {
        ResolvedGetter::MessageByIndex {
            msg_idx,
            key_idx,
            expected_source,
            expected_key,
        } => tuple.messages().get(*msg_idx).and_then(|msg| {
            if msg.source() != expected_source.as_ref() {
                return None;
            }
            msg.entry_by_index(*key_idx).and_then(|(key, value)| {
                if key.as_ref() == expected_key.as_ref() {
                    Some(Arc::clone(value))
                } else {
                    None
                }
            })
        }),
        ResolvedGetter::Affiliate { column_name } => tuple
            .affiliate()
            .and_then(|aff| aff.value(column_name.as_ref()))
            .map(|v| Arc::new(v.clone())),
        ResolvedGetter::Missing { .. } => None,
    }
}

fn fallback_value(
    tuple: &Tuple,
    getter: &OutputValueGetter,
) -> Option<(Arc<Value>, ResolvedGetter)> {
    match getter {
        OutputValueGetter::Affiliate { column_name } => tuple
            .affiliate()
            .and_then(|aff| aff.value(column_name.as_ref()))
            .map(|v| {
                (
                    Arc::new(v.clone()),
                    ResolvedGetter::Affiliate {
                        column_name: Arc::clone(column_name),
                    },
                )
            }),
        OutputValueGetter::MessageByName {
            source_name,
            column_name,
        } => {
            for (msg_idx, msg) in tuple.messages().iter().enumerate() {
                if msg.source() != source_name.as_ref() {
                    continue;
                }
                if let Some((_, value)) = msg.entry_by_name(column_name.as_ref()) {
                    let Some(key_idx) = msg.entries().enumerate().find_map(|(idx, (k, _))| {
                        if k == column_name.as_ref() {
                            Some(idx)
                        } else {
                            None
                        }
                    }) else {
                        continue;
                    };
                    return Some((
                        Arc::clone(value),
                        ResolvedGetter::MessageByIndex {
                            msg_idx,
                            key_idx,
                            expected_source: Arc::clone(source_name),
                            expected_key: Arc::clone(column_name),
                        },
                    ));
                }
            }
            None
        }
    }
}

fn materialize_collection(
    output_schema: &OutputSchema,
    output_source_name: &Arc<str>,
    shared_keys: &Arc<[Arc<str>]>,
    resolved: &mut Option<Vec<ResolvedGetter>>,
    input: &dyn Collection,
    processor_id: &str,
) -> Result<Box<dyn Collection>, ProcessorError> {
    if resolved.is_none() {
        if let Some(sample) = input.rows().first() {
            *resolved = Some(resolve_getters(output_schema, sample));
        }
    }

    let mut missing = BTreeSet::<String>::new();
    let mut rows = Vec::with_capacity(input.num_rows());
    for tuple in input.rows() {
        let mut values = Vec::with_capacity(output_schema.columns.len());
        for (idx, col) in output_schema.columns.iter().enumerate() {
            let resolved_item = resolved.as_ref().and_then(|r| r.get(idx));
            let (value, missing_name) = match resolved_item {
                Some(ResolvedGetter::Missing { column_name }) => (None, column_name.as_ref()),
                Some(other) => (
                    resolved_value(tuple, other).or_else(|| {
                        // Allow re-resolving when tuple layout differs (except for permanent missing).
                        fallback_value(tuple, &col.getter).map(|(v, new_getter)| {
                            if let Some(resolved_vec) = resolved.as_mut() {
                                if let Some(slot) = resolved_vec.get_mut(idx) {
                                    *slot = new_getter;
                                }
                            }
                            v
                        })
                    }),
                    col.name.as_ref(),
                ),
                None => (
                    fallback_value(tuple, &col.getter).map(|(v, _)| v),
                    col.name.as_ref(),
                ),
            };

            if value.is_none() {
                missing.insert(missing_name.to_string());
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
            "memory collection materialize filled NULL for missing columns"
        );
    }

    let batch = RecordBatch::new(rows)
        .map_err(|err| ProcessorError::ProcessingError(format!("invalid record batch: {err}")))?;
    Ok(Box::new(batch))
}

impl Processor for MemoryCollectionMaterializeProcessor {
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
        let output_schema = self.output_schema.clone();
        let output_source_name = Arc::clone(&self.output_source_name);
        let shared_keys = Arc::clone(&self.keys);
        let mut resolved = self.resolved.take();
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "memory collection materialize starting");

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
                                log_broadcast_lagged(&id, skipped, "memory materialize control input");
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
                                        match materialize_collection(
                                            &output_schema,
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
                                        send_with_backpressure(&output, StreamData::control(control_signal)).await?;
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
                                log_broadcast_lagged(&id, skipped, "memory materialize data input");
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
