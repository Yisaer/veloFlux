//! DecoderProcessor - decodes StreamData::Bytes into StreamData::Collection.

use crate::codec::RecordDecoder;
use crate::eventtime::{EventtimeParseError, EventtimeTypeParser};
use crate::planner::decode_projection::DecodeProjection;
use crate::planner::physical::DataDomain;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::data_metrics::{DataMetricDomains, DataMetrics, DECODER_METRICS};
use crate::processor::{ControlSignal, Processor, ProcessorStart, ProcessorStats, StreamData};
use crate::runtime::TaskSpawner;
use crate::shared_stream::AppliedDecodeState;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Clone)]
pub struct EventtimeDecodeConfig {
    pub source_name: String,
    pub column_name: String,
    pub column_index: usize,
    pub type_key: String,
    pub parser: Arc<dyn EventtimeTypeParser>,
}

pub struct DecoderProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stage: DecoderStage,
    metric_domains: DataMetricDomains,
    stats: Arc<ProcessorStats>,
}

impl DecoderProcessor {
    pub fn new(plan_name: impl Into<String>, decoder: Arc<dyn RecordDecoder>) -> Self {
        let plan_name = plan_name.into();
        Self::with_custom_id(plan_name.clone(), decoder)
    }

    pub fn with_custom_id(id: impl Into<String>, decoder: Arc<dyn RecordDecoder>) -> Self {
        Self::with_custom_id_and_channel_capacities(id, decoder, default_channel_capacities())
    }

    pub(crate) fn with_custom_id_and_channel_capacities(
        id: impl Into<String>,
        decoder: Arc<dyn RecordDecoder>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stage: DecoderStage {
                decoder,
                shared_decode_state: None,
                decode_projection: None,
                eventtime: None,
            },
            metric_domains: DataMetricDomains::NONE
                .with_input(DataDomain::Message)
                .with_output(DataDomain::Collection),
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub(crate) fn with_shared_decode_state(
        mut self,
        state: Arc<parking_lot::RwLock<AppliedDecodeState>>,
    ) -> Self {
        self.stage.shared_decode_state = Some(state);
        self
    }

    pub fn with_decode_projection(mut self, projection: DecodeProjection) -> Self {
        self.stage.decode_projection = Some(projection);
        self
    }

    pub fn with_eventtime(mut self, eventtime: EventtimeDecodeConfig) -> Self {
        self.stage.eventtime = Some(eventtime);
        self
    }

    pub(crate) fn into_stage(self) -> DecoderStage {
        self.stage
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    pub(crate) fn set_metric_domains(&mut self, metric_domains: DataMetricDomains) {
        self.metric_domains = metric_domains;
    }
}

impl Processor for DecoderProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let stage = self.stage.clone();
        let processor_id = self.id.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        let data_metrics = DataMetrics::new(stats.as_ref(), DECODER_METRICS, self.metric_domains);
        let base_inputs = std::mem::take(&mut self.inputs);
        let mut input_streams = fan_in_streams(base_inputs);
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        tracing::info!(processor_id = %processor_id, "decoder processor starting");
        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
                            if is_terminal {
                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                tracing::info!(processor_id = %processor_id, "stopped");
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
                                log_received_data(&processor_id, &data);
                                data_metrics.record_input(stats.as_ref(), &data)?;
                                let started = std::time::Instant::now();
                                let decoded = stage.process(data, &stats);
                                stats.record_handle_duration(started.elapsed());
                                let Some(data) = decoded else { continue; };
                                let is_terminal = data.is_terminal();
                                let output_measurement = DataMetrics::measure(&data);
                                let send_res = send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    data,
                                    Some(stats.as_ref()),
                                )
                                .await;
                                send_res?;
                                data_metrics.record_output(stats.as_ref(), output_measurement)?;
                                if is_terminal {
                                    tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %processor_id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "decoder data input");
                                continue;
                            }
                            None => {
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }, processor_span)))
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }

    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>,
    {
        self.inputs.push(receiver.into());
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        self.control_inputs.push(receiver.into());
    }
}

/// Shared synchronous decoder and timestamp preparation, independent of transport.
#[derive(Clone)]
pub(crate) struct DecoderStage {
    decoder: Arc<dyn RecordDecoder>,
    shared_decode_state: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
    decode_projection: Option<DecodeProjection>,
    eventtime: Option<EventtimeDecodeConfig>,
}

impl DecoderStage {
    pub(crate) fn process(&self, data: StreamData, stats: &ProcessorStats) -> Option<StreamData> {
        let batch = match data {
            StreamData::Bytes(payload) => {
                let decoded = if let Some(projection) = &self.decode_projection {
                    self.decoder
                        .decode_with_projection(&payload, Some(projection))
                } else if let Some(state) = &self.shared_decode_state {
                    let projection = Arc::clone(&state.read().decode_projection);
                    self.decoder
                        .decode_with_projection(&payload, Some(&projection))
                } else {
                    self.decoder.decode(&payload)
                };
                match decoded {
                    Ok(batch) => batch,
                    Err(error) => {
                        stats.record_error_logged(
                            "decoder processor error",
                            format!("decode error: {error}"),
                        );
                        return None;
                    }
                }
            }
            StreamData::Collection(collection) if self.eventtime.is_some() => {
                match crate::model::RecordBatch::new(collection.rows().to_vec()) {
                    Ok(batch) => batch,
                    Err(error) => {
                        stats.record_error_logged(
                            "decoder processor error",
                            format!("eventtime batch build error: {error}"),
                        );
                        return None;
                    }
                }
            }
            other => return Some(other),
        };
        let result = apply_eventtime(batch, &self.eventtime);
        if let Some(last) = result.errors.last() {
            stats.record_error_count_logged(
                "decoder processor error",
                result.errors.len() as u64,
                last.clone(),
            );
        }
        result
            .batch
            .map(|batch| StreamData::collection(Box::new(batch)))
    }
}

struct EventtimeApplyResult {
    batch: Option<crate::model::RecordBatch>,
    errors: Vec<String>,
}

fn apply_eventtime(
    batch: crate::model::RecordBatch,
    cfg: &Option<EventtimeDecodeConfig>,
) -> EventtimeApplyResult {
    let Some(cfg) = cfg else {
        return EventtimeApplyResult {
            batch: Some(batch),
            errors: Vec::new(),
        };
    };

    let mut errors = Vec::new();
    let mut rows = batch.into_rows();
    rows.retain_mut(|tuple| match extract_timestamp(tuple, cfg) {
        Ok(ts) => {
            tuple.timestamp = ts;
            true
        }
        Err(err) => {
            errors.push(format!(
                "eventtime parse error: source={}, column={}, type={}, {}",
                cfg.source_name, cfg.column_name, cfg.type_key, err
            ));
            false
        }
    });

    if rows.is_empty() {
        return EventtimeApplyResult {
            batch: None,
            errors,
        };
    }
    match crate::model::RecordBatch::new(rows) {
        Ok(batch) => EventtimeApplyResult {
            batch: Some(batch),
            errors,
        },
        Err(err) => {
            errors.push(format!("eventtime batch build error: {err}"));
            EventtimeApplyResult {
                batch: None,
                errors,
            }
        }
    }
}

fn extract_timestamp(
    tuple: &crate::model::Tuple,
    cfg: &EventtimeDecodeConfig,
) -> Result<SystemTime, EventtimeParseError> {
    let value = tuple
        .value_by_index(cfg.source_name.as_str(), cfg.column_index)
        .ok_or_else(|| {
            EventtimeParseError::new(format!(
                "eventtime column `{}` (index={}) missing in tuple",
                cfg.column_name, cfg.column_index
            ))
        })?;
    cfg.parser.parse(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eventtime::EventtimeTypeRegistry;
    use crate::model::{Message, RecordBatch, Tuple};
    use datatypes::Value;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn apply_eventtime_updates_collection_rows() {
        let registry = EventtimeTypeRegistry::with_builtin_types();
        let parser = registry.resolve("unixtimestamp_ms").expect("parser");
        let message = Message::new(
            Arc::<str>::from("s"),
            vec![Arc::<str>::from("value"), Arc::<str>::from("event_ts")],
            vec![Value::Int64(7), Value::Int64(1_500)],
        );
        let batch = RecordBatch::new(vec![Tuple::new(vec![Arc::new(message)])]).expect("batch");

        let result = apply_eventtime(
            batch,
            &Some(EventtimeDecodeConfig {
                source_name: "s".to_string(),
                column_name: "event_ts".to_string(),
                column_index: 1,
                type_key: "unixtimestamp_ms".to_string(),
                parser,
            }),
        );

        assert!(result.errors.is_empty());
        let batch = result.batch.expect("batch");
        assert_eq!(
            batch.rows()[0].timestamp,
            UNIX_EPOCH + Duration::from_millis(1_500)
        );
    }
}
