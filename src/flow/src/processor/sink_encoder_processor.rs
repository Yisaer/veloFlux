//! Sink encoder processor combines batching and chunked sink encoding.

use crate::codec::SinkEncoder;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::batch_policy::BatchPolicy;
use crate::processor::data_metrics::EncoderMetrics;
use crate::processor::delivery_output::ChannelDeliveryOutput;
use crate::processor::encoder_driver::EncoderDriver;
use crate::processor::encoder_stage::EncoderStage;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio::time::{Duration, Instant, Sleep};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct SinkEncoderProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    encoder: Mutex<Option<Box<dyn SinkEncoder>>>,
    batch_count: Option<usize>,
    batch_duration: Option<Duration>,
    stats: Arc<ProcessorStats>,
}

impl SinkEncoderProcessor {
    pub(crate) fn validate_batch_config(
        count: Option<usize>,
        duration: Option<Duration>,
    ) -> Result<(), ProcessorError> {
        BatchPolicy::validate(count, duration, "sink encoder processor")
    }

    pub fn new(
        id: impl Into<String>,
        encoder: Box<dyn SinkEncoder>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            encoder,
            batch_count,
            batch_duration,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        encoder: Box<dyn SinkEncoder>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
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
            encoder: Mutex::new(Some(encoder)),
            batch_count,
            batch_duration,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub(crate) fn into_stage(self) -> Result<EncoderStage, ProcessorError> {
        Self::validate_batch_config(self.batch_count, self.batch_duration)?;
        let encoder = self.take_encoder()?.ok_or_else(|| {
            ProcessorError::ProcessingError("sink encoder already started".into())
        })?;
        Ok(EncoderStage::new(
            encoder,
            BatchPolicy::new(self.batch_count, self.batch_duration),
        ))
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    fn take_encoder(&self) -> Result<Option<Box<dyn SinkEncoder>>, ProcessorError> {
        self.encoder
            .lock()
            .map(|mut encoder| encoder.take())
            .map_err(|_| ProcessorError::ProcessingError("sink encoder mutex poisoned".to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    fn next_boundary(grid_origin: Instant, duration: Duration, now: Instant) -> Instant {
        BatchPolicy::next_boundary(grid_origin, duration, now)
    }
}

impl Processor for SinkEncoderProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        if let Err(err) = Self::validate_batch_config(self.batch_count, self.batch_duration) {
            return ProcessorStart::failed(spawner, err);
        }
        let encoder = match self.take_encoder() {
            Ok(Some(encoder)) => encoder,
            Ok(None) => {
                return ProcessorStart::failed(
                    spawner,
                    ProcessorError::ProcessingError(
                        "sink encoder processor already started".to_string(),
                    ),
                );
            }
            Err(err) => return ProcessorStart::failed(spawner, err),
        };
        let batch_count = self.batch_count;
        let batch_duration = self.batch_duration;
        let mut stage = EncoderDriver::new(encoder, BatchPolicy::new(batch_count, batch_duration));
        let processor_id = self.id.clone();
        let stats = Arc::clone(&self.stats);
        let metrics = EncoderMetrics::new(stats.as_ref());
        tracing::info!(processor_id = %processor_id, "sink encoder processor starting");

        let processor_span = stats.processor_span();
        ProcessorStart::ready(spawner.spawn(tracing::Instrument::instrument(async move {
            let mut timer: Option<Pin<Box<Sleep>>> = None;
            // Fixed grid origin, captured once before any sample arrives so batch
            // boundaries never re-anchor to a batch's first sample. See `next_boundary`.
            let grid_origin = Instant::now();
            let mut stage_output = ChannelDeliveryOutput {
                output: &output, capacity: channel_capacities.data, stats: &stats,
            };
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            if is_terminal {
                                if let Err(err) = async {
                                    stage.flush(&processor_id, &mut stage_output, Some(&metrics))
                                    .await?;
                                    Ok::<(), ProcessorError>(())
                                }
                                .await
                                {
                                    tracing::error!(processor_id = %processor_id, error = %err, "flush error");
                                    stats.record_error(err.to_string());
                                }
                                send_control_with_backpressure(
                                    &control_output,
                                    channel_capacities.control,
                                    control_signal,
                                )
                                .await?;
                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                return Ok(());
                            }
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    _ = async {
                        if let Some(timer) = &mut timer {
                            timer.as_mut().await;
                        }
                    }, if timer.is_some() => {
                        if let Err(err) = async {
                            stage.flush(&processor_id, &mut stage_output, Some(&metrics))
                            .await?;
                            Ok::<(), ProcessorError>(())
                        }
                        .await
                        {
                            tracing::error!(processor_id = %processor_id, error = %err, "flush error");
                            stats.record_error(err.to_string());
                        }
                        stage.schedule_timer(&mut timer, grid_origin);
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                match data {
                                    StreamData::Collection(collection) => {
                                        stats.record_collection_in(collection.num_rows() as u64);
                                        let handle_start = std::time::Instant::now();
                                        let res = stage.feed(&processor_id, collection, &mut stage_output, &mut timer, grid_origin, Some(&metrics))
                                        .await;
                                        // For synchronous processors, handle duration includes downstream send/backpressure
                                        // time (if a flush happens on the input path).
                                        stats.record_handle_duration(handle_start.elapsed());
                                        if let Err(err) = res {
                                            tracing::error!(
                                                processor_id = %processor_id,
                                                error = %err,
                                                "handle collection error"
                                            );
                                            stats.record_error(err.to_string());
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        if is_terminal {
                                            if let Err(err) = async {
                                                stage.flush(&processor_id, &mut stage_output, Some(&metrics))
                                                .await?;
                                                Ok::<(), ProcessorError>(())
                                            }
                                            .await
                                            {
                                                tracing::error!(processor_id = %processor_id, error = %err, "flush error");
                                                stats.record_error(err.to_string());
                                            }
                                        }
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                            return Ok(());
                                        }
                                        stage.schedule_timer(&mut timer, grid_origin);
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(
                                    &processor_id,
                                    skipped,
                                    "sink encoder data input",
                                );
                                continue;
                            }
                            None => {
                                if let Err(err) = async {
                                    stage.flush(&processor_id, &mut stage_output, Some(&metrics))
                                    .await?;
                                    Ok::<(), ProcessorError>(())
                                }
                                .await
                                {
                                    tracing::error!(processor_id = %processor_id, error = %err, "flush error");
                                    stats.record_error(err.to_string());
                                }
                                tracing::info!(processor_id = %processor_id, "input streams closed");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonEncoder;
    use crate::codec::SinkEncoderFactory;
    use crate::model::{batch_from_columns_simple, RecordBatch};
    use crate::planner::physical::output_layout::{
        OutputColumnLayout, OutputLayout, OutputValueRef,
    };
    use crate::planner::sink::SinkEncoderConfig;
    use crate::processor::EncodedDeliveryFlags;
    use crate::runtime::TaskSpawner;
    use datatypes::{ConcreteDatatype, Int64Type, Value};
    use tokio::runtime::Handle;
    use tokio::time::timeout;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::from_handle(Handle::current())
    }

    fn test_json_runtime() -> Box<dyn SinkEncoder> {
        test_json_runtime_with_config(SinkEncoderConfig::json())
    }

    fn test_json_runtime_with_config(config: SinkEncoderConfig) -> Box<dyn SinkEncoder> {
        let layout = Arc::new(OutputLayout::new(vec![OutputColumnLayout {
            name: Arc::from("amount"),
            data_type: ConcreteDatatype::Int64(Int64Type),
            value_ref: OutputValueRef::Message {
                message_index: 0,
                value_index: 0,
            },
        }]));
        let factory = Arc::new(JsonEncoder::new("json", &config).expect("encoder factory"))
            .with_output_layout(layout)
            .expect("bind output layout");
        factory.start_encoder().expect("encoder runtime")
    }

    async fn recv_output(
        output: &mut LinkReceiver<StreamData>,
    ) -> Result<StreamData, ProcessorError> {
        timeout(Duration::from_secs(2), output.recv())
            .await
            .map_err(|_| ProcessorError::ProcessingError("timed out waiting for output".into()))?
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    #[tokio::test]
    async fn json_encoder_emits_chunked_delivery_and_count_boundaries() {
        let runtime = test_json_runtime();
        let mut processor = SinkEncoderProcessor::new("sink_encoder", runtime, Some(2), None);
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().expect("output");
        let handle = processor.start(&test_spawner());

        for value in [1, 2, 3] {
            let batch = batch_from_columns_simple(vec![(
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(value)],
            )])
            .expect("batch");
            assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        }
        assert!(input.send(StreamData::stream_end()).is_ok());

        let mut deliveries = Vec::new();
        loop {
            let data = recv_output(&mut output).await.expect("output data");
            if data.is_terminal() {
                break;
            }
            match data {
                StreamData::EncodedDelivery { flags, bytes } => deliveries.push((flags, bytes)),
                other => panic!("expected encoded delivery, got {}", other.description()),
            }
        }
        handle.await.expect("join").expect("processor result");

        assert_eq!(deliveries.len(), 4);
        assert!(deliveries[0].0.contains(EncodedDeliveryFlags::START));
        assert!(!deliveries[0].0.contains(EncodedDeliveryFlags::END));
        assert!(deliveries[1].0.contains(EncodedDeliveryFlags::END));
        assert!(!deliveries[1].0.contains(EncodedDeliveryFlags::START));
        assert!(deliveries[2].0.contains(EncodedDeliveryFlags::START));
        assert!(!deliveries[2].0.contains(EncodedDeliveryFlags::END));
        assert!(deliveries[3].0.contains(EncodedDeliveryFlags::END));
        assert!(!deliveries[3].0.contains(EncodedDeliveryFlags::START));

        let first_payload = [deliveries[0].1.as_ref(), deliveries[1].1.as_ref()].concat();
        let second_payload = [deliveries[2].1.as_ref(), deliveries[3].1.as_ref()].concat();
        let first_json: serde_json::Value = serde_json::from_slice(&first_payload).unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_payload).unwrap();

        assert_eq!(
            first_json,
            serde_json::json!([{"amount": 1}, {"amount": 2}])
        );
        assert_eq!(second_json, serde_json::json!([{"amount": 3}]));
    }

    #[tokio::test]
    async fn batch_count_splits_multi_row_collection() {
        let runtime = test_json_runtime();
        let mut processor = SinkEncoderProcessor::new("sink_encoder", runtime, Some(2), None);
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().expect("output");
        let handle = processor.start(&test_spawner());

        let batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());

        let mut deliveries = Vec::new();
        loop {
            let data = recv_output(&mut output).await.expect("output data");
            if data.is_terminal() {
                break;
            }
            match data {
                StreamData::EncodedDelivery { flags, bytes } => deliveries.push((flags, bytes)),
                other => panic!("expected encoded delivery, got {}", other.description()),
            }
        }
        handle.await.expect("join").expect("processor result");

        assert_eq!(deliveries.len(), 3);
        assert!(deliveries[0].0.contains(EncodedDeliveryFlags::START));
        assert!(deliveries[0].0.contains(EncodedDeliveryFlags::END));
        assert!(deliveries[1].0.contains(EncodedDeliveryFlags::START));
        assert!(!deliveries[1].0.contains(EncodedDeliveryFlags::END));
        assert!(deliveries[2].0.contains(EncodedDeliveryFlags::END));
        assert!(!deliveries[2].0.contains(EncodedDeliveryFlags::START));

        let first_json: serde_json::Value = serde_json::from_slice(&deliveries[0].1).unwrap();
        let second_payload = [deliveries[1].1.as_ref(), deliveries[2].1.as_ref()].concat();
        let second_json: serde_json::Value = serde_json::from_slice(&second_payload).unwrap();
        assert_eq!(
            first_json,
            serde_json::json!([{"amount": 1}, {"amount": 2}])
        );
        assert_eq!(second_json, serde_json::json!([{"amount": 3}]));
    }

    #[tokio::test]
    async fn immediate_empty_collection_emits_single_empty_delivery() {
        let runtime = test_json_runtime();
        let mut processor = SinkEncoderProcessor::new("sink_encoder", runtime, None, None);
        let (input, input_rx) = broadcast::channel(8);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().expect("output");
        let handle = processor.start(&test_spawner());

        assert!(input
            .send(StreamData::collection(Box::new(
                RecordBatch::new(Vec::new()).expect("empty batch"),
            )))
            .is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());

        let data = recv_output(&mut output).await.expect("empty delivery");
        let StreamData::EncodedDelivery { flags, bytes } = data else {
            panic!("expected encoded delivery");
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));
        assert!(flags.contains(EncodedDeliveryFlags::END));
        assert_eq!(bytes.as_ref(), b"[]");

        let terminal = recv_output(&mut output).await.expect("terminal");
        assert!(terminal.is_terminal());
        handle.await.expect("join").expect("processor result");
    }

    #[tokio::test]
    async fn immediate_empty_collection_emits_empty_ndjson_delivery() {
        let mut props = serde_json::Map::new();
        props.insert(
            "format".to_string(),
            serde_json::Value::String("ndjson".to_string()),
        );
        let runtime = test_json_runtime_with_config(SinkEncoderConfig::new("json", props));
        let mut processor = SinkEncoderProcessor::new("sink_encoder", runtime, None, None);
        let (input, input_rx) = broadcast::channel(8);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().expect("output");
        let handle = processor.start(&test_spawner());

        assert!(input
            .send(StreamData::collection(Box::new(
                RecordBatch::new(Vec::new()).expect("empty batch"),
            )))
            .is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());

        let data = recv_output(&mut output).await.expect("empty delivery");
        let StreamData::EncodedDelivery { flags, bytes } = data else {
            panic!("expected encoded delivery");
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));
        assert!(flags.contains(EncodedDeliveryFlags::END));
        assert!(bytes.is_empty());

        let terminal = recv_output(&mut output).await.expect("terminal");
        assert!(terminal.is_terminal());
        handle.await.expect("join").expect("processor result");
    }

    #[tokio::test]
    async fn terminal_control_flushes_data_before_forwarding_control() {
        let runtime = test_json_runtime();
        let mut processor =
            SinkEncoderProcessor::new("sink_encoder", runtime, None, Some(Duration::from_secs(60)));
        let (input, input_rx) = broadcast::channel(16);
        let (control, control_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().expect("output");
        let mut control_output = processor
            .subscribe_control_output()
            .expect("control output");
        let handle = processor.start(&test_spawner());

        let first_batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("first batch");
        assert!(input
            .send(StreamData::collection(Box::new(first_batch)))
            .is_ok());

        let first = recv_output(&mut output).await.expect("first chunk");
        let StreamData::EncodedDelivery {
            flags: first_flags,
            bytes: first_bytes,
        } = first
        else {
            panic!("expected encoded delivery");
        };
        assert!(first_flags.contains(EncodedDeliveryFlags::START));
        assert!(!first_flags.contains(EncodedDeliveryFlags::END));

        let second_batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(2)],
        )])
        .expect("second batch");
        assert!(input
            .send(StreamData::collection(Box::new(second_batch)))
            .is_ok());

        let second = recv_output(&mut output).await.expect("second chunk");
        let StreamData::EncodedDelivery {
            flags: second_flags,
            bytes: second_bytes,
        } = second
        else {
            panic!("expected encoded delivery");
        };
        assert!(second_flags.is_empty());

        assert!(control
            .send(ControlSignal::Instant(
                crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 1 },
            ))
            .is_ok());

        let third = recv_output(&mut output).await.expect("final chunk");
        let StreamData::EncodedDelivery {
            flags: third_flags,
            bytes: third_bytes,
        } = third
        else {
            panic!("expected encoded delivery");
        };
        assert!(third_flags.contains(EncodedDeliveryFlags::END));

        let control_signal = timeout(Duration::from_secs(2), control_output.recv())
            .await
            .expect("timed out waiting for control")
            .expect("control output");
        assert!(control_signal.is_terminal());

        let payload = [
            first_bytes.as_ref(),
            second_bytes.as_ref(),
            third_bytes.as_ref(),
        ]
        .concat();
        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([{"amount": 1}, {"amount": 2}]));
        handle.await.expect("join").expect("processor result");
    }

    #[tokio::test]
    async fn zero_batch_duration_fails_when_started_directly() {
        let runtime = test_json_runtime();
        let mut processor =
            SinkEncoderProcessor::new("sink_encoder", runtime, None, Some(Duration::ZERO));

        let start = processor.start(&test_spawner());
        let err = start
            .handle
            .await
            .expect("join")
            .expect_err("zero batch duration should fail");
        assert!(matches!(err, ProcessorError::InvalidConfiguration(_)));
    }

    #[test]
    fn batch_duration_must_fit_supported_millisecond_range() {
        let too_small =
            SinkEncoderProcessor::validate_batch_config(None, Some(Duration::from_nanos(999)))
                .expect_err("sub-millisecond duration should be rejected");
        assert!(matches!(too_small, ProcessorError::InvalidConfiguration(_)));

        let fractional_ms =
            SinkEncoderProcessor::validate_batch_config(None, Some(Duration::from_micros(1500)))
                .expect_err("sub-millisecond precision should be rejected");
        assert!(matches!(
            fractional_ms,
            ProcessorError::InvalidConfiguration(_)
        ));

        let too_large =
            SinkEncoderProcessor::validate_batch_config(None, Some(Duration::from_secs(u64::MAX)))
                .expect_err("duration beyond u64 milliseconds should be rejected");
        assert!(matches!(too_large, ProcessorError::InvalidConfiguration(_)));
    }

    #[test]
    fn next_boundary_aligns_to_fixed_grid_and_is_right_open() {
        let d = Duration::from_secs(10);
        let origin = Instant::now();

        // Samples arriving at different offsets WITHIN the same cell [0, 10s) all
        // resolve to the SAME boundary: the grid never re-anchors to a batch's
        // first sample. This is the core of the fix — without it, each batch's
        // window started at its first sample's arrival and over-collected by one.
        for offset_ms in [1u64, 3_000, 9_999] {
            assert_eq!(
                SinkEncoderProcessor::next_boundary(
                    origin,
                    d,
                    origin + Duration::from_millis(offset_ms),
                ),
                origin + d,
                "offset {offset_ms}ms must still flush at the same grid boundary",
            );
        }
        assert_eq!(
            SinkEncoderProcessor::next_boundary(
                origin,
                d,
                origin + Duration::from_nanos(9_999_999_999),
            ),
            origin + d,
            "sub-millisecond elapsed time must not shift the fixed grid boundary",
        );

        // Subsequent cell.
        assert_eq!(
            SinkEncoderProcessor::next_boundary(origin, d, origin + Duration::from_secs(13)),
            origin + Duration::from_secs(20),
        );

        // Right-open [t, t+D): a `now` sitting exactly on a grid line opens the new
        // cell at that line, so its boundary is one full period later (the sample on
        // the line belongs to the next window, not the one that just closed).
        assert_eq!(
            SinkEncoderProcessor::next_boundary(origin, d, origin),
            origin + d,
        );
        assert_eq!(
            SinkEncoderProcessor::next_boundary(origin, d, origin + d),
            origin + Duration::from_secs(20),
        );

        // Catch-up after an idle gap that spans multiple boundaries: jump straight
        // to the next FUTURE boundary rather than replaying missed ones.
        assert_eq!(
            SinkEncoderProcessor::next_boundary(origin, d, origin + Duration::from_secs(35)),
            origin + Duration::from_secs(40),
        );
    }
}
