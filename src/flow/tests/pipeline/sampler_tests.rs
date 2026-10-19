//! Integration tests for sampler behavior within the pipeline test suite.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::codec::MergerOutputKind;
use flow::planner::sink::{
    CommonSinkProps, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
    SinkEncoderConfig, SinkOutputConfig,
};
use flow::processor::sampler_processor::{MergerConfig, PackerProps};
use flow::processor::{
    EncodedDeliveryFlags, ProcessorStatsHandle, SamplerConfig, SamplingStrategy, StreamData,
};
use flow::{CodecError, FlowInstance, Merger};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

async fn create_stream_with_sampler(
    instance: &FlowInstance,
    stream_name: &str,
    columns: &[&str],
    sampler_config: SamplerConfig,
) {
    let schema = Schema::new(
        columns
            .iter()
            .map(|column| {
                ColumnSchema::new(
                    stream_name.to_string(),
                    (*column).to_string(),
                    ConcreteDatatype::Int64(Int64Type),
                )
            })
            .collect(),
    );
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    )
    .with_sampler(sampler_config);
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream with sampler");
}

fn build_result_sink() -> PipelineSink {
    build_result_sink_with_config(SinkOutputConfig::default(), CommonSinkProps::default())
}

fn build_result_sink_with_output(output: SinkOutputConfig) -> PipelineSink {
    build_result_sink_with_config(output, CommonSinkProps::default())
}

fn build_result_sink_with_config(
    output: SinkOutputConfig,
    common: CommonSinkProps,
) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_sink",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    PipelineSink::new("output_sink", connector)
        .with_forward_to_result(true)
        .with_common_props(common)
        .with_output(output)
}

#[derive(Default)]
struct JsonObjectMerger {
    fields: JsonMap<String, JsonValue>,
}

impl Merger for JsonObjectMerger {
    fn merge(&mut self, data: &[u8]) -> Result<(), CodecError> {
        let value: JsonValue = serde_json::from_slice(data)?;
        let object = value.as_object().ok_or_else(|| {
            CodecError::Other("json object merger expects object payloads".into())
        })?;
        for (key, value) in object {
            self.fields.insert(key.clone(), value.clone());
        }
        Ok(())
    }

    fn trigger(&mut self) -> Result<Option<Vec<u8>>, CodecError> {
        if self.fields.is_empty() {
            return Ok(None);
        }

        let merged = JsonValue::Object(std::mem::take(&mut self.fields));
        Ok(Some(serde_json::to_vec(&merged)?))
    }
}

fn packer_strategy(merger_type: &str) -> SamplingStrategy {
    SamplingStrategy::Packer {
        props: PackerProps {
            merger: MergerConfig {
                merger_type: merger_type.to_string(),
                props: JsonMap::new(),
            },
        },
    }
}

struct JsonOutput {
    receiver: mpsc::Receiver<StreamData>,
    delivery: Vec<u8>,
    active_delivery: bool,
    pending: std::collections::VecDeque<JsonValue>,
}

impl JsonOutput {
    fn new(receiver: mpsc::Receiver<StreamData>) -> Self {
        Self {
            receiver,
            delivery: Vec::new(),
            active_delivery: false,
            pending: Default::default(),
        }
    }

    fn append_encoded_delivery(
        &mut self,
        flags: EncodedDeliveryFlags,
        bytes: bytes::Bytes,
    ) -> Option<JsonValue> {
        if flags.contains(EncodedDeliveryFlags::ABORT) {
            panic!("encoded delivery aborted");
        }
        if flags.contains(EncodedDeliveryFlags::START) {
            self.delivery.clear();
            self.active_delivery = true;
        }
        if !self.active_delivery {
            panic!("encoded delivery chunk without START");
        }
        self.delivery.extend_from_slice(&bytes);
        if flags.contains(EncodedDeliveryFlags::END) {
            self.active_delivery = false;
            return Some(serde_json::from_slice(&self.delivery).expect("invalid JSON payload"));
        }
        None
    }
}

async fn recv_next_json(output: &mut JsonOutput, timeout_duration: Duration) -> JsonValue {
    if let Some(value) = output.pending.pop_front() {
        return value;
    }
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.receiver.recv())
            .await
            .expect("timeout waiting for sampler output")
            .expect("sampler output channel closed");
        match item {
            StreamData::EncodedDelivery { flags, bytes } => {
                if let Some(json) = output.append_encoded_delivery(flags, bytes) {
                    return json;
                }
            }
            StreamData::Control(_) | StreamData::Watermark(_) => continue,
            StreamData::Error(err) => panic!("pipeline returned error: {}", err.message),
            other => panic!("unexpected stream data: {}", other.description()),
        }
    }
}

async fn assert_no_output(output: &mut JsonOutput, timeout_duration: Duration) {
    assert!(
        output.pending.is_empty(),
        "unexpected buffered sampler output"
    );
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, output.receiver.recv()).await {
            Err(_) | Ok(None) => return,
            Ok(Some(StreamData::Control(_))) | Ok(Some(StreamData::Watermark(_))) => continue,
            Ok(Some(StreamData::EncodedDelivery { flags, bytes })) => {
                if let Some(json) = output.append_encoded_delivery(flags, bytes) {
                    panic!("unexpected sampler output: {json}");
                }
            }
            Ok(Some(StreamData::Error(err))) => panic!("pipeline returned error: {}", err.message),
            Ok(Some(other)) => panic!("unexpected sampler output: {}", other.description()),
        }
    }
}

async fn wait_for_sampler_counter(handle: &ProcessorStatsHandle, name: &str, expected: u64) {
    for _ in 0..100 {
        let actual = handle
            .snapshot()
            .stats
            .custom
            .get(name)
            .copied()
            .unwrap_or_default();
        if actual >= expected {
            return;
        }
        tokio::task::yield_now().await;
    }

    let actual = handle
        .snapshot()
        .stats
        .custom
        .get(name)
        .copied()
        .unwrap_or_default();
    panic!("sampler counter {name} did not reach {expected}, actual={actual}");
}

// coverage-covers: processor.sampler.strategy
#[tokio::test(start_paused = true)]
async fn sampler_latest_emits_only_last_value_per_interval() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let interval = Duration::from_millis(100);
    create_stream_with_sampler(
        &instance,
        "latest_stream",
        &["x"],
        SamplerConfig::new(interval),
    )
    .await;

    let mut pipeline = instance
        .build_pipeline("SELECT x FROM latest_stream", vec![build_result_sink()])
        .expect("build pipeline");
    let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

    pipeline.start().await.expect("start pipeline");
    tokio::task::yield_now().await;

    for value in 1..=5 {
        let payload = format!(r#"{{"x": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("latest_stream", StreamData::bytes(payload))
            .await
            .expect("send data to sampler");
    }

    let sampler_handle = pipeline
        .processor_stats()
        .into_iter()
        .find(|handle| handle.processor_id.starts_with("Region_"))
        .expect("sampler stats should exist");
    for _ in 0..20 {
        if sampler_handle.snapshot().stats.custom["messages_in"] == 5 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        sampler_handle.snapshot().stats.custom["messages_in"],
        5,
        "sampler should consume all inputs before the sampling tick"
    );

    tokio::time::advance(interval).await;
    tokio::task::yield_now().await;

    let actual = recv_next_json(&mut output, Duration::from_millis(10)).await;
    assert_eq!(actual, serde_json::json!([{"x": 5}]));
    assert_no_output(&mut output, Duration::from_millis(100)).await;

    timeout(
        Duration::from_secs(2),
        pipeline.close(Duration::from_secs(1)),
    )
    .await
    .expect("close pipeline should not hang")
    .expect("close pipeline");
}

#[tokio::test]
async fn sampler_latest_flushes_buffer_on_close() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    create_stream_with_sampler(
        &instance,
        "flush_stream",
        &["x"],
        SamplerConfig::new(Duration::from_secs(1)),
    )
    .await;

    let mut pipeline = instance
        .build_pipeline("SELECT x FROM flush_stream", vec![build_result_sink()])
        .expect("build pipeline");
    let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

    pipeline.start().await.expect("start pipeline");
    tokio::time::sleep(Duration::from_millis(50)).await;

    for value in [10, 20] {
        let payload = format!(r#"{{"x": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("flush_stream", StreamData::bytes(payload))
            .await
            .expect("send data to sampler");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_no_output(&mut output, Duration::from_millis(100)).await;

    let actual = {
        let close = pipeline.close(Duration::from_secs(1));
        tokio::pin!(close);
        let mut close_done = false;
        let actual = tokio::select! {
            close_result = &mut close => {
                close_done = true;
                close_result.expect("close pipeline");
                recv_next_json(&mut output, Duration::from_millis(500)).await
            }
            actual = recv_next_json(&mut output, Duration::from_secs(2)) => actual,
        };
        if !close_done {
            timeout(Duration::from_secs(2), &mut close)
                .await
                .expect("close pipeline should not hang")
                .expect("close pipeline");
        }
        actual
    };
    assert_eq!(actual, serde_json::json!([{"x": 20}]));
}

#[tokio::test]
async fn sampler_latest_before_filter_and_projection() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let interval = Duration::from_millis(200);
    create_stream_with_sampler(
        &instance,
        "filtered_stream",
        &["a"],
        SamplerConfig::new(interval),
    )
    .await;

    let mut pipeline = instance
        .build_pipeline(
            "SELECT a AS sampled FROM filtered_stream WHERE a > 10",
            vec![build_result_sink()],
        )
        .expect("build pipeline");
    let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

    pipeline.start().await.expect("start pipeline");
    tokio::time::sleep(Duration::from_millis(50)).await;

    for value in [12, 5] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("filtered_stream", StreamData::bytes(payload))
            .await
            .expect("send data to sampler");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // A sampled value that fails the WHERE predicate now produces no output at all
    // (the filter emits nothing instead of an empty collection), so nothing is sent
    // downstream for the non-passing sample.
    assert_no_output(&mut output, Duration::from_millis(400)).await;

    pipeline
        .send_stream_data(
            "filtered_stream",
            StreamData::bytes(br#"{"a": 20}"#.to_vec()),
        )
        .await
        .expect("send passing data to sampler");

    let actual = recv_next_json(&mut output, Duration::from_millis(500)).await;
    assert_eq!(actual, serde_json::json!([{"sampled": 20}]));

    timeout(
        Duration::from_secs(2),
        pipeline.close(Duration::from_secs(1)),
    )
    .await
    .expect("close pipeline should not hang")
    .expect("close pipeline");
}

// coverage-covers: processor.sampler.strategy, sink.output.omit_if_empty
#[tokio::test]
async fn sampler_latest_with_omit_if_empty_suppresses_empty_windows() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let interval = Duration::from_millis(200);
    create_stream_with_sampler(
        &instance,
        "omit_sampler_stream",
        &["a"],
        SamplerConfig::new(interval),
    )
    .await;

    let mut pipeline = instance
        .build_pipeline(
            "SELECT a AS sampled FROM omit_sampler_stream WHERE a > 10",
            vec![build_result_sink_with_output(
                SinkOutputConfig::default().with_omit_if_empty(true),
            )],
        )
        .expect("build pipeline");
    let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

    pipeline.start().await.expect("start pipeline");
    tokio::time::sleep(Duration::from_millis(50)).await;

    for value in [12, 5] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("omit_sampler_stream", StreamData::bytes(payload))
            .await
            .expect("send data to sampler");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_no_output(&mut output, Duration::from_millis(400)).await;

    pipeline
        .send_stream_data(
            "omit_sampler_stream",
            StreamData::bytes(br#"{"a": 20}"#.to_vec()),
        )
        .await
        .expect("send passing data to sampler");

    let actual = recv_next_json(&mut output, Duration::from_millis(500)).await;
    assert_eq!(actual, serde_json::json!([{"sampled": 20}]));

    timeout(
        Duration::from_secs(2),
        pipeline.close(Duration::from_secs(1)),
    )
    .await
    .expect("close pipeline should not hang")
    .expect("close pipeline");
}

// coverage-covers: processor.sampler.strategy, sink.output.row_diff
#[tokio::test]
async fn sampler_latest_before_row_diff_delta_output() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let interval = Duration::from_millis(200);
    create_stream_with_sampler(
        &instance,
        "row_diff_stream",
        &["a"],
        SamplerConfig::new(interval),
    )
    .await;

    let mut pipeline = instance
        .build_pipeline(
            "SELECT a AS sampled FROM row_diff_stream",
            vec![build_result_sink_with_output(SinkOutputConfig::delta())],
        )
        .expect("build pipeline");
    let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

    pipeline.start().await.expect("start pipeline");
    tokio::time::sleep(Duration::from_millis(50)).await;

    for value in [1, 2] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("row_diff_stream", StreamData::bytes(payload))
            .await
            .expect("send first-interval data to sampler");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let first = recv_next_json(&mut output, Duration::from_millis(500)).await;
    assert_eq!(first, serde_json::json!([{"sampled": 2}]));

    for _ in 0..2 {
        pipeline
            .send_stream_data(
                "row_diff_stream",
                StreamData::bytes(br#"{"a": 2}"#.to_vec()),
            )
            .await
            .expect("send repeated second-interval data to sampler");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let second = recv_next_json(&mut output, Duration::from_millis(500)).await;
    assert_eq!(
        second,
        serde_json::json!([{}]),
        "row diff should compare against the sampled output sequence, not raw interval inputs"
    );

    pipeline
        .send_stream_data(
            "row_diff_stream",
            StreamData::bytes(br#"{"a": 3}"#.to_vec()),
        )
        .await
        .expect("send third-interval data to sampler");

    let third = recv_next_json(&mut output, Duration::from_millis(500)).await;
    assert_eq!(third, serde_json::json!([{"sampled": 3}]));

    timeout(
        Duration::from_secs(2),
        pipeline.close(Duration::from_secs(1)),
    )
    .await
    .expect("close pipeline should not hang")
    .expect("close pipeline");
}

// coverage-covers: processor.sampler.strategy, stream.window.count, sink.output.row_diff
#[tokio::test]
async fn sampler_latest_before_streaming_aggregation_then_delta_output() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let interval = Duration::from_millis(200);
    create_stream_with_sampler(
        &instance,
        "agg_sampler_stream",
        &["a"],
        SamplerConfig::new(interval),
    )
    .await;

    let mut pipeline = instance
        .build_pipeline(
            "SELECT sum(a) AS total FROM agg_sampler_stream GROUP BY countwindow(2)",
            vec![build_result_sink_with_output(SinkOutputConfig::delta())],
        )
        .expect("build pipeline");
    let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

    pipeline.start().await.expect("start pipeline");
    tokio::time::sleep(Duration::from_millis(50)).await;

    for value in [1, 2] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("agg_sampler_stream", StreamData::bytes(payload))
            .await
            .expect("send first sampled interval");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_no_output(&mut output, Duration::from_millis(400)).await;

    for value in [3, 4] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("agg_sampler_stream", StreamData::bytes(payload))
            .await
            .expect("send second sampled interval");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let first = recv_next_json(&mut output, Duration::from_millis(500)).await;
    assert_eq!(first, serde_json::json!([{"total": 6}]));

    for value in [1, 2] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("agg_sampler_stream", StreamData::bytes(payload))
            .await
            .expect("send third sampled interval");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_no_output(&mut output, Duration::from_millis(400)).await;

    for value in [3, 4] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("agg_sampler_stream", StreamData::bytes(payload))
            .await
            .expect("send fourth sampled interval");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let second = recv_next_json(&mut output, Duration::from_millis(500)).await;
    assert_eq!(
        second,
        serde_json::json!([{}]),
        "delta output should compare successive aggregated rows built from sampled intervals"
    );

    for value in [0, 0] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("agg_sampler_stream", StreamData::bytes(payload))
            .await
            .expect("send fifth sampled interval");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_no_output(&mut output, Duration::from_millis(400)).await;

    for value in [5, 5] {
        let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("agg_sampler_stream", StreamData::bytes(payload))
            .await
            .expect("send sixth sampled interval");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let third = recv_next_json(&mut output, Duration::from_millis(500)).await;
    assert_eq!(third, serde_json::json!([{"total": 5}]));

    timeout(
        Duration::from_secs(2),
        pipeline.close(Duration::from_secs(1)),
    )
    .await
    .expect("close pipeline should not hang")
    .expect("close pipeline");
}

// coverage-covers: processor.sampler.strategy, stream.window.count, sink.output.batching
#[tokio::test(start_paused = true)]
async fn sampler_latest_before_streaming_aggregation_then_batched_output() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let interval = Duration::from_millis(200);
    create_stream_with_sampler(
        &instance,
        "agg_batch_sampler_stream",
        &["a"],
        SamplerConfig::new(interval),
    )
    .await;

    let mut pipeline = instance
        .build_pipeline(
            "SELECT sum(a) AS total FROM agg_batch_sampler_stream GROUP BY countwindow(2)",
            vec![build_result_sink_with_config(
                SinkOutputConfig::default(),
                CommonSinkProps {
                    batch_count: Some(2),
                    batch_duration: None,
                },
            )],
        )
        .expect("build pipeline");
    let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

    pipeline.start().await.expect("start pipeline");
    tokio::task::yield_now().await;
    tokio::time::advance(interval).await;
    tokio::task::yield_now().await;

    let sampler_handle = pipeline
        .processor_stats()
        .iter()
        .find(|handle| handle.processor_id.starts_with("Region_"))
        .cloned()
        .expect("sampler stats should exist");

    macro_rules! send_sample_pair {
        ($first:expr, $second:expr, $input_count:expr, $output_count:expr, $ordinal:literal) => {{
            for value in [$first, $second] {
                let payload = format!(r#"{{"a": {value}}}"#).into_bytes();
                pipeline
                    .send_stream_data("agg_batch_sampler_stream", StreamData::bytes(payload))
                    .await
                    .expect(concat!("send ", $ordinal, " sampled interval"));
            }
            wait_for_sampler_counter(&sampler_handle, "messages_in", $input_count).await;
            tokio::time::advance(interval).await;
            // A data-lane fence follows the due timer through the same region.
            // Preserve complete encoded outputs for the assertions below.
            let fence = flow::processor::ControlSignal::Barrier(
                flow::processor::BarrierControlSignalKind::SyncTest.with_id($output_count),
            );
            pipeline
                .send_stream_data(
                    "agg_batch_sampler_stream",
                    StreamData::control(fence.clone()),
                )
                .await
                .unwrap();
            loop {
                let data = output.receiver.recv().await.expect("fence output");
                match data {
                    StreamData::Control(signal) if signal == fence => break,
                    StreamData::EncodedDelivery { flags, bytes } => {
                        if let Some(value) = output.append_encoded_delivery(flags, bytes) {
                            output.pending.push_back(value);
                        }
                    }
                    StreamData::Watermark(_) => {}
                    data => panic!("unexpected fence output: {}", data.description()),
                }
            }
        }};
    }

    send_sample_pair!(1, 2, 2, 1, "first");
    send_sample_pair!(3, 4, 4, 2, "second");
    send_sample_pair!(1, 2, 6, 3, "third");
    send_sample_pair!(3, 4, 8, 4, "fourth");

    let first = recv_next_json(&mut output, Duration::from_secs(1)).await;
    assert_eq!(first, serde_json::json!([{"total": 6}, {"total": 6}]));

    send_sample_pair!(0, 0, 10, 5, "fifth");
    send_sample_pair!(5, 5, 12, 6, "sixth");
    send_sample_pair!(0, 0, 14, 7, "seventh");
    send_sample_pair!(5, 5, 16, 8, "eighth");

    let second = recv_next_json(&mut output, Duration::from_secs(1)).await;
    assert_eq!(
        second,
        serde_json::json!([{"total": 5}, {"total": 5}]),
        "streaming encoder batching should emit the second complete batch"
    );

    timeout(
        Duration::from_secs(2),
        pipeline.close(Duration::from_secs(1)),
    )
    .await
    .expect("close pipeline should not hang")
    .expect("close pipeline");
}

#[tokio::test]
async fn sampler_packer_emits_merged_payload_once_per_interval() {
    for inline in [false, true] {
        let instance = FlowInstance::new(
            flow::instance::FlowInstanceOptions::shared_current_runtime("default", None),
        )
        .expect("create flow instance");
        if inline {
            instance
                .merger_registry()
                .register_inline_with_schema_artifact(
                    "json_object_merge",
                    MergerOutputKind::Bytes,
                    |_props, _schema, _artifact| Ok(Box::new(JsonObjectMerger::default())),
                );
        } else {
            instance.merger_registry().register(
                "json_object_merge",
                MergerOutputKind::Bytes,
                |_props, _schema| Ok(Box::new(JsonObjectMerger::default())),
            );
        }
        create_stream_with_sampler(
            &instance,
            "packer_stream",
            &["x", "y"],
            SamplerConfig::new(Duration::from_millis(200))
                .with_strategy(packer_strategy("json_object_merge")),
        )
        .await;

        let sql = "SELECT x, y FROM packer_stream";
        let definition = flow::PipelineDefinition::new("packer_contract", sql, vec![]);
        let explain = instance
            .explain_pipeline(flow::ExplainPipelineTarget::Definition(&definition))
            .unwrap();
        println!(
            "SQL: {sql}; inline merger={inline}\n{}",
            explain.to_pretty_string()
        );
        let execution = explain.execution_to_json();
        let sampler = execution["stages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|stage| {
                stage["name"]
                    .as_str()
                    .unwrap()
                    .starts_with("PhysicalSampler_")
            })
            .unwrap();
        assert_eq!(
            sampler["class"],
            if inline { "engine" } else { "opaque_function" }
        );
        let region = execution["regions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|region| {
                region["stages"]
                    .as_array()
                    .unwrap()
                    .contains(&sampler["index"])
            })
            .unwrap();
        assert_eq!(
            region["stages"].as_array().unwrap().len(),
            if inline { 3 } else { 1 }
        );
        let mut pipeline = instance
            .build_pipeline("SELECT x, y FROM packer_stream", vec![build_result_sink()])
            .expect("build pipeline");
        let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

        pipeline.start().await.expect("start pipeline");
        tokio::time::sleep(Duration::from_millis(50)).await;

        for payload in [
            r#"{"x": 1}"#.as_bytes(),
            r#"{"y": 2}"#.as_bytes(),
            r#"{"x": 3}"#.as_bytes(),
        ] {
            pipeline
                .send_stream_data("packer_stream", StreamData::bytes(payload.to_vec()))
                .await
                .expect("send data to packer sampler");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let actual = recv_next_json(&mut output, Duration::from_millis(500)).await;
        assert_eq!(actual, serde_json::json!([{"x": 3, "y": 2}]));
        assert_no_output(&mut output, Duration::from_millis(100)).await;

        timeout(
            Duration::from_secs(2),
            pipeline.close(Duration::from_secs(1)),
        )
        .await
        .expect("close pipeline should not hang")
        .expect("close pipeline");
    }
}

#[tokio::test]
async fn sampler_packer_flushes_buffer_on_close() {
    for inline in [false, true] {
        let instance = FlowInstance::new(
            flow::instance::FlowInstanceOptions::shared_current_runtime("default", None),
        )
        .expect("create flow instance");
        if inline {
            instance
                .merger_registry()
                .register_inline_with_schema_artifact(
                    "json_object_merge",
                    MergerOutputKind::Bytes,
                    |_props, _schema, _artifact| Ok(Box::new(JsonObjectMerger::default())),
                );
        } else {
            instance.merger_registry().register(
                "json_object_merge",
                MergerOutputKind::Bytes,
                |_props, _schema| Ok(Box::new(JsonObjectMerger::default())),
            );
        }
        create_stream_with_sampler(
            &instance,
            "packer_flush_stream",
            &["x", "y"],
            SamplerConfig::new(Duration::from_secs(1))
                .with_strategy(packer_strategy("json_object_merge")),
        )
        .await;

        let mut pipeline = instance
            .build_pipeline(
                "SELECT x, y FROM packer_flush_stream",
                vec![build_result_sink()],
            )
            .expect("build pipeline");
        let mut output = JsonOutput::new(pipeline.take_output().expect("take output receiver"));

        pipeline.start().await.expect("start pipeline");
        tokio::time::sleep(Duration::from_millis(50)).await;

        for payload in [r#"{"x": 7}"#.as_bytes(), r#"{"y": 9}"#.as_bytes()] {
            pipeline
                .send_stream_data("packer_flush_stream", StreamData::bytes(payload.to_vec()))
                .await
                .expect("send data to packer sampler");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_no_output(&mut output, Duration::from_millis(100)).await;

        let actual = {
            let close = pipeline.close(Duration::from_secs(1));
            tokio::pin!(close);
            let mut close_done = false;
            let actual = tokio::select! {
                close_result = &mut close => {
                    close_done = true;
                    close_result.expect("close pipeline");
                    recv_next_json(&mut output, Duration::from_millis(500)).await
                }
                actual = recv_next_json(&mut output, Duration::from_secs(2)) => actual,
            };
            if !close_done {
                timeout(Duration::from_secs(2), &mut close)
                    .await
                    .expect("close pipeline should not hang")
                    .expect("close pipeline");
            }
            actual
        };
        assert_eq!(actual, serde_json::json!([{"x": 7, "y": 9}]));
    }
}
