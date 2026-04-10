//! Integration tests for shared stream dynamic decode lifecycle behavior.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{MemorySinkProps, PipelineDefinition};
use flow::FlowInstance;
use flow::{CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use serde_json::json;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

use super::common::{make_memory_topics, recv_next_json};

async fn install_shared_mock_json_stream(instance: &FlowInstance, stream_name: &str) {
    let schema = Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "c".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]);
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, true)
        .await
        .expect("create shared mock stream");
}

fn declare_output_topic(instance: &FlowInstance, output_topic: &str) {
    instance
        .declare_memory_topic(
            output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");
}

fn create_memory_sink_pipeline(
    instance: &FlowInstance,
    pipeline_id: &str,
    sql: &str,
    output_topic: &str,
) {
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.to_string())),
    );
    let pipeline = PipelineDefinition::new(pipeline_id.to_string(), sql, vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
}

async fn wait_for_shared_stream_subscriber_count(
    instance: &FlowInstance,
    stream_name: &str,
    expected_subscriber_count: usize,
    timeout_duration: Duration,
) {
    let deadline = Instant::now() + timeout_duration;
    loop {
        let info = instance
            .get_stream(stream_name)
            .await
            .expect("get shared stream")
            .shared_info
            .expect("shared stream info");
        if info.subscriber_count == expected_subscriber_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "shared stream subscriber_count did not converge before timeout: expected={} actual={} decoding_columns={:?}",
            expected_subscriber_count,
            info.subscriber_count,
            info.decoding_columns
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_shared_stream_decoding_columns(
    instance: &FlowInstance,
    stream_name: &str,
    expected_subscriber_count: usize,
    expected_decoding_columns: &[&str],
    timeout_duration: Duration,
) {
    let expected_decoding_columns = expected_decoding_columns
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let deadline = Instant::now() + timeout_duration;
    loop {
        let info = instance
            .get_stream(stream_name)
            .await
            .expect("get shared stream")
            .shared_info
            .expect("shared stream info");
        if info.subscriber_count == expected_subscriber_count
            && info.decoding_columns == expected_decoding_columns
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "shared stream decode state did not converge before timeout: expected_subscribers={} actual_subscribers={} expected_decoding_columns={expected_decoding_columns:?} actual_decoding_columns={:?}",
            expected_subscriber_count,
            info.subscriber_count,
            info.decoding_columns
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn stop_and_delete_pipeline(instance: &FlowInstance, pipeline_id: &str) {
    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

#[tokio::test]
async fn shared_stream_new_consumer_waits_for_required_columns_before_first_tuple() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_readiness";
    install_shared_mock_json_stream(&instance, stream_name).await;

    let (_, output_topic_a) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_new_consumer_waits_for_required_columns_before_first_tuple_a",
    );
    let (_, output_topic_c) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_new_consumer_waits_for_required_columns_before_first_tuple_c",
    );
    declare_output_topic(&instance, &output_topic_a);
    declare_output_topic(&instance, &output_topic_c);

    let mut output_a = instance
        .open_memory_subscribe_bytes(&output_topic_a)
        .expect("subscribe pipeline a output");
    let mut output_c = instance
        .open_memory_subscribe_bytes(&output_topic_c)
        .expect("subscribe pipeline c output");

    let pipeline_a_id = "shared_stream_readiness_pipeline_a";
    let pipeline_c_id = "shared_stream_readiness_pipeline_c";
    create_memory_sink_pipeline(
        &instance,
        pipeline_a_id,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic_a,
    );
    create_memory_sink_pipeline(
        &instance,
        pipeline_c_id,
        &format!("SELECT c FROM {stream_name}"),
        &output_topic_c,
    );

    instance
        .start_pipeline(pipeline_a_id)
        .expect("start pipeline a");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .start_pipeline(pipeline_c_id)
        .expect("start pipeline c");
    wait_for_shared_stream_subscriber_count(&instance, stream_name, 2, Duration::from_secs(5))
        .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":1,"b":2,"c":3}"#.as_ref())
        .await
        .expect("send shared mock payload");

    let actual_a = recv_next_json(&mut output_a, Duration::from_secs(5)).await;
    let actual_c = recv_next_json(&mut output_c, Duration::from_secs(5)).await;
    assert_eq!(actual_a, json!([{"a": 1}]));
    assert_eq!(
        actual_c,
        json!([{"c": 3}]),
        "new consumer should not observe initial NULL values before required columns are applied",
    );

    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "c"],
        Duration::from_secs(5),
    )
    .await;

    stop_and_delete_pipeline(&instance, pipeline_c_id).await;
    stop_and_delete_pipeline(&instance, pipeline_a_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

#[tokio::test]
async fn shared_stream_required_columns_shrink_after_consumer_stop() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_shrink";
    install_shared_mock_json_stream(&instance, stream_name).await;

    let (_, output_topic_a) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_required_columns_shrink_after_consumer_stop_a",
    );
    let (_, output_topic_b) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_required_columns_shrink_after_consumer_stop_b",
    );
    declare_output_topic(&instance, &output_topic_a);
    declare_output_topic(&instance, &output_topic_b);

    let mut output_a = instance
        .open_memory_subscribe_bytes(&output_topic_a)
        .expect("subscribe pipeline a output");
    let mut output_b = instance
        .open_memory_subscribe_bytes(&output_topic_b)
        .expect("subscribe pipeline b output");

    let pipeline_a_id = "shared_stream_shrink_pipeline_a";
    let pipeline_b_id = "shared_stream_shrink_pipeline_b";
    create_memory_sink_pipeline(
        &instance,
        pipeline_a_id,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic_a,
    );
    create_memory_sink_pipeline(
        &instance,
        pipeline_b_id,
        &format!("SELECT b FROM {stream_name}"),
        &output_topic_b,
    );

    instance
        .start_pipeline(pipeline_a_id)
        .expect("start pipeline a");
    instance
        .start_pipeline(pipeline_b_id)
        .expect("start pipeline b");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "b"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":10,"b":20,"c":30}"#.as_ref())
        .await
        .expect("send shared mock payload");

    let actual_a = recv_next_json(&mut output_a, Duration::from_secs(5)).await;
    let actual_b = recv_next_json(&mut output_b, Duration::from_secs(5)).await;
    assert_eq!(actual_a, json!([{"a": 10}]));
    assert_eq!(actual_b, json!([{"b": 20}]));

    instance
        .stop_pipeline(
            pipeline_b_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop pipeline b");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;
    instance
        .delete_pipeline(pipeline_b_id)
        .await
        .expect("delete pipeline b");

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":11,"b":21,"c":31}"#.as_ref())
        .await
        .expect("send post-shrink shared mock payload");

    let actual_a_after_shrink = recv_next_json(&mut output_a, Duration::from_secs(5)).await;
    assert_eq!(actual_a_after_shrink, json!([{"a": 11}]));

    stop_and_delete_pipeline(&instance, pipeline_a_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

#[tokio::test]
async fn shared_stream_wildcard_consumer_forces_full_decode_until_it_stops() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_wildcard";
    install_shared_mock_json_stream(&instance, stream_name).await;

    let (_, output_topic_a) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_wildcard_consumer_forces_full_decode_until_it_stops_a",
    );
    let (_, output_topic_all) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_wildcard_consumer_forces_full_decode_until_it_stops_all",
    );
    declare_output_topic(&instance, &output_topic_a);
    declare_output_topic(&instance, &output_topic_all);

    let mut output_a = instance
        .open_memory_subscribe_bytes(&output_topic_a)
        .expect("subscribe pipeline a output");
    let mut output_all = instance
        .open_memory_subscribe_bytes(&output_topic_all)
        .expect("subscribe wildcard output");

    let pipeline_a_id = "shared_stream_wildcard_pipeline_a";
    let pipeline_all_id = "shared_stream_wildcard_pipeline_all";
    create_memory_sink_pipeline(
        &instance,
        pipeline_a_id,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic_a,
    );
    create_memory_sink_pipeline(
        &instance,
        pipeline_all_id,
        &format!("SELECT * FROM {stream_name}"),
        &output_topic_all,
    );

    instance
        .start_pipeline(pipeline_a_id)
        .expect("start pipeline a");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .start_pipeline(pipeline_all_id)
        .expect("start wildcard pipeline");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "b", "c"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":1,"b":2,"c":3}"#.as_ref())
        .await
        .expect("send wildcard-phase shared mock payload");

    let actual_a = recv_next_json(&mut output_a, Duration::from_secs(5)).await;
    let actual_all = recv_next_json(&mut output_all, Duration::from_secs(5)).await;
    assert_eq!(actual_a, json!([{"a": 1}]));
    assert_eq!(actual_all, json!([{"a": 1, "b": 2, "c": 3}]));

    instance
        .stop_pipeline(
            pipeline_all_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop wildcard pipeline");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;
    instance
        .delete_pipeline(pipeline_all_id)
        .await
        .expect("delete wildcard pipeline");

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":4,"b":5,"c":6}"#.as_ref())
        .await
        .expect("send post-wildcard shared mock payload");

    let actual_a_after_fallback = recv_next_json(&mut output_a, Duration::from_secs(5)).await;
    assert_eq!(actual_a_after_fallback, json!([{"a": 4}]));

    stop_and_delete_pipeline(&instance, pipeline_a_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}
