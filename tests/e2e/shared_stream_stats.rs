use super::{bind_manager_listener_or_skip, default_flow_instances, make_client, random_suffix};
use sdk::{PipelineCreateRequest, StopOptions, StreamCreateRequest};
use serde_json::Value as JsonValue;
use std::time::Duration;

fn find_processor<'a>(processors: &'a [JsonValue], suffix: &str) -> &'a JsonValue {
    processors
        .iter()
        .find(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| id.ends_with(suffix))
        })
        .unwrap_or_else(|| panic!("missing processor with suffix {suffix}"))
}

fn records_value(entry: &JsonValue, field: &str) -> u64 {
    entry["stats"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("missing numeric stats field {field} in {entry}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_reflect_runtime_activity_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let injector = instance.clone();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(
            listener,
            instance,
            storage,
            default_flow_instances(),
            Vec::new(),
        )
        .await
        .expect("start manager server");
    });

    let client = make_client(addr);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let stream_name = format!("e2e_shared_stream_stats_{}", random_suffix());
    let stream_req = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());
    client
        .create_stream(&stream_req)
        .await
        .expect("create shared mock stream");

    let pipeline_id = format!("e2e_shared_stream_pipeline_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let pipeline_req = PipelineCreateRequest::nop(pipeline_id.clone(), sql);
    client
        .create_pipeline(&pipeline_req)
        .await
        .expect("create pipeline");
    client
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    tokio::time::sleep(Duration::from_millis(200)).await;

    injector
        .send_shared_mock_stream_payload(&stream_name, br#"{"value": 42}"#.as_ref())
        .await
        .expect("inject payload into shared mock stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let stats = loop {
        let stats = client
            .shared_stream_stats_in_instance(&stream_name, Some("default"))
            .await
            .expect("get shared stream stats");
        let processors = stats["processors"]
            .as_array()
            .expect("processors should be an array");
        if processors.iter().any(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| id.ends_with("PhysicalDecoder_1"))
                && records_value(entry, "records_in") > 0
        }) {
            break stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "shared stream stats did not reflect runtime activity before timeout: {stats}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(stats["stream"], stream_name);
    assert_eq!(stats["flow_instance_id"], "default");
    assert_eq!(stats["status"], "running");

    let processors = stats["processors"]
        .as_array()
        .expect("processors should be an array");
    let data_source = find_processor(processors, "PhysicalDataSource_0");
    let decoder = find_processor(processors, "PhysicalDecoder_1");
    let result_collect = find_processor(processors, "PhysicalResultCollect_2");

    assert!(
        records_value(data_source, "records_out") > 0,
        "datasource stats should reflect emitted records: {data_source}"
    );
    assert!(
        records_value(decoder, "records_in") > 0,
        "decoder stats should reflect consumed records: {decoder}"
    );
    assert!(
        records_value(result_collect, "records_in") > 0,
        "result collect stats should reflect consumed records: {result_collect}"
    );

    client
        .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
        .await
        .expect("stop pipeline");
    client
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
    client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");

    server.abort();
    let _ = server.await;
}
