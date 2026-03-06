use super::{bind_manager_listener_or_skip, default_flow_instances, make_client, random_suffix};

use serde_json::Value as JsonValue;

use sdk::PipelineCreateRequest;
use sdk::SdkError;
use sdk::StreamCreateRequest;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_build_context_returns_200_and_404() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

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

    let missing = format!("missing_{}", random_suffix());
    let err = client
        .build_pipeline_context(&missing)
        .await
        .expect_err("missing pipeline should return 404");

    match err {
        SdkError::Http { status, .. } => assert_eq!(status.as_u16(), 404),
        other => panic!("expected http 404 error, got: {other:?}"),
    }

    let stream_name = format!("ctx_stream_{}", random_suffix());
    let stream_req = StreamCreateRequest {
        name: stream_name.clone(),
        stream_type: "mock".to_string(),
        schema: serde_json::json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" }
                ]
            }
        }),
        props: serde_json::json!({}),
        shared: false,
        decoder: serde_json::json!({ "type": "json", "props": {} }),
    };

    client
        .create_stream(&stream_req)
        .await
        .expect("create stream");

    let pipeline_id = format!("ctx_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let pipe_req = PipelineCreateRequest::nop(pipeline_id.clone(), sql);

    client
        .create_pipeline(&pipe_req)
        .await
        .expect("create pipeline");

    let ctx: JsonValue = client
        .build_pipeline_context(&pipeline_id)
        .await
        .expect("buildContext");

    assert_eq!(ctx["pipeline"]["id"], JsonValue::String(pipeline_id));
    assert_eq!(
        ctx["streams"][&stream_name]["name"],
        JsonValue::String(stream_name)
    );
    assert!(ctx["shared_mqtt_clients"].is_array());

    server.abort();
    let _ = server.await;
}
