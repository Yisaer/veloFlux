#![cfg(feature = "nng_pubsub")]

use anng::{protocols::pubsub0, Message};
use reqwest::StatusCode;
use serde_json::Value as JsonValue;
use std::time::Duration;

use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use sdk::PipelineCreateRequest;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_nng_stream_fans_out_to_two_pipelines_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");
    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{addr}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let stream_name = format!("e2e_shared_nng_{}", random_suffix());
    let input_url = format!("inproc://vf-e2e-shared-nng-input-{}", random_suffix());
    let output_a_url = format!("inproc://vf-e2e-shared-nng-output-a-{}", random_suffix());
    let output_b_url = format!("inproc://vf-e2e-shared-nng-output-b-{}", random_suffix());

    let input_c_url = std::ffi::CString::new(input_url.clone()).expect("valid input url");
    let mut publisher = pubsub0::Pub0::listen(input_c_url.as_c_str())
        .await
        .expect("start input publisher");

    let output_a_c_url = std::ffi::CString::new(output_a_url.clone()).expect("valid output A url");
    let output_a = pubsub0::Sub0::listen(output_a_c_url.as_c_str())
        .await
        .expect("start output A subscriber");
    let mut output_a = output_a.context();
    output_a.subscribe_to(b"topic/can");

    let output_b_c_url = std::ffi::CString::new(output_b_url.clone()).expect("valid output B url");
    let output_b = pubsub0::Sub0::listen(output_b_c_url.as_c_str())
        .await
        .expect("start output B subscriber");
    let mut output_b = output_b.context();
    output_b.subscribe_to(b"topic/can");

    let create_stream = serde_json::json!({
        "name": stream_name,
        "revision": 1,
        "type": "nng_pubsub",
        "schema": {
            "type": "json",
            "props": {
                "columns": [{"name": "v", "data_type": "int64"}]
            }
        },
        "props": {
            "url": input_url,
            "topic": "topic/can"
        },
        "shared": true,
        "decoder": {"type": "json", "props": {}}
    });
    let stream_response = http
        .post(format!("{manager_base}/streams"))
        .json(&create_stream)
        .send()
        .await
        .expect("create shared nng stream request");
    let stream_status = stream_response.status();
    let stream_body = stream_response.text().await.unwrap_or_default();
    assert_eq!(
        stream_status,
        StatusCode::CREATED,
        "create shared nng stream failed: status={} body={}",
        stream_status,
        stream_body
    );

    let pipeline_a = format!("e2e_shared_nng_pipeline_a_{}", random_suffix());
    let pipeline_b = format!("e2e_shared_nng_pipeline_b_{}", random_suffix());
    let sql = format!("SELECT v FROM {stream_name}");
    let create_pipeline =
        |pipeline_id: &str, sink_id: &str, sink_url: &str| PipelineCreateRequest {
            id: pipeline_id.to_string(),
            revision: 1,
            sql: sql.clone(),
            sinks: vec![serde_json::json!({
                "id": sink_id,
                "type": "nng_pubsub",
                "props": {
                    "url": sink_url,
                    "topic": "topic/can"
                },
                "encoder": {"type": "json", "props": {}}
            })],
            flow_instance_id: None,
        };

    for request in [
        create_pipeline(&pipeline_a, "nng_sink_a", &output_a_url),
        create_pipeline(&pipeline_b, "nng_sink_b", &output_b_url),
    ] {
        let response = http
            .post(format!("{manager_base}/pipelines"))
            .json(&request)
            .send()
            .await
            .expect("create nng pipeline request");
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "create nng pipeline failed: status={} body={}",
            status,
            body
        );
    }

    for pipeline_id in [&pipeline_a, &pipeline_b] {
        let response = http
            .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
            .send()
            .await
            .expect("start nng pipeline request");
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        assert_eq!(
            status,
            StatusCode::OK,
            "start nng pipeline failed: status={} body={}",
            status,
            body
        );
    }

    let stats_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let response = http
            .get(format!(
                "{manager_base}/streams/{stream_name}/shared/stats?flow_instance_id=default"
            ))
            .send()
            .await
            .expect("get shared nng stream stats request");
        assert_eq!(response.status(), StatusCode::OK);
        let stats = response
            .json::<JsonValue>()
            .await
            .expect("decode shared nng stream stats");
        if stats["status"] == "running" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < stats_deadline,
            "shared nng stream did not reach two subscribers: {stats}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let publisher_task = tokio::spawn(async move {
        loop {
            publisher
                .publish(Message::from(&b"topic/can:{\"v\":42}"[..]))
                .await
                .expect("publish nng input frame");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    let (message_a, message_b) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(5), output_a.next()),
        tokio::time::timeout(Duration::from_secs(5), output_b.next()),
    );
    publisher_task.abort();

    let message_a = message_a
        .expect("output A timeout")
        .expect("output A subscriber ended");
    let message_b = message_b
        .expect("output B timeout")
        .expect("output B subscriber ended");
    assert_eq!(message_a.as_slice(), b"topic/can:[{\"v\":42}]");
    assert_eq!(message_b.as_slice(), b"topic/can:[{\"v\":42}]");

    for pipeline_id in [&pipeline_a, &pipeline_b] {
        let response = http
            .post(format!(
                "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
            ))
            .send()
            .await
            .expect("stop nng pipeline request");
        assert_eq!(response.status(), StatusCode::OK);

        let response = http
            .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
            .send()
            .await
            .expect("delete nng pipeline request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    let response = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete shared nng stream request");
    assert_eq!(response.status(), StatusCode::OK);

    server.abort();
    let _ = server.await;
}
