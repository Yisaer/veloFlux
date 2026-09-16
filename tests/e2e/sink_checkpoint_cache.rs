use super::{bind_manager_listener_or_skip, default_flow_instances, random_suffix};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct RetrySinkRecorder {
    attempts: AtomicUsize,
    successful_rows: Mutex<Vec<JsonValue>>,
}

impl RetrySinkRecorder {
    fn attempt_count(&self) -> usize {
        self.attempts.load(Ordering::Acquire)
    }

    fn successful_rows(&self) -> Vec<JsonValue> {
        self.successful_rows
            .lock()
            .expect("lock successful sink rows")
            .clone()
    }
}

async fn receive_rows(
    State(recorder): State<Arc<RetrySinkRecorder>>,
    Json(payload): Json<JsonValue>,
) -> StatusCode {
    let attempt = recorder.attempts.fetch_add(1, Ordering::AcqRel);
    if attempt == 0 {
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    let Some(rows) = payload.as_array() else {
        return StatusCode::BAD_REQUEST;
    };
    recorder
        .successful_rows
        .lock()
        .expect("lock successful sink rows")
        .extend(rows.iter().cloned());
    StatusCode::OK
}

async fn wait_for_manager(http: &reqwest::Client, base: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if http
            .get(format!("{base}/pipelines"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "manager did not start before timeout"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_attempts(recorder: &RetrySinkRecorder, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if recorder.attempt_count() >= expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} sink attempts; observed {}",
            recorder.attempt_count()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_successful_rows(recorder: &RetrySinkRecorder, expected: usize) -> Vec<JsonValue> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows = recorder.successful_rows();
        if rows.len() >= expected {
            return rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} successful sink rows; observed {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn assert_response_status(response: reqwest::Response, expected: StatusCode, action: &str) {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(status, expected, "{action} failed: {status}: {body}");
}

// coverage-covers: pipeline.runtime.checkpoint_recovery, sink.delivery.checkpoint_cache
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_sink_delivery_is_replayed_after_checkpoint_restart() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let recorder = Arc::new(RetrySinkRecorder::default());
    let sink_app = Router::new()
        .route("/rows", post(receive_rows))
        .with_state(Arc::clone(&recorder));
    let Some(sink_listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let sink_addr = sink_listener.local_addr().expect("read sink listener addr");
    let sink_server = tokio::spawn(async move {
        axum::serve(sink_listener, sink_app)
            .await
            .expect("start retry sink server");
    });

    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir(&storage_dir).expect("create storage dir");
    let storage = storage::StorageManager::new(&storage_dir).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let Some(manager_listener) = bind_manager_listener_or_skip().await else {
        sink_server.abort();
        let _ = sink_server.await;
        return;
    };
    let manager_addr = manager_listener
        .local_addr()
        .expect("read manager listener addr");
    let manager_server = tokio::spawn(async move {
        manager::start_server_with_listener(
            manager_listener,
            instance,
            storage,
            default_flow_instances(),
        )
        .await
        .expect("start manager server");
    });

    sdk::install_default_crypto_provider();
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build test HTTP client");
    let manager_base = format!("http://{manager_addr}");
    wait_for_manager(&http, &manager_base).await;

    let stream_name = format!("e2e_sink_checkpoint_stream_{}", random_suffix());
    let create_stream = http
        .post(format!("{manager_base}/streams"))
        .json(&serde_json::json!({
            "name": stream_name,
            "revision": 1,
            "type": "mock",
            "schema": {
                "type": "json",
                "props": {
                    "columns": [{ "name": "value", "data_type": "string" }]
                }
            },
            "props": {
                "payloads": [
                    "{\"value\":\"A\"}",
                    "{\"value\":\"B\"}",
                    "{\"value\":\"C\"}"
                ]
            },
            "decoder": { "type": "json", "props": {} }
        }))
        .send()
        .await
        .expect("create scripted mock stream request");
    assert_response_status(
        create_stream,
        StatusCode::CREATED,
        "create scripted mock stream",
    )
    .await;

    let pipeline_id = format!("e2e_sink_checkpoint_pipe_{}", random_suffix());
    let create_pipeline = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipeline_id,
            "revision": 1,
            "sql": format!("SELECT value FROM {stream_name}"),
            "sinks": [{
                "id": "http_out",
                "type": "http",
                "props": {
                    "url": format!("http://localhost:{}/rows", sink_addr.port()),
                    "content_type": "application/json"
                },
                "encoder": { "type": "json", "props": {} },
                "retry": {
                    "max_attempts": 3,
                    "initial_backoff_ms": 60000,
                    "max_backoff_ms": 60000
                }
            }],
            "options": {
                "checkpoint": { "enabled": true }
            }
        }))
        .send()
        .await
        .expect("create sink checkpoint pipeline request");
    assert_response_status(
        create_pipeline,
        StatusCode::CREATED,
        "create sink checkpoint pipeline",
    )
    .await;

    let start_pipeline = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start sink checkpoint pipeline request");
    assert_response_status(
        start_pipeline,
        StatusCode::OK,
        "start sink checkpoint pipeline",
    )
    .await;

    wait_for_attempts(recorder.as_ref(), 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        recorder.attempt_count(),
        1,
        "sink retried before graceful checkpoint shutdown"
    );
    assert!(
        recorder.successful_rows().is_empty(),
        "the failed delivery must not be recorded as successful"
    );

    let stop_pipeline = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("gracefully stop sink checkpoint pipeline request");
    assert_response_status(
        stop_pipeline,
        StatusCode::OK,
        "gracefully stop sink checkpoint pipeline",
    )
    .await;

    let restart_pipeline = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("restart sink checkpoint pipeline request");
    assert_response_status(
        restart_pipeline,
        StatusCode::OK,
        "restart sink checkpoint pipeline",
    )
    .await;

    let rows = wait_for_successful_rows(recorder.as_ref(), 3).await;
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"value": "A"}),
            serde_json::json!({"value": "B"}),
            serde_json::json!({"value": "C"}),
        ],
        "restored sink deliveries must preserve FIFO order"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        recorder.successful_rows(),
        rows,
        "checkpoint restart emitted unexpected duplicate rows"
    );
    assert_eq!(
        recorder.attempt_count(),
        4,
        "expected one failed attempt and three restored deliveries"
    );

    let stop_pipeline = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop restored sink checkpoint pipeline request");
    assert_response_status(
        stop_pipeline,
        StatusCode::OK,
        "stop restored sink checkpoint pipeline",
    )
    .await;

    manager_server.abort();
    let _ = manager_server.await;
    sink_server.abort();
    let _ = sink_server.await;
}
