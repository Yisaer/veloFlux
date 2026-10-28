use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::Value as JsonValue;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct RowRecorder {
    rows: Mutex<Vec<JsonValue>>,
}

impl RowRecorder {
    fn record(&self, payload: JsonValue) -> Result<(), &'static str> {
        let rows = payload
            .as_array()
            .ok_or("HTTP sink payload must be a JSON array")?;
        self.rows
            .lock()
            .expect("lock row recorder")
            .extend(rows.clone());
        Ok(())
    }

    fn rows(&self) -> Vec<JsonValue> {
        self.rows.lock().expect("lock row recorder").clone()
    }

    fn clear(&self) {
        self.rows.lock().expect("lock row recorder").clear();
    }
}

async fn record_rows(
    State(recorder): State<Arc<RowRecorder>>,
    Json(payload): Json<JsonValue>,
) -> StatusCode {
    match recorder.record(payload) {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::BAD_REQUEST,
    }
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

async fn wait_for_row_count(recorder: &RowRecorder, expected: usize) -> Vec<JsonValue> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let rows = recorder.rows();
        if rows.len() >= expected {
            return rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} HTTP sink rows; received {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn assert_response_status(response: reqwest::Response, expected: StatusCode, action: &str) {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(status, expected, "{action} failed: {status}: {body}");
}

// coverage-covers: source.file.stream, pipeline.runtime.checkpoint_recovery
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_source_checkpoint_resumes_from_last_emitted_offset() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let source_path = temp_dir.path().join("app.log");
    std::fs::write(&source_path, b"committed\npart\n").expect("write initial source file");
    let source_path = std::fs::canonicalize(source_path).expect("canonical source path");

    let recorder = Arc::new(RowRecorder::default());
    let sink_app = Router::new()
        .route("/rows", post(record_rows))
        .with_state(Arc::clone(&recorder));
    let Some(sink_listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let sink_addr = sink_listener.local_addr().expect("read sink listener addr");
    let sink_server = tokio::spawn(async move {
        axum::serve(sink_listener, sink_app)
            .await
            .expect("start HTTP sink recorder");
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

    let http = http_client();
    let manager_base = format!("http://{manager_addr}");
    wait_for_manager(&http, &manager_base).await;

    let stream_name = format!("e2e_file_checkpoint_stream_{}", random_suffix());
    let create_stream = http
        .post(format!("{manager_base}/streams"))
        .json(&serde_json::json!({
            "name": stream_name,
            "revision": 1,
            "type": "file",
            "props": {
                "path": source_path.to_string_lossy()
            },
            "decoder": {
                "type": "file_line",
                "props": {}
            }
        }))
        .send()
        .await
        .expect("create file stream request");
    assert_response_status(create_stream, StatusCode::CREATED, "create file stream").await;

    let pipeline_id = format!("e2e_file_checkpoint_pipe_{}", random_suffix());
    let create_pipeline = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipeline_id,
            "revision": 1,
            "sql": format!("SELECT line, filename FROM {stream_name}"),
            "sinks": [{
                "id": "http_out",
                "type": "http",
                "props": {
                    "url": format!("http://{sink_addr}/rows"),
                    "content_type": "application/json"
                },
                "encoder": {
                    "type": "json",
                    "props": {}
                }
            }],
            "options": {
                "checkpoint": {
                    "enabled": true
                }
            }
        }))
        .send()
        .await
        .expect("create checkpoint pipeline request");
    assert_response_status(
        create_pipeline,
        StatusCode::CREATED,
        "create checkpoint pipeline",
    )
    .await;

    let start_pipeline = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start checkpoint pipeline request");
    assert_response_status(start_pipeline, StatusCode::OK, "start checkpoint pipeline").await;

    let initial_rows = wait_for_row_count(recorder.as_ref(), 1).await;
    assert_eq!(
        initial_rows,
        vec![serde_json::json!({
            "line": "committed\npart\n",
            "filename": "app.log"
        })]
    );

    let stop_pipeline = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("gracefully stop checkpoint pipeline request");
    assert_response_status(
        stop_pipeline,
        StatusCode::OK,
        "gracefully stop checkpoint pipeline",
    )
    .await;
    recorder.clear();

    let restart_pipeline = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("restart checkpoint pipeline request");
    assert_response_status(
        restart_pipeline,
        StatusCode::OK,
        "restart checkpoint pipeline",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        recorder.rows().is_empty(),
        "restored file source replayed rows before new data was appended: {:?}",
        recorder.rows()
    );

    let mut source_file = OpenOptions::new()
        .append(true)
        .open(&source_path)
        .expect("open source file for append");
    source_file
        .write_all(b"new\n")
        .expect("append new source row");
    source_file.flush().expect("flush appended source row");
    drop(source_file);

    let resumed_rows = wait_for_row_count(recorder.as_ref(), 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        resumed_rows,
        vec![serde_json::json!({"line": "new\n", "filename": "app.log"})]
    );
    assert_eq!(
        recorder.rows(),
        resumed_rows,
        "restored file source emitted unexpected additional rows"
    );

    let stop_pipeline = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop restored checkpoint pipeline request");
    assert_response_status(
        stop_pipeline,
        StatusCode::OK,
        "stop restored checkpoint pipeline",
    )
    .await;

    let delete_pipeline = http
        .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete checkpoint pipeline request");
    assert_response_status(
        delete_pipeline,
        StatusCode::OK,
        "delete checkpoint pipeline",
    )
    .await;

    let delete_stream = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete file stream request");
    assert_response_status(delete_stream, StatusCode::OK, "delete file stream").await;

    manager_server.abort();
    let _ = manager_server.await;
    sink_server.abort();
    let _ = sink_server.await;
}

// coverage-covers: source.file.stream, pipeline.runtime.checkpoint_recovery
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_source_file_checkpoint_restores_both_cursors_through_manager() {
    use flow::{CheckpointState, CheckpointStore};

    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let source_paths = ["left.log", "right.log"].map(|name| temp_dir.path().join(name));
    for (path, contents) in source_paths.iter().zip([
        b"left-committed\n".as_slice(),
        b"right-committed\n".as_slice(),
    ]) {
        std::fs::write(path, contents).expect("write committed source data");
    }
    let source_paths =
        source_paths.map(|path| std::fs::canonicalize(path).expect("canonical source path"));
    let recorder = Arc::new(RowRecorder::default());
    let sink_app = Router::new()
        .route("/rows", post(record_rows))
        .with_state(Arc::clone(&recorder));
    let Some(sink_listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let sink_addr = sink_listener.local_addr().expect("sink address");
    let sink_server = tokio::spawn(async move {
        axum::serve(sink_listener, sink_app)
            .await
            .expect("start sink recorder");
    });

    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir(&storage_dir).expect("create storage dir");
    let storage = storage::StorageManager::new(&storage_dir).expect("create durable storage");
    let checkpoints = manager::DurableCheckpointStore::new(
        storage.checkpoint_storage().expect("checkpoint storage"),
    );
    let instance = manager::new_default_flow_instance();
    let Some(manager_listener) = bind_manager_listener_or_skip().await else {
        sink_server.abort();
        let _ = sink_server.await;
        return;
    };
    let manager_addr = manager_listener.local_addr().expect("manager address");
    let manager_server = tokio::spawn(async move {
        manager::start_server_with_listener(
            manager_listener,
            instance,
            storage,
            default_flow_instances(),
        )
        .await
        .expect("start manager");
    });
    let http = http_client();
    let base = format!("http://{manager_addr}");
    wait_for_manager(&http, &base).await;
    let suffix = random_suffix();
    let stream_names = [
        format!("checkpoint_left_{suffix}"),
        format!("checkpoint_right_{suffix}"),
    ];
    for (name, path) in stream_names.iter().zip(&source_paths) {
        let response = http.post(format!("{base}/streams")).json(&serde_json::json!({
            "name": name, "revision": 1, "type": "file", "props": {"path": path.to_string_lossy()},
            "decoder": {"type": "file_line", "props": {}}
        })).send().await.expect("create file stream request");
        assert_response_status(
            response,
            StatusCode::CREATED,
            "create multi-source file stream",
        )
        .await;
    }
    let pipeline_id = format!("multi_file_checkpoint_{suffix}");
    let sql = format!("SELECT coalesce({0}.line, {1}.line) AS content, coalesce({0}.filename, {1}.filename) AS source_file FROM {0}, {1}", stream_names[0], stream_names[1]);
    let response = http.post(format!("{base}/pipelines")).json(&serde_json::json!({
        "id": pipeline_id, "revision": 1, "sql": sql,
        "sinks": [{"id": "http_out", "type": "http", "props": {"url": format!("http://{sink_addr}/rows"), "content_type": "application/json"}, "encoder": {"type": "json", "props": {}}}],
        "options": {"checkpoint": {"enabled": true}}
    })).send().await.expect("create multi-source checkpoint pipeline request");
    assert_response_status(
        response,
        StatusCode::CREATED,
        "create multi-source checkpoint pipeline",
    )
    .await;

    let sorted = |mut rows: Vec<JsonValue>| {
        rows.sort_by_key(|row| {
            row["source_file"]
                .as_str()
                .expect("source filename")
                .to_owned()
        });
        rows
    };
    let committed = vec![
        serde_json::json!({"content": "left-committed\n", "source_file": "left.log"}),
        serde_json::json!({"content": "right-committed\n", "source_file": "right.log"}),
    ];
    let response = http
        .post(format!("{base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    assert_response_status(
        response,
        StatusCode::OK,
        "start multi-source checkpoint pipeline",
    )
    .await;
    assert_eq!(
        sorted(wait_for_row_count(recorder.as_ref(), 2).await),
        committed
    );

    let response = http
        .post(format!(
            "{base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop pipeline request");
    assert_response_status(
        response,
        StatusCode::OK,
        "commit multi-source final checkpoint",
    )
    .await;
    assert_eq!(sorted(recorder.rows()), committed);

    // Read the manager's committed durable manifest, without replacing its store.
    let manifest = checkpoints
        .load_latest("default", &pipeline_id)
        .expect("read durable checkpoint")
        .expect("manager committed a checkpoint");
    let source_snapshots = manifest
        .operator_snapshots
        .iter()
        .filter(|snapshot| snapshot.operator_kind == "datasource")
        .collect::<Vec<_>>();
    assert_eq!(source_snapshots.len(), 2);
    let persisted_paths = source_snapshots
        .iter()
        .map(|snapshot| {
            let CheckpointState::Map(state) = &snapshot.state else {
                panic!("file source snapshot must be a map");
            };
            let Some(CheckpointState::String(path)) = state.get("source_path") else {
                panic!("file source snapshot must identify its path");
            };
            path.clone()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        persisted_paths,
        source_paths
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect()
    );
    recorder.clear();

    // Both appends happen while the pipeline is stopped, before restore through the API.
    for (path, contents) in source_paths.iter().zip([
        b"left-appended\n".as_slice(),
        b"right-appended\n".as_slice(),
    ]) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open stopped source file");
        file.write_all(contents)
            .expect("append stopped source data");
        file.sync_all().expect("sync appended source data");
    }
    let response = http
        .post(format!("{base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("restore pipeline request");
    assert_response_status(
        response,
        StatusCode::OK,
        "restore both file sources through manager",
    )
    .await;
    let appended = vec![
        serde_json::json!({"content": "left-appended\n", "source_file": "left.log"}),
        serde_json::json!({"content": "right-appended\n", "source_file": "right.log"}),
    ];
    assert_eq!(
        sorted(wait_for_row_count(recorder.as_ref(), 2).await),
        appended
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        sorted(recorder.rows()),
        appended,
        "both appended rows must arrive once, without committed-row replay"
    );

    let response = http
        .post(format!(
            "{base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop restored pipeline request");
    assert_response_status(
        response,
        StatusCode::OK,
        "stop restored multi-source pipeline",
    )
    .await;
    assert_eq!(
        sorted(recorder.rows()),
        appended,
        "graceful shutdown must not produce extra rows"
    );
    let next = checkpoints
        .load_latest("default", &pipeline_id)
        .expect("read next durable checkpoint")
        .expect("next checkpoint committed");
    assert!(next.checkpoint_id > manifest.checkpoint_id);
    assert_eq!(
        next.operator_snapshots
            .iter()
            .filter(|snapshot| snapshot.operator_kind == "datasource")
            .count(),
        2
    );
    let response = http
        .delete(format!("{base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete pipeline request");
    assert_response_status(
        response,
        StatusCode::OK,
        "delete multi-source checkpoint pipeline",
    )
    .await;
    for name in &stream_names {
        let response = http
            .delete(format!("{base}/streams/{name}"))
            .send()
            .await
            .expect("delete stream request");
        assert_response_status(response, StatusCode::OK, "delete multi-source file stream").await;
    }
    manager_server.abort();
    let _ = manager_server.await;
    sink_server.abort();
    let _ = sink_server.await;
}
