use std::net::SocketAddr;
use std::time::Duration;

use serde_json::{json, Value as JsonValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&JsonValue>,
) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|err| panic!("connect {addr}: {err}"));

    let body_bytes = body
        .map(|value| serde_json::to_vec(value).expect("encode JSON body"))
        .unwrap_or_default();

    let mut req = String::new();
    req.push_str(&format!("{method} {path} HTTP/1.1\r\n"));
    req.push_str(&format!("Host: {addr}\r\n"));
    req.push_str("Connection: close\r\n");
    if body.is_some() {
        req.push_str("Content-Type: application/json\r\n");
    }
    req.push_str(&format!("Content-Length: {}\r\n", body_bytes.len()));
    req.push_str("\r\n");

    stream
        .write_all(req.as_bytes())
        .await
        .unwrap_or_else(|err| panic!("write request headers: {err}"));
    if !body_bytes.is_empty() {
        stream
            .write_all(&body_bytes)
            .await
            .unwrap_or_else(|err| panic!("write request body: {err}"));
    }

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .unwrap_or_else(|err| panic!("read response: {err}"));

    let header_end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("invalid HTTP response (missing header delimiter)");
    let (head, body) = response.split_at(header_end + 4);
    let head_str = std::str::from_utf8(head).expect("response headers not valid utf8");
    let status_line = head_str.lines().next().expect("missing HTTP status line");
    let code = status_line
        .split_whitespace()
        .nth(1)
        .expect("invalid HTTP status line")
        .parse::<u16>()
        .expect("invalid HTTP status code");

    (code, body.to_vec())
}

fn random_suffix() -> String {
    use rand::{distributions::Alphanumeric, Rng};
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_rapid_start_stop_cycles_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = flow::FlowInstance::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind manager listener");
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage)
            .await
            .expect("start manager server");
    });

    let stream_name = format!("reg_shared_stream_{}", random_suffix());
    let create_stream = json!({
        "name": stream_name,
        "type": "mock",
        "schema": {
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" }
                ]
            }
        },
        "props": {},
        "shared": true,
        "decoder": { "type": "json", "props": {} }
    });
    let (code, body) = http_request(addr, "POST", "/streams", Some(&create_stream)).await;
    assert_eq!(
        code,
        201,
        "create stream failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let sql = format!("SELECT value FROM {stream_name}");
    for cycle in 0..5usize {
        let pipeline_a = format!("pipe_a_{cycle}_{}", random_suffix());
        let pipeline_b = format!("pipe_b_{cycle}_{}", random_suffix());

        let create_a = json!({ "id": pipeline_a, "sql": sql, "sinks": [ { "type": "nop" } ] });
        let create_b = json!({ "id": pipeline_b, "sql": sql, "sinks": [ { "type": "nop" } ] });

        let (code, body) = http_request(addr, "POST", "/pipelines", Some(&create_a)).await;
        assert_eq!(
            code,
            201,
            "create pipeline_a failed (HTTP {code}): {}",
            String::from_utf8_lossy(&body)
        );
        let (code, body) = http_request(addr, "POST", "/pipelines", Some(&create_b)).await;
        assert_eq!(
            code,
            201,
            "create pipeline_b failed (HTTP {code}): {}",
            String::from_utf8_lossy(&body)
        );

        let (code, body) = http_request(
            addr,
            "POST",
            &format!("/pipelines/{}/start", create_a["id"].as_str().unwrap()),
            None,
        )
        .await;
        assert_eq!(
            code,
            200,
            "start pipeline_a failed (HTTP {code}): {}",
            String::from_utf8_lossy(&body)
        );
        let (code, body) = http_request(
            addr,
            "POST",
            &format!("/pipelines/{}/start", create_b["id"].as_str().unwrap()),
            None,
        )
        .await;
        assert_eq!(
            code,
            200,
            "start pipeline_b failed (HTTP {code}): {}",
            String::from_utf8_lossy(&body)
        );

        // Give processors a brief chance to finish async startup work before issuing stop.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let stop_path_a = format!(
            "/pipelines/{}/stop?mode=graceful&timeout_ms=5000",
            create_a["id"].as_str().unwrap()
        );
        let stop_path_b = format!(
            "/pipelines/{}/stop?mode=graceful&timeout_ms=5000",
            create_b["id"].as_str().unwrap()
        );
        let stop_a = http_request(addr, "POST", &stop_path_a, None);
        let stop_b = http_request(addr, "POST", &stop_path_b, None);
        let ((code_a, body_a), (code_b, body_b)) = tokio::join!(stop_a, stop_b);
        assert_eq!(
            code_a,
            200,
            "stop pipeline_a failed (HTTP {code_a}): {}",
            String::from_utf8_lossy(&body_a)
        );
        assert_eq!(
            code_b,
            200,
            "stop pipeline_b failed (HTTP {code_b}): {}",
            String::from_utf8_lossy(&body_b)
        );

        let (code, body) = http_request(
            addr,
            "DELETE",
            &format!("/pipelines/{}", create_a["id"].as_str().unwrap()),
            None,
        )
        .await;
        assert_eq!(
            code,
            200,
            "delete pipeline_a failed (HTTP {code}): {}",
            String::from_utf8_lossy(&body)
        );
        let (code, body) = http_request(
            addr,
            "DELETE",
            &format!("/pipelines/{}", create_b["id"].as_str().unwrap()),
            None,
        )
        .await;
        assert_eq!(
            code,
            200,
            "delete pipeline_b failed (HTTP {code}): {}",
            String::from_utf8_lossy(&body)
        );
    }

    let (code, body) = http_request(addr, "DELETE", &format!("/streams/{stream_name}"), None).await;
    assert_eq!(
        code,
        200,
        "delete stream failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_slow_unsubscribe_during_restart_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = flow::FlowInstance::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind manager listener");
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage)
            .await
            .expect("start manager server");
    });

    let stream_name = format!("reg_shared_stream_slow_unsub_{}", random_suffix());
    let create_stream = json!({
        "name": stream_name,
        "type": "mock",
        "schema": {
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" }
                ]
            }
        },
        "props": {},
        "shared": true,
        "decoder": { "type": "json", "props": {} }
    });
    let (code, body) = http_request(addr, "POST", "/streams", Some(&create_stream)).await;
    assert_eq!(
        code,
        201,
        "create stream failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let sql = format!("SELECT value FROM {stream_name}");
    let pipeline_a = format!("pipe_a_slow_unsub_{}", random_suffix());
    let pipeline_b_v1 = format!("pipe_b_v1_slow_unsub_{}", random_suffix());
    let pipeline_b_v2 = format!("pipe_b_v2_slow_unsub_{}", random_suffix());

    let create_a = json!({ "id": pipeline_a, "sql": sql, "sinks": [ { "type": "nop" } ] });
    let create_b_v1 = json!({ "id": pipeline_b_v1, "sql": sql, "sinks": [ { "type": "nop" } ] });

    let (code, body) = http_request(addr, "POST", "/pipelines", Some(&create_a)).await;
    assert_eq!(
        code,
        201,
        "create pipeline_a failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );
    let (code, body) = http_request(addr, "POST", "/pipelines", Some(&create_b_v1)).await;
    assert_eq!(
        code,
        201,
        "create pipeline_b_v1 failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let (code, body) = http_request(
        addr,
        "POST",
        &format!("/pipelines/{}/start", create_a["id"].as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(
        code,
        200,
        "start pipeline_a failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );
    let (code, body) = http_request(
        addr,
        "POST",
        &format!("/pipelines/{}/start", create_b_v1["id"].as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(
        code,
        200,
        "start pipeline_b_v1 failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Simulate A being slow to unsubscribe while B restarts.
    let stop_path_a = format!(
        "/pipelines/{}/stop?mode=graceful&timeout_ms=5000",
        create_a["id"].as_str().unwrap()
    );
    let slow_stop_a = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        http_request(addr, "POST", &stop_path_a, None).await
    });

    let stop_path_b_v1 = format!(
        "/pipelines/{}/stop?mode=graceful&timeout_ms=5000",
        create_b_v1["id"].as_str().unwrap()
    );
    let (code, body) = http_request(addr, "POST", &stop_path_b_v1, None).await;
    assert_eq!(
        code,
        200,
        "stop pipeline_b_v1 failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );
    let (code, body) = http_request(
        addr,
        "DELETE",
        &format!("/pipelines/{}", create_b_v1["id"].as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(
        code,
        200,
        "delete pipeline_b_v1 failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let create_b_v2 = json!({ "id": pipeline_b_v2, "sql": sql, "sinks": [ { "type": "nop" } ] });
    let (code, body) = http_request(addr, "POST", "/pipelines", Some(&create_b_v2)).await;
    assert_eq!(
        code,
        201,
        "create pipeline_b_v2 failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );
    let (code, body) = http_request(
        addr,
        "POST",
        &format!("/pipelines/{}/start", create_b_v2["id"].as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(
        code,
        200,
        "start pipeline_b_v2 failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let (code, body) = slow_stop_a.await.expect("slow stop task panicked");
    assert_eq!(
        code,
        200,
        "stop pipeline_a failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let stop_path_b_v2 = format!(
        "/pipelines/{}/stop?mode=graceful&timeout_ms=5000",
        create_b_v2["id"].as_str().unwrap()
    );
    let (code, body) = http_request(addr, "POST", &stop_path_b_v2, None).await;
    assert_eq!(
        code,
        200,
        "stop pipeline_b_v2 failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let (code, body) = http_request(
        addr,
        "DELETE",
        &format!("/pipelines/{}", create_b_v2["id"].as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(
        code,
        200,
        "delete pipeline_b_v2 failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let (code, body) = http_request(
        addr,
        "DELETE",
        &format!("/pipelines/{}", create_a["id"].as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(
        code,
        200,
        "delete pipeline_a failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let (code, body) = http_request(addr, "DELETE", &format!("/streams/{stream_name}"), None).await;
    assert_eq!(
        code,
        200,
        "delete stream failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    server.abort();
    let _ = server.await;
}
