use std::net::SocketAddr;

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

async fn bind_manager_listener_or_skip() -> Option<TcpListener> {
    match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => Some(listener),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping test: binding a local listener is not permitted: {err}");
            None
        }
        Err(err) => panic!("bind manager listener: {err}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_build_context_returns_200_and_404() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = flow::FlowInstance::default();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage)
            .await
            .expect("start manager server");
    });

    let missing = format!("missing_{}", random_suffix());
    let (code, _) = http_request(
        addr,
        "GET",
        &format!("/pipelines/{missing}/buildContext"),
        None,
    )
    .await;
    assert_eq!(code, 404);

    let stream_name = format!("ctx_stream_{}", random_suffix());
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
        "shared": false,
        "decoder": { "type": "json", "props": {} }
    });
    let (code, body) = http_request(addr, "POST", "/streams", Some(&create_stream)).await;
    assert_eq!(
        code,
        201,
        "create stream failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let pipeline_id = format!("ctx_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let create_pipeline = json!({ "id": pipeline_id, "sql": sql, "sinks": [ { "type": "nop" } ] });
    let (code, body) = http_request(addr, "POST", "/pipelines", Some(&create_pipeline)).await;
    assert_eq!(
        code,
        201,
        "create pipeline failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );

    let (code, body) = http_request(
        addr,
        "GET",
        &format!(
            "/pipelines/{}/buildContext",
            create_pipeline["id"].as_str().unwrap()
        ),
        None,
    )
    .await;
    assert_eq!(
        code,
        200,
        "buildContext failed (HTTP {code}): {}",
        String::from_utf8_lossy(&body)
    );
    let ctx: JsonValue = serde_json::from_slice(&body).expect("decode buildContext JSON");
    assert_eq!(ctx["pipeline"]["id"], create_pipeline["id"]);
    assert_eq!(
        ctx["streams"][&stream_name]["name"],
        JsonValue::String(stream_name)
    );
    assert!(ctx["shared_mqtt_clients"].is_array());

    server.abort();
    let _ = server.await;
}
