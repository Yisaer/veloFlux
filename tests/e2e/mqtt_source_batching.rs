use super::proto_decoder_e2e::EmbeddedMqttBroker;
use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use flow::connector::MemoryData;
use reqwest::StatusCode;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn post(http: &reqwest::Client, base: &str, path: &str, body: Value, expected: StatusCode) {
    let response = http
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .expect("manager request");
    let status = response.status();
    let body = response.text().await.expect("manager response body");
    assert_eq!(status, expected, "POST {path}: {body}");
}

fn append_rows(payload: &[u8], output: &mut Vec<Value>) {
    let decoded: Value = serde_json::from_slice(payload).expect("decode sink JSON");
    let rows = match decoded {
        Value::Array(rows) => rows,
        row @ Value::Object(_) => vec![row],
        other => panic!("unexpected sink output: {other}"),
    };
    for row in rows {
        // A retained marker proves the entire source-to-sink path is ready.
        if row["seq"] != -1 {
            output.push(row);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_source_burst_preserves_sql_results_order_and_decode_recovery_via_rest() {
    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let temp = tempfile::tempdir().expect("temporary storage");
    let storage = storage::StorageManager::new(temp.path()).expect("storage manager");
    let instance = manager::new_default_flow_instance();
    let output_instance = instance.clone();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let _server = AbortOnDrop(tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("manager server");
    }));
    let broker = EmbeddedMqttBroker::start().await;
    let broker_url = broker.broker_url();
    let port = broker_url
        .rsplit(':')
        .next()
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let suffix = random_suffix();
    let topic = format!("e2e/batching/{suffix}");
    let stream = format!("batch_stream_{suffix}");
    let pipeline = format!("batch_pipeline_{suffix}");
    let output_topic = format!("batch_output_{suffix}");
    let http = http_client();

    let (publisher, mut events) = AsyncClient::new(
        MqttOptions::new(format!("batch-publisher-{suffix}"), "127.0.0.1", port),
        256,
    );
    let (marker_tx, marker_rx) = tokio::sync::oneshot::channel();
    let _pump = AbortOnDrop(tokio::spawn(async move {
        let mut marker_tx = Some(marker_tx);
        loop {
            if let Event::Incoming(Packet::PubAck(_)) =
                events.poll().await.expect("publisher event loop")
            {
                if let Some(tx) = marker_tx.take() {
                    let _ = tx.send(());
                }
            }
        }
    }));
    publisher
        .publish(
            &topic,
            QoS::AtLeastOnce,
            true,
            br#"{"seq":-1,"v":-1}"#.as_slice(),
        )
        .await
        .expect("publish retained readiness marker");
    timeout(Duration::from_secs(10), marker_rx)
        .await
        .expect("marker acknowledgement timeout")
        .expect("marker acknowledgement");

    post(
        &http,
        &base,
        "/streams",
        json!({
            "name": stream, "revision": 1, "type": "mqtt", "shared": false,
            "props": {"broker_url": broker_url, "topic": topic, "qos": 0, "protocol_version": "v5"},
            "schema": {"type": "json", "props": {"columns": [
                {"name": "seq", "data_type": "int64"}, {"name": "v", "data_type": "int64"}
            ]}},
            "decoder": {"type": "json", "props": {}}
        }),
        StatusCode::CREATED,
    )
    .await;
    post(
        &http,
        &base,
        "/memory/topics",
        json!({
            "topic": output_topic, "revision": 1, "kind": "bytes"
        }),
        StatusCode::CREATED,
    )
    .await;
    let mut output = output_instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("output subscription");
    post(
        &http,
        &base,
        "/pipelines",
        json!({
            "id": pipeline, "revision": 1,
            "sql": format!("SELECT seq, v + 1 AS next_value FROM {stream}"),
            "sinks": [{"type": "memory", "props": {"topic": output_topic},
                       "encoder": {"type": "json", "props": {}}}]
        }),
        StatusCode::CREATED,
    )
    .await;
    post(
        &http,
        &base,
        &format!("/pipelines/{pipeline}/start"),
        json!({}),
        StatusCode::OK,
    )
    .await;

    timeout(Duration::from_secs(10), async {
        loop {
            let MemoryData::Bytes(payload) = output.recv().await.expect("readiness output") else {
                panic!("expected encoded output");
            };
            let value: Value = serde_json::from_slice(&payload).unwrap();
            let row = value
                .as_array()
                .and_then(|rows| rows.first())
                .unwrap_or(&value);
            if row == &json!({"seq": -1, "next_value": 0}) {
                break;
            }
        }
    })
    .await
    .expect("source-to-sink readiness timeout");

    // Queue a burst once, without retrying payloads or relying on batch size.
    for sequence in (0..128).step_by(2) {
        if sequence == 64 {
            publisher
                .publish(&topic, QoS::AtLeastOnce, false, b"invalid JSON".as_slice())
                .await
                .expect("publish invalid payload");
        }
        let rows = [
            json!({"seq":sequence,"v":sequence*2}),
            json!({"seq":sequence+1,"v":(sequence+1)*2}),
        ];
        publisher
            .publish(
                &topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&rows).unwrap(),
            )
            .await
            .expect("publish array payload");
    }
    let mut rows = Vec::new();
    timeout(Duration::from_secs(10), async {
        while rows.len() < 128 {
            let MemoryData::Bytes(payload) = output.recv().await.expect("SQL output") else {
                panic!("expected encoded output");
            };
            append_rows(&payload, &mut rows);
        }
    })
    .await
    .expect("complete SQL output timeout");
    let response = http
        .get(format!("{base}/pipelines/{pipeline}/stats"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let stats: Vec<Value> = response.json().await.expect("pipeline statistics");
    assert_eq!(
        stats
            .iter()
            .map(|entry| entry["stats"]["error_count"].as_u64().unwrap_or(0))
            .sum::<u64>(),
        1,
        "{stats:#?}"
    );

    post(
        &http,
        &base,
        &format!("/pipelines/{pipeline}/stop?mode=graceful&timeout_ms=5000"),
        json!({}),
        StatusCode::OK,
    )
    .await;
    // Stopping joins the source and sink before checking for any trailing deliveries.
    while let Ok(result) = timeout(Duration::from_millis(100), output.recv()).await {
        match result {
            Ok(MemoryData::Bytes(payload)) => append_rows(&payload, &mut rows),
            Ok(MemoryData::Collection(_)) => panic!("expected encoded output"),
            Err(_) => break,
        }
    }
    let expected: Vec<_> = (0..128)
        .map(|seq| json!({"seq":seq,"next_value":seq*2+1}))
        .collect();
    assert_eq!(
        rows, expected,
        "SQL values, order, missing rows, or duplicates"
    );
    for path in [
        format!("/pipelines/{pipeline}"),
        format!("/streams/{stream}"),
    ] {
        let response = http.delete(format!("{base}{path}")).send().await.unwrap();
        assert!(
            response.status().is_success(),
            "delete {path}: {}",
            response.text().await.unwrap()
        );
    }
}
