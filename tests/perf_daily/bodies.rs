use serde_json::{json, Value};

pub fn build_stream_body(
    stream_name: &str,
    broker_url: &str,
    topic: &str,
    qos: i64,
    column_names: &[String],
) -> Value {
    json!({
        "name": stream_name,
        "type": "mqtt",
        "props": { "broker_url": broker_url, "topic": topic, "qos": qos },
        "schema": {
            "type": "json",
            "props": {
                "columns": column_names
                    .iter()
                    .map(|name| json!({"name": name, "data_type": "string"}))
                    .collect::<Vec<_>>(),
            }
        },
        "decoder": { "type": "json", "props": {} },
        "shared": false,
    })
}

pub fn build_pipeline_body(pipeline_id: &str, sql: &str) -> Value {
    json!({
        "id": pipeline_id,
        "sql": sql,
        "sinks": [
            {
                "type": "nop",
                "commonSinkProps": { "batchDuration": 100, "batchCount": 50 }
            }
        ],
        "options": {
            "plan_cache": { "enabled": false },
            "eventtime": { "enabled": false }
        }
    })
}
