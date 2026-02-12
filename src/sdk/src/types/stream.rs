use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamCreateRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub stream_type: String,
    pub schema: JsonValue,
    pub props: JsonValue,
    pub shared: bool,
    pub decoder: JsonValue,
}

impl StreamCreateRequest {
    pub fn mock_shared_i64_value(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name,
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
            shared: true,
            decoder: serde_json::json!({ "type": "json", "props": {} }),
        }
    }
}
