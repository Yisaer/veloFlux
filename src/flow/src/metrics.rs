use once_cell::sync::OnceCell;

static FLOW_INSTANCE_ID: OnceCell<String> = OnceCell::new();

pub fn set_flow_instance_id(id: &str) {
    let id = id.trim();
    if id.is_empty() {
        return;
    }
    let _ = FLOW_INSTANCE_ID.set(id.to_string());
}

pub(crate) fn flow_instance_id() -> &'static str {
    FLOW_INSTANCE_ID
        .get()
        .map(|s| s.as_str())
        .unwrap_or("default")
}
