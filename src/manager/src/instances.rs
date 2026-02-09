use flow::FlowInstance;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub const DEFAULT_FLOW_INSTANCE_ID: &str = "default";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FlowInstanceSpec {
    pub id: String,
}

pub fn new_default_flow_instance() -> FlowInstance {
    FlowInstance::new_default()
}

#[derive(Clone)]
pub struct FlowInstances {
    instances: Arc<RwLock<HashMap<String, Arc<FlowInstance>>>>,
}

impl FlowInstances {
    pub fn new(default_instance: FlowInstance) -> Self {
        assert_eq!(
            default_instance.id(),
            DEFAULT_FLOW_INSTANCE_ID,
            "default FlowInstance must have id=default"
        );
        let mut map = HashMap::new();
        map.insert(
            DEFAULT_FLOW_INSTANCE_ID.to_string(),
            Arc::new(default_instance),
        );
        Self {
            instances: Arc::new(RwLock::new(map)),
        }
    }

    pub fn default_instance(&self) -> Arc<FlowInstance> {
        self.get(DEFAULT_FLOW_INSTANCE_ID)
            .expect("default flow instance missing")
    }

    pub fn get(&self, id: &str) -> Option<Arc<FlowInstance>> {
        self.instances.read().get(id).cloned()
    }

    pub fn instances_snapshot(&self) -> Vec<(String, Arc<FlowInstance>)> {
        let guard = self.instances.read();
        let mut out = guard
            .iter()
            .map(|(id, inst)| (id.clone(), Arc::clone(inst)))
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    // Note: extra instances are hosted in worker subprocesses, so this type only stores the
    // in-process default instance.
}
