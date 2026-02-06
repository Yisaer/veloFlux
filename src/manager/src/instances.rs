use flow::{FlowInstance, FlowInstanceSharedRegistries};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub const DEFAULT_FLOW_INSTANCE_ID: &str = "default";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FlowInstanceSpec {
    pub id: String,
}

#[derive(Clone)]
pub struct FlowInstanceFactory {
    shared_registries: FlowInstanceSharedRegistries,
}

impl FlowInstanceFactory {
    pub fn new_default() -> FlowInstance {
        FlowInstance::new_default()
    }

    pub fn from_default_instance(default_instance: &FlowInstance) -> Self {
        Self {
            shared_registries: default_instance.shared_registries(),
        }
    }

    pub fn new_with_id(&self, id: &str) -> FlowInstance {
        FlowInstance::new_with_id(id, Some(self.shared_registries.clone()))
    }
}

pub fn new_default_flow_instance() -> FlowInstance {
    FlowInstanceFactory::new_default()
}

#[derive(Clone)]
pub struct FlowInstances {
    instances: Arc<RwLock<HashMap<String, Arc<FlowInstance>>>>,
    factory: FlowInstanceFactory,
}

impl FlowInstances {
    pub fn new(default_instance: FlowInstance) -> Self {
        assert_eq!(
            default_instance.id(),
            DEFAULT_FLOW_INSTANCE_ID,
            "default FlowInstance must have id=default"
        );
        let factory = FlowInstanceFactory::from_default_instance(&default_instance);
        let mut map = HashMap::new();
        map.insert(
            DEFAULT_FLOW_INSTANCE_ID.to_string(),
            Arc::new(default_instance),
        );
        Self {
            instances: Arc::new(RwLock::new(map)),
            factory,
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

    pub fn insert(&self, id: String, instance: FlowInstance) -> Result<Arc<FlowInstance>, ()> {
        if instance.id() != id {
            return Err(());
        }
        let mut guard = self.instances.write();
        if guard.contains_key(&id) {
            return Err(());
        }
        let instance = Arc::new(instance);
        guard.insert(id, Arc::clone(&instance));
        Ok(instance)
    }

    pub fn create_dedicated_instance(&self, id: &str) -> FlowInstance {
        self.factory.new_with_id(id)
    }
}
