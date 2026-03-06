use flow::{FlowInstance, FlowInstanceDedicatedRuntimeOptions, FlowInstanceSharedRegistries};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub const DEFAULT_FLOW_INSTANCE_ID: &str = "default";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum FlowInstanceBackendKind {
    #[default]
    InProcess,
    WorkerProcess,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowInstanceRuntimeSpec {
    pub worker_threads: Option<usize>,
    pub thread_name_prefix: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowInstanceCgroupSpec {
    pub process_path: Option<String>,
    pub thread_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct FlowInstanceSpec {
    pub id: String,
    pub backend: FlowInstanceBackendKind,
    pub worker_addr: Option<String>,
    pub metrics_addr: Option<String>,
    pub profile_addr: Option<String>,
    pub runtime: FlowInstanceRuntimeSpec,
    pub cgroup: FlowInstanceCgroupSpec,
}

impl Default for FlowInstanceSpec {
    fn default() -> Self {
        Self {
            id: String::new(),
            backend: FlowInstanceBackendKind::InProcess,
            worker_addr: None,
            metrics_addr: None,
            profile_addr: None,
            runtime: FlowInstanceRuntimeSpec::default(),
            cgroup: FlowInstanceCgroupSpec::default(),
        }
    }
}

impl FlowInstanceSpec {
    pub fn process_cgroup_path(&self) -> Option<&str> {
        self.cgroup.process_path.as_deref()
    }

    pub fn thread_cgroup_path(&self) -> Option<&str> {
        self.cgroup.thread_path.as_deref()
    }

    pub fn worker_addr(&self) -> Option<&str> {
        self.worker_addr.as_deref()
    }

    pub fn metrics_addr(&self) -> Option<&str> {
        self.metrics_addr.as_deref()
    }

    pub fn profile_addr(&self) -> Option<&str> {
        self.profile_addr.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowInstanceBackend {
    InProcess,
    WorkerProcess,
}

impl From<FlowInstanceBackendKind> for FlowInstanceBackend {
    fn from(value: FlowInstanceBackendKind) -> Self {
        match value {
            FlowInstanceBackendKind::InProcess => Self::InProcess,
            FlowInstanceBackendKind::WorkerProcess => Self::WorkerProcess,
        }
    }
}

pub fn find_default_flow_instance_spec(
    flow_instances: &[FlowInstanceSpec],
) -> Result<&FlowInstanceSpec, String> {
    let mut default = None;
    for spec in flow_instances {
        let id = spec.id.trim();
        if id.is_empty() {
            return Err("flow_instances contains an empty id".to_string());
        }
        if id == DEFAULT_FLOW_INSTANCE_ID {
            if default.is_some() {
                return Err("flow_instances must contain exactly one default instance".to_string());
            }
            default = Some(spec);
        }
    }

    let default = default.ok_or_else(|| {
        "flow_instances must contain a default in_process flow instance".to_string()
    })?;
    if !matches!(default.backend, FlowInstanceBackendKind::InProcess) {
        return Err("default flow instance must use backend=in_process".to_string());
    }
    Ok(default)
}

pub fn build_in_process_flow_instance(
    spec: &FlowInstanceSpec,
    shared_registries: Option<FlowInstanceSharedRegistries>,
) -> Result<FlowInstance, String> {
    if !matches!(spec.backend, FlowInstanceBackendKind::InProcess) {
        return Err(format!(
            "flow instance {} is not configured as in_process",
            spec.id.trim()
        ));
    }

    let id = spec.id.trim();
    if id.is_empty() {
        return Err("flow instance id must not be empty".to_string());
    }

    Ok(FlowInstance::new(
        flow::instance::FlowInstanceOptions::dedicated_runtime(
            id,
            shared_registries,
            FlowInstanceDedicatedRuntimeOptions {
                worker_threads: spec.runtime.worker_threads,
                thread_name_prefix: spec.runtime.thread_name_prefix.clone(),
                thread_cgroup_path: spec.thread_cgroup_path().map(|path| path.to_string()),
            },
        ),
    ))
}

pub fn new_default_flow_instance() -> FlowInstance {
    build_in_process_flow_instance(
        &FlowInstanceSpec {
            id: DEFAULT_FLOW_INSTANCE_ID.to_string(),
            backend: FlowInstanceBackendKind::InProcess,
            ..FlowInstanceSpec::default()
        },
        None,
    )
    .expect("build default in-process flow instance")
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

    #[allow(dead_code)]
    pub fn insert_local_instance(&self, instance: FlowInstance) -> Option<Arc<FlowInstance>> {
        let id = instance.id().to_string();
        if id == DEFAULT_FLOW_INSTANCE_ID {
            return None;
        }
        self.instances.write().insert(id, Arc::new(instance))
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
}
