use crate::FlowInstanceSpec;
use crate::instances::{
    DEFAULT_FLOW_INSTANCE_ID, FlowInstanceBackend, FlowInstanceBackendKind, FlowInstances,
    find_default_flow_instance_spec,
};
use crate::storage_bridge;
use crate::worker::{FlowWorkerClient, WorkerApplyPipelineRequest, WorkerDesiredState};
use std::collections::HashMap;
use std::sync::Arc;
use storage::{StorageManager, StoredPipelineDesiredState};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, TryAcquireError};

use super::context::build_pipeline_context_payload;
use super::remote::{apply_pipeline_to_worker_with_retry, response_to_message};

#[derive(Clone)]
pub struct AppState {
    pub instances: FlowInstances,
    pub storage: Arc<StorageManager>,
    pub workers: Arc<HashMap<String, FlowWorkerClient>>,
    pub declared_instances: Arc<HashMap<String, FlowInstanceBackend>>,
    pipeline_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}

impl AppState {
    pub fn new(
        instance: flow::FlowInstance,
        storage: StorageManager,
        flow_instances: Vec<FlowInstanceSpec>,
        extra_flow_worker_endpoints: Vec<(String, String)>,
    ) -> Result<Self, String> {
        let instances = FlowInstances::new(instance);
        let storage = Arc::new(storage);
        let mut declared_instances = HashMap::new();
        let mut workers = HashMap::new();
        let state = Self {
            instances,
            storage,
            workers: Arc::new(HashMap::new()),
            declared_instances: Arc::new(HashMap::new()),
            pipeline_op_locks: Arc::new(Mutex::new(HashMap::new())),
        };

        find_default_flow_instance_spec(&flow_instances)?;

        for spec in flow_instances {
            let id = spec.id.trim();
            if id.is_empty() {
                return Err("flow_instances contains an empty id".to_string());
            }
            let backend = FlowInstanceBackend::from(spec.backend);
            if declared_instances.insert(id.to_string(), backend).is_some() {
                return Err(format!("duplicate flow instance id in config: {id}"));
            }
            if id == DEFAULT_FLOW_INSTANCE_ID
                && !matches!(spec.backend, FlowInstanceBackendKind::InProcess)
            {
                return Err("default flow instance must use backend=in_process".to_string());
            }
            if matches!(spec.backend, FlowInstanceBackendKind::WorkerProcess)
                && (spec.worker_addr().is_none()
                    || spec.metrics_addr().is_none()
                    || spec.profile_addr().is_none())
            {
                return Err(format!(
                    "worker_process flow instance {id} requires worker_addr, metrics_addr, and profile_addr"
                ));
            }
        }

        for (id, base_url) in extra_flow_worker_endpoints {
            match declared_instances.get(&id) {
                Some(FlowInstanceBackend::WorkerProcess) => {
                    workers.insert(id, FlowWorkerClient::new(base_url));
                }
                Some(FlowInstanceBackend::InProcess) => {
                    return Err(format!(
                        "flow worker endpoint provided for in_process instance: {id}"
                    ));
                }
                None => {
                    return Err(format!(
                        "flow worker endpoint provided for undeclared instance: {id}"
                    ));
                }
            }
        }
        for (id, backend) in &declared_instances {
            if matches!(backend, FlowInstanceBackend::WorkerProcess) && !workers.contains_key(id) {
                return Err(format!("missing flow worker endpoint for instance: {id}"));
            }
        }

        Ok(Self {
            workers: Arc::new(workers),
            declared_instances: Arc::new(declared_instances),
            ..state
        })
    }

    pub fn is_declared_instance(&self, id: &str) -> bool {
        self.declared_instances.contains_key(id)
    }

    pub fn backend(&self, id: &str) -> Option<FlowInstanceBackend> {
        self.declared_instances.get(id).copied()
    }

    pub fn worker(&self, id: &str) -> Option<FlowWorkerClient> {
        self.workers.get(id).cloned()
    }

    pub async fn bootstrap_from_storage(&self) -> Result<(), String> {
        crate::storage_bridge::hydrate_runtime_from_storage(self.storage.as_ref(), &self.instances)
            .await?;
        self.hydrate_workers_from_storage().await?;
        Ok(())
    }

    pub async fn try_acquire_pipeline_op(
        &self,
        pipeline_id: &str,
    ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        let semaphore = {
            let mut guard = self.pipeline_op_locks.lock().await;
            guard
                .entry(pipeline_id.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(1)))
                .clone()
        };
        semaphore.try_acquire_owned()
    }

    async fn hydrate_workers_from_storage(&self) -> Result<(), String> {
        if self.workers.is_empty() {
            return Ok(());
        }

        let pipelines = self
            .storage
            .list_pipelines()
            .map_err(|e| format!("list pipelines from storage: {e}"))?;

        let mut applied = 0usize;
        for pipeline in pipelines {
            let req = match storage_bridge::pipeline_request_from_stored(&pipeline) {
                Ok(req) => req,
                Err(err) => {
                    tracing::error!(
                        pipeline_id = %pipeline.id,
                        error = %err,
                        "failed to decode stored pipeline"
                    );
                    continue;
                }
            };

            let flow_instance_id = req
                .flow_instance_id
                .clone()
                .unwrap_or_else(|| DEFAULT_FLOW_INSTANCE_ID.to_string());
            if !self.is_declared_instance(&flow_instance_id) {
                tracing::warn!(
                    pipeline_id = %pipeline.id,
                    flow_instance_id = %flow_instance_id,
                    "skipping worker pipeline hydrate: flow instance not declared by config"
                );
                continue;
            }
            if !matches!(
                self.backend(&flow_instance_id),
                Some(FlowInstanceBackend::WorkerProcess)
            ) {
                continue;
            }

            let desired_state = match self
                .storage
                .get_pipeline_run_state(&pipeline.id)
                .map_err(|e| format!("read pipeline run state {}: {e}", pipeline.id))?
            {
                Some(state)
                    if matches!(state.desired_state, StoredPipelineDesiredState::Running) =>
                {
                    WorkerDesiredState::Running
                }
                _ => WorkerDesiredState::Stopped,
            };

            let (streams, shared_mqtt_clients, memory_topics) = match build_pipeline_context_payload(
                &self.instances,
                self.storage.as_ref(),
                &pipeline.id,
                &req,
            ) {
                Ok(payload) => payload,
                Err(resp) => {
                    let msg = response_to_message(*resp).await;
                    tracing::error!(
                        pipeline_id = %pipeline.id,
                        flow_instance_id = %flow_instance_id,
                        error = %msg,
                        "failed to build worker apply context from storage"
                    );
                    continue;
                }
            };

            let Some(worker) = self.worker(&flow_instance_id) else {
                tracing::error!(
                    pipeline_id = %pipeline.id,
                    flow_instance_id = %flow_instance_id,
                    "missing worker client for declared instance"
                );
                continue;
            };

            let worker_req = WorkerApplyPipelineRequest {
                pipeline: req.clone(),
                pipeline_raw_json: pipeline.raw_json.clone(),
                streams,
                shared_mqtt_clients,
                memory_topics,
                desired_state,
            };

            match apply_pipeline_to_worker_with_retry(&worker, &worker_req).await {
                Ok(resp) => {
                    applied += 1;
                    tracing::info!(
                        pipeline_id = %pipeline.id,
                        flow_instance_id = %flow_instance_id,
                        status = %resp.status,
                        "hydrated pipeline into worker"
                    );
                }
                Err(err) => {
                    tracing::error!(
                        pipeline_id = %pipeline.id,
                        flow_instance_id = %flow_instance_id,
                        error = %err,
                        "failed to hydrate pipeline into worker"
                    );
                }
            }
        }

        tracing::info!(count = applied, "worker pipeline hydration completed");
        Ok(())
    }
}
