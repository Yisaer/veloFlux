use crate::FlowInstanceSpec;
use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstances};
use crate::storage_bridge;
use crate::worker::{FlowWorkerClient, WorkerApplyPipelineRequest, WorkerDesiredState};
use std::collections::{BTreeSet, HashMap};
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
    pub declared_extra_instances: Arc<BTreeSet<String>>,
    pipeline_op_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}

impl AppState {
    pub fn new(
        instance: flow::FlowInstance,
        storage: StorageManager,
        extra_flow_instances: Vec<FlowInstanceSpec>,
        extra_flow_worker_endpoints: Vec<(String, String)>,
    ) -> Result<Self, String> {
        let instances = FlowInstances::new(instance);
        let storage = Arc::new(storage);
        let mut declared_extra = BTreeSet::new();
        let mut workers = HashMap::new();
        let state = Self {
            instances,
            storage,
            workers: Arc::new(HashMap::new()),
            declared_extra_instances: Arc::new(BTreeSet::new()),
            pipeline_op_locks: Arc::new(Mutex::new(HashMap::new())),
        };

        for spec in extra_flow_instances {
            let id = spec.id.trim();
            if id.is_empty() {
                return Err("extra_flow_instances contains an empty id".to_string());
            }
            if id == DEFAULT_FLOW_INSTANCE_ID {
                return Err("extra_flow_instances must not include default".to_string());
            }
            if !declared_extra.insert(id.to_string()) {
                return Err(format!("duplicate flow instance id in config: {id}"));
            }
        }

        for (id, base_url) in extra_flow_worker_endpoints {
            if !declared_extra.contains(&id) {
                return Err(format!(
                    "flow worker endpoint provided for undeclared instance: {id}"
                ));
            }
            workers.insert(id, FlowWorkerClient::new(base_url));
        }
        for id in &declared_extra {
            if !workers.contains_key(id) {
                return Err(format!("missing flow worker endpoint for instance: {id}"));
            }
        }

        Ok(Self {
            workers: Arc::new(workers),
            declared_extra_instances: Arc::new(declared_extra),
            ..state
        })
    }

    pub fn is_declared_instance(&self, id: &str) -> bool {
        id == DEFAULT_FLOW_INSTANCE_ID || self.declared_extra_instances.contains(id)
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
            if flow_instance_id == DEFAULT_FLOW_INSTANCE_ID {
                continue;
            }
            if !self.is_declared_instance(&flow_instance_id) {
                tracing::warn!(
                    pipeline_id = %pipeline.id,
                    flow_instance_id = %flow_instance_id,
                    "skipping worker pipeline hydrate: flow instance not declared by config"
                );
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
