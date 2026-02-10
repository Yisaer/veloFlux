use super::context::build_pipeline_context_payload;
use super::types::CreatePipelineRequest;
use crate::instances::FlowInstances;
use crate::worker::{
    FlowWorkerClient, WorkerApplyPipelineRequest, WorkerApplyPipelineResponse, WorkerDesiredState,
};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::time::Duration;
use storage::StorageManager;

pub(super) async fn response_to_message(resp: axum::response::Response) -> String {
    let (parts, body) = resp.into_parts();
    let status = parts.status;
    let bytes = axum::body::to_bytes(body, 64 * 1024)
        .await
        .unwrap_or_default();
    let body = String::from_utf8_lossy(&bytes);
    format!("status={status} body={}", body.trim())
}

pub(super) async fn apply_pipeline_to_worker_with_retry(
    worker: &FlowWorkerClient,
    req: &WorkerApplyPipelineRequest,
) -> Result<WorkerApplyPipelineResponse, String> {
    let mut backoff_ms = 50u64;
    let mut last_err = None;
    for attempt in 1..=8 {
        match worker.apply_pipeline(req).await {
            Ok(resp) => return Ok(resp),
            Err(err) => {
                let retryable = err.starts_with("worker request failed:");
                last_err = Some(err);
                if !retryable || attempt == 8 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(500);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "worker apply failed".to_string()))
}

pub(super) async fn apply_pipeline_in_worker(
    worker: &FlowWorkerClient,
    instances: &FlowInstances,
    storage: &StorageManager,
    pipeline_id: &str,
    pipeline_req: &CreatePipelineRequest,
    pipeline_raw_json: &str,
    desired_state: WorkerDesiredState,
) -> Result<WorkerApplyPipelineResponse, axum::response::Response> {
    let (streams, shared_mqtt_clients, memory_topics) =
        build_pipeline_context_payload(instances, storage, pipeline_id, pipeline_req)
            .map_err(|resp| *resp)?;

    let req = WorkerApplyPipelineRequest {
        pipeline: pipeline_req.clone(),
        pipeline_raw_json: pipeline_raw_json.to_string(),
        streams,
        shared_mqtt_clients,
        memory_topics,
        desired_state,
    };
    worker
        .apply_pipeline(&req)
        .await
        .map_err(|err| (StatusCode::BAD_REQUEST, err).into_response())
}
