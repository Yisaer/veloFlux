use crate::worker_protocol::{
    WorkerApplyPipelineRequest, WorkerApplyPipelineResponse, WorkerPipelineListItem,
};
use flow::processor::ProcessorStatsEntry;
use reqwest::StatusCode;

#[derive(Clone)]
pub struct FlowWorkerClient {
    base_url: String,
    http: reqwest::Client,
}

impl FlowWorkerClient {
    pub fn new(base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("build worker http client");
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub async fn apply_pipeline(
        &self,
        req: &WorkerApplyPipelineRequest,
    ) -> Result<WorkerApplyPipelineResponse, String> {
        let resp = self
            .http
            .post(self.url("/internal/v1/pipelines/apply"))
            .json(req)
            .send()
            .await
            .map_err(|e| format!("worker request failed: {e}"))?;
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read worker response body>".to_string());
            return Err(format!("worker apply failed (HTTP {code}): {body}"));
        }
        resp.json::<WorkerApplyPipelineResponse>()
            .await
            .map_err(|e| format!("decode worker apply response: {e}"))
    }

    pub async fn start_pipeline(&self, id: &str) -> Result<(), String> {
        let resp = self
            .http
            .post(self.url(&format!("/internal/v1/pipelines/{id}/start")))
            .send()
            .await
            .map_err(|e| format!("worker request failed: {e}"))?;
        match resp.status() {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err("not_found".to_string()),
            other => {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<failed to read worker response body>".to_string());
                Err(format!("worker start failed (HTTP {other}): {body}"))
            }
        }
    }

    pub async fn stop_pipeline(&self, id: &str) -> Result<(), String> {
        let resp = self
            .http
            .post(self.url(&format!("/internal/v1/pipelines/{id}/stop")))
            .send()
            .await
            .map_err(|e| format!("worker request failed: {e}"))?;
        match resp.status() {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err("not_found".to_string()),
            other => {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<failed to read worker response body>".to_string());
                Err(format!("worker stop failed (HTTP {other}): {body}"))
            }
        }
    }

    pub async fn delete_pipeline(&self, id: &str) -> Result<(), String> {
        let resp = self
            .http
            .delete(self.url(&format!("/internal/v1/pipelines/{id}")))
            .send()
            .await
            .map_err(|e| format!("worker request failed: {e}"))?;
        match resp.status() {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err("not_found".to_string()),
            other => {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<failed to read worker response body>".to_string());
                Err(format!("worker delete failed (HTTP {other}): {body}"))
            }
        }
    }

    pub async fn list_pipelines(&self) -> Result<Vec<WorkerPipelineListItem>, String> {
        let resp = self
            .http
            .get(self.url("/internal/v1/pipelines"))
            .send()
            .await
            .map_err(|e| format!("worker request failed: {e}"))?;
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read worker response body>".to_string());
            return Err(format!("worker list failed (HTTP {code}): {body}"));
        }
        resp.json::<Vec<WorkerPipelineListItem>>()
            .await
            .map_err(|e| format!("decode worker list response: {e}"))
    }

    pub async fn explain_pipeline(&self, id: &str) -> Result<String, String> {
        let resp = self
            .http
            .get(self.url(&format!("/internal/v1/pipelines/{id}/explain")))
            .send()
            .await
            .map_err(|e| format!("worker request failed: {e}"))?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Err("not_found".to_string());
        }
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read worker response body>".to_string());
            return Err(format!("worker explain failed (HTTP {code}): {body}"));
        }
        resp.text()
            .await
            .map_err(|e| format!("read worker explain response: {e}"))
    }

    pub async fn collect_stats(
        &self,
        id: &str,
        timeout_ms: u64,
    ) -> Result<Vec<ProcessorStatsEntry>, String> {
        let resp = self
            .http
            .get(self.url(&format!(
                "/internal/v1/pipelines/{id}/stats?timeout_ms={timeout_ms}"
            )))
            .send()
            .await
            .map_err(|e| format!("worker request failed: {e}"))?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Err("not_found".to_string());
        }
        if resp.status() == StatusCode::GATEWAY_TIMEOUT {
            return Err("timeout".to_string());
        }
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read worker response body>".to_string());
            return Err(format!("worker stats failed (HTTP {code}): {body}"));
        }
        resp.json::<Vec<ProcessorStatsEntry>>()
            .await
            .map_err(|e| format!("decode worker stats response: {e}"))
    }
}
