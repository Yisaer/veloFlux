use async_trait::async_trait;
use datatypes::{Schema, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::catalog::{HistoryTableProps, TableCapabilities};
use crate::codec::RecordDecoder;
use crate::model::{RecordBatch, Tuple};
use crate::table::history::{
    discover_history_files, extract_history_payloads, prune_history_files,
    read_history_parquet_file, DEFAULT_HISTORY_BATCH_SIZE,
};
use std::path::PathBuf;

/// A structured equality predicate used by a table lookup.
#[derive(Debug, Clone)]
pub struct LookupKey {
    pub column: String,
    pub value: Value,
}

/// A structured predicate that a provider may execute during lookup.
#[derive(Debug, Clone)]
pub enum LookupFilter {
    Equal { column: String, value: Value },
    GreaterThan { column: String, value: Value },
    GreaterThanOrEqual { column: String, value: Value },
    LessThan { column: String, value: Value },
    LessThanOrEqual { column: String, value: Value },
}

/// An inclusive lookup time range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupTimeRange {
    pub column: String,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

/// Per-request resource limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub timeout: Option<Duration>,
}

impl Default for LookupLimits {
    fn default() -> Self {
        Self {
            max_rows: usize::MAX,
            max_bytes: usize::MAX,
            timeout: None,
        }
    }
}

/// Structured input to a table provider lookup.
#[derive(Debug, Clone)]
pub struct LookupRequest {
    pub projection: Vec<String>,
    pub equality_keys: Vec<LookupKey>,
    pub filters: Vec<LookupFilter>,
    pub time_range: Option<LookupTimeRange>,
    pub limits: LookupLimits,
    pub cancellation: CancellationToken,
}

/// Bounded rows returned by a table provider.
#[derive(Debug, Clone)]
pub struct LookupResponse {
    pub rows: Vec<Tuple>,
    pub snapshot_id: Option<String>,
}

/// Request parameters for a bounded table scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanRequest {
    pub batch_size: usize,
}

impl Default for ScanRequest {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_HISTORY_BATCH_SIZE,
        }
    }
}

/// Sink used by a provider to emit one scan batch at a time.
#[derive(Clone)]
pub struct ScanOutput {
    sender: mpsc::Sender<RecordBatch>,
}

impl ScanOutput {
    pub(crate) fn new(sender: mpsc::Sender<RecordBatch>) -> Self {
        Self { sender }
    }

    pub async fn send(&self, batch: RecordBatch) -> Result<(), TableProviderError> {
        self.sender
            .send(batch)
            .await
            .map_err(|_| TableProviderError::Provider("scan output closed".to_string()))
    }
}

/// Metadata returned after a table scan has emitted all batches.
#[derive(Debug, Clone)]
pub struct ScanResponse {
    pub snapshot_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum TableProviderError {
    #[error("lookup request is invalid: {0}")]
    InvalidRequest(String),
    #[error("lookup was cancelled")]
    Cancelled,
    #[error("lookup exceeded the configured resource limit: {0}")]
    ResourceLimit(String),
    #[error("table provider error: {0}")]
    Provider(String),
}

/// Runtime contract for a table provider.
#[async_trait]
pub trait TableProvider: Send + Sync {
    fn schema(&self) -> Arc<Schema>;

    fn capabilities(&self) -> &TableCapabilities;

    async fn scan(
        &self,
        request: ScanRequest,
        output: ScanOutput,
    ) -> Result<ScanResponse, TableProviderError>;

    async fn lookup(&self, request: LookupRequest) -> Result<LookupResponse, TableProviderError>;
}

/// Provider implementation for the existing NanoMQ history table storage.
pub struct HistoryTableProvider {
    schema: Arc<Schema>,
    props: HistoryTableProps,
    decoder: Arc<dyn RecordDecoder>,
    capabilities: TableCapabilities,
}

impl HistoryTableProvider {
    pub fn new(
        schema: Arc<Schema>,
        props: HistoryTableProps,
        decoder: Arc<dyn RecordDecoder>,
    ) -> Self {
        Self {
            schema,
            props,
            decoder,
            capabilities: TableCapabilities::history(),
        }
    }
}

#[async_trait]
impl TableProvider for HistoryTableProvider {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn capabilities(&self) -> &TableCapabilities {
        &self.capabilities
    }

    async fn scan(
        &self,
        request: ScanRequest,
        output: ScanOutput,
    ) -> Result<ScanResponse, TableProviderError> {
        if request.batch_size == 0 {
            return Err(TableProviderError::InvalidRequest(
                "scan batch size must be greater than 0".to_string(),
            ));
        }
        let datasource = PathBuf::from(&self.props.datasource);
        let mut files = discover_history_files(&datasource, &self.props.topic)
            .map_err(|err| TableProviderError::Provider(err.to_string()))?;
        files.sort_by_key(|file| file.seq);
        let files = prune_history_files(files, None, None);
        for file in files {
            let path = file.path.clone();
            let parquet_batches = tokio::task::spawn_blocking(move || {
                read_history_parquet_file(path, request.batch_size)
            })
            .await
            .map_err(|err| TableProviderError::Provider(err.to_string()))?;
            let parquet_batches = match parquet_batches {
                Ok(batches) => batches,
                Err(err) => {
                    tracing::warn!(
                        table = %self.props.topic,
                        file = %file.path.display(),
                        error = %err,
                        "skipping unreadable history file"
                    );
                    continue;
                }
            };
            for batch in parquet_batches {
                let payloads =
                    match extract_history_payloads(&batch, &self.props.time_column, None, None) {
                        Ok(payloads) => payloads,
                        Err(err) => {
                            tracing::warn!(
                                table = %self.props.topic,
                                file = %file.path.display(),
                                error = %err,
                                "skipping history parquet batch"
                            );
                            continue;
                        }
                    };
                for payload in payloads {
                    let decoded = match self.decoder.decode(&payload) {
                        Ok(decoded) => decoded,
                        Err(err) => {
                            tracing::warn!(
                                table = %self.props.topic,
                                file = %file.path.display(),
                                error = %err,
                                "skipping malformed history payload"
                            );
                            continue;
                        }
                    };
                    if decoded.num_rows() > 0 {
                        output.send(decoded).await?;
                    }
                }
            }
        }
        Ok(ScanResponse { snapshot_id: None })
    }

    async fn lookup(&self, _request: LookupRequest) -> Result<LookupResponse, TableProviderError> {
        Err(TableProviderError::Provider(
            "history table lookup is not implemented".to_string(),
        ))
    }
}
