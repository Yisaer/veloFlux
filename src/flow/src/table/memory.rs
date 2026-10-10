use async_trait::async_trait;
use datatypes::{Schema, Value};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::catalog::MemoryMockTableProps;
use crate::catalog::{TableCapabilities, TableLookupCapability};
use crate::codec::{JsonDecoder, RecordDecoder};
use crate::model::{Message, Tuple};
use crate::table::provider::{
    LookupFilter, LookupOutput, LookupRequest, LookupResponse, LookupTimeRange, ScanOutput,
    ScanRequest, ScanResponse, TableProvider, TableProviderError,
};

/// Runtime behavior controls for the in-memory table provider.
#[derive(Debug, Clone, Default)]
pub struct MemoryTableBehavior {
    pub delay: Option<Duration>,
    pub error: Option<String>,
}

/// An in-memory table provider for provider and lookup integration tests.
pub struct MemoryMockTableProvider {
    schema: Arc<Schema>,
    source_name: Arc<str>,
    state: RwLock<MemoryTableState>,
    capabilities: TableCapabilities,
    requests: Mutex<Vec<LookupRequest>>,
    behavior: RwLock<MemoryTableBehavior>,
}

struct MemoryTableState {
    rows: Vec<Tuple>,
    indexes: HashMap<String, HashMap<Value, Vec<usize>>>,
}

impl MemoryMockTableProvider {
    pub fn new(schema: Arc<Schema>) -> Self {
        let equality_keys: Vec<String> = schema
            .column_schemas()
            .iter()
            .map(|column| column.name.clone())
            .collect();
        Self {
            schema,
            source_name: Arc::from("memory_table"),
            state: RwLock::new(MemoryTableState {
                rows: Vec::new(),
                indexes: HashMap::new(),
            }),
            capabilities: TableCapabilities {
                scan: Some(crate::catalog::TableScanCapability {}),
                lookup: Some(TableLookupCapability {
                    lookup_key_sets: equality_keys
                        .iter()
                        .map(|column| vec![column.clone()])
                        .collect(),
                    supports_projection: true,
                    supports_predicates: true,
                    supports_time_range: true,
                }),
            },
            requests: Mutex::new(Vec::new()),
            behavior: RwLock::new(MemoryTableBehavior::default()),
        }
    }

    pub fn from_props(
        schema: Arc<Schema>,
        props: &MemoryMockTableProps,
        source_name: impl Into<String>,
    ) -> Result<Self, TableProviderError> {
        let provider = Self::new(Arc::clone(&schema));
        let decoder = JsonDecoder::new(source_name, schema, serde_json::Map::new());
        let mut rows = Vec::new();
        for row in &props.rows {
            let payload = serde_json::to_vec(row).map_err(|err| {
                TableProviderError::InvalidRequest(format!("invalid memory mock row: {err}"))
            })?;
            let batch = decoder
                .decode(&payload)
                .map_err(|err| TableProviderError::InvalidRequest(err.to_string()))?;
            rows.extend(batch.into_parts().0);
        }
        provider.replace_rows(rows);
        Ok(provider)
    }

    pub fn with_source_name(mut self, source_name: impl Into<Arc<str>>) -> Self {
        self.source_name = source_name.into();
        self
    }

    pub fn with_rows(self, rows: Vec<Tuple>) -> Self {
        self.replace_rows(rows);
        self
    }

    pub fn append_row(&self, row: Tuple) {
        let mut state = self.state.write();
        let row_index = state.rows.len();
        state.rows.push(row.clone());
        Self::add_row_to_indexes(&mut state.indexes, row_index, &row);
    }

    pub fn replace_rows(&self, rows: Vec<Tuple>) {
        let mut state = self.state.write();
        state.rows = rows.clone();
        state.indexes.clear();
        for (row_index, row) in rows.iter().enumerate() {
            Self::add_row_to_indexes(&mut state.indexes, row_index, row);
        }
    }

    pub fn clear_rows(&self) {
        let mut state = self.state.write();
        state.rows.clear();
        state.indexes.clear();
    }

    pub fn set_behavior(&self, behavior: MemoryTableBehavior) {
        *self.behavior.write() = behavior;
    }

    pub fn requests(&self) -> Vec<LookupRequest> {
        self.requests.lock().clone()
    }

    pub fn clear_requests(&self) {
        self.requests.lock().clear();
    }

    fn add_row_to_indexes(
        indexes: &mut HashMap<String, HashMap<Value, Vec<usize>>>,
        row_index: usize,
        row: &Tuple,
    ) {
        for message in row.messages() {
            for (column, value) in message.entries() {
                indexes
                    .entry(column.to_string())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .push(row_index);
            }
        }
    }

    fn value_for_column<'a>(row: &'a Tuple, column: &str) -> Option<&'a Value> {
        row.messages()
            .iter()
            .find_map(|message| message.value(column))
    }

    fn matches_filter(row: &Tuple, filter: &LookupFilter) -> bool {
        match filter {
            LookupFilter::Equal { column, value } => {
                Self::value_for_column(row, column).is_some_and(|actual| actual == value)
            }
            LookupFilter::GreaterThan { column, value } => Self::value_for_column(row, column)
                .and_then(|actual| compare_values(actual, value))
                .is_some_and(|ordering| ordering.is_gt()),
            LookupFilter::GreaterThanOrEqual { column, value } => {
                Self::value_for_column(row, column)
                    .and_then(|actual| compare_values(actual, value))
                    .is_some_and(|ordering| !ordering.is_lt())
            }
            LookupFilter::LessThan { column, value } => Self::value_for_column(row, column)
                .and_then(|actual| compare_values(actual, value))
                .is_some_and(|ordering| ordering.is_lt()),
            LookupFilter::LessThanOrEqual { column, value } => Self::value_for_column(row, column)
                .and_then(|actual| compare_values(actual, value))
                .is_some_and(|ordering| !ordering.is_gt()),
        }
    }

    fn matches_time_range(row: &Tuple, range: &LookupTimeRange) -> bool {
        let Some(value) = Self::value_for_column(row, &range.column) else {
            return false;
        };
        let Some(value) = value_as_i64(value) else {
            return false;
        };
        range.start.is_none_or(|start| value >= start) && range.end.is_none_or(|end| value <= end)
    }

    fn project_row(&self, row: &Tuple, projection: &[String]) -> Tuple {
        if projection.is_empty() {
            return row.clone();
        }
        let keys = projection
            .iter()
            .map(|column| Arc::<str>::from(column.as_str()))
            .collect();
        let values = projection
            .iter()
            .map(|column| {
                Self::value_for_column(row, column)
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect();
        Tuple::with_timestamp(
            Arc::from(vec![Arc::new(Message::new(
                Arc::clone(&self.source_name),
                keys,
                values,
            ))]),
            row.timestamp,
        )
    }
}

#[async_trait]
impl TableProvider for MemoryMockTableProvider {
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
        let rows = self.state.read().rows.clone();
        for chunk in rows.chunks(request.batch_size) {
            let batch = crate::model::RecordBatch::new(chunk.to_vec())
                .map_err(|err| TableProviderError::Provider(err.to_string()))?;
            output.send(batch).await?;
        }
        Ok(ScanResponse { snapshot_id: None })
    }

    async fn lookup(
        &self,
        request: LookupRequest,
        output: LookupOutput,
    ) -> Result<LookupResponse, TableProviderError> {
        self.requests.lock().push(request.clone());
        if request.cancellation.is_cancelled() {
            return Err(TableProviderError::Cancelled);
        }
        if request.batch_size == 0 {
            return Err(TableProviderError::InvalidRequest(
                "lookup batch size must be greater than 0".to_string(),
            ));
        }

        let behavior = self.behavior.read().clone();
        if let Some(delay) = behavior.delay {
            sleep(delay).await;
        }
        if request.cancellation.is_cancelled() {
            return Err(TableProviderError::Cancelled);
        }
        if let Some(error) = behavior.error {
            return Err(TableProviderError::Provider(error));
        }

        let candidate_indexes = {
            let state = self.state.read();
            if request.equality_keys.is_empty() {
                (0..state.rows.len()).collect::<Vec<_>>()
            } else {
                let indexes = &state.indexes;
                let mut candidates = request
                    .equality_keys
                    .iter()
                    .map(|key| {
                        if key.value.is_null() {
                            return Ok(Vec::new());
                        }
                        let index = indexes.get(&key.column).ok_or_else(|| {
                            TableProviderError::InvalidRequest(format!(
                                "lookup column `{}` is not indexed",
                                key.column
                            ))
                        })?;
                        Ok(index.get(&key.value).cloned().unwrap_or_default())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                candidates.sort_by_key(Vec::len);
                let Some(mut result) = candidates.first().cloned() else {
                    return Ok(LookupResponse { snapshot_id: None });
                };
                for candidate in candidates.iter().skip(1) {
                    result.retain(|row_index| candidate.binary_search(row_index).is_ok());
                }
                result.sort_unstable();
                result
            }
        };
        let mut batch = Vec::with_capacity(request.batch_size);
        for row_index in candidate_indexes {
            if request.cancellation.is_cancelled() {
                return Err(TableProviderError::Cancelled);
            }
            let projected_row = {
                let state = self.state.read();
                let Some(row) = state.rows.get(row_index) else {
                    continue;
                };
                if request.equality_keys.iter().any(|key| {
                    Self::value_for_column(row, &key.column).is_none_or(|value| value != &key.value)
                }) {
                    continue;
                }
                if request
                    .filters
                    .iter()
                    .any(|filter| !Self::matches_filter(row, filter))
                {
                    continue;
                }
                if request
                    .time_range
                    .as_ref()
                    .is_some_and(|range| !Self::matches_time_range(row, range))
                {
                    continue;
                }
                self.project_row(row, &request.projection)
            };
            batch.push(projected_row);
            if batch.len() == request.batch_size {
                output.send(std::mem::take(&mut batch)).await?;
            }
        }
        if !batch.is_empty() {
            output.send(batch).await?;
        }
        Ok(LookupResponse { snapshot_id: None })
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int64(value) => Some(*value),
        Value::Uint64(value) => i64::try_from(*value).ok(),
        Value::Timestamp(value) => Some(value.epoch_micros()),
        _ => None,
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => Some(left.cmp(right)),
        (Value::Uint64(left), Value::Uint64(right)) => Some(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right)) => Some(left.cmp(right)),
        _ => value_as_i64(left)
            .zip(value_as_i64(right))
            .map(|(l, r)| l.cmp(&r)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::provider::{LookupKey, LookupOutput};
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, StringType};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "table".to_string(),
                "id".to_string(),
                ConcreteDatatype::String(StringType),
            ),
            ColumnSchema::new(
                "table".to_string(),
                "ts".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "table".to_string(),
                "value".to_string(),
                ConcreteDatatype::String(StringType),
            ),
        ]))
    }

    fn row(id: &str, ts: i64, value: &str) -> Tuple {
        Tuple::new(vec![Arc::new(Message::new(
            "table",
            vec![Arc::from("id"), Arc::from("ts"), Arc::from("value")],
            vec![
                Value::String(id.to_string().into()),
                Value::Int64(ts),
                Value::String(value.to_string().into()),
            ],
        ))])
    }

    fn request() -> LookupRequest {
        LookupRequest {
            projection: Vec::new(),
            equality_keys: Vec::new(),
            filters: Vec::new(),
            time_range: None,
            batch_size: 1024,
            cancellation: CancellationToken::new(),
        }
    }

    async fn lookup_rows(
        provider: Arc<MemoryMockTableProvider>,
        request: LookupRequest,
    ) -> Result<Vec<Tuple>, TableProviderError> {
        let (sender, mut receiver) = mpsc::channel(1);
        let task =
            tokio::spawn(async move { provider.lookup(request, LookupOutput::new(sender)).await });
        let mut rows = Vec::new();
        while let Some(batch) = receiver.recv().await {
            rows.extend(batch);
        }
        task.await.expect("lookup task panicked").map(|_| rows)
    }

    #[tokio::test]
    async fn lookup_filters_projects_and_preserves_row_order() {
        let provider = Arc::new(MemoryMockTableProvider::new(schema()).with_rows(vec![
            row("v1", 100, "first"),
            row("v2", 200, "second"),
            row("v1", 300, "third"),
        ]));
        let mut lookup = request();
        lookup.equality_keys.push(LookupKey {
            column: "id".to_string(),
            value: Value::String("v1".to_string().into()),
        });
        lookup.projection = vec!["value".to_string()];
        lookup.time_range = Some(LookupTimeRange {
            column: "ts".to_string(),
            start: Some(200),
            end: Some(300),
        });

        let rows = lookup_rows(Arc::clone(&provider), lookup).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].value_by_name("memory_table", "value"),
            Some(&Value::String("third".to_string().into()))
        );
        assert_eq!(provider.requests().len(), 1);
    }

    #[tokio::test]
    async fn lookup_uses_all_equality_keys_and_preserves_table_order() {
        let provider = Arc::new(MemoryMockTableProvider::new(schema()).with_rows(vec![
            row("v1", 100, "first"),
            row("v2", 100, "other"),
            row("v1", 200, "second"),
            row("v1", 100, "third"),
        ]));
        let mut lookup = request();
        lookup.equality_keys = vec![
            LookupKey {
                column: "id".to_string(),
                value: Value::String("v1".to_string().into()),
            },
            LookupKey {
                column: "ts".to_string(),
                value: Value::Int64(100),
            },
        ];
        lookup.projection = vec!["value".to_string()];

        let rows = lookup_rows(Arc::clone(&provider), lookup).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].value_by_name("memory_table", "value"),
            Some(&Value::String("first".to_string().into()))
        );
        assert_eq!(
            rows[1].value_by_name("memory_table", "value"),
            Some(&Value::String("third".to_string().into()))
        );
    }

    #[tokio::test]
    async fn rows_can_be_refreshed_and_cancellation_is_observed() {
        let provider =
            Arc::new(MemoryMockTableProvider::new(schema()).with_rows(vec![row("v1", 1, "old")]));
        provider.replace_rows(vec![row("v1", 2, "new")]);
        let rows = lookup_rows(Arc::clone(&provider), request()).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].value_by_name("table", "value"),
            Some(&Value::String("new".to_string().into()))
        );

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut cancelled_request = request();
        cancelled_request.cancellation = cancellation;
        let (sender, _receiver) = mpsc::channel(1);
        assert!(matches!(
            provider
                .lookup(cancelled_request, LookupOutput::new(sender))
                .await,
            Err(TableProviderError::Cancelled)
        ));
    }

    #[tokio::test]
    async fn configured_provider_error_is_returned() {
        let provider = Arc::new(MemoryMockTableProvider::new(schema()));
        provider.set_behavior(MemoryTableBehavior {
            delay: None,
            error: Some("injected failure".to_string()),
        });
        let (sender, _receiver) = mpsc::channel(1);
        assert!(matches!(
            provider.lookup(request(), LookupOutput::new(sender)).await,
            Err(TableProviderError::Provider(message)) if message == "injected failure"
        ));
    }
}
