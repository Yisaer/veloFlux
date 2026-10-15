#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod checkpoint;

pub use checkpoint::{CheckpointStorage, StoredCheckpoint};

const STREAMS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const PIPELINES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("pipelines");
const PIPELINE_RUN_STATES_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("pipeline_run_states");
const SCHEMAS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("schemas");
const SHARED_MQTT_CONFIGS_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("shared_mqtt_client_configs");
const MEMORY_TOPICS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("memory_topics");
const INIT_APPLY_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("init_apply_meta");
const INIT_APPLY_META_KEY: &str = "resource_directory";
const ATTACHMENT_COMMIT_KEY: &str = "attachment_transaction";
const UDFS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("udfs");
const TOMBSTONES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("resource_tombstones");
const TABLES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("tables");
const MODELS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("models");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageNamespace {
    Metadata,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("corrupted record: {0}")]
    Corrupted(&'static str),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("revision conflict: {0}")]
    RevisionConflict(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

impl StorageError {
    fn io(err: std::io::Error) -> Self {
        StorageError::Io(err.to_string())
    }

    fn backend(err: impl std::fmt::Display) -> Self {
        StorageError::Backend(err.to_string())
    }

    fn serialization(err: impl std::fmt::Display) -> Self {
        StorageError::Serialization(err.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredStream {
    pub id: String,
    pub revision: u64,
    /// Normalized stream definition serialized without identity revision.
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredSchema {
    pub name: String,
    pub revision: u64,
    /// Schema type ("proto", "json", etc.)
    pub schema_type: String,
    /// Schema-type-specific properties serialized as JSON.
    pub props_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredPipeline {
    pub id: String,
    pub revision: u64,
    /// Pipeline definition without identity revision, including its accepted deployment run_state.
    pub raw_json: String,
}

impl StoredPipeline {
    /// Last accepted deployment declaration; operational start/stop does not change it.
    pub fn deployment_run_state(&self) -> Result<StoredPipelineDesiredState, StorageError> {
        let definition: serde_json::Value =
            serde_json::from_str(&self.raw_json).map_err(StorageError::serialization)?;
        match definition.get("run_state") {
            Some(value) => {
                serde_json::from_value(value.clone()).map_err(StorageError::serialization)
            }
            None => Ok(StoredPipelineDesiredState::Stopped),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoredPipelineDesiredState {
    Stopped,
    Running,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredPipelineRunState {
    pub pipeline_id: String,
    pub desired_state: StoredPipelineDesiredState,
}

#[derive(Default)]
pub struct MetadataCommitOptions {
    pub replace: bool,
    pub init_meta: Option<StoredInitApplyMeta>,
    pub attachment_transaction: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMqttClientConfig {
    pub key: String,
    pub revision: u64,
    /// Shared MQTT client definition serialized without identity revision.
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StoredMemoryTopicKind {
    Bytes,
    Collection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMemoryTopic {
    pub topic: String,
    pub revision: u64,
    pub kind: StoredMemoryTopicKind,
    pub capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredInitApplyMeta {
    pub bundle_version: String,
    pub applied_at_ms: u64,
}

/// Persisted record for a table (history-backed scan source).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredTable {
    pub id: String,
    pub revision: u64,
    /// Original create-table request serialized without identity revision.
    pub raw_json: String,
}

/// Persisted record for a WASM UDF.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredUdf {
    /// Canonical function name (case-insensitive in SQL, stored lowercase).
    pub name: String,
    pub revision: u64,
    /// SHA-256 hex digest of the uploaded WASM file contents.
    pub wasm_sha256: String,
    /// Original upload metadata serialized without identity revision.
    pub raw_json: String,
}

/// VeloFlux-native input/output column schema stored with a model resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelSchemaColumn {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub cardinality: Option<usize>,
    #[serde(default)]
    pub fields: Option<Vec<ModelSchemaColumn>>,
    #[serde(default)]
    pub element: Option<Box<ModelSchemaColumn>>,
}

/// Persisted metadata for an immutable ONNX model bundle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredModel {
    pub name: String,
    pub revision: u64,
    pub model_sha256: String,
    pub model_size_bytes: u64,
    pub model_card_yaml: String,
    #[serde(default)]
    pub inputs: Vec<ModelSchemaColumn>,
    #[serde(default)]
    pub outputs: Vec<ModelSchemaColumn>,
    pub test_vector_count: u32,
}

/// Resource identity namespace shared by live records and deletion watermarks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    MemoryTopics,
    SharedMqttClients,
    Schemas,
    Streams,
    Pipelines,
    Udfs,
    Tables,
    Models,
}

impl ResourceKind {
    fn table(self) -> TableDefinition<'static, &'static str, &'static [u8]> {
        match self {
            Self::MemoryTopics => MEMORY_TOPICS_TABLE,
            Self::SharedMqttClients => SHARED_MQTT_CONFIGS_TABLE,
            Self::Schemas => SCHEMAS_TABLE,
            Self::Streams => STREAMS_TABLE,
            Self::Pipelines => PIPELINES_TABLE,
            Self::Udfs => UDFS_TABLE,
            Self::Tables => TABLES_TABLE,
            Self::Models => MODELS_TABLE,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredTombstone {
    pub kind: ResourceKind,
    pub identity: String,
    pub revision: u64,
}

impl StoredTombstone {
    fn key(&self) -> Result<String, StorageError> {
        tombstone_key(self.kind, &self.identity)
    }
}

fn tombstone_key(kind: ResourceKind, identity: &str) -> Result<String, StorageError> {
    let identity = if kind == ResourceKind::Udfs {
        identity.to_ascii_lowercase()
    } else {
        identity.to_string()
    };
    serde_json::to_string(&(kind, identity)).map_err(StorageError::serialization)
}

trait ResourceRecord: Serialize {
    const KIND: ResourceKind;
    fn revision(&self) -> u64;
}

macro_rules! resource_record {
    ($record:ty, $kind:ident) => {
        impl ResourceRecord for $record {
            const KIND: ResourceKind = ResourceKind::$kind;
            fn revision(&self) -> u64 {
                self.revision
            }
        }
    };
}
resource_record!(StoredStream, Streams);
resource_record!(StoredSchema, Schemas);
resource_record!(StoredPipeline, Pipelines);
resource_record!(StoredMqttClientConfig, SharedMqttClients);
resource_record!(StoredMemoryTopic, MemoryTopics);
resource_record!(StoredUdf, Udfs);
resource_record!(StoredTable, Tables);
resource_record!(StoredModel, Models);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataExportSnapshot {
    pub tombstones: Vec<StoredTombstone>,
    pub streams: Vec<StoredStream>,
    pub schemas: Vec<StoredSchema>,
    pub pipelines: Vec<StoredPipeline>,
    pub pipeline_run_states: Vec<StoredPipelineRunState>,
    pub mqtt_configs: Vec<StoredMqttClientConfig>,
    pub memory_topics: Vec<StoredMemoryTopic>,
    pub udfs: Vec<StoredUdf>,
    pub tables: Vec<StoredTable>,
    pub models: Vec<StoredModel>,
}

/// Information about an uploaded file returned by the list API.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UploadFileInfo {
    pub name: String,
    pub size_bytes: u64,
    pub modified_at: u64,
}

pub(crate) struct RedbBackend {
    pub(crate) db: Database,
    pub(crate) db_access_lock: Mutex<()>,
}

pub struct MetadataStorage {
    backend: std::sync::Arc<RedbBackend>,
    db_path: PathBuf,
}

impl MetadataStorage {
    /// Open (or create) the metadata namespace.
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db_path = Self::db_path(base_dir.as_ref());
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).map_err(StorageError::io)?;
        }

        let db = if db_path.exists() {
            Database::builder()
                .open(db_path.as_path())
                .map_err(StorageError::backend)?
        } else {
            Database::builder()
                .create(db_path.as_path())
                .map_err(StorageError::backend)?
        };

        let storage = Self {
            backend: std::sync::Arc::new(RedbBackend {
                db,
                db_access_lock: Mutex::new(()),
            }),
            db_path: db_path.clone(),
        };
        storage.ensure_tables()?;
        Ok(storage)
    }

    pub fn namespace(&self) -> StorageNamespace {
        StorageNamespace::Metadata
    }

    pub fn path(&self) -> &Path {
        &self.db_path
    }

    /// Open the independent checkpoint namespace on the same redb backend.
    pub fn checkpoint_storage(&self) -> Result<CheckpointStorage, StorageError> {
        CheckpointStorage::from_backend(std::sync::Arc::clone(&self.backend))
    }

    pub fn create_stream(&self, stream: StoredStream) -> Result<(), StorageError> {
        self.insert_if_absent(STREAMS_TABLE, &stream.id, &stream)
    }

    pub fn get_stream(&self, id: &str) -> Result<Option<StoredStream>, StorageError> {
        self.get_entry(STREAMS_TABLE, id)
    }

    pub fn list_streams(&self) -> Result<Vec<StoredStream>, StorageError> {
        self.list_entries(STREAMS_TABLE)
    }

    pub fn delete_stream(&self, id: &str) -> Result<(), StorageError> {
        self.delete_entry(STREAMS_TABLE, id)
    }

    pub fn create_schema(&self, schema: StoredSchema) -> Result<(), StorageError> {
        self.insert_if_absent(SCHEMAS_TABLE, &schema.name, &schema)
    }

    pub fn replace_schema(&self, schema: StoredSchema) -> Result<(), StorageError> {
        self.put_resource(SCHEMAS_TABLE, &schema.name, &schema)
    }

    pub fn get_schema(&self, name: &str) -> Result<Option<StoredSchema>, StorageError> {
        self.get_entry(SCHEMAS_TABLE, name)
    }

    pub fn list_schemas(&self) -> Result<Vec<StoredSchema>, StorageError> {
        self.list_entries(SCHEMAS_TABLE)
    }

    pub fn delete_schema(&self, name: &str) -> Result<(), StorageError> {
        self.delete_entry(SCHEMAS_TABLE, name)
    }

    pub fn create_pipeline(&self, pipeline: StoredPipeline) -> Result<(), StorageError> {
        self.insert_if_absent(PIPELINES_TABLE, &pipeline.id, &pipeline)
    }

    pub fn get_pipeline(&self, id: &str) -> Result<Option<StoredPipeline>, StorageError> {
        self.get_entry(PIPELINES_TABLE, id)
    }

    pub fn list_pipelines(&self) -> Result<Vec<StoredPipeline>, StorageError> {
        self.list_entries(PIPELINES_TABLE)
    }

    pub fn delete_pipeline(&self, id: &str) -> Result<(), StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(StorageError::backend)?;
        {
            let mut pipelines = txn
                .open_table(PIPELINES_TABLE)
                .map_err(StorageError::backend)?;
            let removed = pipelines.remove(id).map_err(StorageError::backend)?;
            if removed.is_none() {
                return Err(StorageError::NotFound(id.to_string()));
            }

            let mut run_states = txn
                .open_table(PIPELINE_RUN_STATES_TABLE)
                .map_err(StorageError::backend)?;
            let _ = run_states.remove(id).map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    pub fn put_pipeline_run_state(
        &self,
        state: StoredPipelineRunState,
    ) -> Result<(), StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(StorageError::backend)?;
        {
            let mut table = txn
                .open_table(PIPELINE_RUN_STATES_TABLE)
                .map_err(StorageError::backend)?;
            let encoded = encode_record(&state)?;
            table
                .insert(state.pipeline_id.as_str(), encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    pub fn get_pipeline_run_state(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<StoredPipelineRunState>, StorageError> {
        self.get_entry(PIPELINE_RUN_STATES_TABLE, pipeline_id)
    }

    pub fn delete_pipeline_run_state(&self, pipeline_id: &str) -> Result<(), StorageError> {
        self.delete_entry(PIPELINE_RUN_STATES_TABLE, pipeline_id)
    }

    pub fn create_mqtt_config(&self, config: StoredMqttClientConfig) -> Result<(), StorageError> {
        self.insert_if_absent(SHARED_MQTT_CONFIGS_TABLE, &config.key, &config)
    }

    pub fn get_mqtt_config(
        &self,
        key: &str,
    ) -> Result<Option<StoredMqttClientConfig>, StorageError> {
        self.get_entry(SHARED_MQTT_CONFIGS_TABLE, key)
    }

    pub fn list_mqtt_configs(&self) -> Result<Vec<StoredMqttClientConfig>, StorageError> {
        self.list_entries(SHARED_MQTT_CONFIGS_TABLE)
    }

    pub fn delete_mqtt_config(&self, key: &str) -> Result<(), StorageError> {
        self.delete_entry(SHARED_MQTT_CONFIGS_TABLE, key)
    }

    pub fn create_memory_topic(&self, topic: StoredMemoryTopic) -> Result<(), StorageError> {
        self.insert_if_absent(MEMORY_TOPICS_TABLE, &topic.topic, &topic)
    }

    pub fn get_memory_topic(&self, topic: &str) -> Result<Option<StoredMemoryTopic>, StorageError> {
        self.get_entry(MEMORY_TOPICS_TABLE, topic)
    }

    pub fn list_memory_topics(&self) -> Result<Vec<StoredMemoryTopic>, StorageError> {
        self.list_entries(MEMORY_TOPICS_TABLE)
    }

    pub fn delete_memory_topic(&self, topic: &str) -> Result<(), StorageError> {
        self.delete_entry(MEMORY_TOPICS_TABLE, topic)
    }

    pub fn create_udf(&self, mut udf: StoredUdf) -> Result<(), StorageError> {
        udf.name = udf.name.to_ascii_lowercase();
        self.insert_if_absent(UDFS_TABLE, &udf.name, &udf)
    }

    pub fn get_udf(&self, name: &str) -> Result<Option<StoredUdf>, StorageError> {
        self.get_entry(UDFS_TABLE, &name.to_ascii_lowercase())
    }

    pub fn list_udfs(&self) -> Result<Vec<StoredUdf>, StorageError> {
        self.list_entries(UDFS_TABLE)
    }

    pub fn delete_udf(&self, name: &str) -> Result<(), StorageError> {
        self.delete_entry(UDFS_TABLE, &name.to_ascii_lowercase())
    }

    pub fn create_table(&self, table: StoredTable) -> Result<(), StorageError> {
        self.insert_if_absent(TABLES_TABLE, &table.id, &table)
    }

    pub fn get_table(&self, id: &str) -> Result<Option<StoredTable>, StorageError> {
        self.get_entry(TABLES_TABLE, id)
    }

    pub fn list_tables(&self) -> Result<Vec<StoredTable>, StorageError> {
        self.list_entries(TABLES_TABLE)
    }

    pub fn delete_table(&self, id: &str) -> Result<(), StorageError> {
        self.delete_entry(TABLES_TABLE, id)
    }

    pub fn create_model(&self, model: StoredModel) -> Result<(), StorageError> {
        self.insert_if_absent(MODELS_TABLE, &model.name, &model)
    }

    pub fn get_model(&self, name: &str) -> Result<Option<StoredModel>, StorageError> {
        self.get_entry(MODELS_TABLE, name)
    }

    pub fn list_models(&self) -> Result<Vec<StoredModel>, StorageError> {
        self.list_entries(MODELS_TABLE)
    }

    pub fn delete_model(&self, name: &str) -> Result<(), StorageError> {
        self.delete_entry(MODELS_TABLE, name)
    }

    pub fn export_snapshot(&self) -> Result<MetadataExportSnapshot, StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_read()
            .map_err(StorageError::backend)?;
        Ok(MetadataExportSnapshot {
            tombstones: Self::list_entries_in_read_txn(&txn, TOMBSTONES_TABLE)?,
            streams: Self::list_entries_in_read_txn(&txn, STREAMS_TABLE)?,
            schemas: Self::list_entries_in_read_txn(&txn, SCHEMAS_TABLE)?,
            pipelines: Self::list_entries_in_read_txn(&txn, PIPELINES_TABLE)?,
            pipeline_run_states: Self::list_entries_in_read_txn(&txn, PIPELINE_RUN_STATES_TABLE)?,
            mqtt_configs: Self::list_entries_in_read_txn(&txn, SHARED_MQTT_CONFIGS_TABLE)?,
            memory_topics: Self::list_entries_in_read_txn(&txn, MEMORY_TOPICS_TABLE)?,
            udfs: Self::list_entries_in_read_txn(&txn, UDFS_TABLE)?,
            tables: Self::list_entries_in_read_txn(&txn, TABLES_TABLE)?,
            models: Self::list_entries_in_read_txn(&txn, MODELS_TABLE)?,
        })
    }

    pub fn get_tombstone(
        &self,
        kind: ResourceKind,
        identity: &str,
    ) -> Result<Option<StoredTombstone>, StorageError> {
        self.get_entry(TOMBSTONES_TABLE, &tombstone_key(kind, identity)?)
    }

    /// Observe only the requested deletion identities in one read transaction.
    pub fn get_tombstones(
        &self,
        identities: &std::collections::BTreeSet<(ResourceKind, String)>,
    ) -> Result<Vec<StoredTombstone>, StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_read()
            .map_err(StorageError::backend)?;
        let table = txn
            .open_table(TOMBSTONES_TABLE)
            .map_err(StorageError::backend)?;
        let mut markers = Vec::new();
        for (kind, identity) in identities {
            if let Some(raw) = table
                .get(tombstone_key(*kind, identity)?.as_str())
                .map_err(StorageError::backend)?
            {
                markers.push(decode_record(raw.value())?);
            }
        }
        Ok(markers)
    }

    pub fn list_tombstones(&self) -> Result<Vec<StoredTombstone>, StorageError> {
        self.list_entries(TOMBSTONES_TABLE)
    }

    pub fn replace_snapshot(&self, snapshot: MetadataExportSnapshot) -> Result<(), StorageError> {
        self.commit_snapshot(
            snapshot,
            MetadataCommitOptions {
                replace: true,
                ..Default::default()
            },
        )
    }

    pub fn attachment_transaction(&self) -> Result<Option<String>, StorageError> {
        self.get_entry(INIT_APPLY_META_TABLE, ATTACHMENT_COMMIT_KEY)
    }

    pub fn get_init_apply_meta(&self) -> Result<Option<StoredInitApplyMeta>, StorageError> {
        self.get_entry(INIT_APPLY_META_TABLE, INIT_APPLY_META_KEY)
    }

    pub fn put_init_apply_meta(&self, meta: StoredInitApplyMeta) -> Result<(), StorageError> {
        self.put_entry(INIT_APPLY_META_TABLE, INIT_APPLY_META_KEY, &meta)
    }

    pub fn commit_snapshot(
        &self,
        mut snapshot: MetadataExportSnapshot,
        options: MetadataCommitOptions,
    ) -> Result<(), StorageError> {
        for udf in &mut snapshot.udfs {
            udf.name = udf.name.to_ascii_lowercase();
        }
        for marker in &mut snapshot.tombstones {
            if marker.kind == ResourceKind::Udfs {
                marker.identity = marker.identity.to_ascii_lowercase();
            }
        }
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(StorageError::backend)?;
        let mut identities = std::collections::BTreeSet::new();
        macro_rules! check_resources {
            ($values:expr, $kind:ident, $key:ident) => {
                for resource in $values {
                    let identity = resource.$key.as_str();
                    if !identities.insert((ResourceKind::$kind, identity.to_string())) {
                        return Err(StorageError::InvalidInput(format!(
                            "duplicate resource identity: {identity}"
                        )));
                    }
                    if !options.replace {
                        Self::check_recreation(
                            &txn,
                            ResourceKind::$kind,
                            identity,
                            resource.revision,
                        )?;
                    }
                }
            };
        }
        check_resources!(&snapshot.streams, Streams, id);
        check_resources!(&snapshot.schemas, Schemas, name);
        check_resources!(&snapshot.pipelines, Pipelines, id);
        check_resources!(&snapshot.mqtt_configs, SharedMqttClients, key);
        check_resources!(&snapshot.memory_topics, MemoryTopics, topic);
        check_resources!(&snapshot.udfs, Udfs, name);
        check_resources!(&snapshot.tables, Tables, id);
        check_resources!(&snapshot.models, Models, name);
        for marker in &snapshot.tombstones {
            if marker.identity.is_empty()
                || marker.revision == 0
                || marker.revision > (1_u64 << 53) - 1
            {
                return Err(StorageError::InvalidInput(
                    "invalid tombstone identity or revision".to_string(),
                ));
            }
            if !identities.insert((marker.kind, marker.identity.clone())) {
                return Err(StorageError::InvalidInput(format!(
                    "duplicate resource/deletion identity: {}",
                    marker.identity
                )));
            }
            if !options.replace {
                Self::check_deletion(&txn, marker)?;
            }
        }
        if options.replace {
            Self::replace_table_entries(&txn, TOMBSTONES_TABLE, &[] as &[StoredTombstone], |v| {
                v.identity.as_str()
            })?;
        }
        macro_rules! write_entries {
            ($txn:expr, $table:expr, $values:expr, $key:expr $(,)?) => {
                if options.replace {
                    Self::replace_table_entries($txn, $table, $values, $key)
                } else {
                    Self::insert_entries($txn, $table, $values, $key)
                }
            };
        }
        let mut declarations = snapshot
            .pipeline_run_states
            .iter()
            .map(|state| (state.pipeline_id.as_str(), &state.desired_state))
            .collect::<std::collections::BTreeMap<_, _>>();
        for pipeline in &mut snapshot.pipelines {
            let Some(desired_state) = declarations.remove(pipeline.id.as_str()) else {
                continue;
            };
            let mut definition: serde_json::Value =
                serde_json::from_str(&pipeline.raw_json).map_err(StorageError::serialization)?;
            definition
                .as_object_mut()
                .ok_or_else(|| {
                    StorageError::InvalidInput("pipeline definition must be an object".to_string())
                })?
                .insert(
                    "run_state".to_string(),
                    serde_json::to_value(desired_state).map_err(StorageError::serialization)?,
                );
            pipeline.raw_json =
                serde_json::to_string(&definition).map_err(StorageError::serialization)?;
        }
        if let Some((id, _)) = declarations.first_key_value() {
            return Err(StorageError::InvalidInput(format!(
                "deployment state has no pipeline in snapshot: {id}"
            )));
        }
        write_entries!(&txn, STREAMS_TABLE, &snapshot.streams, |stream| {
            stream.id.as_str()
        })?;
        write_entries!(&txn, SCHEMAS_TABLE, &snapshot.schemas, |schema| {
            schema.name.as_str()
        })?;
        write_entries!(&txn, PIPELINES_TABLE, &snapshot.pipelines, |pipeline| {
            pipeline.id.as_str()
        })?;
        write_entries!(
            &txn,
            PIPELINE_RUN_STATES_TABLE,
            &snapshot.pipeline_run_states,
            |state| state.pipeline_id.as_str(),
        )?;
        write_entries!(
            &txn,
            SHARED_MQTT_CONFIGS_TABLE,
            &snapshot.mqtt_configs,
            |cfg| cfg.key.as_str(),
        )?;
        write_entries!(
            &txn,
            MEMORY_TOPICS_TABLE,
            &snapshot.memory_topics,
            |topic| topic.topic.as_str(),
        )?;
        write_entries!(&txn, UDFS_TABLE, &snapshot.udfs, |udf| udf.name.as_str())?;
        write_entries!(&txn, TABLES_TABLE, &snapshot.tables, |table| {
            table.id.as_str()
        })?;
        write_entries!(&txn, MODELS_TABLE, &snapshot.models, |model| {
            model.name.as_str()
        })?;
        for marker in &snapshot.tombstones {
            {
                let mut live = txn
                    .open_table(marker.kind.table())
                    .map_err(StorageError::backend)?;
                live.remove(marker.identity.as_str())
                    .map_err(StorageError::backend)?;
            }
            if marker.kind == ResourceKind::Pipelines {
                let mut states = txn
                    .open_table(PIPELINE_RUN_STATES_TABLE)
                    .map_err(StorageError::backend)?;
                states
                    .remove(marker.identity.as_str())
                    .map_err(StorageError::backend)?;
            }
            Self::put_entry_in_txn(&txn, TOMBSTONES_TABLE, &marker.key()?, marker)?;
        }
        if let Some(meta) = options.init_meta {
            Self::put_entry_in_txn(&txn, INIT_APPLY_META_TABLE, INIT_APPLY_META_KEY, &meta)?;
        }
        if let Some(transaction) = options.attachment_transaction {
            Self::put_entry_in_txn(
                &txn,
                INIT_APPLY_META_TABLE,
                ATTACHMENT_COMMIT_KEY,
                &transaction,
            )?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn db_path(base_dir: &Path) -> PathBuf {
        base_dir.join("metadata.redb")
    }

    fn lock_db(&self) -> Result<MutexGuard<'_, ()>, StorageError> {
        self.backend
            .db_access_lock
            .lock()
            .map_err(|err| StorageError::backend(format!("metadata storage lock poisoned: {err}")))
    }

    fn ensure_tables(&self) -> Result<(), StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(StorageError::backend)?;
        txn.open_table(STREAMS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(PIPELINES_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(PIPELINE_RUN_STATES_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(SHARED_MQTT_CONFIGS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(MEMORY_TOPICS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(INIT_APPLY_META_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(UDFS_TABLE).map_err(StorageError::backend)?;
        txn.open_table(TABLES_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(TOMBSTONES_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(SCHEMAS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(MODELS_TABLE)
            .map_err(StorageError::backend)?;
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn insert_if_absent<T: ResourceRecord>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(StorageError::backend)?;
        Self::check_recreation(&txn, T::KIND, key, value.revision())?;
        {
            let mut table = txn.open_table(table).map_err(StorageError::backend)?;
            if table.get(key).map_err(StorageError::backend)?.is_some() {
                return Err(StorageError::AlreadyExists(key.to_string()));
            }
            let encoded = encode_record(value)?;
            table
                .insert(key, encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn put_resource<T: ResourceRecord>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(StorageError::backend)?;
        Self::check_recreation(&txn, T::KIND, key, value.revision())?;
        Self::put_entry_in_txn(&txn, table, key, value)?;
        txn.commit().map_err(StorageError::backend)
    }

    fn check_recreation(
        txn: &WriteTransaction,
        kind: ResourceKind,
        identity: &str,
        revision: u64,
    ) -> Result<(), StorageError> {
        let key = tombstone_key(kind, identity)?;
        let mut markers = txn
            .open_table(TOMBSTONES_TABLE)
            .map_err(StorageError::backend)?;
        let marker: Option<StoredTombstone> = markers
            .get(key.as_str())
            .map_err(StorageError::backend)?
            .map(|raw| decode_record(raw.value()))
            .transpose()?;
        if let Some(marker) = marker {
            if revision <= marker.revision {
                return Err(StorageError::RevisionConflict(format!(
                    "{:?} {identity}: incoming {revision}, deleted {}",
                    kind, marker.revision
                )));
            }
            markers
                .remove(key.as_str())
                .map_err(StorageError::backend)?;
        }
        Ok(())
    }

    fn check_deletion(
        txn: &WriteTransaction,
        marker: &StoredTombstone,
    ) -> Result<(), StorageError> {
        let live = txn
            .open_table(marker.kind.table())
            .map_err(StorageError::backend)?;
        let revision = match live
            .get(marker.identity.as_str())
            .map_err(StorageError::backend)?
        {
            Some(raw) => {
                macro_rules! revision {
                    ($ty:ty) => {
                        decode_record::<$ty>(raw.value())?.revision
                    };
                }
                Some(match marker.kind {
                    ResourceKind::Streams => revision!(StoredStream),
                    ResourceKind::Schemas => revision!(StoredSchema),
                    ResourceKind::Pipelines => revision!(StoredPipeline),
                    ResourceKind::SharedMqttClients => revision!(StoredMqttClientConfig),
                    ResourceKind::MemoryTopics => revision!(StoredMemoryTopic),
                    ResourceKind::Udfs => revision!(StoredUdf),
                    ResourceKind::Tables => revision!(StoredTable),
                    ResourceKind::Models => revision!(StoredModel),
                })
            }
            None => None,
        };
        if revision.is_some_and(|revision| marker.revision <= revision) {
            return Err(StorageError::RevisionConflict(format!(
                "{:?} {}: deletion revision {} does not exceed live revision",
                marker.kind, marker.identity, marker.revision
            )));
        }
        let markers = txn
            .open_table(TOMBSTONES_TABLE)
            .map_err(StorageError::backend)?;
        if let Some(raw) = markers
            .get(marker.key()?.as_str())
            .map_err(StorageError::backend)?
        {
            let current: StoredTombstone = decode_record(raw.value())?;
            if marker.revision < current.revision {
                return Err(StorageError::RevisionConflict(format!(
                    "older deletion revision for {}",
                    marker.identity
                )));
            }
        }
        Ok(())
    }

    fn put_entry<T: Serialize>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(StorageError::backend)?;
        Self::put_entry_in_txn(&txn, table, key, value)?;
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn get_entry<T: DeserializeOwned>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
    ) -> Result<Option<T>, StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_read()
            .map_err(StorageError::backend)?;
        let table = txn.open_table(table).map_err(StorageError::backend)?;
        let result = match table.get(key).map_err(StorageError::backend)? {
            Some(value) => {
                let raw = value.value();
                Some(decode_record(raw)?)
            }
            None => None,
        };
        Ok(result)
    }

    fn delete_entry(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
    ) -> Result<(), StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(StorageError::backend)?;
        {
            let mut table = txn.open_table(table).map_err(StorageError::backend)?;
            let removed = table.remove(key).map_err(StorageError::backend)?;
            if removed.is_none() {
                return Err(StorageError::NotFound(key.to_string()));
            }
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn list_entries<T: DeserializeOwned>(
        &self,
        table: TableDefinition<&str, &[u8]>,
    ) -> Result<Vec<T>, StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_read()
            .map_err(StorageError::backend)?;
        Self::list_entries_in_read_txn(&txn, table)
    }

    fn list_entries_in_read_txn<T: DeserializeOwned>(
        txn: &ReadTransaction,
        table: TableDefinition<&str, &[u8]>,
    ) -> Result<Vec<T>, StorageError> {
        let table = txn.open_table(table).map_err(StorageError::backend)?;
        let mut items = Vec::new();
        for entry in table.range::<&str>(..).map_err(StorageError::backend)? {
            let (_, value) = entry.map_err(StorageError::backend)?;
            items.push(decode_record(value.value())?);
        }
        Ok(items)
    }

    fn replace_table_entries<T, F>(
        txn: &WriteTransaction,
        table: TableDefinition<&str, &[u8]>,
        values: &[T],
        key_fn: F,
    ) -> Result<(), StorageError>
    where
        T: Serialize,
        F: Fn(&T) -> &str,
    {
        let mut table = txn.open_table(table).map_err(StorageError::backend)?;
        drop(table.drain::<&str>(..).map_err(StorageError::backend)?);
        for value in values {
            let encoded = encode_record(value)?;
            table
                .insert(key_fn(value), encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        Ok(())
    }

    fn insert_entries<T, F>(
        txn: &WriteTransaction,
        table: TableDefinition<&str, &[u8]>,
        values: &[T],
        key_fn: F,
    ) -> Result<(), StorageError>
    where
        T: Serialize,
        F: Fn(&T) -> &str,
    {
        let mut table = txn.open_table(table).map_err(StorageError::backend)?;
        for value in values {
            let encoded = encode_record(value)?;
            table
                .insert(key_fn(value), encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        Ok(())
    }

    fn put_entry_in_txn<T: Serialize>(
        txn: &WriteTransaction,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let mut table = txn.open_table(table).map_err(StorageError::backend)?;
        let encoded = encode_record(value)?;
        table
            .insert(key, encoded.as_slice())
            .map_err(StorageError::backend)?;
        Ok(())
    }
}

/// Recursively copy upload files from src to dst. Returns the count of copied files.
fn copy_uploads_recursive(src: &Path, dst: &Path) -> Result<usize, StorageError> {
    copy_uploads_recursive_inner(src, dst, true)
}

fn copy_uploads_recursive_inner(
    src: &Path,
    dst: &Path,
    is_root: bool,
) -> Result<usize, StorageError> {
    let mut copied = 0usize;
    for entry in std::fs::read_dir(src).map_err(StorageError::io)? {
        let entry = entry.map_err(StorageError::io)?;
        let file_type = entry.file_type().map_err(StorageError::io)?;
        let raw_name = entry.file_name().to_string_lossy().into_owned();
        if is_root && raw_name == "tmp" {
            continue;
        }
        if raw_name.starts_with('.') && raw_name.ends_with(".tmp") {
            continue;
        }
        if file_type.is_dir() {
            let sub_dst = dst.join(&raw_name);
            std::fs::create_dir_all(&sub_dst).map_err(StorageError::io)?;
            copied += copy_uploads_recursive_inner(&entry.path(), &sub_dst, false)?;
        } else if file_type.is_file() {
            let target = dst.join(&raw_name);
            std::fs::copy(entry.path(), &target).map_err(StorageError::io)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))
                    .map_err(StorageError::io)?;
            }
            copied += 1;
        }
    }
    Ok(copied)
}

/// Helper: recursively collect files from an uploads subdirectory.
fn collect_upload_entries(
    dir: &Path,
    prefix: &str,
    files: &mut Vec<UploadFileInfo>,
) -> Result<(), StorageError> {
    for entry in std::fs::read_dir(dir).map_err(StorageError::io)? {
        let entry = entry.map_err(StorageError::io)?;
        let file_type = entry.file_type().map_err(StorageError::io)?;
        let raw_name = entry.file_name().to_string_lossy().into_owned();
        if prefix.is_empty() && raw_name == "tmp" {
            continue;
        }
        if raw_name.starts_with('.') && raw_name.ends_with(".tmp") {
            continue;
        }
        if file_type.is_dir() {
            let sub_prefix = if prefix.is_empty() {
                raw_name.clone()
            } else {
                format!("{prefix}/{raw_name}")
            };
            collect_upload_entries(&entry.path(), &sub_prefix, files)?;
        } else if file_type.is_file() {
            let metadata = entry.metadata().map_err(StorageError::io)?;
            let name = if prefix.is_empty() {
                raw_name
            } else {
                format!("{prefix}/{raw_name}")
            };
            let modified_at = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            files.push(UploadFileInfo {
                name,
                size_bytes: metadata.len(),
                modified_at,
            });
        }
    }
    Ok(())
}

/// Facade that exposes metadata and independent checkpoint storage capabilities.
pub struct StorageManager {
    metadata: MetadataStorage,
    base_dir: PathBuf,
}

impl StorageManager {
    /// Create a storage manager; initializes the metadata namespace under base_dir/metadata.
    /// Also creates base_dir/wasm_files/ for WASM UDF binaries.
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let metadata = MetadataStorage::open(base_dir.join("metadata"))?;
        let wasm_dir = base_dir.join("wasm_files");
        fs::create_dir_all(&wasm_dir).map_err(StorageError::io)?;
        let storage = Self { metadata, base_dir };
        storage.prepare_uploads_tmp_dir()?;
        Ok(storage)
    }

    pub fn metadata(&self) -> &MetadataStorage {
        &self.metadata
    }

    /// Open the independent checkpoint namespace on the shared redb backend.
    pub fn checkpoint_storage(&self) -> Result<CheckpointStorage, StorageError> {
        self.metadata.checkpoint_storage()
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Returns the directory where WASM UDF binaries are persisted.
    pub fn wasm_files_dir(&self) -> PathBuf {
        self.base_dir.join("wasm_files")
    }

    /// Returns the directory where uploaded user files are persisted.
    pub fn uploads_dir(&self) -> PathBuf {
        self.base_dir.join("uploads")
    }

    /// Returns the reserved directory for request-scoped temporary uploads.
    pub fn uploads_tmp_dir(&self) -> PathBuf {
        self.uploads_dir().join("tmp")
    }

    /// Returns the private directory containing installed schema sources.
    pub fn schemas_dir(&self) -> PathBuf {
        self.base_dir.join("schemas")
    }

    /// Save an uploaded file to the uploads directory. Overwrites if a file with
    /// the same name already exists. Uses a temp-file-and-rename pattern to avoid
    /// partial writes.
    pub fn save_upload(&self, name: &str, data: &[u8]) -> Result<(), StorageError> {
        let target = self.uploads_dir().join(name);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(StorageError::io)?;
        }
        let file_stem = target.file_name().ok_or_else(|| {
            StorageError::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty file name",
            ))
        })?;
        let tmp = target.with_file_name(format!(".{}.tmp", file_stem.to_string_lossy()));
        fs::write(&tmp, data).map_err(StorageError::io)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))
                .map_err(StorageError::io)?;
        }
        fs::rename(&tmp, &target).map_err(StorageError::io)?;
        Ok(())
    }

    /// Read an uploaded file from the uploads directory.
    pub fn read_upload(&self, name: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.uploads_dir().join(name);
        fs::read(&path).map_err(StorageError::io)
    }

    /// Delete an uploaded file. Returns Ok(true) if deleted, Ok(false) if not found.
    pub fn delete_upload(&self, name: &str) -> Result<bool, StorageError> {
        let path = self.uploads_dir().join(name);
        match fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StorageError::io(e)),
        }
    }

    /// List all files in the uploads directory recursively with metadata.
    pub fn list_uploads(&self) -> Result<Vec<UploadFileInfo>, StorageError> {
        let uploads_dir = self.uploads_dir();
        if !uploads_dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        collect_upload_entries(&uploads_dir, "", &mut files)?;
        files.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(files)
    }

    fn prepare_uploads_tmp_dir(&self) -> Result<(), StorageError> {
        let tmp_dir = self.uploads_tmp_dir();
        match fs::symlink_metadata(&tmp_dir) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                fs::remove_dir_all(&tmp_dir).map_err(StorageError::io)?;
            }
            Ok(_) => {
                fs::remove_file(&tmp_dir).map_err(StorageError::io)?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(StorageError::io(err)),
        }
        fs::create_dir_all(&tmp_dir).map_err(StorageError::io)
    }

    /// Copy files from a source uploads directory into the data-dir uploads
    /// directory. Handles nested subdirectories. Skips temp files. Returns the
    /// count of copied files.
    pub fn copy_uploads_from_dir(&self, src_dir: &Path) -> Result<usize, StorageError> {
        if !src_dir.exists() || !src_dir.is_dir() {
            return Ok(0);
        }
        let dst_dir = self.uploads_dir();
        fs::create_dir_all(&dst_dir).map_err(StorageError::io)?;
        copy_uploads_recursive(src_dir, &dst_dir)
    }

    pub fn create_stream(&self, stream: StoredStream) -> Result<(), StorageError> {
        self.metadata.create_stream(stream)
    }

    pub fn get_stream(&self, id: &str) -> Result<Option<StoredStream>, StorageError> {
        self.metadata.get_stream(id)
    }

    pub fn list_streams(&self) -> Result<Vec<StoredStream>, StorageError> {
        self.metadata.list_streams()
    }

    pub fn delete_stream(&self, id: &str) -> Result<(), StorageError> {
        self.metadata.delete_stream(id)
    }

    pub fn create_schema(&self, schema: StoredSchema) -> Result<(), StorageError> {
        self.metadata.create_schema(schema)
    }

    pub fn replace_schema(&self, schema: StoredSchema) -> Result<(), StorageError> {
        self.metadata.replace_schema(schema)
    }

    pub fn get_schema(&self, name: &str) -> Result<Option<StoredSchema>, StorageError> {
        self.metadata.get_schema(name)
    }

    pub fn list_schemas(&self) -> Result<Vec<StoredSchema>, StorageError> {
        self.metadata.list_schemas()
    }

    pub fn delete_schema(&self, name: &str) -> Result<(), StorageError> {
        self.metadata.delete_schema(name)
    }

    pub fn create_pipeline(&self, pipeline: StoredPipeline) -> Result<(), StorageError> {
        self.metadata.create_pipeline(pipeline)
    }

    pub fn get_pipeline(&self, id: &str) -> Result<Option<StoredPipeline>, StorageError> {
        self.metadata.get_pipeline(id)
    }

    pub fn list_pipelines(&self) -> Result<Vec<StoredPipeline>, StorageError> {
        self.metadata.list_pipelines()
    }

    pub fn delete_pipeline(&self, id: &str) -> Result<(), StorageError> {
        self.metadata.delete_pipeline(id)
    }

    pub fn put_pipeline_run_state(
        &self,
        state: StoredPipelineRunState,
    ) -> Result<(), StorageError> {
        self.metadata.put_pipeline_run_state(state)
    }

    pub fn get_pipeline_run_state(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<StoredPipelineRunState>, StorageError> {
        self.metadata.get_pipeline_run_state(pipeline_id)
    }

    pub fn delete_pipeline_run_state(&self, pipeline_id: &str) -> Result<(), StorageError> {
        self.metadata.delete_pipeline_run_state(pipeline_id)
    }

    pub fn create_mqtt_config(&self, config: StoredMqttClientConfig) -> Result<(), StorageError> {
        self.metadata.create_mqtt_config(config)
    }

    pub fn get_mqtt_config(
        &self,
        key: &str,
    ) -> Result<Option<StoredMqttClientConfig>, StorageError> {
        self.metadata.get_mqtt_config(key)
    }

    pub fn list_mqtt_configs(&self) -> Result<Vec<StoredMqttClientConfig>, StorageError> {
        self.metadata.list_mqtt_configs()
    }

    pub fn delete_mqtt_config(&self, key: &str) -> Result<(), StorageError> {
        self.metadata.delete_mqtt_config(key)
    }

    pub fn create_memory_topic(&self, topic: StoredMemoryTopic) -> Result<(), StorageError> {
        self.metadata.create_memory_topic(topic)
    }

    pub fn get_memory_topic(&self, topic: &str) -> Result<Option<StoredMemoryTopic>, StorageError> {
        self.metadata.get_memory_topic(topic)
    }

    pub fn list_memory_topics(&self) -> Result<Vec<StoredMemoryTopic>, StorageError> {
        self.metadata.list_memory_topics()
    }

    pub fn delete_memory_topic(&self, topic: &str) -> Result<(), StorageError> {
        self.metadata.delete_memory_topic(topic)
    }

    pub fn create_udf(&self, udf: StoredUdf) -> Result<(), StorageError> {
        self.metadata.create_udf(udf)
    }

    pub fn get_udf(&self, name: &str) -> Result<Option<StoredUdf>, StorageError> {
        self.metadata.get_udf(name)
    }

    pub fn list_udfs(&self) -> Result<Vec<StoredUdf>, StorageError> {
        self.metadata.list_udfs()
    }

    pub fn delete_udf(&self, name: &str) -> Result<(), StorageError> {
        self.metadata.delete_udf(name)
    }

    pub fn create_table(&self, table: StoredTable) -> Result<(), StorageError> {
        self.metadata.create_table(table)
    }

    pub fn get_table(&self, id: &str) -> Result<Option<StoredTable>, StorageError> {
        self.metadata.get_table(id)
    }

    pub fn list_tables(&self) -> Result<Vec<StoredTable>, StorageError> {
        self.metadata.list_tables()
    }

    pub fn delete_table(&self, id: &str) -> Result<(), StorageError> {
        self.metadata.delete_table(id)
    }

    pub fn create_model(&self, model: StoredModel) -> Result<(), StorageError> {
        self.metadata.create_model(model)
    }

    pub fn get_model(&self, name: &str) -> Result<Option<StoredModel>, StorageError> {
        self.metadata.get_model(name)
    }

    pub fn list_models(&self) -> Result<Vec<StoredModel>, StorageError> {
        self.metadata.list_models()
    }

    pub fn delete_model(&self, name: &str) -> Result<(), StorageError> {
        self.metadata.delete_model(name)
    }

    /// Returns the directory containing installed model bundles.
    pub fn models_dir(&self) -> PathBuf {
        self.base_dir.join("models")
    }

    /// Publish an extracted model bundle under its immutable model name.
    pub fn publish_model_bundle(&self, name: &str, staged_dir: &Path) -> Result<(), StorageError> {
        fs::create_dir_all(self.models_dir()).map_err(StorageError::io)?;
        let target = self.model_bundle_path(name)?;
        if target.exists() {
            return Err(StorageError::AlreadyExists(name.to_string()));
        }
        fs::rename(staged_dir, target).map_err(StorageError::io)
    }

    pub fn remove_model_bundle(&self, name: &str) -> Result<(), StorageError> {
        let path = self.model_bundle_path(name)?;
        match fs::remove_dir_all(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::io(error)),
        }
    }

    fn model_bundle_path(&self, name: &str) -> Result<PathBuf, StorageError> {
        let mut components = Path::new(name).components();
        if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
            return Err(StorageError::InvalidInput(
                "model name must be a single filesystem path component".to_string(),
            ));
        }
        Ok(self.models_dir().join(name))
    }

    pub fn get_tombstone(
        &self,
        kind: ResourceKind,
        identity: &str,
    ) -> Result<Option<StoredTombstone>, StorageError> {
        self.metadata.get_tombstone(kind, identity)
    }

    pub fn get_tombstones(
        &self,
        identities: &std::collections::BTreeSet<(ResourceKind, String)>,
    ) -> Result<Vec<StoredTombstone>, StorageError> {
        self.metadata.get_tombstones(identities)
    }

    pub fn list_tombstones(&self) -> Result<Vec<StoredTombstone>, StorageError> {
        self.metadata.list_tombstones()
    }

    pub fn export_metadata_snapshot(&self) -> Result<MetadataExportSnapshot, StorageError> {
        self.metadata.export_snapshot()
    }

    pub fn replace_metadata_snapshot(
        &self,
        snapshot: MetadataExportSnapshot,
    ) -> Result<(), StorageError> {
        self.metadata.replace_snapshot(snapshot)
    }

    pub fn attachment_transaction(&self) -> Result<Option<String>, StorageError> {
        self.metadata.attachment_transaction()
    }

    pub fn get_init_apply_meta(&self) -> Result<Option<StoredInitApplyMeta>, StorageError> {
        self.metadata.get_init_apply_meta()
    }

    pub fn put_init_apply_meta(&self, meta: StoredInitApplyMeta) -> Result<(), StorageError> {
        self.metadata.put_init_apply_meta(meta)
    }

    pub fn commit_metadata_snapshot(
        &self,
        snapshot: MetadataExportSnapshot,
        options: MetadataCommitOptions,
    ) -> Result<(), StorageError> {
        self.metadata.commit_snapshot(snapshot, options)
    }
}

fn encode_record<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    bincode::serialize(value).map_err(StorageError::serialization)
}

fn decode_record<T: DeserializeOwned>(raw: &[u8]) -> Result<T, StorageError> {
    bincode::deserialize(raw).map_err(StorageError::serialization)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn empty_snapshot() -> MetadataExportSnapshot {
        MetadataExportSnapshot {
            tombstones: vec![],
            streams: vec![],
            schemas: vec![],
            pipelines: vec![],
            pipeline_run_states: vec![],
            mqtt_configs: vec![],
            memory_topics: vec![],
            udfs: vec![],
            tables: vec![],
            models: vec![],
        }
    }

    #[test]
    fn revisioned_delete_survives_restart_and_requires_newer_recreation() {
        let dir = tempdir().expect("directory");
        let marker = StoredTombstone {
            kind: ResourceKind::Streams,
            identity: "sensor".into(),
            revision: 11,
        };
        {
            let store = MetadataStorage::open(dir.path()).expect("storage");
            let mut snapshot = empty_snapshot();
            snapshot.tombstones.push(marker.clone());
            store
                .commit_snapshot(snapshot.clone(), Default::default())
                .expect("delete missing resource");
            store
                .commit_snapshot(snapshot, Default::default())
                .expect("replay deletion");
        }
        let store = MetadataStorage::open(dir.path()).expect("reopen");
        assert_eq!(
            store
                .get_tombstone(ResourceKind::Streams, "sensor")
                .expect("read"),
            Some(marker)
        );
        assert!(matches!(
            store.delete_stream("sensor"),
            Err(StorageError::NotFound(_))
        ));
        for revision in [10, 11] {
            let record = StoredStream {
                id: "sensor".into(),
                revision,
                raw_json: "{}".into(),
            };
            assert!(matches!(
                store.create_stream(record),
                Err(StorageError::RevisionConflict(_))
            ));
        }
        store
            .create_stream(StoredStream {
                id: "sensor".into(),
                revision: 12,
                raw_json: "{}".into(),
            })
            .expect("recreate");
        assert_eq!(
            store
                .get_tombstone(ResourceKind::Streams, "sensor")
                .expect("read"),
            None
        );
        let mut delayed = empty_snapshot();
        delayed.tombstones.push(StoredTombstone {
            kind: ResourceKind::Streams,
            identity: "sensor".into(),
            revision: 11,
        });
        assert!(matches!(
            store.commit_snapshot(delayed, Default::default()),
            Err(StorageError::RevisionConflict(_))
        ));
        assert_eq!(
            store
                .get_stream("sensor")
                .expect("read")
                .expect("record")
                .revision,
            12
        );
    }

    #[test]
    fn snapshot_deletions_are_atomic_and_full_restore_replaces_watermarks() {
        let dir = tempdir().expect("directory");
        let store = MetadataStorage::open(dir.path()).expect("storage");
        let mut snapshot = empty_snapshot();
        snapshot.tombstones.push(StoredTombstone {
            kind: ResourceKind::Streams,
            identity: "sensor".into(),
            revision: 11,
        });
        store
            .commit_snapshot(snapshot.clone(), Default::default())
            .expect("delete");
        snapshot.streams.push(StoredStream {
            id: "sensor".into(),
            revision: 12,
            raw_json: "{}".into(),
        });
        assert!(matches!(
            store.commit_snapshot(snapshot, Default::default()),
            Err(StorageError::InvalidInput(_))
        ));
        assert!(store.get_stream("sensor").expect("read").is_none());
        assert_eq!(
            store
                .get_tombstone(ResourceKind::Streams, "sensor")
                .expect("read")
                .expect("marker")
                .revision,
            11
        );
        store
            .replace_snapshot(empty_snapshot())
            .expect("restore old snapshot");
        assert!(store
            .get_tombstone(ResourceKind::Streams, "sensor")
            .expect("read")
            .is_none());
        store
            .create_stream(StoredStream {
                id: "sensor".into(),
                revision: 1,
                raw_json: "{}".into(),
            })
            .expect("watermark replaced");
    }

    #[test]
    fn deletion_watermarks_cover_every_resource_storage_write() {
        let root = tempdir().unwrap();
        let store = MetadataStorage::open(root.path()).unwrap();
        let mut delta = empty_snapshot();
        for kind in [
            ResourceKind::MemoryTopics,
            ResourceKind::SharedMqttClients,
            ResourceKind::Schemas,
            ResourceKind::Streams,
            ResourceKind::Pipelines,
            ResourceKind::Udfs,
            ResourceKind::Tables,
        ] {
            delta.tombstones.push(StoredTombstone {
                kind,
                identity: "removed".into(),
                revision: 2,
            });
        }
        store.commit_snapshot(delta, Default::default()).unwrap();
        let mut restore = empty_snapshot();
        restore.memory_topics.push(StoredMemoryTopic {
            topic: "removed".into(),
            revision: 1,
            kind: StoredMemoryTopicKind::Bytes,
            capacity: 8,
        });
        restore.mqtt_configs.push(StoredMqttClientConfig {
            key: "removed".into(),
            revision: 1,
            raw_json: "{}".into(),
        });
        restore.schemas.push(StoredSchema {
            name: "removed".into(),
            revision: 1,
            schema_type: "json".into(),
            props_json: "{}".into(),
        });
        restore.streams.push(StoredStream {
            id: "removed".into(),
            revision: 1,
            raw_json: "{}".into(),
        });
        restore.pipelines.push(StoredPipeline {
            id: "removed".into(),
            revision: 1,
            raw_json: "{}".into(),
        });
        restore.udfs.push(StoredUdf {
            name: "removed".into(),
            revision: 1,
            wasm_sha256: "a".repeat(64),
            raw_json: "{}".into(),
        });
        restore.tables.push(StoredTable {
            id: "removed".into(),
            revision: 1,
            raw_json: "{}".into(),
        });
        assert!(matches!(
            store.create_memory_topic(restore.memory_topics[0].clone()),
            Err(StorageError::RevisionConflict(_))
        ));
        assert!(matches!(
            store.create_mqtt_config(restore.mqtt_configs[0].clone()),
            Err(StorageError::RevisionConflict(_))
        ));
        assert!(matches!(
            store.replace_schema(restore.schemas[0].clone()),
            Err(StorageError::RevisionConflict(_))
        ));
        assert!(matches!(
            store.create_stream(restore.streams[0].clone()),
            Err(StorageError::RevisionConflict(_))
        ));
        assert!(matches!(
            store.create_pipeline(restore.pipelines[0].clone()),
            Err(StorageError::RevisionConflict(_))
        ));
        assert!(matches!(
            store.create_udf(restore.udfs[0].clone()),
            Err(StorageError::RevisionConflict(_))
        ));
        assert!(matches!(
            store.create_table(restore.tables[0].clone()),
            Err(StorageError::RevisionConflict(_))
        ));
        assert!(matches!(
            store.commit_snapshot(restore.clone(), Default::default()),
            Err(StorageError::RevisionConflict(_))
        ));
        for v in &mut restore.memory_topics {
            v.revision = 3;
        }
        for v in &mut restore.mqtt_configs {
            v.revision = 3;
        }
        for v in &mut restore.schemas {
            v.revision = 3;
        }
        for v in &mut restore.streams {
            v.revision = 3;
        }
        for v in &mut restore.pipelines {
            v.revision = 3;
        }
        for v in &mut restore.udfs {
            v.revision = 3;
        }
        for v in &mut restore.tables {
            v.revision = 3;
        }
        let mut uppercase = restore.udfs[0].clone();
        uppercase.name = "REMOVED".into();
        uppercase.revision = 2;
        assert!(matches!(
            store.create_udf(uppercase),
            Err(StorageError::RevisionConflict(_))
        ));
        store.commit_snapshot(restore, Default::default()).unwrap();
        assert!(store.list_tombstones().unwrap().is_empty());
    }

    fn sample_stream() -> StoredStream {
        StoredStream {
            id: "stream_1".to_string(),
            revision: 1,
            raw_json: r#"{"name":"stream_1","type":"mqtt","schema":{"columns":[]}}"#.to_string(),
        }
    }

    fn sample_pipeline() -> StoredPipeline {
        StoredPipeline {
            id: "pipe_1".to_string(),
            revision: 1,
            raw_json: serde_json::json!({"id":"pipe_1", "sql":"SELECT * FROM stream_1", "sinks":[], "run_state":"Running"}).to_string(),
        }
    }

    fn sample_mqtt_config() -> StoredMqttClientConfig {
        StoredMqttClientConfig {
            key: "shared_a".to_string(),
            revision: 1,
            raw_json: r#"{"key":"shared_a","broker_url":"tcp://localhost:1883","topic":"foo/bar","client_id":"client_a","qos":1}"#.to_string(),
        }
    }

    fn sample_pipeline_run_state() -> StoredPipelineRunState {
        StoredPipelineRunState {
            pipeline_id: "pipe_1".to_string(),
            desired_state: StoredPipelineDesiredState::Running,
        }
    }

    fn sample_memory_topic() -> StoredMemoryTopic {
        StoredMemoryTopic {
            topic: "topic_1".to_string(),
            revision: 1,
            kind: StoredMemoryTopicKind::Bytes,
            capacity: 16,
        }
    }

    fn sample_udf(name: &str) -> StoredUdf {
        StoredUdf {
            name: name.to_string(),
            revision: 1,
            wasm_sha256: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
                .to_string(),
            raw_json: format!(r#"{{"name":"{name}","description":"test udf"}}"#),
        }
    }

    fn sample_init_apply_meta() -> StoredInitApplyMeta {
        StoredInitApplyMeta {
            bundle_version: "test-bundle-1".to_string(),
            applied_at_ms: 1234,
        }
    }

    #[test]
    fn udf_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let udf = sample_udf("my_udf");
        storage.create_udf(udf.clone()).unwrap();
        assert_eq!(storage.get_udf("my_udf").unwrap(), Some(udf.clone()));

        let all = storage.list_udfs().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "my_udf");

        storage.delete_udf("my_udf").unwrap();
        assert!(storage.get_udf("my_udf").unwrap().is_none());
    }

    #[test]
    fn udf_create_fails_on_duplicate() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let udf = sample_udf("my_udf");
        storage.create_udf(udf.clone()).unwrap();
        match storage.create_udf(udf) {
            Err(StorageError::AlreadyExists(name)) => assert_eq!(name, "my_udf"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn udf_delete_nonexistent() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        match storage.delete_udf("nonexistent") {
            Err(StorageError::NotFound(name)) => assert_eq!(name, "nonexistent"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    fn sample_table() -> StoredTable {
        StoredTable {
            id: "table_1".to_string(),
            revision: 1,
            raw_json: r#"{"name":"table_1","table_type":"history","schema":{"columns":[]},"props":{"datasource":"ds1","topic":"t1"}}"#.to_string(),
        }
    }

    #[test]
    fn table_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let table = sample_table();
        storage.create_table(table.clone()).unwrap();
        assert_eq!(storage.get_table("table_1").unwrap(), Some(table.clone()));

        let all = storage.list_tables().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, "table_1");

        storage.delete_table("table_1").unwrap();
        assert!(storage.get_table("table_1").unwrap().is_none());
    }

    #[test]
    fn table_create_fails_on_duplicate() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let table = sample_table();
        storage.create_table(table.clone()).unwrap();
        match storage.create_table(table) {
            Err(StorageError::AlreadyExists(name)) => assert_eq!(name, "table_1"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn stream_pipeline_mqtt_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let stream = sample_stream();
        storage.create_stream(stream.clone()).unwrap();
        assert_eq!(
            storage.get_stream(&stream.id).unwrap(),
            Some(stream.clone())
        );
        let streams = storage.list_streams().unwrap();
        assert_eq!(streams.len(), 1);

        let pipeline = sample_pipeline();
        storage.create_pipeline(pipeline.clone()).unwrap();
        assert_eq!(
            storage.get_pipeline(&pipeline.id).unwrap(),
            Some(pipeline.clone())
        );
        let pipelines = storage.list_pipelines().unwrap();
        assert_eq!(pipelines.len(), 1);

        let mqtt = sample_mqtt_config();
        storage.create_mqtt_config(mqtt.clone()).unwrap();
        assert_eq!(
            storage.get_mqtt_config(&mqtt.key).unwrap(),
            Some(mqtt.clone())
        );
        let mqtts = storage.list_mqtt_configs().unwrap();
        assert_eq!(mqtts.len(), 1);

        storage.delete_stream(&stream.id).unwrap();
        storage.delete_pipeline(&pipeline.id).unwrap();
        storage.delete_mqtt_config(&mqtt.key).unwrap();

        assert!(storage.get_stream(&stream.id).unwrap().is_none());
        assert!(storage.get_pipeline(&pipeline.id).unwrap().is_none());
        assert!(storage.get_mqtt_config(&mqtt.key).unwrap().is_none());
    }

    #[test]
    fn create_fails_on_duplicate_keys() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let stream = sample_stream();
        storage.create_stream(stream.clone()).unwrap();
        match storage.create_stream(stream) {
            Err(StorageError::AlreadyExists(id)) => assert_eq!(id, "stream_1"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn storage_manager_forwards_to_metadata() {
        let dir = tempdir().unwrap();
        let manager = StorageManager::new(dir.path()).unwrap();

        let stream = sample_stream();
        manager.create_stream(stream.clone()).unwrap();
        assert!(manager.get_stream(&stream.id).unwrap().is_some());

        let pipeline = sample_pipeline();
        manager.create_pipeline(pipeline.clone()).unwrap();
        assert!(manager.get_pipeline(&pipeline.id).unwrap().is_some());

        let mqtt = sample_mqtt_config();
        manager.create_mqtt_config(mqtt.clone()).unwrap();
        assert!(manager.get_mqtt_config(&mqtt.key).unwrap().is_some());

        manager.delete_stream(&stream.id).unwrap();
        manager.delete_pipeline(&pipeline.id).unwrap();
        manager.delete_mqtt_config(&mqtt.key).unwrap();
    }

    #[test]
    fn export_snapshot_captures_all_metadata_tables() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let stream = sample_stream();
        let pipeline = sample_pipeline();
        let mqtt = sample_mqtt_config();
        let run_state = sample_pipeline_run_state();
        let memory_topic = sample_memory_topic();

        storage.create_stream(stream.clone()).unwrap();
        storage.create_pipeline(pipeline.clone()).unwrap();
        storage.create_mqtt_config(mqtt.clone()).unwrap();
        storage.put_pipeline_run_state(run_state.clone()).unwrap();
        storage.create_memory_topic(memory_topic.clone()).unwrap();
        let udf = sample_udf("udf_1");
        storage.create_udf(udf.clone()).unwrap();

        let snapshot = storage.export_metadata_snapshot().unwrap();

        assert_eq!(snapshot.streams, vec![stream]);
        assert_eq!(snapshot.pipelines, vec![pipeline]);
        assert_eq!(snapshot.mqtt_configs, vec![mqtt]);
        assert_eq!(snapshot.pipeline_run_states, vec![run_state]);
        assert_eq!(snapshot.memory_topics, vec![memory_topic]);
        assert_eq!(snapshot.udfs, vec![udf]);
    }

    #[test]
    fn init_apply_snapshot_upserts_existing_and_commits_atomically() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let existing_stream = sample_stream();
        storage.create_stream(existing_stream).unwrap();

        let snapshot = MetadataExportSnapshot {
            tombstones: Vec::new(),
            streams: vec![sample_stream()],
            schemas: vec![],
            pipelines: vec![sample_pipeline()],
            pipeline_run_states: vec![sample_pipeline_run_state()],
            mqtt_configs: vec![sample_mqtt_config()],
            memory_topics: vec![sample_memory_topic()],
            udfs: vec![],
            tables: vec![],
            models: vec![],
        };

        storage
            .commit_metadata_snapshot(
                snapshot,
                MetadataCommitOptions {
                    init_meta: Some(sample_init_apply_meta()),
                    ..Default::default()
                },
            )
            .expect("upsert init snapshot");

        assert_eq!(storage.list_streams().unwrap(), vec![sample_stream()]);
        assert_eq!(storage.list_pipelines().unwrap(), vec![sample_pipeline()]);
        assert_eq!(
            storage.list_mqtt_configs().unwrap(),
            vec![sample_mqtt_config()]
        );
        assert_eq!(
            storage.list_memory_topics().unwrap(),
            vec![sample_memory_topic()]
        );
        assert_eq!(
            storage.get_init_apply_meta().unwrap(),
            Some(sample_init_apply_meta())
        );
    }

    #[test]
    fn init_apply_snapshot_persists_resources_and_meta() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let stream = sample_stream();
        let pipeline = sample_pipeline();
        let mqtt = sample_mqtt_config();
        let run_state = sample_pipeline_run_state();
        let memory_topic = sample_memory_topic();
        let meta = sample_init_apply_meta();

        let snapshot = MetadataExportSnapshot {
            tombstones: Vec::new(),
            streams: vec![stream.clone()],
            schemas: vec![],
            pipelines: vec![pipeline.clone()],
            pipeline_run_states: vec![run_state.clone()],
            mqtt_configs: vec![mqtt.clone()],
            memory_topics: vec![memory_topic.clone()],
            udfs: vec![],
            tables: vec![],
            models: vec![],
        };

        storage
            .commit_metadata_snapshot(
                snapshot,
                MetadataCommitOptions {
                    init_meta: Some(meta.clone()),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(storage.list_streams().unwrap(), vec![stream]);
        assert_eq!(storage.list_pipelines().unwrap(), vec![pipeline]);
        assert_eq!(storage.list_mqtt_configs().unwrap(), vec![mqtt]);
        assert_eq!(storage.list_memory_topics().unwrap(), vec![memory_topic]);
        assert_eq!(
            storage.get_pipeline_run_state("pipe_1").unwrap(),
            Some(run_state)
        );
        assert_eq!(storage.get_init_apply_meta().unwrap(), Some(meta));
    }

    #[test]
    fn upload_save_read_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage.save_upload("test.txt", b"hello world").unwrap();
        let data = storage.read_upload("test.txt").unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn upload_overwrite() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage.save_upload("conf.txt", b"old").unwrap();
        storage.save_upload("conf.txt", b"new").unwrap();
        let data = storage.read_upload("conf.txt").unwrap();
        assert_eq!(data, b"new");
    }

    #[test]
    fn upload_delete_nonexistent_returns_false() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        assert!(!storage.delete_upload("missing.txt").unwrap());
    }

    #[test]
    fn upload_list_empty_and_nonempty() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let list = storage.list_uploads().unwrap();
        assert!(list.is_empty());

        storage.save_upload("a.txt", b"a").unwrap();
        storage.save_upload("b.txt", b"bb").unwrap();

        let list = storage.list_uploads().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "a.txt");
        assert_eq!(list[0].size_bytes, 1);
        assert_eq!(list[1].name, "b.txt");
        assert_eq!(list[1].size_bytes, 2);
    }

    #[test]
    fn upload_list_includes_nested_files() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage
            .save_upload("proto/sensor.proto", b"syntax proto3")
            .unwrap();
        storage
            .save_upload("proto/common/types.proto", b"message T {}")
            .unwrap();
        storage.save_upload("dbc/can.dbc", b"VERSION").unwrap();

        let list = storage.list_uploads().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].name, "dbc/can.dbc");
        assert_eq!(list[1].name, "proto/common/types.proto");
        assert_eq!(list[2].name, "proto/sensor.proto");
    }

    #[test]
    fn upload_list_hides_reserved_tmp_directory() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        std::fs::write(
            storage.uploads_tmp_dir().join(".upload-stale.zip"),
            b"stale",
        )
        .unwrap();
        storage.save_upload("visible.txt", b"visible").unwrap();

        let list = storage.list_uploads().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "visible.txt");
    }

    #[test]
    fn startup_clears_reserved_tmp_directory() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let stale = storage.uploads_tmp_dir().join(".upload-stale.zip");
        std::fs::write(&stale, b"stale").unwrap();
        drop(storage);

        let storage = StorageManager::new(dir.path()).unwrap();
        assert!(storage.uploads_tmp_dir().is_dir());
        assert!(!stale.exists());
    }

    #[test]
    fn upload_copy_uploads_from_dir_copies_files() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let src = dir.path().join("src_uploads");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("cert.pem"), b"cert-data").unwrap();
        std::fs::write(src.join("sub/key.pem"), b"key-data").unwrap();

        let copied = storage.copy_uploads_from_dir(&src).unwrap();
        assert_eq!(copied, 2);

        assert_eq!(
            storage.read_upload("cert.pem").unwrap(),
            b"cert-data".to_vec()
        );
        assert_eq!(
            storage.read_upload("sub/key.pem").unwrap(),
            b"key-data".to_vec()
        );
    }

    #[test]
    fn upload_save_read_nested_path() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage
            .save_upload("proto/sub/sensor.proto", b"message Sensor {}")
            .unwrap();
        let data = storage.read_upload("proto/sub/sensor.proto").unwrap();
        assert_eq!(data, b"message Sensor {}");

        let list = storage.list_uploads().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "proto/sub/sensor.proto");
    }
}
