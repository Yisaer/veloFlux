//! Durable attachment publication coordinated with the metadata transaction.
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use storage::{MetadataCommitOptions, MetadataExportSnapshot, StorageManager};

use crate::schema::source::PreparedSchemaTree;

const JOURNAL: &str = ".attachment-transaction.json";
const MODEL_JOURNAL: &str = ".model-transaction.json";

#[derive(Serialize, Deserialize)]
struct Publication {
    id: String,
    schema_work: Option<PathBuf>,
    had_schemas: bool,
    #[serde(default)]
    model_work: Option<PathBuf>,
    #[serde(default)]
    had_models: bool,
    wasm_files: Vec<String>,
}

#[derive(Serialize, Deserialize)]
enum ModelOperation {
    Upload,
    Delete,
}

#[derive(Serialize, Deserialize)]
struct ModelPublication {
    name: String,
    operation: ModelOperation,
    work: PathBuf,
    had_bundle: bool,
}

pub(crate) fn needs_recovery(storage: &StorageManager) -> bool {
    storage.base_dir().join(JOURNAL).exists() || storage.base_dir().join(MODEL_JOURNAL).exists()
}

fn sync_directory(path: &Path) -> Result<(), String> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|err| format!("sync directory {}: {err}", path.display()))
}

fn sync_tree(path: &Path) -> Result<(), String> {
    for entry in fs::read_dir(path).map_err(|err| err.to_string())? {
        let entry = entry.map_err(|err| err.to_string())?;
        let kind = entry.file_type().map_err(|err| err.to_string())?;
        if kind.is_dir() {
            sync_tree(&entry.path())?;
        } else if kind.is_file() {
            fs::File::open(entry.path())
                .and_then(|file| file.sync_all())
                .map_err(|err| err.to_string())?;
        } else {
            return Err("attachment staging contains a non-regular entry".to_string());
        }
    }
    sync_directory(path)
}

pub(crate) fn publish_and_commit(
    storage: &StorageManager,
    snapshot: MetadataExportSnapshot,
    mut options: MetadataCommitOptions,
    mut schemas: Option<PreparedSchemaTree>,
    wasm_staging: &Path,
    model_staging: Option<&Path>,
) -> Result<Vec<PathBuf>, String> {
    recover(storage)?;
    fs::create_dir_all(storage.wasm_files_dir()).map_err(|err| err.to_string())?;
    let schema_work = schemas
        .as_ref()
        .and_then(PreparedSchemaTree::recovery_paths)
        .and_then(|(_, staged, _)| staged.parent().and_then(Path::file_name).map(PathBuf::from));
    if let Some(tree) = &schemas
        && let Some((_, staged, _)) = tree.recovery_paths()
    {
        sync_tree(staged)?;
    }
    sync_tree(wasm_staging)?;
    let model_work = prepare_model_work(storage, model_staging)?;
    let wasm_files = fs::read_dir(wasm_staging)
        .map_err(|err| err.to_string())?
        .map(|entry| {
            entry.map_err(|err| err.to_string()).and_then(|entry| {
                entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "invalid WASM filename".to_string())
            })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|name| !storage.wasm_files_dir().join(name).exists())
        .collect::<Vec<_>>();
    if schema_work.is_none() && wasm_files.is_empty() && model_work.is_none() {
        // Existing hashes still require byte validation before metadata commits.
        crate::apply_core::install_staged_wasm(wasm_staging, &storage.wasm_files_dir())?;
        storage
            .commit_metadata_snapshot(snapshot, options)
            .map_err(|err| format!("commit metadata: {err}"))?;
        return Ok(Vec::new());
    }
    let publication = Publication {
        id: uuid::Uuid::new_v4().to_string(),
        schema_work,
        had_schemas: storage.schemas_dir().exists(),
        model_work: model_work.as_ref().map(|(name, _, _)| name.clone()),
        had_models: storage.models_dir().exists(),
        wasm_files,
    };
    let temporary = storage.base_dir().join(format!("{JOURNAL}.tmp"));
    let mut file = fs::File::create(&temporary).map_err(|err| err.to_string())?;
    file.write_all(&serde_json::to_vec(&publication).map_err(|err| err.to_string())?)
        .and_then(|()| file.sync_all())
        .map_err(|err| err.to_string())?;
    fs::rename(&temporary, storage.base_dir().join(JOURNAL)).map_err(|err| err.to_string())?;
    sync_directory(storage.base_dir())?;
    options.attachment_transaction = Some(publication.id.clone());
    let result: Result<Vec<PathBuf>, String> = (|| {
        if let Some(tree) = &mut schemas {
            tree.activate()?;
        }
        activate_model_work(storage, model_work.as_ref())?;
        sync_directory(storage.base_dir())?;
        let installed =
            crate::apply_core::install_staged_wasm(wasm_staging, &storage.wasm_files_dir())?;
        sync_directory(&storage.wasm_files_dir())?;
        storage
            .commit_metadata_snapshot(snapshot, options)
            .map_err(|err| format!("commit metadata: {err}"))?;
        Ok(installed)
    })();
    // Recovery owns the files from this point; Drop must not undo a committed
    // publication or delete the only backup after rollback failed.
    match result {
        Ok(installed) => {
            if let Some(tree) = &mut schemas {
                tree.finish();
            }
            if let Err(err) = recover(storage) {
                tracing::warn!(error = %err, "committed attachment cleanup remains pending");
            }
            Ok(installed)
        }
        Err(mut error) => {
            if let Some(tree) = &mut schemas {
                tree.preserve_for_recovery();
            }
            if let Err(err) = recover(storage) {
                error.push_str(&format!("; rollback attachments: {err}"));
            }
            Err(error)
        }
    }
}

/// A journal written before publication and an id committed with metadata
/// determine whether startup rolls back files or finishes their cleanup.
pub(crate) fn recover(storage: &StorageManager) -> Result<(), String> {
    recover_model_transaction(storage)?;
    let journal = storage.base_dir().join(JOURNAL);
    let bytes = match fs::read(&journal) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(format!("read attachment journal: {err}")),
    };
    let publication: Publication =
        serde_json::from_slice(&bytes).map_err(|err| format!("parse attachment journal: {err}"))?;
    let committed = storage
        .attachment_transaction()
        .map_err(|err| err.to_string())?
        .as_deref()
        == Some(&publication.id);
    if let Some(name) = &publication.schema_work {
        if !matches!(
            name.components().collect::<Vec<_>>().as_slice(),
            [std::path::Component::Normal(_)]
        ) || !name
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(".schemas.import."))
        {
            return Err("unsafe schema recovery directory in attachment journal".to_string());
        }
        let work = storage.base_dir().join(name);
        if let Ok(metadata) = fs::symlink_metadata(&work)
            && !metadata.is_dir()
        {
            return Err("schema recovery root is not a regular directory".to_string());
        }
        let backup = work.join("backup");
        let target = storage.schemas_dir();
        if !committed {
            if backup.exists() {
                remove_tree(&target)?;
                fs::rename(&backup, &target)
                    .map_err(|err| format!("restore schema backup: {err}"))?;
            } else if !publication.had_schemas {
                remove_tree(&target)?;
            }
        }
        remove_tree(&work)?;
    }
    if let Some(name) = &publication.model_work {
        if !matches!(
            name.components().collect::<Vec<_>>().as_slice(),
            [std::path::Component::Normal(_)]
        ) || !name
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(".models.import."))
        {
            return Err("unsafe model recovery directory in attachment journal".to_string());
        }
        let work = storage.base_dir().join(name);
        let staged = work.join("staged");
        let backup = work.join("backup");
        if !committed {
            if backup.exists() {
                remove_tree(&storage.models_dir())?;
                fs::rename(&backup, storage.models_dir())
                    .map_err(|err| format!("restore previous model bundles: {err}"))?;
            } else if !publication.had_models {
                remove_tree(&storage.models_dir())?;
            }
        }
        let _ = staged;
        remove_tree(&work)?;
    }
    if !committed {
        for name in &publication.wasm_files {
            if !name.strip_suffix(".wasm").is_some_and(|hash| {
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            }) {
                return Err("unsafe WASM recovery filename in attachment journal".to_string());
            }
            match fs::remove_file(storage.wasm_files_dir().join(name)) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(format!("rollback WASM {name}: {err}")),
            }
        }
    }
    sync_directory(&storage.wasm_files_dir())?;
    sync_directory(storage.base_dir())?;
    fs::remove_file(journal).map_err(|err| format!("remove attachment journal: {err}"))?;
    sync_directory(storage.base_dir())
}

/// Publish a direct model upload with crash-recoverable file and metadata ordering.
#[cfg(feature = "onnx")]
pub(crate) fn publish_model_upload(
    storage: &StorageManager,
    model: storage::StoredModel,
    staged_source: &Path,
) -> Result<(), String> {
    recover_model_transaction(storage)?;
    crate::resource_id::validate_model_name(&model.name)?;
    if storage
        .get_model(&model.name)
        .map_err(|err| err.to_string())?
        .is_some()
    {
        return Err(format!("model '{}' already exists", model.name));
    }

    let target = storage.models_dir().join(&model.name);
    if target.exists() {
        // A bundle without metadata is an orphan from an interrupted legacy upload.
        storage
            .remove_model_bundle(&model.name)
            .map_err(|err| format!("remove orphaned model bundle: {err}"))?;
    }
    let publication = prepare_model_transaction(
        storage,
        &model.name,
        staged_source,
        ModelOperation::Upload,
        false,
    )?;
    write_model_journal(storage, &publication)?;
    let staged = storage.base_dir().join(&publication.work).join("staged");
    if let Err(error) = fs::rename(staged_source, &staged) {
        let recovery = recover_model_transaction(storage)
            .err()
            .unwrap_or_else(|| "completed".to_string());
        return Err(format!(
            "stage model upload: {error}; recovery: {}",
            recovery
        ));
    }
    if let Err(error) = fs::rename(&staged, &target) {
        let _ = recover_model_transaction(storage);
        return Err(format!("publish model bundle: {error}"));
    }
    sync_directory(&storage.models_dir())?;
    if let Err(error) = storage.create_model(model) {
        let recovery = recover_model_transaction(storage);
        return Err(format_model_transaction_error(
            "commit model metadata",
            error,
            recovery,
        ));
    }
    finish_model_transaction(storage, &publication)
}

/// Delete a direct model resource with crash-recoverable file and metadata ordering.
#[cfg(feature = "onnx")]
pub(crate) fn delete_model(storage: &StorageManager, name: &str) -> Result<(), String> {
    recover_model_transaction(storage)?;
    crate::resource_id::validate_model_name(name)?;
    if storage
        .get_model(name)
        .map_err(|err| err.to_string())?
        .is_none()
    {
        return Err(format!("model '{name}' not found"));
    }
    let publication = prepare_model_transaction(
        storage,
        name,
        &storage.models_dir().join(name),
        ModelOperation::Delete,
        true,
    )?;
    write_model_journal(storage, &publication)?;
    let work = storage.base_dir().join(&publication.work);
    let staged = work.join("staged");
    let backup = work.join("backup");
    if publication.had_bundle
        && let Err(error) = fs::rename(storage.models_dir().join(name), &staged)
            .and_then(|()| fs::rename(&staged, &backup))
    {
        let recovery = recover_model_transaction(storage)
            .err()
            .unwrap_or_else(|| "completed".to_string());
        return Err(format!(
            "stage model deletion: {error}; recovery: {}",
            recovery
        ));
    }
    if let Err(error) = storage.delete_model(name) {
        let recovery = recover_model_transaction(storage);
        return Err(format_model_transaction_error(
            "delete model metadata",
            error,
            recovery,
        ));
    }
    finish_model_transaction(storage, &publication)
}

#[cfg(feature = "onnx")]
fn prepare_model_transaction(
    storage: &StorageManager,
    name: &str,
    source: &Path,
    operation: ModelOperation,
    allow_missing_source: bool,
) -> Result<ModelPublication, String> {
    let work = PathBuf::from(format!(".model-operation.{}", uuid::Uuid::new_v4()));
    let root = storage.base_dir().join(&work);
    let had_bundle = source.is_dir();
    if !had_bundle && !allow_missing_source {
        return Err(format!(
            "model bundle {} is not a directory",
            source.display()
        ));
    }
    fs::create_dir(&root)
        .map_err(|error| format!("create model transaction directory: {error}"))?;
    fs::create_dir_all(storage.models_dir())
        .map_err(|error| format!("create model bundles directory: {error}"))?;
    Ok(ModelPublication {
        name: name.to_string(),
        operation,
        work,
        had_bundle,
    })
}

#[cfg(feature = "onnx")]
fn write_model_journal(
    storage: &StorageManager,
    publication: &ModelPublication,
) -> Result<(), String> {
    let temporary = storage.base_dir().join(format!("{MODEL_JOURNAL}.tmp"));
    let mut file = fs::File::create(&temporary).map_err(|error| error.to_string())?;
    file.write_all(&serde_json::to_vec(publication).map_err(|error| error.to_string())?)
        .and_then(|()| file.sync_all())
        .map_err(|error| error.to_string())?;
    fs::rename(&temporary, storage.base_dir().join(MODEL_JOURNAL))
        .map_err(|error| error.to_string())?;
    sync_directory(storage.base_dir())
}

#[cfg(feature = "onnx")]
fn finish_model_transaction(
    storage: &StorageManager,
    publication: &ModelPublication,
) -> Result<(), String> {
    remove_tree(&storage.base_dir().join(&publication.work))?;
    fs::remove_file(storage.base_dir().join(MODEL_JOURNAL))
        .map_err(|error| format!("remove model transaction journal: {error}"))?;
    sync_directory(storage.base_dir())
}

fn recover_model_transaction(storage: &StorageManager) -> Result<(), String> {
    let journal = storage.base_dir().join(MODEL_JOURNAL);
    let bytes = match fs::read(&journal) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("read model transaction journal: {error}")),
    };
    let publication: ModelPublication = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse model transaction journal: {error}"))?;
    crate::resource_id::validate_model_name(&publication.name)?;
    if !matches!(
        publication.work.components().collect::<Vec<_>>().as_slice(),
        [std::path::Component::Normal(_)]
    ) || !publication
        .work
        .file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with(".model-operation."))
    {
        return Err("unsafe model transaction directory in journal".to_string());
    }
    let root = storage.base_dir().join(&publication.work);
    let staged = root.join("staged");
    let backup = root.join("backup");
    let target = storage.models_dir().join(&publication.name);
    let metadata_exists = storage
        .get_model(&publication.name)
        .map_err(|error| error.to_string())?
        .is_some();
    match publication.operation {
        ModelOperation::Upload => {
            if metadata_exists {
                remove_tree(&root)?;
            } else {
                remove_tree(&target)?;
                remove_tree(&root)?;
            }
        }
        ModelOperation::Delete => {
            if metadata_exists {
                if backup.exists() {
                    fs::create_dir_all(storage.models_dir())
                        .map_err(|error| format!("create model directory: {error}"))?;
                    fs::rename(&backup, &target)
                        .map_err(|error| format!("restore deleted model bundle: {error}"))?;
                } else if staged.exists() {
                    fs::create_dir_all(storage.models_dir())
                        .map_err(|error| format!("create model directory: {error}"))?;
                    fs::rename(&staged, &target)
                        .map_err(|error| format!("restore staged model bundle: {error}"))?;
                }
            } else {
                remove_tree(&backup)?;
                remove_tree(&staged)?;
                remove_tree(&root)?;
            }
        }
    }
    if journal.exists() {
        fs::remove_file(journal).map_err(|error| format!("remove model journal: {error}"))?;
    }
    sync_directory(storage.base_dir())
}

#[cfg(feature = "onnx")]
fn format_model_transaction_error(
    operation: &str,
    error: storage::StorageError,
    recovery: Result<(), String>,
) -> String {
    match recovery {
        Ok(()) => format!("{operation}: {error}"),
        Err(recovery_error) => format!("{operation}: {error}; recovery failed: {recovery_error}"),
    }
}

fn prepare_model_work(
    storage: &StorageManager,
    source: Option<&Path>,
) -> Result<Option<(PathBuf, PathBuf, PathBuf)>, String> {
    let Some(source) = source else {
        return Ok(None);
    };
    if !source.is_dir() {
        return Err(format!(
            "model staging root {} is not a directory",
            source.display()
        ));
    }
    let name = PathBuf::from(format!(".models.import.{}", uuid::Uuid::new_v4()));
    let root = storage.base_dir().join(&name);
    let staged = root.join("staged");
    let backup = root.join("backup");
    fs::create_dir(&root)
        .map_err(|err| format!("create model publication work directory: {err}"))?;
    if let Err(err) = fs::rename(source, &staged) {
        let _ = fs::remove_dir_all(&root);
        return Err(format!("stage model bundles: {err}"));
    }
    Ok(Some((name, staged, backup)))
}

fn activate_model_work(
    storage: &StorageManager,
    work: Option<&(PathBuf, PathBuf, PathBuf)>,
) -> Result<(), String> {
    let Some((_, staged, backup)) = work else {
        return Ok(());
    };
    let target = storage.models_dir();
    if target.exists() {
        fs::rename(&target, backup)
            .map_err(|err| format!("backup installed model bundles: {err}"))?;
    }
    if let Err(err) = fs::rename(staged, &target) {
        if backup.exists() {
            let _ = fs::rename(backup, &target);
        }
        return Err(format!("activate imported model bundles: {err}"));
    }
    Ok(())
}

fn remove_tree(path: &Path) -> Result<(), String> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("remove {}: {err}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> MetadataExportSnapshot {
        MetadataExportSnapshot {
            tombstones: Vec::new(),
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
    fn metadata_failure_rolls_back_published_files_and_transaction_entries() {
        let dir = tempfile::tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let schemas = storage.schemas_dir();
        fs::create_dir_all(&schemas).unwrap();
        fs::write(schemas.join("old_source"), b"old").unwrap();
        let sources = tempfile::tempdir().unwrap();
        fs::write(sources.path().join("new_source"), b"new").unwrap();
        let tree = PreparedSchemaTree::prepare(&storage, sources.path()).unwrap();
        let wasm = tempfile::tempdir_in(storage.base_dir()).unwrap();
        let filename = format!("{}.wasm", "a".repeat(64));
        fs::write(wasm.path().join(&filename), b"test artifact").unwrap();
        let mut delta = snapshot();
        delta.memory_topics.push(storage::StoredMemoryTopic {
            topic: "valid_topic".to_string(),
            revision: 1,
            kind: storage::StoredMemoryTopicKind::Collection,
            capacity: 16,
        });
        // This invariant is checked inside the real metadata write transaction,
        // after topic insertion and before commit.
        delta
            .pipeline_run_states
            .push(storage::StoredPipelineRunState {
                pipeline_id: "missing_pipeline".to_string(),
                desired_state: storage::StoredPipelineDesiredState::Stopped,
            });
        let err = publish_and_commit(
            &storage,
            delta,
            MetadataCommitOptions::default(),
            Some(tree),
            wasm.path(),
            None,
        )
        .unwrap_err();
        assert!(err.contains("deployment state has no pipeline"), "{err}");
        assert!(storage.get_memory_topic("valid_topic").unwrap().is_none());
        assert_eq!(fs::read(schemas.join("old_source")).unwrap(), b"old");
        assert!(!schemas.join("new_source").exists());
        assert!(!storage.wasm_files_dir().join(filename).exists());
        assert!(!needs_recovery(&storage));
    }

    #[test]
    fn restart_distinguishes_uncommitted_and_committed_publication() {
        for committed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let storage = StorageManager::new(dir.path()).unwrap();
            let work = tempfile::Builder::new()
                .prefix(".schemas.import.")
                .tempdir_in(storage.base_dir())
                .unwrap()
                .keep();
            fs::create_dir(work.join("backup")).unwrap();
            fs::write(work.join("backup/old_source"), b"old").unwrap();
            fs::create_dir_all(storage.schemas_dir()).unwrap();
            fs::write(storage.schemas_dir().join("new_source"), b"new").unwrap();
            let publication = Publication {
                id: "interrupted_publication".to_string(),
                schema_work: Some(PathBuf::from(work.file_name().unwrap())),
                had_schemas: true,
                model_work: None,
                had_models: false,
                wasm_files: vec![],
            };
            fs::write(
                storage.base_dir().join(JOURNAL),
                serde_json::to_vec(&publication).unwrap(),
            )
            .unwrap();
            if committed {
                storage
                    .commit_metadata_snapshot(
                        snapshot(),
                        MetadataCommitOptions {
                            attachment_transaction: Some(publication.id),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
            drop(storage);
            let storage = StorageManager::new(dir.path()).unwrap();
            recover(&storage).unwrap();
            assert_eq!(storage.schemas_dir().join("new_source").exists(), committed);
            assert_eq!(
                storage.schemas_dir().join("old_source").exists(),
                !committed
            );
            assert!(!work.exists());
            assert!(!needs_recovery(&storage));
        }
    }
}
