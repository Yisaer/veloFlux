//! Durable attachment publication coordinated with the metadata transaction.
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use storage::{MetadataCommitOptions, MetadataExportSnapshot, StorageManager};

use crate::schema::source::PreparedSchemaTree;

const JOURNAL: &str = ".attachment-transaction.json";

#[derive(Serialize, Deserialize)]
struct Publication {
    id: String,
    schema_work: Option<PathBuf>,
    had_schemas: bool,
    wasm_files: Vec<String>,
}

pub(crate) fn needs_recovery(storage: &StorageManager) -> bool {
    storage.base_dir().join(JOURNAL).exists()
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
    if schema_work.is_none() && wasm_files.is_empty() {
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
            streams: vec![],
            schemas: vec![],
            pipelines: vec![],
            pipeline_run_states: vec![],
            mqtt_configs: vec![],
            memory_topics: vec![],
            udfs: vec![],
            tables: vec![],
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
