use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde::de::DeserializeOwned;
use storage::{MetadataExportSnapshot, StorageManager, StoredInitApplyMeta};

use crate::export::{
    ExportResources, RESOURCE_DIRECTORY_FORMAT_VERSION, ResourceManifestV1, build_export_resources,
    validate_bundle_version,
};
use crate::import::validate_and_build_snapshot;
use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::schema::source::PreparedSchemaTree;
use crate::startup::StartupPhase;

const INIT_APPLY_PHASE: &str = "init_directory_apply";
const MANIFEST_FILE: &str = "manifest.json";
const MAX_SOURCE_ENTRIES: usize = 4096;
const MAX_SOURCE_FILE_SIZE: u64 = 512 * 1024 * 1024;
const MAX_SOURCE_TOTAL_SIZE: u64 = 512 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInitManifest {
    format_version: u32,
    bundle_version: String,
    #[serde(default)]
    resources: RawInitResources,
    #[serde(default)]
    deletions: RawInitResources,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInitResources {
    #[serde(default)]
    memory_topics: Vec<serde_json::Value>,
    #[serde(default)]
    shared_mqtt_clients: Vec<serde_json::Value>,
    #[serde(default)]
    schemas: Vec<serde_json::Value>,
    #[serde(default)]
    streams: Vec<serde_json::Value>,
    #[serde(default)]
    pipelines: Vec<serde_json::Value>,
    #[serde(default)]
    udfs: Vec<serde_json::Value>,
    #[serde(default)]
    tables: Vec<serde_json::Value>,
}

struct ParsedInitManifest {
    manifest: ResourceManifestV1,
    had_entry_errors: bool,
}

pub(crate) fn apply_init_directory_if_needed<F>(
    storage: &StorageManager,
    init_dir: Option<&Path>,
    is_declared_instance: &F,
) -> Result<(), String>
where
    F: Fn(&str) -> bool,
{
    crate::attachments::recover(storage)?;
    let staging_root = storage.base_dir().join(".init-staging");
    if let Err(err) =
        fs::create_dir_all(&staging_root).map_err(|err| format!("create init staging root: {err}"))
    {
        tracing::warn!(
            phase = INIT_APPLY_PHASE,
            result = "skipped",
            reason = %err,
            "startup init directory"
        );
        return Ok(());
    }
    if let Err(err) = clean_staging_root(&staging_root) {
        tracing::warn!(
            phase = INIT_APPLY_PHASE,
            result = "skipped",
            reason = %err,
            "startup init directory"
        );
        return Ok(());
    }

    let Some(init_dir) = init_dir else {
        tracing::info!(
            phase = INIT_APPLY_PHASE,
            result = "skipped",
            reason = "not_configured",
            "startup init directory"
        );
        return Ok(());
    };

    let init_dir = match resolve_init_directory(storage, init_dir) {
        Ok(path) => path,
        Err(err) => {
            tracing::warn!(
                phase = INIT_APPLY_PHASE,
                result = "skipped",
                init_path = %init_dir.display(),
                reason = %err,
                "startup init directory"
            );
            return Ok(());
        }
    };
    let manifest_path = init_dir.join(MANIFEST_FILE);
    let raw = match fs::read(&manifest_path) {
        Ok(raw) => raw,
        Err(err) => {
            tracing::warn!(
                phase = INIT_APPLY_PHASE,
                result = "skipped",
                init_path = %init_dir.display(),
                reason = %format!("read manifest.json: {err}"),
                "startup init directory"
            );
            return Ok(());
        }
    };
    let raw_manifest: RawInitManifest = match serde_json::from_slice(&raw) {
        Ok(manifest) => manifest,
        Err(err) => {
            tracing::warn!(
                phase = INIT_APPLY_PHASE,
                result = "skipped",
                init_path = %init_dir.display(),
                reason = %format!("parse manifest.json: {err}"),
                "startup init directory"
            );
            return Ok(());
        }
    };
    if raw_manifest.format_version != RESOURCE_DIRECTORY_FORMAT_VERSION {
        tracing::warn!(
            phase = INIT_APPLY_PHASE,
            result = "skipped",
            init_path = %init_dir.display(),
            format_version = raw_manifest.format_version,
            reason = "unsupported_format_version",
            "startup init directory"
        );
        return Ok(());
    }
    if let Err(err) = validate_bundle_version(&raw_manifest.bundle_version) {
        tracing::warn!(
            phase = INIT_APPLY_PHASE,
            result = "skipped",
            init_path = %init_dir.display(),
            reason = %err,
            "startup init directory"
        );
        return Ok(());
    }

    let parsed = parse_init_entries(raw_manifest);
    let manifest = parsed.manifest;

    let applied_state = match storage.get_init_apply_meta() {
        Ok(state) => state,
        Err(err) => {
            tracing::warn!(
                phase = INIT_APPLY_PHASE,
                result = "skipped",
                reason = %format!("read init apply state: {err}"),
                "startup init directory"
            );
            return Ok(());
        }
    };
    if applied_state.is_some_and(|state| state.bundle_version == manifest.bundle_version) {
        tracing::info!(
            phase = INIT_APPLY_PHASE,
            result = "skipped_unchanged",
            init_path = %init_dir.display(),
            bundle_version = %manifest.bundle_version,
            "startup init directory"
        );
        return Ok(());
    }

    let phase = StartupPhase::new("manager", DEFAULT_FLOW_INSTANCE_ID, INIT_APPLY_PHASE);
    let result = apply_manifest(
        storage,
        &init_dir,
        manifest,
        parsed.had_entry_errors,
        is_declared_instance,
    );
    match &result {
        Ok(summary) => {
            tracing::info!(
                phase = INIT_APPLY_PHASE,
                result = "applied",
                resources_applied = summary.resources_applied,
                kept_existing = summary.kept_existing,
                schema_files_installed = summary.schema_files,
                wasm_files_installed = summary.wasm_files,
                "startup init directory"
            );
            phase.log_success();
        }
        Err(err) => phase.log_failure(err),
    }
    if let Err(err) = result {
        tracing::warn!(
            phase = INIT_APPLY_PHASE,
            result = "partial_or_skipped",
            reason = %err,
            "startup init directory did not complete"
        );
    }
    Ok(())
}

fn parse_init_entries(mut raw: RawInitManifest) -> ParsedInitManifest {
    let mut cross_errors = false;
    macro_rules! reject_cross_duplicates {
        ($field:ident, $id:literal, $canonical:expr) => {
            let identity = |item: &serde_json::Value| item.get($id).and_then(serde_json::Value::as_str).map(|id| if $canonical { id.to_ascii_lowercase() } else { id.to_string() });
            let upserts = raw.resources.$field.iter().filter_map(identity).collect::<BTreeSet<_>>();
            let deletions = raw.deletions.$field.iter().filter_map(identity).collect::<BTreeSet<_>>();
            let duplicated = upserts.intersection(&deletions).cloned().collect::<BTreeSet<_>>();
            if !duplicated.is_empty() {
                cross_errors = true;
                tracing::warn!(kind = stringify!($field), identities = ?duplicated, "init resource/deletion duplicates rejected");
                raw.resources.$field.retain(|item| identity(item).is_none_or(|id| !duplicated.contains(&id)));
                raw.deletions.$field.retain(|item| identity(item).is_none_or(|id| !duplicated.contains(&id)));
            }
        };
    }
    reject_cross_duplicates!(memory_topics, "topic", false);
    reject_cross_duplicates!(shared_mqtt_clients, "key", false);
    reject_cross_duplicates!(schemas, "name", false);
    reject_cross_duplicates!(streams, "name", false);
    reject_cross_duplicates!(pipelines, "id", false);
    reject_cross_duplicates!(udfs, "name", true);
    reject_cross_duplicates!(tables, "name", false);
    let bundle_version = raw.bundle_version;
    let (memory_topics, memory_errors) = decode_resource_entries(
        &bundle_version,
        "memory_topics",
        "topic",
        raw.resources.memory_topics,
        false,
    );
    let (shared_mqtt_clients, mqtt_errors) = decode_resource_entries(
        &bundle_version,
        "shared_mqtt_clients",
        "key",
        raw.resources.shared_mqtt_clients,
        false,
    );
    let (schemas, schema_errors) = decode_resource_entries(
        &bundle_version,
        "schemas",
        "name",
        raw.resources.schemas,
        false,
    );
    let (streams, stream_errors) = decode_resource_entries(
        &bundle_version,
        "streams",
        "name",
        raw.resources.streams,
        false,
    );
    let (pipelines, pipeline_errors) = decode_resource_entries(
        &bundle_version,
        "pipelines",
        "id",
        raw.resources.pipelines,
        false,
    );
    let (mut udfs, udf_errors): (Vec<crate::export::ExportUdf>, bool) =
        decode_resource_entries(&bundle_version, "udfs", "name", raw.resources.udfs, true);
    for udf in &mut udfs {
        udf.name = udf.name.to_ascii_lowercase();
    }
    let (tables, table_errors) = decode_resource_entries(
        &bundle_version,
        "tables",
        "name",
        raw.resources.tables,
        false,
    );
    let mut deletions = crate::export::ExportDeletions::default();
    let mut deletion_errors = cross_errors;
    macro_rules! decode_deletions {
        ($field:ident, $identity:literal, $canonical:expr) => {
            let (items, errors) = decode_resource_entries(
                &bundle_version,
                stringify!($field),
                $identity,
                raw.deletions.$field,
                $canonical,
            );
            deletions.$field = items;
            deletion_errors |= errors;
        };
    }
    decode_deletions!(memory_topics, "topic", false);
    decode_deletions!(shared_mqtt_clients, "key", false);
    decode_deletions!(schemas, "name", false);
    decode_deletions!(streams, "name", false);
    decode_deletions!(pipelines, "id", false);
    decode_deletions!(udfs, "name", true);
    decode_deletions!(tables, "name", false);
    let valid_markers = deletions.tombstones().into_iter().filter(|marker| {
        let single = crate::export::ExportDeletions::from_tombstones(vec![marker.clone()]);
        match single.validate(&ExportResources::default()) {
            Ok(()) => true,
            Err(error) => {
                deletion_errors = true;
                tracing::warn!(kind = crate::resource_delete::kind_name(marker.kind), identity = %marker.identity, error = %error, "invalid init deletion");
                false
            }
        }
    }).collect();
    deletions = crate::export::ExportDeletions::from_tombstones(valid_markers);
    ParsedInitManifest {
        manifest: ResourceManifestV1 {
            deletions,
            format_version: raw.format_version,
            bundle_version,
            resources: ExportResources {
                memory_topics,
                shared_mqtt_clients,
                schemas,
                streams,
                pipelines,
                udfs,
                tables,
            },
        },
        had_entry_errors: deletion_errors
            || memory_errors
            || mqtt_errors
            || schema_errors
            || stream_errors
            || pipeline_errors
            || udf_errors
            || table_errors,
    }
}

fn decode_resource_entries<T>(
    bundle_version: &str,
    resource_kind: &str,
    identity_field: &str,
    entries: Vec<serde_json::Value>,
    lowercase_identity: bool,
) -> (Vec<T>, bool)
where
    T: DeserializeOwned,
{
    let mut identities = Vec::with_capacity(entries.len());
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for entry in &entries {
        let identity = entry
            .get(identity_field)
            .and_then(serde_json::Value::as_str)
            .map(|value| {
                if lowercase_identity {
                    value.to_ascii_lowercase()
                } else {
                    value.to_string()
                }
            });
        if let Some(identity) = &identity {
            *counts.entry(identity.clone()).or_default() += 1;
        }
        identities.push(identity);
    }

    let mut decoded = Vec::new();
    let mut had_errors = false;
    for (index, (entry, identity)) in entries.into_iter().zip(identities).enumerate() {
        if identity
            .as_ref()
            .and_then(|identity| counts.get(identity))
            .is_some_and(|count| *count > 1)
        {
            had_errors = true;
            log_init_resource_result(
                bundle_version,
                resource_kind,
                identity.as_deref(),
                entry.get("revision").and_then(serde_json::Value::as_u64),
                None,
                "failed_validation",
                "duplicate_identity",
                Some(index),
            );
            continue;
        }
        match serde_json::from_value(entry) {
            Ok(resource) => decoded.push(resource),
            Err(err) => {
                had_errors = true;
                log_init_resource_result(
                    bundle_version,
                    resource_kind,
                    identity.as_deref(),
                    None,
                    None,
                    "failed_validation",
                    &err.to_string(),
                    Some(index),
                );
            }
        }
    }
    (decoded, had_errors)
}

#[allow(clippy::too_many_arguments)]
fn log_init_resource_result(
    bundle_version: &str,
    resource_kind: &str,
    resource_id: Option<&str>,
    incoming_revision: Option<u64>,
    current_revision: Option<u64>,
    result: &str,
    reason: &str,
    resource_index: Option<usize>,
) {
    tracing::info!(
        phase = INIT_APPLY_PHASE,
        bundle_version,
        resource_kind,
        resource_id = resource_id.unwrap_or("<unknown>"),
        incoming_revision,
        current_revision,
        result,
        reason,
        resource_index,
        "startup init resource"
    );
}

fn resolve_init_directory(storage: &StorageManager, path: &Path) -> Result<PathBuf, String> {
    let init_dir = path
        .canonicalize()
        .map_err(|err| format!("resolve init directory: {err}"))?;
    if !fs::symlink_metadata(&init_dir)
        .map_err(|err| format!("inspect init directory: {err}"))?
        .file_type()
        .is_dir()
    {
        return Err("init path is not a directory".to_string());
    }
    let data_dir = storage
        .base_dir()
        .canonicalize()
        .map_err(|err| format!("resolve data directory: {err}"))?;
    if init_dir.starts_with(&data_dir) {
        return Err("init directory must not be inside data directory".to_string());
    }
    Ok(init_dir)
}

fn apply_manifest<F>(
    storage: &StorageManager,
    init_dir: &Path,
    manifest: ResourceManifestV1,
    had_entry_errors: bool,
    is_declared_instance: &F,
) -> Result<ApplySummary, String>
where
    F: Fn(&str) -> bool,
{
    validate_incoming_identities(&manifest.resources)?;
    let live = build_export_resources(storage)?;
    let initial_live = live.clone();
    let current_tombstones =
        relevant_tombstones(storage, &manifest.resources, &manifest.deletions)?;
    let candidate = build_apply_candidate(
        &manifest.bundle_version,
        live,
        manifest.resources,
        CandidateOptions {
            policy: ApplyPolicy::BestEffort,
            tombstones: current_tombstones.clone(),
            deletions: manifest.deletions,
        },
    )?;
    let mut pending_resources = candidate.pending;
    let results = candidate.results;
    let pending_deletions = candidate.deletions;
    let cleanup_deletions = results
        .iter()
        .filter(|result| {
            result.operation == "delete"
                && (result.result == "deleted"
                    || (result.result == "ignored_not_newer"
                        && result.current_revision == Some(result.incoming_revision)))
        })
        .filter_map(|result| {
            current_tombstones
                .iter()
                .chain(&pending_deletions)
                .find(|marker| {
                    crate::resource_delete::kind_name(marker.kind) == result.kind
                        && marker.identity == result.identity
                        && marker.revision == result.incoming_revision
                })
                .cloned()
        })
        .collect::<Vec<_>>();
    let kept_existing = results
        .iter()
        .filter(|result| result.result == "ignored_not_newer")
        .count();
    let staging_root = storage.base_dir().join(".init-staging");
    let work = tempfile::Builder::new()
        .prefix("apply.")
        .tempdir_in(&staging_root)
        .map_err(|err| format!("create init staging directory: {err}"))?;

    let mut limits = SourceLimits::default();
    let mut preparation_errors = false;
    let combined_schemas = if pending_resources.schemas.is_empty() {
        storage.schemas_dir()
    } else {
        let combined = work.path().join("schemas");
        fs::create_dir(&combined).map_err(|err| format!("create schema staging root: {err}"))?;
        copy_tree_limited(&storage.schemas_dir(), &combined, &mut limits)?;
        let mut prepared_schemas = Vec::with_capacity(pending_resources.schemas.len());
        for schema in pending_resources.schemas.drain(..) {
            let source = init_dir
                .join("schemas")
                .join(schema.schema_type.trim().to_ascii_lowercase())
                .join(&schema.name);
            let copy_result =
                if source.exists() {
                    let destination = combined
                        .join(schema.schema_type.trim().to_ascii_lowercase())
                        .join(&schema.name);
                    fs::create_dir_all(destination.parent().ok_or_else(|| {
                        format!("schema {} has no destination parent", schema.name)
                    })?)
                    .map_err(|err| format!("create schema type staging directory: {err}"))
                    .and_then(|()| copy_tree_limited(&source, &destination, &mut limits))
                } else {
                    Ok(())
                };
            match copy_result {
                Ok(()) => prepared_schemas.push(schema),
                Err(err) => {
                    preparation_errors = true;
                    log_init_resource_result(
                        &manifest.bundle_version,
                        "schemas",
                        Some(&schema.name),
                        Some(schema.revision),
                        None,
                        "failed_install",
                        &err,
                        None,
                    );
                }
            }
        }
        pending_resources.schemas = prepared_schemas;
        combined
    };

    let (mut pending_resources, mut pending_snapshot, validation_errors) =
        validate_pending_resources_best_effort(
            &manifest.bundle_version,
            initial_live.clone(),
            pending_resources,
            Some(&combined_schemas),
            is_declared_instance,
        );

    let wasm_source = work.path().join("wasm-source");
    fs::create_dir(&wasm_source).map_err(|err| format!("create WASM source staging: {err}"))?;
    let mut prepared_udfs = Vec::with_capacity(pending_resources.udfs.len());
    for udf in pending_resources.udfs.drain(..) {
        let file_name = format!("{}.wasm", udf.wasm_sha256);
        let udf_source = work.path().join("one-udf-source");
        let udf_staged = work.path().join("one-udf-staged");
        if let Err(err) = fs::create_dir_all(&udf_source)
            .map_err(|err| format!("create per-UDF source staging: {err}"))
            .and_then(|()| {
                fs::create_dir_all(&udf_staged)
                    .map_err(|err| format!("create per-UDF validation staging: {err}"))
            })
            .and_then(|()| {
                copy_regular_file_limited(
                    &init_dir.join("wasm_files").join(&file_name),
                    &udf_source.join(&file_name),
                    &mut limits,
                )
            })
            .and_then(|()| {
                crate::import::validate_and_copy_udfs_for_import(
                    std::slice::from_ref(
                        pending_snapshot
                            .udfs
                            .iter()
                            .find(|stored| stored.name == udf.name)
                            .ok_or_else(|| {
                                format!("validated UDF {} is missing from snapshot", udf.name)
                            })?,
                    ),
                    &udf_source,
                    &udf_staged,
                )
            })
            .and_then(|()| {
                copy_regular_file_limited(
                    &udf_staged.join(&file_name),
                    &wasm_source.join(&file_name),
                    &mut limits,
                )
            })
        {
            preparation_errors = true;
            pending_snapshot
                .udfs
                .retain(|stored| stored.name != udf.name);
            log_init_resource_result(
                &manifest.bundle_version,
                "udfs",
                Some(&udf.name),
                Some(udf.revision),
                None,
                "failed_install",
                &err,
                None,
            );
        } else {
            prepared_udfs.push(udf);
        }
        let _ = fs::remove_dir_all(&udf_source);
        let _ = fs::remove_dir_all(&udf_staged);
    }
    pending_resources.udfs = prepared_udfs;
    let staged_wasm = work.path().join("wasm_files");
    fs::create_dir(&staged_wasm).map_err(|err| format!("create WASM staging directory: {err}"))?;
    crate::import::validate_and_copy_udfs_for_import(
        &pending_snapshot.udfs,
        &wasm_source,
        &staged_wasm,
    )?;

    let final_schemas = if pending_resources.schemas.is_empty() {
        storage.schemas_dir()
    } else {
        let final_schemas = work.path().join("final-schemas");
        fs::create_dir(&final_schemas)
            .map_err(|err| format!("create final schema staging root: {err}"))?;
        copy_tree_limited(&storage.schemas_dir(), &final_schemas, &mut limits)?;
        for schema in &pending_resources.schemas {
            let source = init_dir
                .join("schemas")
                .join(schema.schema_type.trim().to_ascii_lowercase())
                .join(&schema.name);
            if source.exists() {
                let destination = final_schemas
                    .join(schema.schema_type.trim().to_ascii_lowercase())
                    .join(&schema.name);
                fs::create_dir_all(destination.parent().ok_or_else(|| {
                    format!("schema {} has no final destination parent", schema.name)
                })?)
                .map_err(|err| format!("create final schema type directory: {err}"))?;
                copy_tree_limited(&source, &destination, &mut limits)?;
            }
        }
        final_schemas
    };
    let mut schema_tree = if pending_resources.schemas.is_empty() {
        None
    } else {
        Some(PreparedSchemaTree::prepare(storage, &final_schemas)?)
    };
    let accepted = build_apply_candidate(
        &manifest.bundle_version,
        initial_live,
        pending_resources.clone(),
        CandidateOptions {
            policy: ApplyPolicy::BestEffort,
            tombstones: Vec::new(),
            deletions: Default::default(),
        },
    )?;
    let (deletions, deletion_errors) =
        crate::resource_delete::select_init_deletions(accepted.resources, pending_deletions)?;
    let deletion_count = deletions.len();
    pending_snapshot.tombstones = deletions;
    let meta = if had_entry_errors || validation_errors || preparation_errors || deletion_errors {
        None
    } else {
        Some(StoredInitApplyMeta {
            bundle_version: manifest.bundle_version,
            applied_at_ms: unix_time_ms(SystemTime::now())?,
        })
    };
    let completion_meta = if cleanup_deletions.is_empty() {
        None
    } else {
        meta.clone()
    };
    let commit_meta = if cleanup_deletions.is_empty() {
        meta
    } else {
        None
    };
    let installed_wasm = crate::attachments::publish_and_commit(
        storage,
        pending_snapshot,
        storage::MetadataCommitOptions {
            init_meta: commit_meta,
            ..Default::default()
        },
        schema_tree.take(),
        &staged_wasm,
    )?;

    let mut cleanup_failed = false;
    let mut wasm_reclaimed = false;
    for marker in cleanup_deletions {
        // A rejected init deletion has no committed marker and must not clean
        // up the dependency that its retained owner still uses.
        if storage
            .get_tombstone(marker.kind, &marker.identity)
            .map_err(|error| error.to_string())?
            .is_none_or(|current| current.revision != marker.revision)
        {
            continue;
        }
        if marker.kind == storage::ResourceKind::Udfs {
            if wasm_reclaimed {
                continue;
            }
            wasm_reclaimed = true;
        }
        if let Err(error) =
            crate::resource_delete::cleanup_persisted(storage, marker.kind, &marker.identity)
        {
            cleanup_failed = true;
            tracing::warn!(identity = %marker.identity, error = %error, "init deletion cleanup failed");
        }
    }
    if !cleanup_failed && let Some(meta) = completion_meta {
        storage
            .put_init_apply_meta(meta)
            .map_err(|error| error.to_string())?;
    }
    Ok(ApplySummary {
        resources_applied: count_resources(&pending_resources) + deletion_count,
        kept_existing,
        schema_files: limits.entries,
        wasm_files: installed_wasm.len(),
    })
}

pub(crate) fn empty_resources() -> ExportResources {
    ExportResources {
        memory_topics: Vec::new(),
        shared_mqtt_clients: Vec::new(),
        schemas: Vec::new(),
        streams: Vec::new(),
        pipelines: Vec::new(),
        udfs: Vec::new(),
        tables: Vec::new(),
    }
}

fn append_snapshot(target: &mut MetadataExportSnapshot, mut source: MetadataExportSnapshot) {
    target.streams.append(&mut source.streams);
    target.schemas.append(&mut source.schemas);
    target.pipelines.append(&mut source.pipelines);
    target
        .pipeline_run_states
        .append(&mut source.pipeline_run_states);
    target.mqtt_configs.append(&mut source.mqtt_configs);
    target.memory_topics.append(&mut source.memory_topics);
    target.udfs.append(&mut source.udfs);
    target.tables.append(&mut source.tables);
}

pub(crate) fn empty_snapshot() -> MetadataExportSnapshot {
    MetadataExportSnapshot {
        tombstones: Vec::new(),
        streams: Vec::new(),
        schemas: Vec::new(),
        pipelines: Vec::new(),
        pipeline_run_states: Vec::new(),
        mqtt_configs: Vec::new(),
        memory_topics: Vec::new(),
        udfs: Vec::new(),
        tables: Vec::new(),
    }
}

fn validation_result_label(error: &str) -> &'static str {
    if error.contains("missing")
        || error.contains("not found")
        || error.contains("unknown stream")
        || error.contains("undeclared flow instance")
    {
        "failed_dependency"
    } else {
        "failed_validation"
    }
}

fn validate_pending_resources_best_effort<F>(
    bundle_version: &str,
    mut effective: ExportResources,
    pending: ExportResources,
    schema_source_root: Option<&Path>,
    is_declared_instance: &F,
) -> (ExportResources, MetadataExportSnapshot, bool)
where
    F: Fn(&str) -> bool,
{
    let mut accepted = empty_resources();
    let mut collected = empty_snapshot();
    let mut had_errors = false;

    macro_rules! process_foundation {
        ($field:ident, $kind:literal, $id:expr, $revision:expr) => {
            for item in pending.$field {
                let id = $id(&item);
                let incoming_revision = $revision(&item);
                let current_revision = effective
                    .$field
                    .iter()
                    .find(|current| $id(current) == id)
                    .map($revision);
                let mut proposed = effective.clone();
                upsert_resource(&mut proposed.$field, item.clone(), $id);
                let mut validation_view = proposed.clone();
                validation_view.streams.clear();
                validation_view.pipelines.clear();
                validation_view.tables.clear();
                match validate_resources(
                    bundle_version,
                    validation_view,
                    schema_source_root,
                    is_declared_instance,
                ) {
                    Ok(snapshot) => {
                        let mut selected = empty_resources();
                        selected.$field.push(item.clone());
                        append_snapshot(
                            &mut collected,
                            filter_pending_snapshot(snapshot, &selected),
                        );
                        accepted.$field.push(item);
                        effective = proposed;
                        log_init_resource_result(
                            bundle_version,
                            $kind,
                            Some(&id),
                            Some(incoming_revision),
                            current_revision,
                            if current_revision.is_some() {
                                "updated"
                            } else {
                                "created"
                            },
                            "revision_won",
                            None,
                        );
                    }
                    Err(err) => {
                        had_errors = true;
                        log_init_resource_result(
                            bundle_version,
                            $kind,
                            Some(&id),
                            Some(incoming_revision),
                            current_revision,
                            validation_result_label(&err),
                            &err,
                            None,
                        );
                    }
                }
            }
        };
    }

    process_foundation!(
        schemas,
        "schemas",
        |item: &crate::export::ExportSchema| item.name.clone(),
        |item: &crate::export::ExportSchema| item.revision
    );
    process_foundation!(
        udfs,
        "udfs",
        |item: &crate::export::ExportUdf| item.name.to_ascii_lowercase(),
        |item: &crate::export::ExportUdf| item.revision
    );
    process_foundation!(
        memory_topics,
        "memory_topics",
        |item: &crate::export::ExportMemoryTopic| item.topic.clone(),
        |item: &crate::export::ExportMemoryTopic| item.revision
    );
    process_foundation!(
        shared_mqtt_clients,
        "shared_mqtt_clients",
        |item: &crate::export::ExportSharedMqttClient| item.definition.key.clone(),
        |item: &crate::export::ExportSharedMqttClient| item.revision
    );

    for item in pending.tables {
        let id = item.definition.name.clone();
        let incoming_revision = item.definition.revision;
        let current_revision = effective
            .tables
            .iter()
            .find(|current| current.definition.name == id)
            .map(|current| current.definition.revision);
        let mut proposed = effective.clone();
        upsert_resource(&mut proposed.tables, item.clone(), |table| {
            table.definition.name.clone()
        });
        let mut validation_view = proposed.clone();
        validation_view.tables = vec![item.clone()];
        validation_view.pipelines.clear();
        match validate_resources(
            bundle_version,
            validation_view,
            schema_source_root,
            is_declared_instance,
        ) {
            Ok(snapshot) => {
                let mut selected = empty_resources();
                selected.tables.push(item.clone());
                append_snapshot(&mut collected, filter_pending_snapshot(snapshot, &selected));
                accepted.tables.push(item);
                effective = proposed;
                log_init_resource_result(
                    bundle_version,
                    "tables",
                    Some(&id),
                    Some(incoming_revision),
                    current_revision,
                    if current_revision.is_some() {
                        "updated"
                    } else {
                        "created"
                    },
                    "revision_won",
                    None,
                );
            }
            Err(err) => {
                had_errors = true;
                log_init_resource_result(
                    bundle_version,
                    "tables",
                    Some(&id),
                    Some(incoming_revision),
                    current_revision,
                    validation_result_label(&err),
                    &err,
                    None,
                );
            }
        }
    }

    for item in pending.streams {
        let id = item.name.clone();
        let incoming_revision = item.revision;
        let current_revision = effective
            .streams
            .iter()
            .find(|current| current.name == id)
            .map(|current| current.revision);
        let mut proposed = effective.clone();
        upsert_resource(&mut proposed.streams, item.clone(), |stream| {
            stream.name.clone()
        });
        let mut validation_view = proposed.clone();
        validation_view.streams = vec![item.clone()];
        validation_view.pipelines.clear();
        match validate_resources(
            bundle_version,
            validation_view,
            schema_source_root,
            is_declared_instance,
        ) {
            Ok(snapshot) => {
                let mut selected = empty_resources();
                selected.streams.push(item.clone());
                append_snapshot(&mut collected, filter_pending_snapshot(snapshot, &selected));
                accepted.streams.push(item);
                effective = proposed;
                log_init_resource_result(
                    bundle_version,
                    "streams",
                    Some(&id),
                    Some(incoming_revision),
                    current_revision,
                    if current_revision.is_some() {
                        "updated"
                    } else {
                        "created"
                    },
                    "revision_won",
                    None,
                );
            }
            Err(err) => {
                had_errors = true;
                log_init_resource_result(
                    bundle_version,
                    "streams",
                    Some(&id),
                    Some(incoming_revision),
                    current_revision,
                    validation_result_label(&err),
                    &err,
                    None,
                );
            }
        }
    }

    for item in pending.pipelines {
        let id = item.definition.id.clone();
        let incoming_revision = item.definition.revision;
        let current_revision = effective
            .pipelines
            .iter()
            .find(|current| current.definition.id == id)
            .map(|current| current.definition.revision);
        let mut proposed = effective.clone();
        upsert_resource(&mut proposed.pipelines, item.clone(), |pipeline| {
            pipeline.definition.id.clone()
        });
        let mut validation_view = proposed.clone();
        validation_view.pipelines = vec![item.clone()];
        match validate_resources(
            bundle_version,
            validation_view,
            schema_source_root,
            is_declared_instance,
        ) {
            Ok(snapshot) => {
                let mut selected = empty_resources();
                selected.pipelines.push(item.clone());
                append_snapshot(&mut collected, filter_pending_snapshot(snapshot, &selected));
                accepted.pipelines.push(item);
                effective = proposed;
                log_init_resource_result(
                    bundle_version,
                    "pipelines",
                    Some(&id),
                    Some(incoming_revision),
                    current_revision,
                    if current_revision.is_some() {
                        "updated"
                    } else {
                        "created"
                    },
                    "revision_won",
                    None,
                );
            }
            Err(err) => {
                had_errors = true;
                log_init_resource_result(
                    bundle_version,
                    "pipelines",
                    Some(&id),
                    Some(incoming_revision),
                    current_revision,
                    validation_result_label(&err),
                    &err,
                    None,
                );
            }
        }
    }

    (accepted, collected, had_errors)
}

fn validate_resources<F>(
    bundle_version: &str,
    resources: ExportResources,
    schema_source_root: Option<&Path>,
    is_declared_instance: &F,
) -> Result<MetadataExportSnapshot, String>
where
    F: Fn(&str) -> bool,
{
    validate_and_build_snapshot(
        &ResourceManifestV1 {
            deletions: Default::default(),
            format_version: RESOURCE_DIRECTORY_FORMAT_VERSION,
            bundle_version: bundle_version.to_string(),
            resources,
        },
        schema_source_root,
        is_declared_instance,
    )
}

fn upsert_resource<T, F>(resources: &mut Vec<T>, item: T, identity: F)
where
    F: Fn(&T) -> String,
{
    let id = identity(&item);
    if let Some(position) = resources
        .iter()
        .position(|resource| identity(resource) == id)
    {
        resources[position] = item;
    } else {
        resources.push(item);
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ApplyPolicy {
    BestEffort,
    Atomic,
}

#[derive(Clone, serde::Serialize)]
pub(crate) struct ResourceApplyResult {
    pub kind: String,
    pub identity: String,
    pub incoming_revision: u64,
    pub current_revision: Option<u64>,
    pub operation: &'static str,
    pub result: &'static str,
    pub runtime_applied: Option<bool>,
    pub error: Option<String>,
}

pub(crate) fn relevant_tombstones(
    storage: &StorageManager,
    resources: &ExportResources,
    deletions: &crate::export::ExportDeletions,
) -> Result<Vec<storage::StoredTombstone>, String> {
    let mut identities = resources.identities();
    identities.extend(
        deletions
            .tombstones()
            .into_iter()
            .map(|marker| (marker.kind, marker.identity)),
    );
    storage
        .get_tombstones(&identities)
        .map_err(|error| error.to_string())
}

pub(crate) struct CandidateOptions {
    pub policy: ApplyPolicy,
    pub tombstones: Vec<storage::StoredTombstone>,
    pub deletions: crate::export::ExportDeletions,
}

pub(crate) struct ApplyCandidate {
    pub resources: ExportResources,
    pub pending: ExportResources,
    pub deletions: Vec<storage::StoredTombstone>,
    pub results: Vec<ResourceApplyResult>,
}

pub(crate) fn build_apply_candidate(
    bundle_version: &str,
    mut live: ExportResources,
    incoming: ExportResources,
    options: CandidateOptions,
) -> Result<ApplyCandidate, String> {
    let policy = options.policy;
    let tombstones = options
        .tombstones
        .into_iter()
        .map(|marker| ((marker.kind, marker.identity.clone()), marker))
        .collect::<BTreeMap<_, _>>();
    let incoming_deletions = options.deletions.tombstones();
    let mut pending = ExportResources {
        memory_topics: Vec::new(),
        shared_mqtt_clients: Vec::new(),
        schemas: Vec::new(),
        streams: Vec::new(),
        pipelines: Vec::new(),
        udfs: Vec::new(),
        tables: Vec::new(),
    };
    let mut results = Vec::new();
    macro_rules! merge {
        ($field:ident, $kind:ident, $id:expr, $revision:expr) => {{
            for item in incoming.$field {
                let id = $id(&item);
                let incoming_revision = $revision(&item);
                let position = live.$field.iter().position(|live| $id(live) == id);
                let marker = tombstones.get(&(storage::ResourceKind::$kind, id.clone()));
                let current_revision = position.map(|position| $revision(&live.$field[position])).or_else(|| marker.map(|marker| marker.revision));
                if let Some(marker) = marker && incoming_revision <= marker.revision {
                    if matches!(policy, ApplyPolicy::Atomic) {
                        return Err(format!("{}: {} {id}: incoming {incoming_revision}, deleted {}", if incoming_revision == marker.revision { "same_revision_different_operation" } else { "older_revision" }, stringify!($field), marker.revision));
                    }
                    results.push(ResourceApplyResult { kind: stringify!($field).to_string(), identity: id, incoming_revision, current_revision, operation: "upsert", result: "ignored_not_newer", runtime_applied: None, error: None });
                    continue;
                }
                let mut outcome = "created";
                if let Some(position) = position {
                    let current_revision = $revision(&live.$field[position]);
                    if incoming_revision <= current_revision {
                        if matches!(policy, ApplyPolicy::Atomic) {
                            if incoming_revision < current_revision {
                                return Err(format!("older_revision: {} {id}: incoming {incoming_revision}, current {current_revision}", stringify!($field)));
                            }
                            if crate::revision::normalized_spec_without_revision(&item)?
                                != crate::revision::normalized_spec_without_revision(&live.$field[position])? {
                                return Err(format!("same_revision_different_spec: {} {id}: revision {incoming_revision}", stringify!($field)));
                            }
                            outcome = "idempotent";
                        } else {
                            outcome = "ignored_not_newer";
                            log_init_resource_result(bundle_version, stringify!($field), Some(&id), Some(incoming_revision), Some(current_revision), outcome, "incoming_revision_not_greater", None);
                        }
                    } else {
                        outcome = "updated";
                        pending.$field.push(item.clone());
                        live.$field[position] = item;
                    }
                } else {
                    pending.$field.push(item.clone());
                    live.$field.push(item);
                }
                results.push(ResourceApplyResult {
                    kind: stringify!($field).to_string(), identity: id,
                    incoming_revision, current_revision, operation: "upsert", result: outcome,
                    runtime_applied: None, error: None,
                });
            }
        }};
    }
    merge!(
        memory_topics,
        MemoryTopics,
        |item: &crate::export::ExportMemoryTopic| { item.topic.clone() },
        |item: &crate::export::ExportMemoryTopic| item.revision
    );
    merge!(
        shared_mqtt_clients,
        SharedMqttClients,
        |item: &crate::export::ExportSharedMqttClient| item.definition.key.clone(),
        |item: &crate::export::ExportSharedMqttClient| item.revision
    );
    merge!(
        schemas,
        Schemas,
        |item: &crate::export::ExportSchema| item.name.clone(),
        |item: &crate::export::ExportSchema| item.revision
    );
    merge!(
        streams,
        Streams,
        |item: &crate::stream::CreateStreamRequest| item.name.clone(),
        |item: &crate::stream::CreateStreamRequest| item.revision
    );
    merge!(
        pipelines,
        Pipelines,
        |item: &crate::export::ExportPipeline| item.definition.id.clone(),
        |item: &crate::export::ExportPipeline| item.definition.revision
    );
    merge!(
        udfs,
        Udfs,
        |item: &crate::export::ExportUdf| item.name.to_ascii_lowercase(),
        |item: &crate::export::ExportUdf| item.revision
    );
    merge!(
        tables,
        Tables,
        |item: &crate::export::ExportTable| item.definition.name.clone(),
        |item: &crate::export::ExportTable| item.definition.revision
    );
    let mut deletions = Vec::new();
    macro_rules! delete {
        ($field:ident, $kind:ident, $id:expr, $revision:expr) => {
            for marker in incoming_deletions
                .iter()
                .filter(|marker| marker.kind == storage::ResourceKind::$kind)
                .cloned()
            {
                let position = live
                    .$field
                    .iter()
                    .position(|item| $id(item) == marker.identity);
                let live_revision = position.map(|position| $revision(&live.$field[position]));
                let deleted_revision = tombstones
                    .get(&(marker.kind, marker.identity.clone()))
                    .map(|current| current.revision);
                let current_revision = live_revision.or(deleted_revision);
                let outcome =
                    if current_revision.is_some_and(|revision| marker.revision <= revision) {
                        if matches!(policy, ApplyPolicy::BestEffort) {
                            "ignored_not_newer"
                        } else if deleted_revision == Some(marker.revision) {
                            "idempotent"
                        } else {
                            return Err(format!(
                                "{}: {} {}: incoming {}, current {}",
                                if current_revision == Some(marker.revision) {
                                    "same_revision_different_operation"
                                } else {
                                    "older_revision"
                                },
                                stringify!($field),
                                marker.identity,
                                marker.revision,
                                current_revision.unwrap_or(0)
                            ));
                        }
                    } else {
                        if let Some(position) = position {
                            live.$field.remove(position);
                        }
                        deletions.push(marker.clone());
                        "deleted"
                    };
                results.push(ResourceApplyResult {
                    kind: stringify!($field).to_string(),
                    identity: marker.identity.clone(),
                    incoming_revision: marker.revision,
                    current_revision,
                    operation: "delete",
                    result: outcome,
                    runtime_applied: None,
                    error: None,
                });
            }
        };
    }
    delete!(
        memory_topics,
        MemoryTopics,
        |item: &crate::export::ExportMemoryTopic| item.topic.clone(),
        |item: &crate::export::ExportMemoryTopic| item.revision
    );
    delete!(
        shared_mqtt_clients,
        SharedMqttClients,
        |item: &crate::export::ExportSharedMqttClient| item.definition.key.clone(),
        |item: &crate::export::ExportSharedMqttClient| item.revision
    );
    delete!(
        schemas,
        Schemas,
        |item: &crate::export::ExportSchema| item.name.clone(),
        |item: &crate::export::ExportSchema| item.revision
    );
    delete!(
        streams,
        Streams,
        |item: &crate::stream::CreateStreamRequest| item.name.clone(),
        |item: &crate::stream::CreateStreamRequest| item.revision
    );
    delete!(
        pipelines,
        Pipelines,
        |item: &crate::export::ExportPipeline| item.definition.id.clone(),
        |item: &crate::export::ExportPipeline| item.definition.revision
    );
    delete!(
        udfs,
        Udfs,
        |item: &crate::export::ExportUdf| item.name.to_ascii_lowercase(),
        |item: &crate::export::ExportUdf| item.revision
    );
    delete!(
        tables,
        Tables,
        |item: &crate::export::ExportTable| item.definition.name.clone(),
        |item: &crate::export::ExportTable| item.definition.revision
    );
    Ok(ApplyCandidate {
        resources: live,
        pending,
        deletions,
        results,
    })
}

pub(crate) fn validate_incoming_identities(resources: &ExportResources) -> Result<(), String> {
    fn unique<'a>(kind: &str, identities: impl IntoIterator<Item = &'a str>) -> Result<(), String> {
        let mut seen = BTreeSet::new();
        for identity in identities {
            if !seen.insert(identity.to_string()) {
                return Err(format!(
                    "duplicate {kind} identity in resource manifest: {identity}"
                ));
            }
        }
        Ok(())
    }
    unique(
        "memory topic",
        resources
            .memory_topics
            .iter()
            .map(|item| item.topic.as_str()),
    )?;
    unique(
        "shared MQTT client",
        resources
            .shared_mqtt_clients
            .iter()
            .map(|item| item.definition.key.as_str()),
    )?;
    unique(
        "schema",
        resources.schemas.iter().map(|item| item.name.as_str()),
    )?;
    unique(
        "stream",
        resources.streams.iter().map(|item| item.name.as_str()),
    )?;
    unique(
        "pipeline",
        resources
            .pipelines
            .iter()
            .map(|item| item.definition.id.as_str()),
    )?;
    unique("UDF", resources.udfs.iter().map(|item| item.name.as_str()))?;
    unique(
        "table",
        resources
            .tables
            .iter()
            .map(|item| item.definition.name.as_str()),
    )
}

pub(crate) fn filter_pending_snapshot(
    candidate: MetadataExportSnapshot,
    pending: &ExportResources,
) -> MetadataExportSnapshot {
    let stream_ids = pending
        .streams
        .iter()
        .map(|v| v.name.as_str())
        .collect::<BTreeSet<_>>();
    let schema_ids = pending
        .schemas
        .iter()
        .map(|v| v.name.as_str())
        .collect::<BTreeSet<_>>();
    let pipeline_ids = pending
        .pipelines
        .iter()
        .map(|v| v.definition.id.as_str())
        .collect::<BTreeSet<_>>();
    let mqtt_ids = pending
        .shared_mqtt_clients
        .iter()
        .map(|v| v.definition.key.as_str())
        .collect::<BTreeSet<_>>();
    let topic_ids = pending
        .memory_topics
        .iter()
        .map(|v| v.topic.as_str())
        .collect::<BTreeSet<_>>();
    let udf_ids = pending
        .udfs
        .iter()
        .map(|v| v.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let table_ids = pending
        .tables
        .iter()
        .map(|v| v.definition.name.as_str())
        .collect::<BTreeSet<_>>();
    MetadataExportSnapshot {
        tombstones: Vec::new(),
        streams: candidate
            .streams
            .into_iter()
            .filter(|v| stream_ids.contains(v.id.as_str()))
            .collect(),
        schemas: candidate
            .schemas
            .into_iter()
            .filter(|v| schema_ids.contains(v.name.as_str()))
            .collect(),
        pipelines: candidate
            .pipelines
            .into_iter()
            .filter(|v| pipeline_ids.contains(v.id.as_str()))
            .collect(),
        pipeline_run_states: candidate
            .pipeline_run_states
            .into_iter()
            .filter(|v| pipeline_ids.contains(v.pipeline_id.as_str()))
            .collect(),
        mqtt_configs: candidate
            .mqtt_configs
            .into_iter()
            .filter(|v| mqtt_ids.contains(v.key.as_str()))
            .collect(),
        memory_topics: candidate
            .memory_topics
            .into_iter()
            .filter(|v| topic_ids.contains(v.topic.as_str()))
            .collect(),
        udfs: candidate
            .udfs
            .into_iter()
            .filter(|v| udf_ids.contains(v.name.as_str()))
            .collect(),
        tables: candidate
            .tables
            .into_iter()
            .filter(|v| table_ids.contains(v.id.as_str()))
            .collect(),
    }
}

#[derive(Default)]
pub(crate) struct SourceLimits {
    entries: usize,
    bytes: u64,
}

pub(crate) fn copy_tree_limited(
    source: &Path,
    destination: &Path,
    limits: &mut SourceLimits,
) -> Result<(), String> {
    let metadata = match fs::symlink_metadata(source) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(format!("inspect source `{}`: {err}", source.display())),
    };
    if !metadata.file_type().is_dir() {
        return Err(format!("source `{}` is not a directory", source.display()));
    }
    fs::create_dir_all(destination)
        .map_err(|err| format!("create `{}`: {err}", destination.display()))?;
    for entry in
        fs::read_dir(source).map_err(|err| format!("read `{}`: {err}", source.display()))?
    {
        let entry = entry.map_err(|err| format!("read source entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("inspect source entry: {err}"))?;
        limits.entries += 1;
        if limits.entries > MAX_SOURCE_ENTRIES {
            return Err(format!(
                "init source has too many entries: {}",
                limits.entries
            ));
        }
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree_limited(&entry.path(), &target, limits)?;
        } else if file_type.is_file() {
            let size = entry
                .metadata()
                .map_err(|err| format!("inspect source file: {err}"))?
                .len();
            if size > MAX_SOURCE_FILE_SIZE {
                return Err(format!(
                    "init source file `{}` is too large",
                    entry.path().display()
                ));
            }
            limits.bytes = limits
                .bytes
                .checked_add(size)
                .ok_or_else(|| "init source size overflow".to_string())?;
            if limits.bytes > MAX_SOURCE_TOTAL_SIZE {
                return Err("init source total size exceeds limit".to_string());
            }
            fs::copy(entry.path(), &target)
                .map_err(|err| format!("copy source file `{}`: {err}", entry.path().display()))?;
        } else {
            return Err(format!(
                "init source entry `{}` is not a regular file or directory",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

pub(crate) fn copy_regular_file_limited(
    source: &Path,
    destination: &Path,
    limits: &mut SourceLimits,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(source)
        .map_err(|err| format!("inspect source file `{}`: {err}", source.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "source `{}` is not a regular file",
            source.display()
        ));
    }
    let size = metadata.len();
    if size > MAX_SOURCE_FILE_SIZE {
        return Err(format!(
            "init source file `{}` is too large",
            source.display()
        ));
    }
    limits.entries += 1;
    limits.bytes = limits
        .bytes
        .checked_add(size)
        .ok_or_else(|| "init source size overflow".to_string())?;
    if limits.entries > MAX_SOURCE_ENTRIES || limits.bytes > MAX_SOURCE_TOTAL_SIZE {
        return Err("init source limits exceeded".to_string());
    }
    fs::copy(source, destination)
        .map_err(|err| format!("copy source file `{}`: {err}", source.display()))?;
    Ok(())
}

pub(crate) fn install_staged_wasm(
    source: &Path,
    destination: &Path,
) -> Result<Vec<PathBuf>, String> {
    fs::create_dir_all(destination).map_err(|err| format!("create WASM directory: {err}"))?;
    let mut entries = fs::read_dir(source)
        .map_err(|err| format!("read staged WASM directory: {err}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("read staged WASM entry: {err}"))?;
    entries.sort_by_key(fs::DirEntry::file_name);

    let mut installed = Vec::new();
    let install_result = (|| {
        for entry in entries {
            let source_path = entry.path();
            let target = destination.join(entry.file_name());
            if target.exists() {
                if !files_equal(&source_path, &target)? {
                    return Err(format!(
                        "existing managed WASM `{}` does not match staged content",
                        target.display()
                    ));
                }
                continue;
            }
            fs::rename(&source_path, &target)
                .map_err(|err| format!("install WASM `{}`: {err}", target.display()))?;
            installed.push(target);
        }
        Ok(())
    })();

    if let Err(err) = install_result {
        return Err(rollback_wasm(&installed, err));
    }

    Ok(installed)
}

pub(crate) fn rollback_wasm(paths: &[PathBuf], mut error: String) -> String {
    for path in paths {
        if let Err(err) = fs::remove_file(path) {
            error.push_str(&format!("; rollback WASM `{}`: {err}", path.display()));
        }
    }
    error
}

pub(crate) fn files_equal(left: &Path, right: &Path) -> Result<bool, String> {
    let left_len = fs::metadata(left)
        .map_err(|err| format!("inspect staged WASM `{}`: {err}", left.display()))?
        .len();
    let right_len = fs::metadata(right)
        .map_err(|err| format!("inspect managed WASM `{}`: {err}", right.display()))?
        .len();
    if left_len != right_len {
        return Ok(false);
    }

    let mut left = fs::File::open(left)
        .map_err(|err| format!("open staged WASM `{}`: {err}", left.display()))?;
    let mut right = fs::File::open(right)
        .map_err(|err| format!("open managed WASM `{}`: {err}", right.display()))?;
    let mut left_buffer = [0_u8; 64 * 1024];
    let mut right_buffer = [0_u8; 64 * 1024];
    loop {
        let left_read = left
            .read(&mut left_buffer)
            .map_err(|err| format!("read staged WASM: {err}"))?;
        let right_read = right
            .read(&mut right_buffer)
            .map_err(|err| format!("read managed WASM: {err}"))?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

fn clean_staging_root(path: &Path) -> Result<(), String> {
    for entry in fs::read_dir(path).map_err(|err| format!("read init staging root: {err}"))? {
        let entry = entry.map_err(|err| format!("read init staging entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("inspect init staging entry: {err}"))?;
        if file_type.is_dir() {
            fs::remove_dir_all(entry.path())
                .map_err(|err| format!("remove stale init staging: {err}"))?;
        } else {
            fs::remove_file(entry.path())
                .map_err(|err| format!("remove stale init staging file: {err}"))?;
        }
    }
    Ok(())
}

fn count_resources(resources: &ExportResources) -> usize {
    resources.memory_topics.len()
        + resources.shared_mqtt_clients.len()
        + resources.schemas.len()
        + resources.streams.len()
        + resources.pipelines.len()
        + resources.udfs.len()
        + resources.tables.len()
}

fn unix_time_ms(value: SystemTime) -> Result<u64, String> {
    let millis = value
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system time before unix epoch: {err}"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| "unix time overflow".to_string())
}

struct ApplySummary {
    resources_applied: usize,
    kept_existing: usize,
    schema_files: usize,
    wasm_files: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::{StorageManager, StoredMemoryTopicKind};
    use tempfile::tempdir;

    fn manifest(version: &str, topics: &[(&str, usize, u64)]) -> ResourceManifestV1 {
        ResourceManifestV1 {
            deletions: Default::default(),
            format_version: RESOURCE_DIRECTORY_FORMAT_VERSION,
            bundle_version: version.to_string(),
            resources: ExportResources {
                memory_topics: topics
                    .iter()
                    .map(
                        |(topic, capacity, revision)| crate::export::ExportMemoryTopic {
                            topic: (*topic).to_string(),
                            revision: *revision,
                            kind: StoredMemoryTopicKind::Bytes,
                            capacity: *capacity,
                        },
                    )
                    .collect(),
                shared_mqtt_clients: Vec::new(),
                schemas: Vec::new(),
                streams: Vec::new(),
                pipelines: Vec::new(),
                udfs: Vec::new(),
                tables: Vec::new(),
            },
        }
    }

    #[test]
    fn init_delete_preserves_dependencies_when_dependent_deletion_is_invalid() {
        let root = tempdir().unwrap();
        let storage = StorageManager::new(root.path().join("data")).unwrap();
        let init = root.path().join("init");
        fs::create_dir_all(&init).unwrap();
        storage
            .create_memory_topic(storage::StoredMemoryTopic {
                topic: "retained_topic".into(),
                revision: 1,
                kind: storage::StoredMemoryTopicKind::Bytes,
                capacity: 8,
            })
            .unwrap();
        storage
            .create_memory_topic(storage::StoredMemoryTopic {
                topic: "independent_topic".into(),
                revision: 1,
                kind: storage::StoredMemoryTopicKind::Bytes,
                capacity: 8,
            })
            .unwrap();
        let stream: crate::stream::CreateStreamRequest = serde_json::from_value(serde_json::json!({"name":"retained_stream","revision":1,"type":"memory","props":{"topic":"retained_topic"}})).unwrap();
        storage
            .create_stream(crate::storage_bridge::stored_stream_from_request(&stream).unwrap())
            .unwrap();
        let raw = serde_json::json!({"format_version":1,"bundle_version":"failed_dependent_delete","deletions":{
            "streams":[{"name":"retained_stream","revision":0}],
            "memory_topics":[{"topic":"retained_topic","revision":2},{"topic":"independent_topic","revision":2}]
        }});
        fs::write(init.join(MANIFEST_FILE), serde_json::to_vec(&raw).unwrap()).unwrap();
        apply_init_directory_if_needed(&storage, Some(&init), &|_| true).unwrap();
        assert!(storage.get_stream("retained_stream").unwrap().is_some());
        assert!(
            storage
                .get_memory_topic("retained_topic")
                .unwrap()
                .is_some()
        );
        assert!(
            storage
                .get_tombstone(storage::ResourceKind::MemoryTopics, "retained_topic")
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .get_memory_topic("independent_topic")
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .get_tombstone(storage::ResourceKind::MemoryTopics, "independent_topic")
                .unwrap()
                .is_some()
        );
        assert!(storage.get_init_apply_meta().unwrap().is_none());
    }

    #[test]
    fn init_delete_replay_ignores_old_revisions_and_recreates_only_newer_resources() {
        let root = tempdir().unwrap();
        let storage = StorageManager::new(root.path().join("data")).unwrap();
        let init = root.path().join("init");
        fs::create_dir_all(&init).unwrap();
        let deletion = serde_json::json!({"format_version":1,"bundle_version":"delete_missing","deletions":{"memory_topics":[{"topic":"topic_a","revision":5}]}});
        fs::write(
            init.join(MANIFEST_FILE),
            serde_json::to_vec(&deletion).unwrap(),
        )
        .unwrap();
        apply_init_directory_if_needed(&storage, Some(&init), &|_| true).unwrap();
        assert_eq!(
            storage
                .get_tombstone(storage::ResourceKind::MemoryTopics, "topic_a")
                .unwrap()
                .unwrap()
                .revision,
            5
        );
        for revision in [4, 5, 6] {
            write_manifest(
                &init,
                &manifest(&format!("upsert_{revision}"), &[("topic_a", 8, revision)]),
            );
            apply_init_directory_if_needed(&storage, Some(&init), &|_| true).unwrap();
            assert_eq!(
                storage.get_memory_topic("topic_a").unwrap().is_some(),
                revision == 6
            );
        }
        let mut delayed = deletion;
        delayed["bundle_version"] = serde_json::json!("delayed_delete");
        fs::write(
            init.join(MANIFEST_FILE),
            serde_json::to_vec(&delayed).unwrap(),
        )
        .unwrap();
        apply_init_directory_if_needed(&storage, Some(&init), &|_| true).unwrap();
        assert_eq!(
            storage
                .get_memory_topic("topic_a")
                .unwrap()
                .unwrap()
                .revision,
            6
        );
    }

    #[test]
    fn init_udf_recreation_uses_canonical_identity_through_snapshot_selection() {
        let raw: RawInitManifest = serde_json::from_value(serde_json::json!({
            "format_version":1,"bundle_version":"recreate_udf","resources":{"udfs":[{"name":"TRANSFORM","revision":10,"wasm_sha256":"a".repeat(64)}]}
        })).unwrap();
        let parsed = parse_init_entries(raw);
        let selected = build_apply_candidate(
            "recreate_udf",
            ExportResources::default(),
            parsed.manifest.resources,
            CandidateOptions {
                policy: ApplyPolicy::BestEffort,
                tombstones: vec![storage::StoredTombstone {
                    kind: storage::ResourceKind::Udfs,
                    identity: "transform".into(),
                    revision: 9,
                }],
                deletions: Default::default(),
            },
        )
        .unwrap();
        assert_eq!(selected.pending.udfs[0].name, "transform");
        let mut snapshot = empty_snapshot();
        snapshot.udfs.push(storage::StoredUdf {
            name: "transform".into(),
            revision: 10,
            wasm_sha256: "a".repeat(64),
            raw_json: "{}".into(),
        });
        assert_eq!(
            filter_pending_snapshot(snapshot, &selected.pending)
                .udfs
                .len(),
            1
        );
    }

    #[test]
    fn init_resource_delete_duplicates_reject_both_and_keep_independent_operations() {
        let root = tempdir().unwrap();
        let storage = StorageManager::new(root.path().join("data")).unwrap();
        let init = root.path().join("init");
        fs::create_dir_all(&init).unwrap();
        let raw = serde_json::json!({"format_version":1,"bundle_version":"cross_duplicates",
            "resources":{"memory_topics":[{"topic":"duplicate_topic","revision":1,"kind":"bytes","capacity":8}]},
            "deletions":{"memory_topics":[{"topic":"duplicate_topic","revision":2},{"topic":"independent_topic","revision":3}]}});
        fs::write(init.join(MANIFEST_FILE), serde_json::to_vec(&raw).unwrap()).unwrap();
        apply_init_directory_if_needed(&storage, Some(&init), &|_| true).unwrap();
        assert!(
            storage
                .get_memory_topic("duplicate_topic")
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .get_tombstone(storage::ResourceKind::MemoryTopics, "duplicate_topic")
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .get_tombstone(storage::ResourceKind::MemoryTopics, "independent_topic")
                .unwrap()
                .is_some()
        );
        assert!(storage.get_init_apply_meta().unwrap().is_none());
    }

    fn write_manifest(init_dir: &Path, manifest: &ResourceManifestV1) {
        fs::create_dir_all(init_dir).expect("create init directory");
        fs::write(
            init_dir.join(MANIFEST_FILE),
            serde_json::to_vec(manifest).expect("serialize manifest"),
        )
        .expect("write manifest");
    }

    #[test]
    fn apply_creates_missing_resources_and_same_version_does_not_restore_deletion() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        write_manifest(&init_dir, &manifest("v1", &[("topic_a", 16, 1)]));

        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true).expect("first apply");
        assert!(storage.get_memory_topic("topic_a").unwrap().is_some());
        assert_eq!(
            storage
                .get_init_apply_meta()
                .unwrap()
                .unwrap()
                .bundle_version,
            "v1"
        );

        storage
            .delete_memory_topic("topic_a")
            .expect("delete topic");
        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true).expect("skip");
        assert!(storage.get_memory_topic("topic_a").unwrap().is_none());
    }

    #[test]
    fn new_version_keeps_existing_resource_and_creates_missing_resource() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        write_manifest(&init_dir, &manifest("v1", &[("topic_a", 16, 1)]));
        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true).expect("first apply");

        write_manifest(
            &init_dir,
            &manifest("v2", &[("topic_a", 64, 2), ("topic_b", 32, 1)]),
        );
        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true).expect("second apply");

        assert_eq!(
            storage
                .get_memory_topic("topic_a")
                .unwrap()
                .unwrap()
                .capacity,
            64
        );
        assert_eq!(
            storage
                .get_memory_topic("topic_b")
                .unwrap()
                .unwrap()
                .capacity,
            32
        );
    }

    #[test]
    fn malformed_manifest_is_skipped_without_advancing_apply_state() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        fs::create_dir_all(&init_dir).expect("create init directory");
        fs::write(init_dir.join(MANIFEST_FILE), b"{").expect("write malformed manifest");

        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true)
            .expect("malformed manifest is non-fatal");
        assert!(storage.get_init_apply_meta().unwrap().is_none());
    }

    #[test]
    fn invalid_entry_does_not_block_independent_valid_entry() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        fs::create_dir_all(&init_dir).expect("create init directory");
        fs::write(
            init_dir.join(MANIFEST_FILE),
            serde_json::to_vec(&serde_json::json!({
                "format_version": 1,
                "bundle_version": "partial-v1",
                "resources": {
                    "memory_topics": [
                        {"topic": "valid_topic", "revision": 1, "kind": "bytes", "capacity": 8},
                        {"topic": "invalid_topic", "kind": "bytes", "capacity": 8}
                    ],
                    "shared_mqtt_clients": [],
                    "schemas": [],
                    "streams": [],
                    "pipelines": [],
                    "udfs": []
                }
            }))
            .expect("serialize manifest"),
        )
        .expect("write manifest");

        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true)
            .expect("partial init is non-fatal");

        assert!(storage.get_memory_topic("valid_topic").unwrap().is_some());
        assert!(storage.get_memory_topic("invalid_topic").unwrap().is_none());
        assert!(
            storage.get_init_apply_meta().unwrap().is_none(),
            "partial static failure must not advance bundle_version"
        );
    }

    #[test]
    fn duplicate_identity_skips_all_duplicates_but_applies_other_resources() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        fs::create_dir_all(&init_dir).expect("create init directory");
        fs::write(
            init_dir.join(MANIFEST_FILE),
            serde_json::to_vec(&serde_json::json!({
                "format_version": 1,
                "bundle_version": "duplicates-v1",
                "resources": {
                    "memory_topics": [
                        {"topic": "duplicate_topic", "revision": 1, "kind": "bytes", "capacity": 8},
                        {"topic": "duplicate_topic", "revision": 2, "kind": "bytes", "capacity": 16},
                        {"topic": "other_topic", "revision": 1, "kind": "bytes", "capacity": 4}
                    ],
                    "shared_mqtt_clients": [],
                    "schemas": [],
                    "streams": [],
                    "pipelines": [],
                    "udfs": []
                }
            }))
            .expect("serialize manifest"),
        )
        .expect("write manifest");

        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true)
            .expect("duplicate entry is non-fatal");

        assert!(
            storage
                .get_memory_topic("duplicate_topic")
                .unwrap()
                .is_none()
        );
        assert!(storage.get_memory_topic("other_topic").unwrap().is_some());
        assert!(storage.get_init_apply_meta().unwrap().is_none());
    }

    #[test]
    fn missing_bundle_version_is_skipped_without_failing_startup() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        fs::create_dir_all(&init_dir).expect("create init directory");
        fs::write(
            init_dir.join(MANIFEST_FILE),
            br#"{"format_version":1,"resources":{"memory_topics":[],"shared_mqtt_clients":[],"schemas":[],"streams":[],"pipelines":[],"udfs":[]}}"#,
        )
        .expect("write manifest");

        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true)
            .expect("missing bundle_version must be a soft init failure");
        assert!(storage.get_init_apply_meta().unwrap().is_none());
    }

    #[test]
    fn install_staged_wasm_rejects_existing_file_with_different_content() {
        let root = tempdir().expect("root");
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).expect("create source");
        fs::create_dir(&destination).expect("create destination");
        fs::write(source.join("a.wasm"), b"new").expect("write staged WASM");
        fs::write(destination.join("a.wasm"), b"old").expect("write managed WASM");

        let err = install_staged_wasm(&source, &destination)
            .expect_err("different existing content must fail");

        assert!(err.contains("does not match staged content"));
        assert_eq!(
            fs::read(destination.join("a.wasm")).expect("read managed WASM"),
            b"old"
        );
    }

    #[test]
    fn install_staged_wasm_rolls_back_files_installed_before_failure() {
        let root = tempdir().expect("root");
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).expect("create source");
        fs::create_dir(&destination).expect("create destination");
        fs::write(source.join("a.wasm"), b"first").expect("write first staged WASM");
        fs::write(source.join("b.wasm"), b"new").expect("write second staged WASM");
        fs::write(destination.join("b.wasm"), b"old").expect("write conflicting managed WASM");

        install_staged_wasm(&source, &destination).expect_err("second install must fail");

        assert!(!destination.join("a.wasm").exists());
        assert_eq!(
            fs::read(destination.join("b.wasm")).expect("read conflicting managed WASM"),
            b"old"
        );
    }
}
