use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Json;
use axum::extract::{FromRequest, Multipart, Query, Request, State};
const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
use axum::http::{StatusCode, header::CONTENT_TYPE};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use storage::StoredPipelineDesiredState;

use crate::apply_core::{self, ApplyPolicy, ResourceApplyResult};
use crate::export::{ExportResources, ResourceManifestV1};
use crate::pipeline::AppState;
use crate::storage_bridge;
use crate::stream::{PreparedStreamRuntimeRefresh, ResolvedSchema, SchemaConfigRequest};
use crate::streaming_upload::{TemporaryUpload, is_zip_filename};

/// Batch configuration and ordinary commands share a boundary; normal commands
/// retain their existing per-resource parallelism and locks.
pub(crate) async fn coordinate_metadata(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() == axum::http::Method::DELETE
        && crate::resource_delete::route_kind(request.uri().path()).is_some()
    {
        match Query::<crate::resource_delete::DeleteQuery>::try_from_uri(request.uri()) {
            Ok(Query(query))
                if query.revision.is_some()
                    || crate::resource_delete::route_kind(request.uri().path())
                        == Some(storage::ResourceKind::MemoryTopics) =>
            {
                return next.run(request).await;
            }
            Err(error) => return error.into_response(),
            _ => {}
        }
    }
    if request.method() == axum::http::Method::GET
        || matches!(request.uri().path(), "/storage/apply" | "/storage/import")
    {
        return next.run(request).await;
    }
    let _permit = match state.try_acquire_metadata_operation(false) {
        Ok(permit) => permit,
        Err(_) => {
            return failure(
                StatusCode::CONFLICT,
                "metadata_busy",
                "another metadata operation is in progress".to_string(),
            );
        }
    };
    if crate::attachments::needs_recovery(&state.storage) {
        return failure(
            StatusCode::CONFLICT,
            "attachment_recovery_required",
            "an attachment publication requires recovery; restart or retry a storage operation"
                .to_string(),
        );
    }
    next.run(request).await
}

pub(crate) fn acquire_exclusive_metadata(
    state: &AppState,
) -> Result<tokio::sync::OwnedSemaphorePermit, Box<Response>> {
    let permit = state.try_acquire_metadata_operation(true).map_err(|_| {
        Box::new(failure(
            StatusCode::CONFLICT,
            "metadata_busy",
            "another metadata operation is in progress".to_string(),
        ))
    })?;
    crate::attachments::recover(&state.storage).map_err(|err| {
        Box::new(failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "attachment_recovery_failed",
            err,
        ))
    })?;
    Ok(permit)
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct ApplyQuery {
    cascade: bool,
}

#[derive(Serialize)]
pub(crate) struct ApplyResponse {
    bundle_version: String,
    metadata_committed: bool,
    applied_to_runtime: Option<bool>,
    resources: Vec<ResourceApplyResult>,
    affected_resources: Vec<ResourceApplyResult>,
}

pub(crate) fn failure(status: StatusCode, code: &str, message: String) -> Response {
    (
        status,
        Json(serde_json::json!({
            "metadata_committed": false, "applied_to_runtime": false,
            "error": { "code": code, "message": message }
        })),
    )
        .into_response()
}

pub(crate) async fn apply_storage_handler(
    State(state): State<AppState>,
    Query(query): Query<ApplyQuery>,
    request: Request,
) -> Response {
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let work = match tempfile::Builder::new()
        .prefix(".apply.")
        .tempdir_in(state.storage.base_dir())
    {
        Ok(work) => work,
        Err(err) => {
            return failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                "preparation_failed",
                err.to_string(),
            );
        }
    };
    let manifest: ResourceManifestV1 = if content_type == "application/json" {
        let bytes = match axum::body::to_bytes(request.into_body(), MAX_MANIFEST_BYTES).await {
            Ok(bytes) => bytes,
            Err(err) => {
                return failure(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "invalid_manifest",
                    format!("manifest body exceeds limit or could not be read: {err}"),
                );
            }
        };
        match serde_json::from_slice(&bytes) {
            Ok(manifest) => manifest,
            Err(err) => {
                return failure(StatusCode::BAD_REQUEST, "invalid_manifest", err.to_string());
            }
        }
    } else if content_type == "multipart/form-data" {
        let mut multipart = match Multipart::from_request(request, &state).await {
            Ok(multipart) => multipart,
            Err(err) => return failure(err.status(), "invalid_upload", err.body_text()),
        };
        let mut upload = None;
        loop {
            let field = match multipart.next_field().await {
                Ok(Some(field)) => field,
                Ok(None) => break,
                Err(err) => return failure(err.status(), "invalid_upload", err.body_text()),
            };
            if field.name() != Some("file")
                || upload.is_some()
                || !is_zip_filename(field.file_name())
            {
                return failure(
                    StatusCode::BAD_REQUEST,
                    "invalid_upload",
                    "expected exactly one ZIP field named 'file'".to_string(),
                );
            }
            upload = match TemporaryUpload::receive(&state.storage, field, Some("zip")).await {
                Ok(upload) => Some(upload),
                Err(err) => return failure(err.status, "invalid_upload", err.message),
            };
        }
        let Some(upload) = upload else {
            return failure(
                StatusCode::BAD_REQUEST,
                "invalid_upload",
                "field 'file' is required".to_string(),
            );
        };
        if let Err(err) = crate::import::extract_zip_file(upload.path(), work.path()) {
            return failure(StatusCode::BAD_REQUEST, "invalid_archive", err);
        }
        let manifest_path = work.path().join("manifest.json");
        if std::fs::metadata(&manifest_path)
            .is_ok_and(|meta| meta.len() > MAX_MANIFEST_BYTES as u64)
        {
            return failure(
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid_manifest",
                "manifest exceeds 8 MiB".to_string(),
            );
        }
        match std::fs::read(manifest_path)
            .map_err(|err| err.to_string())
            .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|err| err.to_string()))
        {
            Ok(manifest) => manifest,
            Err(err) => return failure(StatusCode::BAD_REQUEST, "invalid_manifest", err),
        }
    } else {
        return failure(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "use application/json or multipart/form-data".to_string(),
        );
    };
    let _metadata_permit = match acquire_exclusive_metadata(&state) {
        Ok(permit) => permit,
        Err(response) => return *response,
    };
    let _storage_permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(_) => {
            return failure(
                StatusCode::CONFLICT,
                "metadata_busy",
                "another storage operation is in progress".to_string(),
            );
        }
    };
    match apply_manifest(&state, manifest, work.path(), query.cascade).await {
        Ok(result) => Json(result).into_response(),
        Err((status, code, message)) => failure(status, code, message),
    }
}

type ApplyError = (StatusCode, &'static str, String);
fn invalid(message: String) -> ApplyError {
    (StatusCode::BAD_REQUEST, "invalid_resources", message)
}
fn internal(message: String) -> ApplyError {
    (StatusCode::INTERNAL_SERVER_ERROR, "commit_failed", message)
}

fn normalize(resources: &mut ExportResources) -> Result<(), String> {
    for stream in &mut resources.streams {
        stream.normalize();
    }
    for pipeline in &mut resources.pipelines {
        pipeline.definition.normalize();
        pipeline.definition.flow_instance_id =
            Some(crate::resource_id::defaulted_flow_instance_id(
                pipeline.definition.flow_instance_id.as_deref(),
            )?);
    }
    for schema in &mut resources.schemas {
        schema.schema_type = schema.schema_type.trim().to_ascii_lowercase();
    }
    for udf in &mut resources.udfs {
        udf.name = udf.name.to_ascii_lowercase();
    }
    Ok(())
}

pub(crate) async fn apply_manifest(
    state: &AppState,
    mut manifest: ResourceManifestV1,
    source: &Path,
    cascade: bool,
) -> Result<ApplyResponse, ApplyError> {
    crate::export::validate_resource_manifest(&manifest).map_err(invalid)?;
    normalize(&mut manifest.resources).map_err(invalid)?;
    apply_core::validate_incoming_identities(&manifest.resources).map_err(invalid)?;
    manifest
        .deletions
        .validate(&manifest.resources)
        .map_err(invalid)?;
    let mut live = crate::export::build_export_resources(&state.storage).map_err(internal)?;
    // Compare against the accepted declaration, never the operational desired state.
    for pipeline in &mut live.pipelines {
        pipeline.run_state = state
            .storage
            .get_pipeline(&pipeline.definition.id)
            .map_err(|err| internal(err.to_string()))?
            .ok_or_else(|| internal("stored pipeline missing".to_string()))?
            .deployment_run_state()
            .map_err(|err| internal(err.to_string()))?;
    }
    normalize(&mut live).map_err(internal)?;
    let previous = live.clone();
    let submitted_scope = runtime_scope(state, &previous, &manifest.resources, cascade);
    let tombstones =
        apply_core::relevant_tombstones(&state.storage, &manifest.resources, &manifest.deletions)
            .map_err(internal)?;
    let candidate = apply_core::build_apply_candidate(
        &manifest.bundle_version,
        live,
        manifest.resources,
        apply_core::CandidateOptions {
            policy: ApplyPolicy::Atomic,
            tombstones,
            deletions: manifest.deletions,
        },
    )
    .map_err(|err| (StatusCode::CONFLICT, "revision_conflict", err))?;
    let pending_deletions = candidate.deletions;
    let pending = candidate.pending;
    let mut results = candidate.results;
    let candidate = candidate.resources;
    if let Some(error) = crate::resource_delete::blockers(&candidate, &pending_deletions)
        .map_err(invalid)?
        .into_values()
        .next()
    {
        return Err((StatusCode::CONFLICT, "resource_referenced", error));
    }
    for result in results.iter().filter(|result| {
        result.operation == "upsert" && result.kind == "schemas" && result.result == "idempotent"
    }) {
        let schema = candidate
            .schemas
            .iter()
            .find(|schema| schema.name == result.identity)
            .ok_or_else(|| internal("selected schema missing".to_string()))?;
        let relative = Path::new(&schema.schema_type).join(&schema.name);
        let provided = source.join("schemas").join(&relative);
        if provided.exists()
            && !attachment_trees_equal(&provided, &state.storage.schemas_dir().join(relative))
                .map_err(invalid)?
        {
            return Err((
                StatusCode::CONFLICT,
                "revision_conflict",
                format!(
                    "same_revision_different_spec: schemas {}: attachment content differs at revision {}",
                    result.identity, result.incoming_revision
                ),
            ));
        }
    }
    for result in results.iter().filter(|result| {
        result.operation == "upsert" && result.kind == "udfs" && result.result == "idempotent"
    }) {
        let udf = candidate
            .udfs
            .iter()
            .find(|udf| udf.name == result.identity)
            .ok_or_else(|| internal("selected UDF missing".to_string()))?;
        let filename = format!("{}.wasm", udf.wasm_sha256);
        let provided = source.join("wasm_files").join(&filename);
        if provided.exists()
            && !apply_core::files_equal(&provided, &state.storage.wasm_files_dir().join(filename))
                .map_err(invalid)?
        {
            return Err(invalid(format!(
                "UDF {} attachment does not match its accepted SHA-256",
                udf.name
            )));
        }
    }
    if results.iter().all(|result| result.result == "idempotent") {
        cleanup_deletions(state, &mut results).await;
        read_runtime_results(state, &mut results);
        let affected = related_results(state, &submitted_scope, &results).map_err(internal)?;
        return Ok(ApplyResponse {
            bundle_version: manifest.bundle_version,
            metadata_committed: true,
            applied_to_runtime: combined_runtime_result(&results, &affected),
            resources: results,
            affected_resources: affected,
        });
    }

    let prepared = prepare_attachments(state, source, &pending)?;
    let mut validation_view = candidate.clone();
    // Retained pipelines are not replanned as a prerequisite for accepting an upstream update.
    validation_view.pipelines = pending.pipelines.clone();
    let snapshot = crate::import::validate_and_build_snapshot(
        &ResourceManifestV1 {
            deletions: Default::default(),
            format_version: manifest.format_version,
            bundle_version: manifest.bundle_version.clone(),
            resources: validation_view,
        },
        Some(&prepared.schemas),
        &|id| state.is_declared_instance(id),
    )
    .map_err(invalid)?;
    let mut delta = apply_core::filter_pending_snapshot(snapshot, &pending);
    delta.tombstones = pending_deletions;
    crate::import::validate_and_copy_udfs_for_import(
        &delta.udfs,
        &prepared.wasm_source,
        &prepared.wasm_staged,
    )
    .map_err(invalid)?;
    validate_online_definitions(state, &candidate, &pending, &prepared)
        .await
        .map_err(invalid)?;

    let mut schema_tree = if pending.schemas.is_empty() {
        None
    } else {
        Some(
            crate::schema::source::PreparedSchemaTree::prepare(&state.storage, &prepared.schemas)
                .map_err(internal)?,
        )
    };
    let execution_scope = runtime_scope(state, &previous, &pending, cascade);
    crate::attachments::publish_and_commit(
        &state.storage,
        delta,
        Default::default(),
        schema_tree.take(),
        &prepared.wasm_staged,
    )
    .map_err(internal)?;
    let mut affected = apply_runtime(state, execution_scope, &pending, &mut results).await;
    read_runtime_results(state, &mut results);
    match related_results(state, &submitted_scope, &results) {
        Ok(related) => {
            for result in related {
                if !affected.iter().any(|existing| {
                    existing.kind == result.kind && existing.identity == result.identity
                }) {
                    affected.push(result);
                }
            }
        }
        Err(error) => {
            for result in &mut results {
                result.runtime_applied = Some(false);
                result.error = Some(error.clone());
            }
        }
    }
    let applied_to_runtime = combined_runtime_result(&results, &affected);
    Ok(ApplyResponse {
        bundle_version: manifest.bundle_version,
        metadata_committed: true,
        applied_to_runtime,
        resources: results,
        affected_resources: affected,
    })
}

fn attachment_trees_equal(left: &Path, right: &Path) -> Result<bool, String> {
    let left_meta = std::fs::symlink_metadata(left).map_err(|err| err.to_string())?;
    let right_meta = match std::fs::symlink_metadata(right) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err.to_string()),
    };
    if left_meta.is_file() && right_meta.is_file() {
        return apply_core::files_equal(left, right);
    }
    if !left_meta.is_dir() || !right_meta.is_dir() {
        return Ok(false);
    }
    let entries = |path: &Path| -> Result<BTreeSet<std::ffi::OsString>, String> {
        std::fs::read_dir(path)
            .map_err(|err| err.to_string())?
            .map(|entry| {
                entry
                    .map(|entry| entry.file_name())
                    .map_err(|err| err.to_string())
            })
            .collect()
    };
    let left_entries = entries(left)?;
    if left_entries != entries(right)? {
        return Ok(false);
    }
    for name in left_entries {
        if !attachment_trees_equal(&left.join(&name), &right.join(&name))? {
            return Ok(false);
        }
    }
    Ok(true)
}

struct PreparedAttachments {
    _work: tempfile::TempDir,
    schemas: PathBuf,
    wasm_source: PathBuf,
    wasm_staged: PathBuf,
}

fn prepare_attachments(
    state: &AppState,
    source: &Path,
    pending: &ExportResources,
) -> Result<PreparedAttachments, ApplyError> {
    let work = tempfile::Builder::new()
        .prefix(".apply.prepare.")
        .tempdir_in(state.storage.base_dir())
        .map_err(|err| internal(err.to_string()))?;
    let schemas = work.path().join("schemas");
    std::fs::create_dir(&schemas).map_err(|err| internal(err.to_string()))?;
    let mut limits = apply_core::SourceLimits::default();
    if !pending.schemas.is_empty() {
        apply_core::copy_tree_limited(&state.storage.schemas_dir(), &schemas, &mut limits)
            .map_err(internal)?;
    }
    for schema in &pending.schemas {
        crate::resource_id::validate_resource_id(
            crate::resource_id::ResourceIdKind::SchemaName,
            &schema.name,
        )
        .map_err(invalid)?;
        // Type is a registry key, not an arbitrary path component.
        if !matches!(
            Path::new(&schema.schema_type)
                .components()
                .collect::<Vec<_>>()
                .as_slice(),
            [std::path::Component::Normal(_)]
        ) {
            return Err(invalid(format!(
                "unsupported schema type: {}",
                schema.schema_type
            )));
        }
        let relative = Path::new(&schema.schema_type).join(&schema.name);
        let incoming = source.join("schemas").join(&relative);
        if incoming.exists() {
            let destination = schemas.join(relative);
            if destination.exists() {
                std::fs::remove_dir_all(&destination).map_err(|err| internal(err.to_string()))?;
            }
            std::fs::create_dir_all(
                destination
                    .parent()
                    .ok_or_else(|| internal("schema parent missing".to_string()))?,
            )
            .map_err(|err| internal(err.to_string()))?;
            apply_core::copy_tree_limited(&incoming, &destination, &mut limits).map_err(invalid)?;
        }
    }
    let wasm_source = work.path().join("wasm-source");
    let wasm_staged = work.path().join("wasm-staged");
    std::fs::create_dir(&wasm_source).map_err(|err| internal(err.to_string()))?;
    std::fs::create_dir(&wasm_staged).map_err(|err| internal(err.to_string()))?;
    for udf in &pending.udfs {
        if udf.wasm_sha256.len() != 64
            || !udf
                .wasm_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid(format!("invalid WASM SHA-256 for {}", udf.name)));
        }
        let name = format!("{}.wasm", udf.wasm_sha256);
        let incoming = source.join("wasm_files").join(&name);
        let existing = state.storage.wasm_files_dir().join(&name);
        apply_core::copy_regular_file_limited(
            if incoming.exists() {
                &incoming
            } else {
                &existing
            },
            &wasm_source.join(name),
            &mut limits,
        )
        .map_err(invalid)?;
    }
    Ok(PreparedAttachments {
        _work: work,
        schemas: if pending.schemas.is_empty() {
            state.storage.schemas_dir()
        } else {
            schemas
        },
        wasm_source,
        wasm_staged,
    })
}

fn resolve_schema(
    config: &SchemaConfigRequest,
    name: &str,
    resources: &ExportResources,
    root: &Path,
) -> Result<ResolvedSchema, String> {
    let (schema_type, props) = if let Some(reference) = &config.r#ref {
        let schema = resources
            .schemas
            .iter()
            .find(|schema| &schema.name == reference)
            .ok_or_else(|| format!("missing schema {reference}"))?;
        let mut props = schema.props.clone();
        crate::schema::source::resolve_props_from_root(
            root,
            reference,
            &schema.schema_type,
            &mut props,
        )?;
        (schema.schema_type.as_str(), props)
    } else {
        (config.schema_type.as_str(), config.props.clone())
    };
    let (schema, bundle, artifact) =
        crate::stream::schema_registry().parse(schema_type, name, &props)?;
    Ok(ResolvedSchema::new(schema, bundle, artifact))
}

async fn validate_online_definitions(
    state: &AppState,
    candidate: &ExportResources,
    pending: &ExportResources,
    files: &PreparedAttachments,
) -> Result<(), String> {
    let live_instance = state.instances.default_instance();
    let view =
        flow::FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
            "apply_validation",
            Some(live_instance.shared_registries()),
        ))
        .map_err(|err| err.to_string())?;
    view.set_secret_context(live_instance.secret_context());
    view.set_property_registry(live_instance.property_registry());
    for topic in &candidate.memory_topics {
        let kind = memory_kind(&topic.kind);
        view.declare_memory_topic(&topic.topic, kind, topic.capacity)
            .map_err(|err| err.to_string())?;
    }
    let mut required = pending
        .streams
        .iter()
        .map(|stream| stream.name.clone())
        .chain(
            pending
                .tables
                .iter()
                .map(|table| table.definition.name.clone()),
        )
        .collect::<BTreeSet<_>>();
    for pipeline in &pending.pipelines {
        required.extend(crate::pipeline::referenced_streams_from_pipeline_sql(
            &pipeline.definition,
            &view,
        )?);
    }
    for stream in &candidate.streams {
        if !required.contains(&stream.name) {
            continue;
        }
        let stored = storage_bridge::stored_stream_from_request(stream)?;
        let definition = if stream.schema.r#ref.is_none() {
            storage_bridge::stream_definition_from_stored(
                &stored,
                view.decoder_registry().as_ref(),
            )?
        } else {
            let schema = resolve_schema(&stream.schema, &stream.name, candidate, &files.schemas)?;
            let props = crate::stream::build_stream_props(&stream.stream_type, &stream.props)?;
            let decoder = crate::stream::build_stream_decoder(
                stream,
                view.decoder_registry().as_ref(),
                &schema,
            )?;
            let mut definition = flow::catalog::StreamDefinition::new(
                stream.name.clone(),
                Arc::clone(&schema.logical_schema),
                props,
                decoder,
            )
            .with_revision(stream.revision);
            if let Some(eventtime) = &stream.eventtime {
                definition = definition.with_eventtime(flow::catalog::EventtimeDefinition::new(
                    eventtime.column.clone(),
                    eventtime.eventtime_type.clone(),
                ));
            }
            if let Some(sampler) = &stream.sampler {
                definition = definition.with_sampler(sampler.clone());
            }
            definition
        };
        if let flow::catalog::StreamProps::Mqtt(props) = definition.props()
            && let Some(key) = &props.connector_key
        {
            require_mqtt(candidate, key)?;
        }
        view.create_stream(definition, false)
            .await
            .map_err(|err| format!("stream {}: {err}", stream.name))?;
    }
    for table in &candidate.tables {
        let request = &table.definition;
        if !required.contains(&request.name) {
            continue;
        }
        let schema = resolve_schema(&request.schema, &request.name, candidate, &files.schemas)?;
        let props = crate::table::build_table_props(&request.table_type, &request.props)?;
        let decoder =
            crate::table::build_table_decoder(request, view.decoder_registry().as_ref(), &schema)?;
        view.create_table(flow::catalog::TableDefinition::new(
            request.name.clone(),
            Arc::clone(&schema.logical_schema),
            props,
            decoder,
        ))
        .await
        .map_err(|err| err.to_string())?;
    }
    #[cfg(feature = "wasm_udf")]
    if !pending.pipelines.is_empty() {
        load_udfs(
            &view,
            &candidate.udfs,
            &files.wasm_source,
            &state.storage.wasm_files_dir(),
        )?;
    }
    let mqtt_clients = candidate
        .shared_mqtt_clients
        .iter()
        .map(|client| (client.definition.key.clone(), client.definition.clone()))
        .collect::<BTreeMap<_, _>>();
    for pipeline in &pending.pipelines {
        for sink in &pipeline.definition.sinks {
            let props = sink.props.to_value();
            if sink.sink_type.eq_ignore_ascii_case("mqtt")
                && let Some(key) = props
                    .get("connector_key")
                    .and_then(serde_json::Value::as_str)
            {
                require_mqtt(candidate, key)?;
            }
        }
        let definition = crate::pipeline::build_pipeline_definition(
            &pipeline.definition,
            view.encoder_registry().as_ref(),
            &view,
            crate::pipeline::PipelineBuildOptions {
                shared_mqtt_clients: Some(&mqtt_clients),
            },
        )?;
        view.explain_pipeline(flow::ExplainPipelineTarget::Definition(&definition))
            .map_err(|err| format!("pipeline {}: {err}", pipeline.definition.id))?;
    }
    Ok(())
}

fn require_mqtt(resources: &ExportResources, key: &str) -> Result<(), String> {
    if resources
        .shared_mqtt_clients
        .iter()
        .any(|client| client.definition.key == key)
    {
        Ok(())
    } else {
        Err(format!("missing shared MQTT client: {key}"))
    }
}

fn memory_kind(kind: &storage::StoredMemoryTopicKind) -> flow::connector::MemoryTopicKind {
    match kind {
        storage::StoredMemoryTopicKind::Bytes => flow::connector::MemoryTopicKind::Bytes,
        storage::StoredMemoryTopicKind::Collection => flow::connector::MemoryTopicKind::Collection,
    }
}

#[cfg(feature = "wasm_udf")]
fn load_udfs(
    instance: &flow::FlowInstance,
    udfs: &[crate::export::ExportUdf],
    prepared: &Path,
    existing: &Path,
) -> Result<(), String> {
    if udfs.is_empty() {
        return Ok(());
    }
    let engine = udf::WasmEngine::new().map_err(|err| err.to_string())?;
    let mut functions: Vec<Arc<dyn flow::CustomFunc>> = Vec::with_capacity(udfs.len());
    for udf in udfs {
        let file = format!("{}.wasm", udf.wasm_sha256);
        let staged = prepared.join(&file);
        let bytes = std::fs::read(if staged.exists() {
            staged
        } else {
            existing.join(file)
        })
        .map_err(|err| err.to_string())?;
        functions.push(Arc::new(
            engine
                .instantiate(&udf.name, &bytes)
                .map_err(|err| err.to_string())?,
        ));
    }
    instance.set_custom_func_registry(
        flow::CustomFuncRegistry::with_builtins_and_wasm(functions)
            .map_err(|err| err.to_string())?,
    );
    Ok(())
}

fn combined_runtime_result(
    requested: &[ResourceApplyResult],
    affected: &[ResourceApplyResult],
) -> Option<bool> {
    let mut known = true;
    for result in requested.iter().chain(affected) {
        match result.runtime_applied {
            Some(false) => return Some(false),
            None => known = false,
            Some(true) => {}
        }
    }
    known.then_some(true)
}

async fn cleanup_deletions(state: &AppState, results: &mut [ResourceApplyResult]) {
    let mut owner_failure = None;
    for kind in [
        storage::ResourceKind::Pipelines,
        storage::ResourceKind::Streams,
        storage::ResourceKind::Tables,
        storage::ResourceKind::SharedMqttClients,
        storage::ResourceKind::MemoryTopics,
        storage::ResourceKind::Udfs,
        storage::ResourceKind::Schemas,
    ] {
        let inherited_failure = owner_failure.clone();
        let wasm_cleanup = if kind == storage::ResourceKind::Udfs
            && inherited_failure.is_none()
            && results
                .iter()
                .any(|result| result.operation == "delete" && result.kind == "udfs")
        {
            crate::resource_delete::cleanup_persisted(&state.storage, kind, "")
        } else {
            Ok(())
        };
        for result in results.iter_mut().filter(|result| {
            result.operation == "delete" && result.kind == crate::resource_delete::kind_name(kind)
        }) {
            let outcome = if let Some(error) = &inherited_failure {
                Err(format!("dependent deletion failed: {error}"))
            } else if let Err(error) = &wasm_cleanup {
                Err(error.clone())
            } else {
                crate::resource_delete::cleanup(state, kind, &result.identity).await
            };
            if let Err(error) = &outcome
                && matches!(
                    kind,
                    storage::ResourceKind::Pipelines
                        | storage::ResourceKind::Streams
                        | storage::ResourceKind::Tables
                )
            {
                owner_failure = Some(error.clone());
            }
            record_runtime(result, outcome);
        }
    }
}

fn read_runtime_results(state: &AppState, results: &mut [ResourceApplyResult]) {
    for result in results {
        if result.runtime_applied.is_some() {
            continue;
        }
        let observed = observe_runtime(state, result);
        match observed {
            Ok(applied) => result.runtime_applied = applied,
            Err(error) => {
                result.error = Some(error);
            }
        }
    }
}

fn observe_runtime(
    state: &AppState,
    result: &mut ResourceApplyResult,
) -> Result<Option<bool>, String> {
    match result.kind.as_str() {
        "schemas" => Ok(Some(
            crate::stream::named_schema_store()
                .get_resolved(&result.identity)
                .is_some_and(|schema| schema.schema_version == Some(result.incoming_revision)),
        )),
        "memory_topics" => {
            let stored = state
                .storage
                .get_memory_topic(&result.identity)
                .map_err(|err| err.to_string())?
                .ok_or("missing committed topic")?;
            Ok(Some(state.instances.instances_snapshot().iter().all(
                |(_, instance)| {
                    instance.memory_topic_capacity(&stored.topic) == Some(stored.capacity)
                        && instance.memory_topic_kind(&stored.topic)
                            == Some(memory_kind(&stored.kind))
                },
            )))
        }
        "shared_mqtt_clients" => mqtt_runtime_matches(state, &result.identity).map(Some),
        "pipelines" => {
            let stored = state
                .storage
                .get_pipeline(&result.identity)
                .map_err(|err| err.to_string())?
                .ok_or("missing committed pipeline")?;
            let request = storage_bridge::pipeline_request_from_stored(&stored)?;
            let Some(instance) = state.local_instance(
                request
                    .flow_instance_id
                    .as_deref()
                    .unwrap_or(crate::DEFAULT_FLOW_INSTANCE_ID),
            ) else {
                return Ok(Some(false));
            };
            let snapshot = instance
                .list_pipelines()
                .into_iter()
                .find(|snapshot| snapshot.definition.id() == result.identity);
            match snapshot {
                Some(snapshot) if snapshot.definition.revision() != result.incoming_revision => {
                    Ok(Some(false))
                }
                Some(snapshot) if snapshot.status == flow::pipeline::PipelineStatus::Failed => {
                    result.error = Some(
                        snapshot
                            .last_runtime_error
                            .map(|error| error.reason)
                            .unwrap_or_else(|| "pipeline runtime failed".to_string()),
                    );
                    Ok(Some(false))
                }
                Some(_) if result.result == "current" => Ok(None),
                Some(_) => Ok(Some(true)),
                None => Ok(Some(false)),
            }
        }
        "streams" => {
            let stored = state
                .storage
                .get_stream(&result.identity)
                .map_err(|err| err.to_string())?
                .ok_or("missing committed stream")?;
            let request = storage_bridge::stream_request_from_stored(&stored)?;
            let expected_schema_version = request
                .schema
                .r#ref
                .as_deref()
                .map(|reference| {
                    state
                        .storage
                        .get_schema(reference)
                        .map_err(|err| err.to_string())?
                        .map(|schema| schema.revision)
                        .ok_or_else(|| format!("missing committed schema {reference}"))
                })
                .transpose()?;
            let mut known = true;
            for (_, instance) in state.instances.instances_snapshot() {
                let Some(definition) = instance.stream_definition(&result.identity) else {
                    return Ok(Some(false));
                };
                if definition.schema_version() != expected_schema_version {
                    return Ok(Some(false));
                }
                match definition.revision() {
                    Some(revision) if revision != result.incoming_revision => {
                        return Ok(Some(false));
                    }
                    None => known = false,
                    Some(_) => {}
                }
            }
            Ok(known.then_some(true))
        }
        _ => Ok(None),
    }
}

fn mqtt_runtime_matches(state: &AppState, key: &str) -> Result<bool, String> {
    let stored = state
        .storage
        .get_mqtt_config(key)
        .map_err(|err| err.to_string())?
        .ok_or("missing committed MQTT client")?;
    let expected = serde_json::to_value(storage_bridge::mqtt_config_from_stored(&stored))
        .map_err(|err| err.to_string())?;
    for (_, instance) in state.instances.instances_snapshot() {
        let Some(config) = instance.get_shared_mqtt_client(key) else {
            return Ok(false);
        };
        if serde_json::to_value(config).map_err(|err| err.to_string())? != expected {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Protect runtime objects still referenced by pipelines outside this update.
async fn protected_runtime_resources(
    state: &AppState,
    pending: &ExportResources,
) -> Result<BTreeMap<String, String>, String> {
    let clients = pending
        .shared_mqtt_clients
        .iter()
        .map(|client| client.definition.key.as_str())
        .collect::<BTreeSet<_>>();
    let mut protected = BTreeMap::new();
    for (_, instance) in state.instances.instances_snapshot() {
        let streams = instance
            .list_streams()
            .await
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|stream| (stream.definition.id().to_string(), stream))
            .collect::<BTreeMap<_, _>>();
        for pipeline in instance.list_pipelines() {
            let id = pipeline.definition.id();
            let mut protect = |kind: &str, identity: &str| {
                protected.entry(format!("{kind}/{identity}")).or_insert_with(|| {
                    format!("runtime replacement deferred: retained pipeline {id} uses {kind}/{identity}")
                });
            };
            for key in &pipeline.shared_mqtt_client_keys {
                if clients.contains(key.as_str()) {
                    protect("shared_mqtt_clients", key);
                }
            }
            for name in &pipeline.streams {
                if streams
                    .get(name)
                    .is_some_and(|stream| stream.shared_info.is_some())
                {
                    protect("streams", name);
                }
            }
        }
    }
    for stream in state
        .storage
        .list_streams()
        .map_err(|err| err.to_string())?
    {
        let request = storage_bridge::stream_request_from_stored(&stream)?;
        if let Some(key) = request
            .props
            .fields
            .get("connector_key")
            .and_then(serde_json::Value::as_str)
            && let Some(error) = protected
                .get(&format!("shared_mqtt_clients/{key}"))
                .cloned()
        {
            protected
                .entry(format!("streams/{}", stream.id))
                .or_insert(error);
        }
    }
    Ok(protected)
}

fn record_runtime(result: &mut ResourceApplyResult, outcome: Result<(), String>) {
    result.runtime_applied = Some(outcome.is_ok());
    result.error = outcome.err();
}

struct RuntimeScope {
    sources: BTreeSet<String>,
    pipelines: BTreeSet<String>,
}

fn runtime_scope(
    state: &AppState,
    previous: &ExportResources,
    pending: &ExportResources,
    cascade: bool,
) -> RuntimeScope {
    let mut sources = pending
        .streams
        .iter()
        .map(|stream| stream.name.clone())
        .chain(
            pending
                .tables
                .iter()
                .map(|table| table.definition.name.clone()),
        )
        .collect::<BTreeSet<_>>();
    let mut pipeline_ids = pending
        .pipelines
        .iter()
        .map(|pipeline| pipeline.definition.id.clone())
        .collect::<BTreeSet<_>>();
    if !cascade {
        return RuntimeScope {
            sources,
            pipelines: pipeline_ids,
        };
    }
    let schemas = pending
        .schemas
        .iter()
        .map(|schema| schema.name.as_str())
        .collect::<BTreeSet<_>>();
    let clients = pending
        .shared_mqtt_clients
        .iter()
        .map(|client| client.definition.key.as_str())
        .collect::<BTreeSet<_>>();
    let topics = pending
        .memory_topics
        .iter()
        .map(|topic| topic.topic.as_str())
        .collect::<BTreeSet<_>>();
    for stream in &previous.streams {
        let props = stream.props.to_value();
        if stream
            .schema
            .r#ref
            .as_deref()
            .is_some_and(|name| schemas.contains(name))
            || props
                .get("connector_key")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|key| clients.contains(key))
            || props
                .get("topic")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|topic| topics.contains(topic))
        {
            sources.insert(stream.name.clone());
        }
    }
    for table in &previous.tables {
        if table
            .definition
            .schema
            .r#ref
            .as_deref()
            .is_some_and(|name| schemas.contains(name))
        {
            sources.insert(table.definition.name.clone());
        }
    }
    let runtime_pipelines = state
        .instances
        .instances_snapshot()
        .into_iter()
        .flat_map(|(_, instance)| instance.list_pipelines())
        .collect::<Vec<_>>();
    for pipeline in &previous.pipelines {
        let referenced = match crate::pipeline::referenced_streams_from_pipeline_sql(
            &pipeline.definition,
            state.instances.default_instance().as_ref(),
        ) {
            Ok(referenced) => referenced,
            Err(err) => {
                tracing::warn!(pipeline_id = %pipeline.definition.id, %err,
                    "apply: dependency parsing failed; using runtime bindings or conservative scope");
                let bound = runtime_pipelines
                    .iter()
                    .filter(|runtime| runtime.definition.id() == pipeline.definition.id)
                    .flat_map(|runtime| runtime.streams.iter().cloned())
                    .collect::<BTreeSet<_>>();
                if bound.is_empty() {
                    // An absent graph cannot establish that upstream changes are unrelated.
                    sources.clone()
                } else {
                    bound
                }
            }
        };
        let sink_dependency_changed = pipeline.definition.sinks.iter().any(|sink| {
            let props = sink.props.to_value();
            props
                .get("connector_key")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|key| clients.contains(key))
                || (sink.sink_type.eq_ignore_ascii_case("memory")
                    && props
                        .get("topic")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|topic| topics.contains(topic)))
        });
        if !pending.udfs.is_empty()
            || referenced.iter().any(|name| sources.contains(name))
            || sink_dependency_changed
        {
            pipeline_ids.insert(pipeline.definition.id.clone());
        }
    }
    for pipeline in runtime_pipelines {
        let source_changed = pipeline.streams.iter().any(|name| sources.contains(name))
            || pipeline
                .shared_mqtt_client_keys
                .iter()
                .any(|key| clients.contains(key.as_str()));
        let sink_changed = pipeline
            .definition
            .sinks()
            .iter()
            .any(|sink| match &sink.props {
                flow::pipeline::SinkProps::Mqtt(props) => props
                    .connector_key
                    .as_deref()
                    .is_some_and(|key| clients.contains(key)),
                flow::pipeline::SinkProps::Memory(props) => topics.contains(props.topic.as_str()),
                _ => false,
            });
        if source_changed || sink_changed || !pending.udfs.is_empty() {
            pipeline_ids.insert(pipeline.definition.id().to_string());
        }
    }
    RuntimeScope {
        sources,
        pipelines: pipeline_ids,
    }
}

fn related_results(
    state: &AppState,
    scope: &RuntimeScope,
    requested: &[ResourceApplyResult],
) -> Result<Vec<ResourceApplyResult>, String> {
    let mut results = Vec::new();
    for id in &scope.sources {
        let resource = state
            .storage
            .get_stream(id)
            .map(|stream| stream.map(|stream| ("streams", stream.revision)))
            .and_then(|stream| match stream {
                Some(stream) => Ok(Some(stream)),
                None => state
                    .storage
                    .get_table(id)
                    .map(|table| table.map(|table| ("tables", table.revision))),
            });
        match resource {
            Ok(Some((kind, revision))) => {
                append_related(&mut results, requested, kind, id, revision)
            }
            Ok(None) => {}
            Err(err) => return Err(format!("read affected source {id}: {err}")),
        }
    }
    for id in &scope.pipelines {
        match state.storage.get_pipeline(id) {
            Ok(Some(pipeline)) => {
                append_related(&mut results, requested, "pipelines", id, pipeline.revision)
            }
            Ok(None) => {}
            Err(err) => return Err(format!("read affected pipeline {id}: {err}")),
        }
    }
    read_runtime_results(state, &mut results);
    Ok(results)
}

fn append_related(
    results: &mut Vec<ResourceApplyResult>,
    requested: &[ResourceApplyResult],
    kind: &str,
    id: &str,
    revision: u64,
) {
    if requested
        .iter()
        .any(|result| result.kind == kind && result.identity == id)
    {
        return;
    }
    results.push(ResourceApplyResult {
        kind: kind.to_string(),
        identity: id.to_string(),
        incoming_revision: revision,
        current_revision: Some(revision),
        operation: "upsert",
        result: "current",
        runtime_applied: None,
        error: None,
    });
}

async fn apply_runtime(
    state: &AppState,
    scope: RuntimeScope,
    pending: &ExportResources,
    results: &mut [ResourceApplyResult],
) -> Vec<ResourceApplyResult> {
    let RuntimeScope {
        sources,
        pipelines: pipeline_ids,
    } = scope;
    let mut pipeline_results = BTreeMap::new();
    let mut affected_resources = Vec::new();
    let mut stop_error = None;
    for id in &pipeline_ids {
        for (_, instance) in state.instances.instances_snapshot() {
            match instance.delete_pipeline(id).await {
                Ok(_) | Err(flow::pipeline::PipelineError::NotFound(_)) => {}
                Err(err) => {
                    stop_error = Some(format!("stop affected pipeline {id}: {err}"));
                }
            }
        }
    }
    cleanup_deletions(state, results).await;
    let protected = match protected_runtime_resources(state, pending).await {
        Ok(protected) => protected,
        Err(error) => {
            stop_error = Some(error);
            BTreeMap::new()
        }
    };
    // A failed stop can leave old consumers active. Report all dependent updates
    // as unapplied rather than replacing connectors underneath those consumers.
    if let Some(err) = &stop_error {
        for result in results
            .iter_mut()
            .filter(|result| result.operation == "upsert" && result.result != "idempotent")
        {
            record_runtime(result, Err(err.clone()));
        }
    } else {
        let schema_result =
            storage_bridge::hydrate_schemas_from_storage(&state.storage).map(|_| ());
        let udf_result = if let Some(result) = results.iter().find(|result| {
            result.operation == "upsert" && result.kind == "udfs" && result.result != "idempotent"
        }) {
            Some(apply_runtime_resource(state, result, &schema_result).await)
        } else {
            None
        };
        for result in results.iter_mut().filter(|result| {
            result.operation == "upsert"
                && result.result != "idempotent"
                && result.kind != "pipelines"
        }) {
            let outcome = if let Some(error) =
                protected.get(&format!("{}/{}", result.kind, result.identity))
            {
                Err(error.clone())
            } else if result.kind == "udfs" {
                udf_result
                    .clone()
                    .unwrap_or_else(|| Err("UDF installation not attempted".to_string()))
            } else {
                apply_runtime_resource(state, result, &schema_result).await
            };
            record_runtime(result, outcome);
        }
        // Retained sources can require a runtime rebind without a metadata update.
        for name in &sources {
            if pending.streams.iter().any(|stream| &stream.name == name)
                || pending
                    .tables
                    .iter()
                    .any(|table| &table.definition.name == name)
            {
                continue;
            }
            let (kind, revision) = match state.storage.get_stream(name) {
                Ok(Some(stored)) => ("streams", stored.revision),
                Ok(None) => match state.storage.get_table(name) {
                    Ok(Some(stored)) => ("tables", stored.revision),
                    _ => continue,
                },
                Err(err) => {
                    stop_error = Some(err.to_string());
                    continue;
                }
            };
            let mut result = ResourceApplyResult {
                kind: kind.to_string(),
                identity: name.clone(),
                incoming_revision: revision,
                current_revision: Some(revision),
                operation: "upsert",
                result: "rebound",
                runtime_applied: None,
                error: None,
            };
            let outcome = if let Some(error) = protected.get(&format!("{kind}/{name}")) {
                Err(error.clone())
            } else {
                apply_runtime_resource(state, &result, &schema_result).await
            };
            record_runtime(&mut result, outcome);
            affected_resources.push(result);
        }
    }

    read_runtime_results(state, results);
    for id in pipeline_ids {
        let stored = match state.storage.get_pipeline(&id) {
            Ok(Some(stored)) => stored,
            _ => continue,
        };
        let mut result = ResourceApplyResult {
            kind: "pipelines".to_string(),
            identity: id.clone(),
            incoming_revision: stored.revision,
            current_revision: Some(stored.revision),
            operation: "upsert",
            result: "rebound",
            runtime_applied: None,
            error: None,
        };
        let outcome = if let Some(err) = &stop_error {
            Err(err.clone())
        } else {
            match runtime_dependency_error(state, &stored, results, &affected_resources) {
                Ok(Some(error)) => Err(error),
                Ok(None) => install_pipeline(state, &stored).await,
                Err(error) => Err(error),
            }
        };
        if let Err(err) = &outcome {
            for (_, instance) in state.instances.instances_snapshot() {
                let _ = instance.mark_pipeline_failed(&id, "apply", err.clone());
            }
        }
        record_runtime(&mut result, outcome);
        pipeline_results.insert(id, result);
    }
    for result in results
        .iter_mut()
        .filter(|result| result.kind == "pipelines")
    {
        if let Some(runtime) = pipeline_results.remove(&result.identity) {
            result.runtime_applied = runtime.runtime_applied;
            result.error = runtime.error;
        }
    }
    affected_resources.extend(pipeline_results.into_values());
    affected_resources
}

fn runtime_dependency_error(
    state: &AppState,
    stored: &storage::StoredPipeline,
    results: &[ResourceApplyResult],
    affected: &[ResourceApplyResult],
) -> Result<Option<String>, String> {
    let request = storage_bridge::pipeline_request_from_stored(stored)?;
    let mut dependencies = BTreeSet::new();
    for name in crate::pipeline::referenced_streams_from_pipeline_sql(
        &request,
        state.instances.default_instance().as_ref(),
    )? {
        if let Some(stream) = state
            .storage
            .get_stream(&name)
            .map_err(|err| err.to_string())?
        {
            dependencies.insert(format!("streams/{name}"));
            let request = storage_bridge::stream_request_from_stored(&stream)?;
            if let Some(schema) = &request.schema.r#ref {
                dependencies.insert(format!("schemas/{schema}"));
            }
            let props = request.props.to_value();
            if let Some(key) = props
                .get("connector_key")
                .and_then(serde_json::Value::as_str)
            {
                dependencies.insert(format!("shared_mqtt_clients/{key}"));
            }
            if request.stream_type.eq_ignore_ascii_case("memory")
                && let Some(topic) = props.get("topic").and_then(serde_json::Value::as_str)
            {
                dependencies.insert(format!("memory_topics/{topic}"));
            }
        } else if let Some(table) = state
            .storage
            .get_table(&name)
            .map_err(|err| err.to_string())?
        {
            dependencies.insert(format!("tables/{name}"));
            let request = storage_bridge::table_request_from_stored(&table)?;
            if let Some(schema) = &request.schema.r#ref {
                dependencies.insert(format!("schemas/{schema}"));
            }
        }
    }
    for sink in &request.sinks {
        let props = sink.props.to_value();
        if let Some(key) = props
            .get("connector_key")
            .and_then(serde_json::Value::as_str)
        {
            dependencies.insert(format!("shared_mqtt_clients/{key}"));
        }
        if sink.sink_type.eq_ignore_ascii_case("memory")
            && let Some(topic) = props.get("topic").and_then(serde_json::Value::as_str)
        {
            dependencies.insert(format!("memory_topics/{topic}"));
        }
    }
    for result in results.iter().chain(affected) {
        if result.runtime_applied == Some(false)
            && (result.kind == "udfs"
                || dependencies.contains(&format!("{}/{}", result.kind, result.identity)))
        {
            return Ok(Some(format!(
                "runtime dependency {}/{} is unavailable: {}",
                result.kind,
                result.identity,
                result.error.as_deref().unwrap_or("not applied")
            )));
        }
    }
    let mut observed = Vec::new();
    for dependency in dependencies {
        let (kind, identity) = dependency
            .split_once('/')
            .ok_or("invalid runtime dependency")?;
        let revision = match kind {
            "streams" => state
                .storage
                .get_stream(identity)
                .map(|stored| stored.map(|stored| stored.revision)),
            "schemas" => state
                .storage
                .get_schema(identity)
                .map(|stored| stored.map(|stored| stored.revision)),
            "shared_mqtt_clients" => state
                .storage
                .get_mqtt_config(identity)
                .map(|stored| stored.map(|stored| stored.revision)),
            "memory_topics" => state
                .storage
                .get_memory_topic(identity)
                .map(|stored| stored.map(|stored| stored.revision)),
            _ => continue,
        }
        .map_err(|err| err.to_string())?
        .ok_or_else(|| format!("missing committed dependency {dependency}"))?;
        append_related(&mut observed, &[], kind, identity, revision);
    }
    read_runtime_results(state, &mut observed);
    if let Some(result) = observed
        .iter()
        .find(|result| result.runtime_applied == Some(false) || result.error.is_some())
    {
        return Ok(Some(format!(
            "runtime dependency {}/{} is not at its committed configuration",
            result.kind, result.identity
        )));
    }
    Ok(None)
}

async fn apply_runtime_resource(
    state: &AppState,
    result: &ResourceApplyResult,
    schema_result: &Result<(), String>,
) -> Result<(), String> {
    match result.kind.as_str() {
        "schemas" => {
            schema_result.clone()?;
            if crate::stream::named_schema_store()
                .get_resolved(&result.identity)
                .is_some_and(|schema| schema.schema_version == Some(result.incoming_revision))
            {
                Ok(())
            } else {
                Err("committed schema was not hydrated".to_string())
            }
        }
        "memory_topics" => {
            let topic = state
                .storage
                .get_memory_topic(&result.identity)
                .map_err(|err| err.to_string())?
                .ok_or("missing committed topic")?;
            for (_, instance) in state.instances.instances_snapshot() {
                instance
                    .declare_memory_topic(&topic.topic, memory_kind(&topic.kind), topic.capacity)
                    .map_err(|err| err.to_string())?;
            }
            Ok(())
        }
        "shared_mqtt_clients" => {
            let stored = state
                .storage
                .get_mqtt_config(&result.identity)
                .map_err(|err| err.to_string())?
                .ok_or("missing committed MQTT client")?;
            for (_, instance) in state.instances.instances_snapshot() {
                if instance.get_shared_mqtt_client(&result.identity).is_some() {
                    instance
                        .drop_shared_mqtt_client(&result.identity)
                        .map_err(|err| err.to_string())?;
                }
                instance
                    .create_shared_mqtt_client(storage_bridge::mqtt_config_from_stored(&stored))
                    .await
                    .map_err(|err| err.to_string())?;
            }
            Ok(())
        }
        "streams" => {
            let refresh = PreparedStreamRuntimeRefresh::from_storage(state, &result.identity)?;
            if let Some(reference) = &refresh.schema_reference {
                require_current_schema(state, reference)?;
            }
            if let Some(key) = &refresh.mqtt_reference
                && !mqtt_runtime_matches(state, key)?
            {
                return Err(format!(
                    "MQTT client {key} is not at its committed configuration"
                ));
            }
            refresh.apply().await
        }
        "tables" => {
            let stored = state
                .storage
                .get_table(&result.identity)
                .map_err(|err| err.to_string())?
                .ok_or("missing committed table")?;
            let request = storage_bridge::table_request_from_stored(&stored)?;
            if let Some(reference) = &request.schema.r#ref {
                require_current_schema(state, reference)?;
            }
            crate::table::refresh_table_runtime(state, &result.identity).await
        }
        "udfs" => {
            #[cfg(feature = "wasm_udf")]
            {
                let resources = crate::export::build_export_resources(&state.storage)?;
                for (_, instance) in state.instances.instances_snapshot() {
                    load_udfs(
                        &instance,
                        &resources.udfs,
                        &state.storage.wasm_files_dir(),
                        &state.storage.wasm_files_dir(),
                    )?;
                }
                Ok(())
            }
            #[cfg(not(feature = "wasm_udf"))]
            {
                Err("WASM UDF runtime is disabled in this build".to_string())
            }
        }
        _ => Err("unsupported runtime resource".to_string()),
    }
}

fn require_current_schema(state: &AppState, reference: &str) -> Result<(), String> {
    let stored = state
        .storage
        .get_schema(reference)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| format!("missing committed schema {reference}"))?;
    if crate::stream::named_schema_store()
        .get_resolved(reference)
        .is_some_and(|schema| schema.schema_version == Some(stored.revision))
    {
        Ok(())
    } else {
        Err(format!(
            "schema {reference} revision {} is not installed in runtime",
            stored.revision
        ))
    }
}

pub(crate) async fn install_pipeline(
    state: &AppState,
    stored: &storage::StoredPipeline,
) -> Result<(), String> {
    let request = storage_bridge::pipeline_request_from_stored(stored)?;
    let instance = state
        .local_instance(
            request
                .flow_instance_id
                .as_deref()
                .unwrap_or(crate::DEFAULT_FLOW_INSTANCE_ID),
        )
        .ok_or("flow instance is unavailable")?;
    let definition = storage_bridge::pipeline_definition_from_stored(
        stored,
        instance.encoder_registry().as_ref(),
        &instance,
    )?;
    instance
        .create_pipeline(flow::CreatePipelineRequest::new(definition))
        .map_err(|err| err.to_string())?;
    let running = state
        .storage
        .get_pipeline_run_state(&stored.id)
        .map_err(|err| err.to_string())?
        .is_some_and(|state| state.desired_state == StoredPipelineDesiredState::Running);
    if running && request.options.schedule.is_none() {
        instance
            .start_pipeline(&stored.id)
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

pub(crate) async fn ensure_pipeline_installed(state: &AppState, id: &str) -> Result<(), String> {
    let stored = state
        .storage
        .get_pipeline(id)
        .map_err(|err| err.to_string())?
        .ok_or("pipeline metadata is missing")?;
    let request = storage_bridge::pipeline_request_from_stored(&stored)?;
    let instance = state
        .local_instance(
            request
                .flow_instance_id
                .as_deref()
                .unwrap_or(crate::DEFAULT_FLOW_INSTANCE_ID),
        )
        .ok_or("flow instance is unavailable")?;
    let current = instance
        .list_pipelines()
        .into_iter()
        .find(|pipeline| pipeline.definition.id() == id);
    let needs_install =
        current.is_none_or(|pipeline| pipeline.definition.revision() != stored.revision);
    if needs_install {
        for (_, instance) in state.instances.instances_snapshot() {
            match instance.delete_pipeline(id).await {
                Ok(_) | Err(flow::pipeline::PipelineError::NotFound(_)) => {}
                Err(err) => return Err(err.to_string()),
            }
        }
        install_pipeline(state, &stored).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    fn state(path: &Path) -> AppState {
        AppState::new(
            crate::new_default_flow_instance(),
            storage::StorageManager::new(path).unwrap(),
            vec![crate::FlowInstanceSpec {
                id: "default".to_string(),
                ..Default::default()
            }],
            0,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn revisioned_delete_is_atomic_replayable_and_blocks_old_recreation() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        assert_eq!(post(&state, &bundle()).await.0, StatusCode::OK);
        state
            .storage
            .checkpoint_storage()
            .unwrap()
            .commit_record("default", "apply_pipeline", 1, &[1])
            .unwrap();
        let deletion = json!({"format_version":1,"bundle_version":"delete_batch","deletions":{
            "memory_topics":[{"topic":"apply_feed","revision":2}],
            "streams":[{"name":"apply_stream","revision":2}],
            "pipelines":[{"id":"apply_pipeline","revision":2}]
        }});
        let (status, response) = post(&state, &deletion).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response["applied_to_runtime"], true);
        assert_eq!(state.storage.list_tombstones().unwrap().len(), 3);
        assert!(
            state
                .instances
                .default_instance()
                .list_pipelines()
                .is_empty()
        );
        assert!(
            state
                .instances
                .default_instance()
                .list_streams()
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            state
                .instances
                .default_instance()
                .memory_topic_kind("apply_feed")
                .is_none()
        );
        assert!(
            state
                .storage
                .checkpoint_storage()
                .unwrap()
                .load_latest_record("default", "apply_pipeline")
                .unwrap()
                .is_none()
        );
        let (status, replay) = post_with_query(&state, &deletion, "?cascade=true").await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["applied_to_runtime"], true);
        assert_eq!(post(&state, &bundle()).await.0, StatusCode::CONFLICT);
        let mut recreate = bundle();
        for kind in ["memory_topics", "streams", "pipelines"] {
            recreate["resources"][kind][0]["revision"] = json!(3);
        }
        assert_eq!(post(&state, &recreate).await.0, StatusCode::OK);
        assert!(state.storage.list_tombstones().unwrap().is_empty());
        assert_eq!(post(&state, &deletion).await.0, StatusCode::CONFLICT);
        assert_eq!(state.instances.default_instance().list_pipelines().len(), 1);
    }

    #[tokio::test]
    async fn revisioned_delete_checks_final_references_and_stops_direct_updates_first() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        assert_eq!(post(&state, &bundle()).await.0, StatusCode::OK);
        let mut deletion = json!({"format_version":1,"bundle_version":"delete_referenced","deletions":{"streams":[{"name":"apply_stream","revision":2}]}});
        let (status, rejected) = post(&state, &deletion).await;
        assert_eq!(status, StatusCode::CONFLICT, "{rejected}");
        assert_eq!(rejected["error"]["code"], "resource_referenced");
        assert!(state.storage.list_tombstones().unwrap().is_empty());
        let mut resources = bundle()["resources"].clone();
        resources["memory_topics"] = json!([]);
        resources["streams"][0]["name"] = json!("replacement_stream");
        resources["pipelines"][0]["revision"] = json!(2);
        resources["pipelines"][0]["sql"] = json!("SELECT value FROM replacement_stream");
        deletion["resources"] = resources;
        let (status, applied) = post(&state, &deletion).await;
        assert_eq!(status, StatusCode::OK, "{applied}");
        assert_eq!(applied["applied_to_runtime"], true);
        assert!(state.storage.get_stream("apply_stream").unwrap().is_none());
        assert_eq!(
            state.instances.default_instance().list_pipelines()[0].streams,
            vec!["replacement_stream"]
        );
    }

    #[tokio::test]
    async fn revisioned_single_delete_and_unversioned_delete_have_explicit_watermarks() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        for uri in ["/streams/missing?revision=2", "/streams/missing?revision=2"] {
            let response = crate::build_app(state.clone())
                .oneshot(
                    Request::builder()
                        .method("DELETE")
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        let mut stream = bundle()["resources"]["streams"][0].clone();
        stream["name"] = json!("missing");
        stream["type"] = json!("mock");
        stream["props"] = json!({});
        stream["decoder"] = json!({"type":"json","props":{}});
        let response = crate::build_app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&stream).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let response = crate::build_app(state.clone())
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/streams/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            state
                .storage
                .get_tombstone(storage::ResourceKind::Streams, "missing")
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        for revision in [0, crate::revision::MAX_RESOURCE_REVISION + 1] {
            let response = crate::build_app(state.clone())
                .oneshot(
                    Request::builder()
                        .method("DELETE")
                        .uri(format!("/streams/missing?revision={revision}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn revisioned_delete_reports_runtime_failure_and_replay_retries_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        assert_eq!(post(&state, &bundle()).await.0, StatusCode::OK);
        // Model a retained runtime consumer after its metadata was removed.
        state.storage.delete_pipeline("apply_pipeline").unwrap();
        let deletion = json!({"format_version":1,"bundle_version":"runtime_cleanup","deletions":{"streams":[{"name":"apply_stream","revision":2}]}});
        let (status, result) = post(&state, &deletion).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["metadata_committed"], true);
        assert_eq!(result["applied_to_runtime"], false);
        assert!(
            state
                .storage
                .get_tombstone(storage::ResourceKind::Streams, "apply_stream")
                .unwrap()
                .is_some()
        );
        state
            .instances
            .default_instance()
            .delete_pipeline("apply_pipeline")
            .await
            .unwrap();
        let (status, replay) = post(&state, &deletion).await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["applied_to_runtime"], true);
    }

    #[test]
    fn revisioned_manifest_rejects_null_and_duplicate_operation_identities() {
        for value in [
            json!({"format_version":1,"bundle_version":"empty","deletions":null}),
            json!({"format_version":1,"bundle_version":"empty","deletions":{"streams":null}}),
            json!({"format_version":1,"bundle_version":"empty","resources":null}),
        ] {
            assert!(serde_json::from_value::<ResourceManifestV1>(value).is_err());
        }
        let mut manifest: ResourceManifestV1 = serde_json::from_value(bundle()).unwrap();
        manifest
            .deletions
            .streams
            .push(crate::export::NameDeletion {
                name: "apply_stream".into(),
                revision: 2,
            });
        assert!(
            manifest
                .deletions
                .validate(&manifest.resources)
                .unwrap_err()
                .contains("duplicate")
        );
    }

    fn bundle() -> Value {
        json!({"format_version": 1, "bundle_version": "test_deployment", "resources": {
            "memory_topics": [{"topic": "apply_feed", "revision": 1, "kind": "collection", "capacity": 16}],
            "shared_mqtt_clients": [], "schemas": [], "tables": [], "udfs": [],
            "streams": [{"name": "apply_stream", "revision": 1, "type": "memory",
                "schema": {"type": "json", "props": {"columns": [{"name": "value", "data_type": "int64"}]}},
                "props": {"topic": "apply_feed"}, "decoder": {"type": "none"}}],
            "pipelines": [{"id": "apply_pipeline", "revision": 1, "sql": "SELECT value FROM apply_stream", "run_state": "Stopped",
                "sinks": [{"type": "nop", "props": {"log": false}, "encoder": {"type": "json", "props": {}}}]}]
        }})
    }

    async fn post(state: &AppState, value: &Value) -> (StatusCode, Value) {
        post_with_query(state, value, "").await
    }

    async fn post_with_query(state: &AppState, value: &Value, query: &str) -> (StatusCode, Value) {
        let response = crate::build_app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/storage/apply{query}"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(value).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn lifecycle(state: &AppState, action: &str) -> StatusCode {
        crate::build_app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/pipelines/apply_pipeline/{action}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    async fn put_schema(state: &AppState, schema: &Value, cascade: bool) -> (StatusCode, Value) {
        let response = crate::build_app(state.clone())
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/schemas/{}?cascade={cascade}",
                        schema["name"].as_str().unwrap()
                    ))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(schema).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8(bytes.to_vec()).unwrap()));
        (status, body)
    }

    async fn stream_schema_status(state: &AppState, name: &str) -> Value {
        let response = crate::build_app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("/streams/describe/{name}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        body["schema_status"].clone()
    }

    #[tokio::test]
    async fn shared_schema_status_reports_bound_versions_and_missing_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut initial = bundle();
        initial["resources"]["memory_topics"] = json!([]);
        initial["resources"]["schemas"] = json!([{"name":"shared_status_schema", "revision":1, "type":"json", "props":{"columns":[{"name":"value", "data_type":"int64"}]}}]);
        initial["resources"]["streams"][0]["schema"] = json!({"ref":"shared_status_schema"});
        initial["resources"]["streams"][0]["type"] = json!("mock");
        initial["resources"]["streams"][0]["props"] = json!({});
        initial["resources"]["streams"][0]["decoder"] = json!({"type":"json"});
        initial["resources"]["streams"][0]["shared"] = json!(true);
        assert_eq!(post(&state, &initial).await.0, StatusCode::OK);
        let mut schema = initial["resources"]["schemas"][0].clone();
        schema["revision"] = json!(2);
        assert_eq!(put_schema(&state, &schema, false).await.0, StatusCode::OK);
        let status = stream_schema_status(&state, "apply_stream").await;
        assert_eq!(status["stored_schema_revision"], 2);
        assert_eq!(status["instances"][0]["runtime_schema_revision"], 1);
        let instance = state.instances.default_instance();
        let old = instance.stream_definition("apply_stream").unwrap();
        assert_eq!(
            instance
                .get_stream("apply_stream")
                .await
                .unwrap()
                .shared_info
                .unwrap()
                .schema_version,
            Some(1)
        );
        instance.delete_pipeline("apply_pipeline").await.unwrap();
        instance.delete_stream("apply_stream").await.unwrap();
        let status = stream_schema_status(&state, "apply_stream").await;
        assert_eq!(
            status["instances"][0]["catalog_schema_revision"],
            Value::Null
        );
        assert_eq!(
            status["instances"][0]["runtime_schema_revision"],
            Value::Null
        );
        // A catalog definition alone is not evidence of a shared runtime binding.
        instance
            .create_stream(old.as_ref().clone(), false)
            .await
            .unwrap();
        let status = stream_schema_status(&state, "apply_stream").await;
        assert_eq!(status["instances"][0]["catalog_schema_revision"], 1);
        assert_eq!(
            status["instances"][0]["runtime_schema_revision"],
            Value::Null
        );
        drop(instance);
        drop(state);
        let restarted = self::state(dir.path());
        restarted.bootstrap_from_storage().await.unwrap();
        let status = stream_schema_status(&restarted, "apply_stream").await;
        assert_eq!(status["stored_schema_revision"], 2);
        assert_eq!(status["instances"][0]["runtime_schema_revision"], 2);
        schema["revision"] = json!(3);
        assert_eq!(
            put_schema(&restarted, &schema, true).await.0,
            StatusCode::OK
        );
        let status = stream_schema_status(&restarted, "apply_stream").await;
        assert_eq!(status["stored_schema_revision"], 3);
        assert_eq!(status["instances"][0]["runtime_schema_revision"], 3);
        crate::named_schema_store().remove("shared_status_schema");
    }

    #[tokio::test]
    async fn schema_and_apply_preserve_dependants_until_cascade_is_requested() {
        for api in ["schema", "apply"] {
            let dir = tempfile::tempdir().unwrap();
            let state = state(dir.path());
            let schema_name = format!("noncascade_{api}_schema");
            let mut initial = bundle();
            initial["resources"]["schemas"] = json!([{"name":schema_name, "revision":1, "type":"json", "props":{"columns":[{"name":"value", "data_type":"int64"}]}}]);
            initial["resources"]["streams"][0]["schema"] = json!({"ref":schema_name});
            initial["resources"]["tables"] = json!([{"name":"apply_table", "revision":1, "type":"memory_mock", "schema":{"ref":schema_name}, "props":{"rows":[{"value":1}]}}]);
            initial["resources"]["pipelines"][0]["run_state"] = json!("Running");
            assert_eq!(post(&state, &initial).await.0, StatusCode::OK);
            let instance = state.instances.default_instance();
            let old_stream = instance.stream_definition("apply_stream").unwrap();
            let old_table = instance.get_table("apply_table").await.unwrap().unwrap();
            let old_pipeline = instance.list_pipelines()[0].definition.clone();
            let mut update = initial.clone();
            for kind in ["memory_topics", "streams", "tables", "pipelines"] {
                update["resources"][kind] = json!([]);
            }
            update["resources"]["schemas"][0]["revision"] = json!(2);
            update["resources"]["schemas"][0]["props"]["columns"][0]["data_type"] =
                json!("float64");
            let stream_permit = state.try_acquire_stream_op("apply_stream").await.unwrap();
            let pipeline_permit = state
                .try_acquire_pipeline_op("apply_pipeline")
                .await
                .unwrap();
            let (status, result) = if api == "schema" {
                put_schema(&state, &update["resources"]["schemas"][0], false).await
            } else {
                post(&state, &update).await
            };
            assert_eq!(status, StatusCode::OK, "{result}");
            if api == "apply" {
                assert_eq!(result["applied_to_runtime"], true, "{result}");
                assert_eq!(result["affected_resources"], json!([]));
            } else {
                assert_eq!(result["stream_refresh"], json!([]));
                assert_eq!(result["table_refresh"], json!([]));
                assert_eq!(
                    result["dependent_refresh"],
                    json!({"status":"skipped", "reason":"cascade_disabled"})
                );
            }
            drop(stream_permit);
            drop(pipeline_permit);
            assert_eq!(
                state
                    .storage
                    .get_schema(&schema_name)
                    .unwrap()
                    .unwrap()
                    .revision,
                2
            );
            assert!(Arc::ptr_eq(
                &old_stream,
                &instance.stream_definition("apply_stream").unwrap()
            ));
            assert!(Arc::ptr_eq(
                &old_table,
                &instance.get_table("apply_table").await.unwrap().unwrap()
            ));
            assert!(Arc::ptr_eq(
                &old_pipeline,
                &instance.list_pipelines()[0].definition
            ));
            assert_eq!(
                instance.list_pipelines()[0].status,
                flow::pipeline::PipelineStatus::Running
            );

            let mut fresh = initial["resources"]["streams"][0].clone();
            fresh["name"] = json!("fresh_stream");
            let response = crate::stream::create_stream_handler(
                State(state.clone()),
                Json(serde_json::from_value(fresh).unwrap()),
            )
            .await
            .into_response();
            assert_eq!(response.status(), StatusCode::CREATED);
            let fresh = instance.stream_definition("fresh_stream").unwrap();
            assert_eq!(fresh.schema_version(), Some(2));
            assert!(matches!(
                fresh.schema().column_schemas()[0].data_type,
                flow::ConcreteDatatype::Float64(_)
            ));

            let old_status = stream_schema_status(&state, "apply_stream").await;
            assert_eq!(old_status["stored_schema_revision"], 2);
            assert_eq!(old_status["instances"][0]["runtime_schema_revision"], 1);
            let fresh_status = stream_schema_status(&state, "fresh_stream").await;
            assert_eq!(fresh_status["stored_schema_revision"], 2);
            assert_eq!(fresh_status["instances"][0]["runtime_schema_revision"], 2);

            // A same-revision replay does not turn a non-cascading update into a cascade.
            let (status, result) = if api == "schema" {
                put_schema(&state, &update["resources"]["schemas"][0], true).await
            } else {
                post_with_query(&state, &update, "?cascade=true").await
            };
            assert_eq!(status, StatusCode::OK, "{result}");
            assert!(Arc::ptr_eq(
                &old_stream,
                &instance.stream_definition("apply_stream").unwrap()
            ));
            assert!(Arc::ptr_eq(
                &old_pipeline,
                &instance.list_pipelines()[0].definition
            ));

            if api == "schema" {
                assert_eq!(
                    result["dependent_refresh"],
                    json!({"status":"skipped", "reason":"same_revision"})
                );
            }
            update["resources"]["schemas"][0]["revision"] = json!(3);
            let (status, result) = if api == "schema" {
                put_schema(&state, &update["resources"]["schemas"][0], true).await
            } else {
                post_with_query(&state, &update, "?cascade=true").await
            };
            assert_eq!(status, StatusCode::OK, "{result}");
            assert_eq!(
                instance
                    .stream_definition("apply_stream")
                    .unwrap()
                    .schema_version(),
                Some(3)
            );
            assert!(!Arc::ptr_eq(
                &old_table,
                &instance.get_table("apply_table").await.unwrap().unwrap()
            ));
            assert!(!Arc::ptr_eq(
                &old_pipeline,
                &instance.list_pipelines()[0].definition
            ));
            assert_eq!(
                instance.list_pipelines()[0].status,
                flow::pipeline::PipelineStatus::Running
            );
            let status = stream_schema_status(&state, "apply_stream").await;
            assert_eq!(status["stored_schema_revision"], 3);
            assert_eq!(status["instances"][0]["runtime_schema_revision"], 3);
            if api == "schema" {
                assert_eq!(
                    result["dependent_refresh"],
                    json!({"status":"requested", "reason":null})
                );
            }
            assert_eq!(lifecycle(&state, "stop").await, StatusCode::OK);
            crate::named_schema_store().remove(&schema_name);
        }
    }

    #[tokio::test]
    async fn schema_cascade_conflicts_with_stream_switching_to_its_reference() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut initial = bundle();
        initial["resources"]["schemas"] = json!([{"name":"reference_race_schema", "revision":1, "type":"json", "props":{"columns":[{"name":"value", "data_type":"int64"}]}}]);
        initial["resources"]["pipelines"][0]["run_state"] = json!("Running");
        assert_eq!(post(&state, &initial).await.0, StatusCode::OK);
        let mut stream = initial["resources"]["streams"][0].clone();
        stream["revision"] = json!(2);
        stream["schema"] = json!({"ref":"reference_race_schema"});
        let mut switch = Box::pin(crate::stream::upsert_stream_handler(
            State(state.clone()),
            axum::extract::Path("apply_stream".into()),
            Query(crate::stream::UpsertStreamQuery { cascade: true }),
            Json(serde_json::from_value(stream.clone()).unwrap()),
        ));
        std::future::poll_fn(|cx| {
            assert!(switch.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let mut schema = initial["resources"]["schemas"][0].clone();
        schema["revision"] = json!(2);
        schema["props"]["columns"][0]["data_type"] = json!("float64");
        assert_eq!(
            put_schema(&state, &schema, true).await.0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            state
                .storage
                .get_schema("reference_race_schema")
                .unwrap()
                .unwrap()
                .revision,
            1
        );
        assert_eq!(switch.await.into_response().status(), StatusCode::OK);
        assert_eq!(put_schema(&state, &schema, true).await.0, StatusCode::OK);
        let instance = state.instances.default_instance();
        assert_eq!(
            instance
                .stream_definition("apply_stream")
                .unwrap()
                .schema_version(),
            Some(2)
        );
        assert_eq!(
            instance.list_pipelines()[0].status,
            flow::pipeline::PipelineStatus::Running
        );

        // Exercise the opposite acquisition order: reference creation and update
        // must not pass a cascading schema operation already holding its guard.
        let guard = crate::schema::operation_guard(&state, "reference_race_schema", true)
            .await
            .unwrap();
        let mut fresh = stream.clone();
        fresh["name"] = json!("blocked_reference_stream");
        assert_eq!(
            crate::stream::create_stream_handler(
                State(state.clone()),
                Json(serde_json::from_value(fresh).unwrap())
            )
            .await
            .into_response()
            .status(),
            StatusCode::CONFLICT
        );
        stream["revision"] = json!(3);
        assert_eq!(
            crate::stream::upsert_stream_handler(
                State(state.clone()),
                axum::extract::Path("apply_stream".into()),
                Query(crate::stream::UpsertStreamQuery { cascade: false }),
                Json(serde_json::from_value(stream).unwrap()),
            )
            .await
            .into_response()
            .status(),
            StatusCode::CONFLICT
        );
        assert!(
            state
                .storage
                .get_stream("blocked_reference_stream")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            state
                .storage
                .get_stream("apply_stream")
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        drop(guard);
        assert_eq!(lifecycle(&state, "stop").await, StatusCode::OK);
        crate::named_schema_store().remove("reference_race_schema");
    }

    #[tokio::test]
    async fn dependency_parse_failure_preserves_bindings_and_conservatively_selects_absent_graphs()
    {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut initial = bundle();
        initial["resources"]["memory_topics"] = json!([]);
        initial["resources"]["streams"][0]["type"] = json!("mock");
        initial["resources"]["streams"][0]["props"] = json!({});
        initial["resources"]["streams"][0]["decoder"] = json!({"type":"json"});
        initial["resources"]["streams"][0]["shared"] = json!(true);
        let (status, result) = post(&state, &initial).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["applied_to_runtime"], true, "{result}");
        let mut previous = crate::export::build_export_resources(&state.storage).unwrap();
        previous.pipelines[0].definition.sql = "SELECT * FROM".to_string();
        assert!(
            crate::pipeline::referenced_streams_from_pipeline_sql(
                &previous.pipelines[0].definition,
                state.instances.default_instance().as_ref()
            )
            .is_err()
        );
        let mut pending = previous.clone();
        pending.pipelines.clear();
        pending.memory_topics.clear();
        let scope = runtime_scope(&state, &previous, &pending, true);
        assert!(scope.pipelines.contains("apply_pipeline"));
        pending.streams[0].name = "unrelated_stream".to_string();
        let scope = runtime_scope(&state, &previous, &pending, true);
        assert!(
            !scope.pipelines.contains("apply_pipeline"),
            "known bindings must not restart unrelated pipelines"
        );
        let mut stored = state
            .storage
            .get_pipeline("apply_pipeline")
            .unwrap()
            .unwrap();
        let mut raw: Value = serde_json::from_str(&stored.raw_json).unwrap();
        raw["sql"] = json!("SELECT * FROM");
        stored.raw_json = serde_json::to_string(&raw).unwrap();
        let mut snapshot = state.storage.export_metadata_snapshot().unwrap();
        snapshot.pipelines[0] = stored;
        state
            .storage
            .commit_metadata_snapshot(snapshot, Default::default())
            .unwrap();
        let mut update = initial.clone();
        update["resources"]["pipelines"] = json!([]);
        update["resources"]["streams"][0]["revision"] = json!(2);
        let (status, result) = post(&state, &update).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["metadata_committed"], true);
        let stream = result["resources"]
            .as_array()
            .unwrap()
            .iter()
            .find(|resource| resource["kind"] == "streams")
            .unwrap();
        assert_eq!(
            stream["runtime_applied"], false,
            "retained shared consumer must remain protected"
        );
        assert_eq!(
            state
                .instances
                .default_instance()
                .stream_definition("apply_stream")
                .unwrap()
                .revision(),
            Some(1)
        );
        assert_eq!(
            state
                .storage
                .get_stream("apply_stream")
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        state
            .instances
            .default_instance()
            .delete_pipeline("apply_pipeline")
            .await
            .unwrap();
        let scope = runtime_scope(&state, &previous, &pending, true);
        assert!(
            scope.pipelines.contains("apply_pipeline"),
            "absent bindings must not silently discard unknown dependencies"
        );

        update["resources"]["streams"][0]["revision"] = json!(3);
        let (status, result) = post(&state, &update).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["metadata_committed"], true);
        assert_eq!(
            state
                .storage
                .get_stream("apply_stream")
                .unwrap()
                .unwrap()
                .revision,
            3
        );
    }

    #[tokio::test]
    async fn cascade_query_controls_retained_pipeline_rebuilding_without_reapplying_replay() {
        for (query, restart, run_state, expected_status) in [
            (
                "",
                false,
                "Running",
                flow::pipeline::PipelineStatus::Running,
            ),
            (
                "?cascade=false",
                false,
                "Running",
                flow::pipeline::PipelineStatus::Running,
            ),
            (
                "?cascade=true",
                true,
                "Running",
                flow::pipeline::PipelineStatus::Running,
            ),
            (
                "?cascade=true",
                true,
                "Stopped",
                flow::pipeline::PipelineStatus::Stopped,
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let state = state(dir.path());
            let mut initial = bundle();
            initial["resources"]["schemas"] = json!([{"name":"apply_restart_schema", "revision":1, "type":"json", "props":{"columns":[{"name":"value", "data_type":"int64"}]}}]);
            initial["resources"]["streams"][0]["schema"] = json!({"ref":"apply_restart_schema"});
            initial["resources"]["tables"] = json!([{"name":"apply_table", "revision":1, "type":"memory_mock", "schema":{"ref":"apply_restart_schema"}, "props":{"rows":[{"value":1}]}}]);
            initial["resources"]["pipelines"][0]["sql"] = json!(
                "SELECT apply_stream.value FROM apply_stream INNER JOIN apply_table ON apply_stream.value = apply_table.value"
            );
            initial["resources"]["pipelines"][0]["run_state"] = json!(run_state);
            assert_eq!(post(&state, &initial).await.0, StatusCode::OK);
            let instance = state.instances.default_instance();
            let before = instance.list_pipelines()[0].definition.clone();
            let old_table = instance.get_table("apply_table").await.unwrap().unwrap();
            let mut update = initial;
            update["resources"]["memory_topics"] = json!([]);
            // An explicitly submitted identical pipeline is still an idempotent declaration.
            update["resources"]["schemas"][0]["revision"] = json!(2);
            update["resources"]["streams"][0]["revision"] = json!(2);
            update["resources"]["tables"][0]["revision"] = json!(2);
            update["resources"]["tables"][0]["props"]["rows"] = json!([{"value":2}]);
            let (status, result) = post_with_query(&state, &update, query).await;
            assert_eq!(status, StatusCode::OK, "{result}");
            assert_eq!(result["metadata_committed"], true);
            let after = instance.list_pipelines()[0].clone();
            assert_eq!(after.status, expected_status);
            assert_eq!(
                Arc::ptr_eq(&before, &after.definition),
                !restart,
                "{result}"
            );
            let stream = instance.stream_definition("apply_stream").unwrap();
            assert_eq!(stream.revision(), Some(2));
            assert_eq!(stream.schema_version(), Some(2));
            assert!(!Arc::ptr_eq(
                &old_table,
                &instance.get_table("apply_table").await.unwrap().unwrap()
            ));
            let (status, replay) = post_with_query(&state, &update, "?cascade=true").await;
            assert_eq!(status, StatusCode::OK, "{replay}");
            assert!(Arc::ptr_eq(
                &after.definition,
                &instance.list_pipelines()[0].definition
            ));
            assert_eq!(instance.list_pipelines()[0].status, expected_status);
            assert_eq!(lifecycle(&state, "stop").await, StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn newer_submitted_pipeline_obeys_run_state_without_restarting_retained_pipeline() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut initial = bundle();
        initial["resources"]["pipelines"][0]["run_state"] = json!("Running");
        let mut retained = initial["resources"]["pipelines"][0].clone();
        retained["id"] = json!("retained_pipeline");
        initial["resources"]["pipelines"]
            .as_array_mut()
            .unwrap()
            .push(retained);
        assert_eq!(post(&state, &initial).await.0, StatusCode::OK);
        let instance = state.instances.default_instance();
        let before = instance
            .list_pipelines()
            .into_iter()
            .find(|pipeline| pipeline.definition.id() == "retained_pipeline")
            .unwrap();
        let mut update = initial;
        update["resources"]["memory_topics"] = json!([]);
        update["resources"]["streams"][0]["revision"] = json!(2);
        update["resources"]["pipelines"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        for (revision, run_state, expected) in [
            (2, "Stopped", flow::pipeline::PipelineStatus::Stopped),
            (3, "Running", flow::pipeline::PipelineStatus::Running),
        ] {
            update["resources"]["pipelines"][0]["revision"] = json!(revision);
            update["resources"]["pipelines"][0]["run_state"] = json!(run_state);
            let (status, result) = post_with_query(&state, &update, "?cascade=false").await;
            assert_eq!(status, StatusCode::OK, "{result}");
            let snapshots = instance.list_pipelines();
            let direct = snapshots
                .iter()
                .find(|pipeline| pipeline.definition.id() == "apply_pipeline")
                .unwrap();
            assert_eq!(direct.definition.revision(), revision);
            assert_eq!(direct.status, expected);
            let retained = snapshots
                .iter()
                .find(|pipeline| pipeline.definition.id() == "retained_pipeline")
                .unwrap();
            assert!(Arc::ptr_eq(&before.definition, &retained.definition));
            assert_eq!(retained.status, flow::pipeline::PipelineStatus::Running);
        }
        for id in ["apply_pipeline", "retained_pipeline"] {
            instance
                .stop_pipeline(
                    id,
                    flow::pipeline::PipelineStopMode::Quick,
                    std::time::Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn mqtt_update_preserves_retained_consumers_and_blocks_direct_dependencies() {
        for mode in ["sink", "source", "shared_source"] {
            let dir = tempfile::tempdir().unwrap();
            let state = state(dir.path());
            let mut initial = bundle();
            initial["resources"]["shared_mqtt_clients"] = json!([{"key":"apply_protected_mqtt", "revision":1, "broker_url":"tcp://127.0.0.1:1", "topic":"input", "client_id":format!("old_{mode}"), "qos":0, "protocol_version":"v3"}]);
            initial["resources"]["pipelines"][0]["run_state"] = json!("Running");
            if mode == "sink" {
                initial["resources"]["pipelines"][0]["sinks"] = json!([{"type":"mqtt", "props":{"topic":"output", "connector_key":"apply_protected_mqtt", "qos":0}, "encoder":{"type":"json", "props":{}}}]);
            } else {
                initial["resources"]["streams"][0]["type"] = json!("mqtt");
                initial["resources"]["streams"][0]["props"] =
                    json!({"topic":"input", "qos":0, "connector_key":"apply_protected_mqtt"});
                initial["resources"]["streams"][0]["decoder"] = json!({"type":"json", "props":{}});
                initial["resources"]["streams"][0]["shared"] = json!(mode == "shared_source");
            }
            let mut retained = initial["resources"]["pipelines"][0].clone();
            retained["id"] = json!("retained_pipeline");
            initial["resources"]["pipelines"]
                .as_array_mut()
                .unwrap()
                .push(retained);
            let (status, result) = post(&state, &initial).await;
            assert_eq!(status, StatusCode::OK, "{result}");
            assert_eq!(result["applied_to_runtime"], true, "{result}");
            let instance = state.instances.default_instance();
            let before = instance
                .list_pipelines()
                .into_iter()
                .find(|pipeline| pipeline.definition.id() == "retained_pipeline")
                .unwrap();
            let old_client = instance
                .get_shared_mqtt_client("apply_protected_mqtt")
                .unwrap();
            let old_stream = instance.stream_definition("apply_stream").unwrap();
            if mode == "shared_source" {
                let mut source_only = initial.clone();
                for kind in ["memory_topics", "shared_mqtt_clients", "pipelines"] {
                    source_only["resources"][kind] = json!([]);
                }
                source_only["resources"]["streams"][0]["revision"] = json!(2);
                let (status, result) = post(&state, &source_only).await;
                assert_eq!(status, StatusCode::OK, "{result}");
                assert_eq!(result["metadata_committed"], true);
                assert_eq!(result["resources"][0]["runtime_applied"], false, "{result}");
                assert!(Arc::ptr_eq(
                    &old_stream,
                    &instance.stream_definition("apply_stream").unwrap()
                ));
            }
            let mut update = initial;
            update["resources"]["memory_topics"] = json!([]);
            update["resources"]["shared_mqtt_clients"][0]["revision"] = json!(2);
            update["resources"]["shared_mqtt_clients"][0]["client_id"] =
                json!(format!("new_{mode}"));
            update["resources"]["streams"][0]["revision"] = json!(2);
            update["resources"]["pipelines"]
                .as_array_mut()
                .unwrap()
                .truncate(1);
            update["resources"]["pipelines"][0]["revision"] = json!(2);
            let (status, result) = post(&state, &update).await;
            assert_eq!(status, StatusCode::OK, "{result}");
            assert_eq!(result["metadata_committed"], true);
            assert_eq!(result["applied_to_runtime"], false);
            let resources = result["resources"].as_array().unwrap();
            let mqtt = resources
                .iter()
                .find(|resource| resource["kind"] == "shared_mqtt_clients")
                .unwrap();
            assert_eq!(mqtt["runtime_applied"], false, "{result}");
            assert!(
                mqtt["error"]
                    .as_str()
                    .unwrap()
                    .contains("retained_pipeline")
            );
            assert_eq!(
                resources
                    .iter()
                    .find(|resource| resource["kind"] == "pipelines")
                    .unwrap()["runtime_applied"],
                false,
                "{result}"
            );
            assert_eq!(
                instance
                    .get_shared_mqtt_client("apply_protected_mqtt")
                    .unwrap()
                    .client_id,
                old_client.client_id
            );
            if mode != "sink" {
                assert!(Arc::ptr_eq(
                    &old_stream,
                    &instance.stream_definition("apply_stream").unwrap()
                ));
                assert_eq!(
                    resources
                        .iter()
                        .find(|resource| resource["kind"] == "streams")
                        .unwrap()["runtime_applied"],
                    false,
                    "{result}"
                );
            }
            let retained = instance
                .list_pipelines()
                .into_iter()
                .find(|pipeline| pipeline.definition.id() == "retained_pipeline")
                .unwrap();
            assert!(Arc::ptr_eq(&before.definition, &retained.definition));
            assert_eq!(retained.status, flow::pipeline::PipelineStatus::Running);
            let (status, replay) = post_with_query(&state, &update, "?cascade=true").await;
            assert_eq!(status, StatusCode::OK, "{replay}");
            assert_eq!(replay["applied_to_runtime"], false);
            assert_eq!(
                instance
                    .get_shared_mqtt_client("apply_protected_mqtt")
                    .unwrap()
                    .client_id,
                old_client.client_id
            );
            let retained = instance
                .list_pipelines()
                .into_iter()
                .find(|pipeline| pipeline.definition.id() == "retained_pipeline")
                .unwrap();
            assert!(Arc::ptr_eq(&before.definition, &retained.definition));
            update["resources"]["shared_mqtt_clients"][0]["revision"] = json!(3);
            update["resources"]["streams"][0]["revision"] = json!(3);
            update["resources"]["pipelines"][0]["revision"] = json!(3);
            let (status, result) = post_with_query(&state, &update, "?cascade=true").await;
            assert_eq!(status, StatusCode::OK, "{result}");
            assert_eq!(result["applied_to_runtime"], true, "{result}");
            assert_eq!(
                instance
                    .get_shared_mqtt_client("apply_protected_mqtt")
                    .unwrap()
                    .client_id,
                format!("new_{mode}")
            );
            let retained = instance
                .list_pipelines()
                .into_iter()
                .find(|pipeline| pipeline.definition.id() == "retained_pipeline")
                .unwrap();
            assert!(!Arc::ptr_eq(&before.definition, &retained.definition));
            assert_eq!(retained.status, flow::pipeline::PipelineStatus::Running);
            for id in ["apply_pipeline", "retained_pipeline"] {
                instance
                    .stop_pipeline(
                        id,
                        flow::pipeline::PipelineStopMode::Quick,
                        std::time::Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn mqtt_protection_uses_bound_keys_after_catalog_changes_without_restart() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut initial = bundle();
        initial["resources"]["shared_mqtt_clients"] = json!([
            {"key":"bound_old", "revision":1, "broker_url":"tcp://127.0.0.1:1", "topic":"input", "client_id":"bound_old", "qos":0, "protocol_version":"v3"},
            {"key":"catalog_new", "revision":1, "broker_url":"tcp://127.0.0.1:1", "topic":"input", "client_id":"catalog_new", "qos":0, "protocol_version":"v3"}
        ]);
        initial["resources"]["streams"][0]["type"] = json!("mqtt");
        initial["resources"]["streams"][0]["props"] =
            json!({"topic":"input", "qos":0, "connector_key":"bound_old"});
        initial["resources"]["streams"][0]["decoder"] = json!({"type":"json", "props":{}});
        initial["resources"]["pipelines"][0]["run_state"] = json!("Running");
        let (status, result) = post(&state, &initial).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["applied_to_runtime"], true, "{result}");
        let instance = state.instances.default_instance();
        let before = instance.list_pipelines()[0].clone();
        assert_eq!(before.shared_mqtt_client_keys, vec!["bound_old"]);
        let mut source_update = initial.clone();
        for kind in ["memory_topics", "shared_mqtt_clients", "pipelines"] {
            source_update["resources"][kind] = json!([]);
        }
        source_update["resources"]["streams"][0]["revision"] = json!(2);
        source_update["resources"]["streams"][0]["props"]["connector_key"] = json!("catalog_new");
        let (status, result) = post(&state, &source_update).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["resources"][0]["runtime_applied"], true, "{result}");
        let stream = instance.stream_definition("apply_stream").unwrap();
        let flow::catalog::StreamProps::Mqtt(props) = stream.props() else {
            panic!("expected MQTT source");
        };
        assert_eq!(props.connector_key.as_deref(), Some("catalog_new"));
        assert_eq!(
            instance.list_pipelines()[0].shared_mqtt_client_keys,
            vec!["bound_old"]
        );
        let mut client_update = initial;
        for kind in ["memory_topics", "streams", "pipelines"] {
            client_update["resources"][kind] = json!([]);
        }
        client_update["resources"]["shared_mqtt_clients"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        client_update["resources"]["shared_mqtt_clients"][0]["revision"] = json!(2);
        client_update["resources"]["shared_mqtt_clients"][0]["client_id"] = json!("updated_old");
        let (status, result) = post(&state, &client_update).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["resources"][0]["runtime_applied"], false, "{result}");
        assert_eq!(
            instance
                .get_shared_mqtt_client("bound_old")
                .unwrap()
                .client_id,
            "bound_old"
        );
        assert!(Arc::ptr_eq(
            &before.definition,
            &instance.list_pipelines()[0].definition
        ));
        client_update["resources"]["shared_mqtt_clients"][0]["revision"] = json!(3);
        let (status, result) = post_with_query(&state, &client_update, "?cascade=true").await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["applied_to_runtime"], true, "{result}");
        let rebuilt = instance.list_pipelines()[0].clone();
        assert!(!Arc::ptr_eq(&before.definition, &rebuilt.definition));
        assert_eq!(rebuilt.shared_mqtt_client_keys, vec!["catalog_new"]);
        assert_eq!(rebuilt.status, flow::pipeline::PipelineStatus::Running);
        assert_eq!(
            instance
                .get_shared_mqtt_client("bound_old")
                .unwrap()
                .client_id,
            "updated_old"
        );
        assert_eq!(lifecycle(&state, "stop").await, StatusCode::OK);
    }

    #[tokio::test]
    async fn dependency_create_and_operational_replay_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let state1 = state(dir.path());
        let mut manifest = bundle();
        manifest["resources"]["pipelines"][0]["run_state"] = json!("Running");
        let (status, result) = post(&state1, &manifest).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["metadata_committed"], true);
        assert_eq!(result["applied_to_runtime"], true, "{result}");
        assert_eq!(lifecycle(&state1, "start").await, StatusCode::OK);
        let (status, replay) = post(&state1, &manifest).await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["applied_to_runtime"], true, "{replay}");
        assert!(
            replay["resources"]
                .as_array()
                .unwrap()
                .iter()
                .any(
                    |resource| resource["kind"] == "streams" && resource["runtime_applied"] == true
                )
        );
        assert!(
            state1
                .instances
                .default_instance()
                .list_pipelines()
                .iter()
                .any(|pipeline| pipeline.status == flow::pipeline::PipelineStatus::Running)
        );
        assert_eq!(lifecycle(&state1, "stop").await, StatusCode::OK);
        drop(state1);
        let state2 = state(dir.path());
        state2.bootstrap_from_storage().await.unwrap();
        let (status, replay) = post(&state2, &manifest).await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert!(
            replay["resources"]
                .as_array()
                .unwrap()
                .iter()
                .all(|result| result["result"] == "idempotent")
        );
        assert_eq!(
            state2
                .storage
                .get_pipeline_run_state("apply_pipeline")
                .unwrap()
                .unwrap()
                .desired_state,
            StoredPipelineDesiredState::Stopped
        );
    }

    #[tokio::test]
    async fn version_and_static_errors_reject_the_entire_batch() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let manifest = bundle();
        assert_eq!(post(&state, &manifest).await.0, StatusCode::OK);
        let before = state.storage.metadata().export_snapshot().unwrap();
        let mut conflict = manifest.clone();
        conflict["resources"]["pipelines"][0]["run_state"] = json!("Running");
        conflict["resources"]["memory_topics"][0]["revision"] = json!(2);
        assert_eq!(post(&state, &conflict).await.0, StatusCode::CONFLICT);
        assert_eq!(state.storage.metadata().export_snapshot().unwrap(), before);
        let mut invalid = manifest.clone();
        invalid["resources"]["pipelines"][0]["revision"] = json!(2);
        invalid["resources"]["pipelines"][0]["sql"] =
            json!("SELECT missing_column FROM apply_stream");
        invalid["resources"]["memory_topics"][0]["revision"] = json!(2);
        let (status, result) = post(&state, &invalid).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{result}");
        assert_eq!(state.storage.metadata().export_snapshot().unwrap(), before);
        let mut newer = manifest.clone();
        newer["resources"]["pipelines"][0]["revision"] = json!(2);
        newer["resources"]["pipelines"][0]["sql"] =
            json!("SELECT value + 1 AS incremented FROM apply_stream");
        newer["resources"]["memory_topics"] = json!([]);
        let (status, result) = post(&state, &newer).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert!(
            state
                .storage
                .get_memory_topic("apply_feed")
                .unwrap()
                .is_some()
        );
        let (status, result) = post(&state, &manifest).await;
        assert_eq!(status, StatusCode::CONFLICT, "{result}");
        assert!(
            result["error"]["message"]
                .as_str()
                .unwrap()
                .contains("older_revision")
        );

        let static_before = state.storage.metadata().export_snapshot().unwrap();
        let mut mqtt_manifest = bundle();
        mqtt_manifest["resources"]["shared_mqtt_clients"] = json!([{
            "key": "apply_mqtt", "revision": 1, "broker_url": "tcp://127.0.0.1:1883",
            "topic": "input", "client_id": "apply", "qos": 0, "protocol_version": "v3"
        }]);
        mqtt_manifest["resources"]["pipelines"][0]["sinks"] = json!([{
            "type": "mqtt", "props": {"connector_key": "apply_mqtt", "topic": "output", "qos": 0,
                "user_properties": [{"key": "deployment", "value": "test"}]},
            "encoder": {"type": "json", "props": {}}
        }]);
        let mut proposed: ResourceManifestV1 = serde_json::from_value(mqtt_manifest).unwrap();
        normalize(&mut proposed.resources).unwrap();
        let files = prepare_attachments(&state, dir.path(), &proposed.resources)
            .unwrap_or_else(|_| panic!("prepare inline attachments"));
        let error =
            validate_online_definitions(&state, &proposed.resources, &proposed.resources, &files)
                .await
                .unwrap_err();
        assert!(
            error.contains("require effective protocol_version `v5`"),
            "{error}"
        );
        proposed.resources.shared_mqtt_clients[0]
            .definition
            .protocol_version = flow::MqttProtocolVersion::V5;
        validate_online_definitions(&state, &proposed.resources, &proposed.resources, &files)
            .await
            .unwrap();
        assert!(
            state
                .instances
                .default_instance()
                .get_shared_mqtt_client("apply_mqtt")
                .is_none()
        );
        assert_eq!(
            state.storage.metadata().export_snapshot().unwrap(),
            static_before
        );
    }

    #[tokio::test]
    async fn upstream_runtime_failure_is_retried_on_restart_without_reapplying_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut initial = bundle();
        initial["resources"]["pipelines"][0]["run_state"] = json!("Running");
        assert_eq!(post(&state, &initial).await.0, StatusCode::OK);
        let mut manifest = initial;
        manifest["resources"]["streams"] = json!([]);
        manifest["resources"]["pipelines"] = json!([]);
        manifest["resources"]["memory_topics"][0]["revision"] = json!(2);
        manifest["resources"]["memory_topics"][0]["capacity"] = json!(32);
        let (status, result) = post(&state, &manifest).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["metadata_committed"], true);
        assert_eq!(result["applied_to_runtime"], false);
        assert_eq!(
            state
                .storage
                .get_memory_topic("apply_feed")
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        let (status, replay) = post(&state, &manifest).await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["resources"][0]["result"], "idempotent");
        assert_eq!(replay["resources"][0]["runtime_applied"], false);
        assert_eq!(
            state
                .instances
                .default_instance()
                .memory_topic_capacity("apply_feed"),
            Some(16)
        );
        state
            .instances
            .default_instance()
            .stop_pipeline(
                "apply_pipeline",
                flow::pipeline::PipelineStopMode::Quick,
                std::time::Duration::from_secs(5),
            )
            .await
            .unwrap();
        drop(state);
        let restarted = self::state(dir.path());
        restarted.bootstrap_from_storage().await.unwrap();
        assert_eq!(
            restarted
                .instances
                .default_instance()
                .memory_topic_capacity("apply_feed"),
            Some(32)
        );
        assert!(
            restarted
                .instances
                .default_instance()
                .list_pipelines()
                .iter()
                .any(|pipeline| pipeline.status == flow::pipeline::PipelineStatus::Running)
        );
        assert_eq!(lifecycle(&restarted, "start").await, StatusCode::OK);
        assert!(
            restarted
                .instances
                .default_instance()
                .list_pipelines()
                .iter()
                .any(|pipeline| pipeline.status == flow::pipeline::PipelineStatus::Running)
        );
    }

    #[tokio::test]
    async fn zip_and_json_share_manifest_and_accept_empty_deletions() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let manifest: ResourceManifestV1 = serde_json::from_value(bundle()).unwrap();
        let bytes = crate::export::build_zip(
            &manifest,
            &[],
            &state.storage.wasm_files_dir(),
            &state.storage.schemas_dir(),
        )
        .unwrap();
        let mut multipart = b"--apply_boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"apply.zip\"\r\nContent-Type: application/zip\r\n\r\n".to_vec();
        multipart.extend(bytes);
        multipart.extend(b"\r\n--apply_boundary--\r\n");
        let response = crate::build_app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/storage/apply?cascade=true")
                    .header(CONTENT_TYPE, "multipart/form-data; boundary=apply_boundary")
                    .body(Body::from(multipart))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(post(&state, &bundle()).await.0, StatusCode::OK);
        let mut deletion = bundle();
        deletion["deletions"] = json!({});
        assert_eq!(post(&state, &deletion).await.0, StatusCode::OK);
        let response = crate::build_app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/import")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn replay_of_interrupted_update_observes_runtime_and_restart_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut initial = bundle();
        initial["resources"]["schemas"] = json!([{"name": "apply_pending_schema", "revision": 1, "type": "json", "props": {"columns": [{"name": "value", "data_type": "int64"}]}}]);
        initial["resources"]["streams"][0]["schema"] = json!({"ref": "apply_pending_schema"});
        assert_eq!(post(&state, &initial).await.0, StatusCode::OK);
        let mut update = initial.clone();
        for kind in ["memory_topics", "streams", "pipelines"] {
            update["resources"][kind] = json!([]);
        }
        update["resources"]["schemas"][0]["revision"] = json!(2);
        let manifest: ResourceManifestV1 = serde_json::from_value(update.clone()).unwrap();
        let delta = crate::import::validate_and_build_snapshot(&manifest, None, &|id| {
            state.is_declared_instance(id)
        })
        .unwrap();
        state
            .storage
            .commit_metadata_snapshot(delta, Default::default())
            .unwrap();
        let before = state.instances.default_instance().list_pipelines()[0]
            .definition
            .clone();
        let (status, result) = post_with_query(&state, &update, "?cascade=true").await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["metadata_committed"], true);
        assert_eq!(result["applied_to_runtime"], false);
        assert!(
            result["affected_resources"]
                .as_array()
                .unwrap()
                .iter()
                .any(|resource| resource["kind"] == "streams"
                    && resource["runtime_applied"] == false)
        );
        assert!(Arc::ptr_eq(
            &before,
            &state.instances.default_instance().list_pipelines()[0].definition
        ));
        state
            .storage
            .put_pipeline_run_state(storage::StoredPipelineRunState {
                pipeline_id: "apply_pipeline".to_string(),
                desired_state: StoredPipelineDesiredState::Running,
            })
            .unwrap();
        drop(state);
        let restarted = self::state(dir.path());
        restarted.bootstrap_from_storage().await.unwrap();
        assert_eq!(
            restarted.instances.default_instance().list_pipelines()[0].status,
            flow::pipeline::PipelineStatus::Running
        );
        let (status, result) = post_with_query(&restarted, &update, "?cascade=true").await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert!(result["applied_to_runtime"].is_null());
        assert_eq!(lifecycle(&restarted, "start").await, StatusCode::OK);
        assert_eq!(lifecycle(&restarted, "stop").await, StatusCode::OK);
        let mut stream_update = initial.clone();
        for kind in ["memory_topics", "schemas", "pipelines"] {
            stream_update["resources"][kind] = json!([]);
        }
        stream_update["resources"]["streams"][0]["revision"] = json!(2);
        let request =
            serde_json::from_value(stream_update["resources"]["streams"][0].clone()).unwrap();
        let delta = storage::MetadataExportSnapshot {
            tombstones: Vec::new(),
            streams: vec![storage_bridge::stored_stream_from_request(&request).unwrap()],
            schemas: vec![],
            pipelines: vec![],
            pipeline_run_states: vec![],
            mqtt_configs: vec![],
            memory_topics: vec![],
            udfs: vec![],
            tables: vec![],
        };
        restarted
            .storage
            .commit_metadata_snapshot(delta, Default::default())
            .unwrap();
        let (status, result) = post(&restarted, &stream_update).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["resources"][0]["runtime_applied"], false, "{result}");
        assert_eq!(
            restarted
                .instances
                .default_instance()
                .stream_definition("apply_stream")
                .unwrap()
                .revision(),
            Some(1)
        );
        drop(restarted);
        let recovered = self::state(dir.path());
        recovered.bootstrap_from_storage().await.unwrap();
        let (status, result) = post(&recovered, &stream_update).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["resources"][0]["runtime_applied"], true, "{result}");
        assert_eq!(
            recovered
                .instances
                .default_instance()
                .stream_definition("apply_stream")
                .unwrap()
                .revision(),
            Some(2)
        );
        stream_update["resources"]["streams"][0]["revision"] = json!(3);
        let (status, result) = post(&recovered, &stream_update).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["resources"][0]["runtime_applied"], true, "{result}");
        assert_eq!(
            recovered
                .instances
                .default_instance()
                .stream_definition("apply_stream")
                .unwrap()
                .revision(),
            Some(3)
        );
        let instance = recovered.instances.default_instance();
        let replacement = instance
            .stream_definition("apply_stream")
            .unwrap()
            .as_ref()
            .clone()
            .with_revision(4);
        // Memory streams cannot install a shared runtime; the failed replacement
        // must not advertise its candidate revision in the catalog.
        assert!(instance.replace_stream(replacement, true).await.is_err());
        assert_eq!(
            instance
                .stream_definition("apply_stream")
                .unwrap()
                .revision(),
            Some(3)
        );
        let (status, replay) = post(&recovered, &stream_update).await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["resources"][0]["runtime_applied"], true, "{replay}");
    }

    #[tokio::test]
    async fn schema_only_replay_reports_retained_pipeline_failure_without_retry() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut initial = bundle();
        initial["resources"]["schemas"] = json!([{"name": "apply_named_schema", "revision": 1, "type": "json", "props": {"columns": [{"name": "value", "data_type": "int64"}]}}]);
        initial["resources"]["streams"][0]["schema"] = json!({"ref": "apply_named_schema"});
        let (status, result) = post(&state, &initial).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        let mut update = initial.clone();
        for kind in ["memory_topics", "streams", "pipelines"] {
            update["resources"][kind] = json!([]);
        }
        update["resources"]["schemas"][0]["revision"] = json!(2);
        update["resources"]["schemas"][0]["props"]["columns"][0]["name"] = json!("other_column");
        for _ in 0..2 {
            let (status, result) = post_with_query(&state, &update, "?cascade=true").await;
            assert_eq!(status, StatusCode::OK, "{result}");
            assert_eq!(result["metadata_committed"], true);
            assert_eq!(result["applied_to_runtime"], false);
            assert_eq!(result["resources"][0]["runtime_applied"], true);
            assert!(
                result["affected_resources"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|resource| resource["kind"] == "pipelines"
                        && resource["runtime_applied"] == false)
            );
        }
        assert_eq!(
            state
                .storage
                .get_schema("apply_named_schema")
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        assert_eq!(
            state
                .storage
                .get_pipeline("apply_pipeline")
                .unwrap()
                .unwrap()
                .revision,
            1
        );
    }

    #[tokio::test]
    async fn newer_pipeline_without_run_state_stops_and_missing_attachment_rejects_batch() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let mut manifest = bundle();
        manifest["resources"]["pipelines"][0]["run_state"] = json!("Running");
        let (status, result) = post(&state, &manifest).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        manifest["resources"]["pipelines"][0]["revision"] = json!(2);
        manifest["resources"]["pipelines"][0]
            .as_object_mut()
            .unwrap()
            .remove("run_state");
        let (status, result) = post(&state, &manifest).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(
            state
                .storage
                .get_pipeline_run_state("apply_pipeline")
                .unwrap()
                .unwrap()
                .desired_state,
            StoredPipelineDesiredState::Stopped
        );
        assert!(
            state
                .instances
                .default_instance()
                .list_pipelines()
                .iter()
                .all(|pipeline| pipeline.status == flow::pipeline::PipelineStatus::Stopped)
        );
        let before = state.storage.export_metadata_snapshot().unwrap();
        manifest["resources"]["udfs"] =
            json!([{"name": "apply_function", "revision": 1, "wasm_sha256": "a".repeat(64)}]);
        assert_eq!(post(&state, &manifest).await.0, StatusCode::BAD_REQUEST);
        assert_eq!(state.storage.export_metadata_snapshot().unwrap(), before);
    }

    #[tokio::test]
    async fn file_backed_schema_zip_and_json_reuse_validate_attachment_identity() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let files = tempfile::tempdir().unwrap();
        let source = files.path().join("schemas/proto/apply_proto_schema");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(
            source.join("simple.proto"),
            b"syntax = \"proto3\"; message Simple { int64 value = 1; }",
        )
        .unwrap();
        let mut manifest = bundle();
        for kind in ["memory_topics", "streams", "pipelines"] {
            manifest["resources"][kind] = json!([]);
        }
        manifest["resources"]["schemas"] = json!([{"name": "apply_proto_schema", "revision": 1, "type": "proto", "props": {"proto_path": "simple.proto", "message_type": "Simple"}}]);
        assert_eq!(post(&state, &manifest).await.0, StatusCode::BAD_REQUEST);
        let archive = |manifest: &Value| {
            let manifest: ResourceManifestV1 = serde_json::from_value(manifest.clone()).unwrap();
            crate::export::build_zip(
                &manifest,
                &[],
                &files.path().join("wasm_files"),
                &files.path().join("schemas"),
            )
            .unwrap()
        };
        let upload = |bytes: Vec<u8>| {
            let mut multipart = b"--schema_boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"schema.zip\"\r\n\r\n".to_vec();
            multipart.extend(bytes);
            multipart.extend(b"\r\n--schema_boundary--\r\n");
            Request::builder()
                .method("POST")
                .uri("/storage/apply")
                .header(
                    CONTENT_TYPE,
                    "multipart/form-data; boundary=schema_boundary",
                )
                .body(Body::from(multipart))
                .unwrap()
        };
        let response = crate::build_app(state.clone())
            .oneshot(upload(archive(&manifest)))
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        manifest["resources"]["schemas"][0]["revision"] = json!(2);
        let (status, result) = post(&state, &manifest).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        std::fs::write(
            source.join("simple.proto"),
            b"syntax = \"proto3\"; message Simple { string value = 1; }",
        )
        .unwrap();
        let response = crate::build_app(state.clone())
            .oneshot(upload(archive(&manifest)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            std::fs::read_to_string(
                state
                    .storage
                    .schemas_dir()
                    .join("proto/apply_proto_schema/simple.proto")
            )
            .unwrap()
            .contains("int64")
        );
    }

    #[tokio::test]
    async fn crud_and_batch_share_the_metadata_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let permit = state.try_acquire_metadata_operation(false).unwrap();
        assert_eq!(post(&state, &bundle()).await.0, StatusCode::CONFLICT);
        assert!(state.storage.list_streams().unwrap().is_empty());
        drop(permit);
        assert_eq!(post(&state, &bundle()).await.0, StatusCode::OK);
        let _exclusive = state.try_acquire_metadata_operation(true).unwrap();
        assert_eq!(lifecycle(&state, "start").await, StatusCode::CONFLICT);
        assert_eq!(
            state
                .storage
                .get_pipeline_run_state("apply_pipeline")
                .unwrap()
                .unwrap()
                .desired_state,
            StoredPipelineDesiredState::Stopped
        );
    }
}
