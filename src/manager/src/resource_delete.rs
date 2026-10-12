//! Dependency checks and cleanup shared by revisioned resource deletions.
use std::collections::{BTreeMap, BTreeSet};

use crate::export::ExportResources;
use storage::{ResourceKind, StoredTombstone};

pub(crate) fn kind_name(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::MemoryTopics => "memory_topics",
        ResourceKind::SharedMqttClients => "shared_mqtt_clients",
        ResourceKind::Schemas => "schemas",
        ResourceKind::Streams => "streams",
        ResourceKind::Pipelines => "pipelines",
        ResourceKind::Udfs => "udfs",
        ResourceKind::Tables => "tables",
        ResourceKind::Models => "models",
    }
}

/// Check only deleted identities; unrelated retained pipelines are not replanned.
pub(crate) fn blockers(
    resources: &ExportResources,
    deletions: &[StoredTombstone],
) -> Result<BTreeMap<(ResourceKind, String), String>, String> {
    if deletions.is_empty() {
        return Ok(BTreeMap::new());
    }
    let deleted = deletions
        .iter()
        .map(|v| (v.kind, v.identity.clone()))
        .collect::<BTreeSet<_>>();
    let inspect_sql = deleted.iter().any(|(kind, _)| {
        matches!(
            kind,
            ResourceKind::Streams | ResourceKind::Tables | ResourceKind::Udfs
        )
    });
    let inspect_udfs = deleted.iter().any(|(kind, _)| *kind == ResourceKind::Udfs);
    let mut blocked = BTreeMap::new();
    let mut reference = |kind, identity: &str, owner: String| {
        let identity = identity.trim();
        if deleted.contains(&(kind, identity.to_string())) {
            blocked.insert(
                (kind, identity.to_string()),
                format!(
                    "{} {identity} is still referenced by {owner}",
                    kind_name(kind)
                ),
            );
        }
    };
    for stream in &resources.streams {
        let owner = format!("stream {}", stream.name);
        if let Some(name) = &stream.schema.r#ref {
            reference(ResourceKind::Schemas, name, owner.clone());
        }
        let props = stream.props.to_value();
        if stream.stream_type.eq_ignore_ascii_case("mqtt")
            && let Some(key) = props
                .get("connector_key")
                .and_then(serde_json::Value::as_str)
        {
            reference(ResourceKind::SharedMqttClients, key, owner.clone());
        }
        if stream.stream_type.eq_ignore_ascii_case("memory")
            && let Some(topic) = props.get("topic").and_then(serde_json::Value::as_str)
        {
            reference(ResourceKind::MemoryTopics, topic, owner);
        }
    }
    for table in &resources.tables {
        if let Some(name) = &table.definition.schema.r#ref {
            reference(
                ResourceKind::Schemas,
                name,
                format!("table {}", table.definition.name),
            );
        }
    }
    for pipeline in &resources.pipelines {
        let request = &pipeline.definition;
        let owner = format!("pipeline {}", request.id);
        let statement = if inspect_sql {
            Some(parser::parse_sql(&request.sql).map_err(|err| format!("{}: {err}", request.id))?)
        } else {
            None
        };
        if let Some(statement) = &statement {
            for source in &statement.source_infos {
                reference(ResourceKind::Streams, &source.name, owner.clone());
                reference(ResourceKind::Tables, &source.name, owner.clone());
            }
        }
        for sink in &request.sinks {
            let props = sink.props.to_value();
            if sink.sink_type.eq_ignore_ascii_case("mqtt")
                && let Some(key) = props
                    .get("connector_key")
                    .and_then(serde_json::Value::as_str)
            {
                reference(ResourceKind::SharedMqttClients, key, owner.clone());
            }
            if sink.sink_type.eq_ignore_ascii_case("memory")
                && let Some(topic) = props.get("topic").and_then(serde_json::Value::as_str)
            {
                reference(ResourceKind::MemoryTopics, topic, owner.clone());
            }
        }
        if inspect_udfs && let Some(statement) = &statement {
            for name in parser::expression_extractor::referenced_functions(statement) {
                reference(
                    ResourceKind::Udfs,
                    &name.to_ascii_lowercase(),
                    owner.clone(),
                );
            }
        }
    }
    Ok(blocked)
}

pub(crate) fn remove(resources: &mut ExportResources, marker: &StoredTombstone) {
    match marker.kind {
        ResourceKind::MemoryTopics => resources
            .memory_topics
            .retain(|v| v.topic != marker.identity),
        ResourceKind::SharedMqttClients => resources
            .shared_mqtt_clients
            .retain(|v| v.definition.key != marker.identity),
        ResourceKind::Schemas => resources.schemas.retain(|v| v.name != marker.identity),
        ResourceKind::Streams => resources.streams.retain(|v| v.name != marker.identity),
        ResourceKind::Pipelines => resources
            .pipelines
            .retain(|v| v.definition.id != marker.identity),
        ResourceKind::Udfs => resources
            .udfs
            .retain(|v| !v.name.eq_ignore_ascii_case(&marker.identity)),
        ResourceKind::Tables => resources
            .tables
            .retain(|v| v.definition.name != marker.identity),
        ResourceKind::Models => resources.models.retain(|v| v.name != marker.identity),
    }
}

/// Failed dependent operations remain in the final state and protect their dependencies.
pub(crate) fn select_init_deletions(
    effective: ExportResources,
    mut pending: Vec<StoredTombstone>,
) -> Result<(Vec<StoredTombstone>, bool), String> {
    let mut had_errors = false;
    loop {
        let mut final_state = effective.clone();
        for marker in &pending {
            remove(&mut final_state, marker);
        }
        let blocked = match blockers(&final_state, &pending) {
            Ok(blocked) => blocked,
            Err(error) => {
                tracing::warn!(error = %error, "init deletion dependencies cannot be established");
                pending.retain(|marker| marker.kind == ResourceKind::Pipelines);
                return Ok((pending, true));
            }
        };
        if blocked.is_empty() {
            return Ok((pending, had_errors));
        }
        had_errors = true;
        pending.retain(|marker| {
            let Some(error) = blocked.get(&(marker.kind, marker.identity.clone())) else { return true; };
            tracing::warn!(kind = kind_name(marker.kind), identity = %marker.identity, revision = marker.revision, error = %error, "init deletion rejected");
            false
        });
        // Accepted upserts are already present in effective; rejected deletions
        // remain there and can block another deletion on the next iteration.
    }
}

use crate::pipeline::AppState;
use axum::{
    Json,
    extract::{OriginalUri, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Default, serde::Deserialize)]
pub(crate) struct DeleteQuery {
    pub revision: Option<u64>,
}

pub(crate) fn route_kind(path: &str) -> Option<ResourceKind> {
    let (prefix, identity) = path.rsplit_once('/')?;
    if identity.is_empty() {
        return None;
    }
    match prefix {
        "/pipelines" => Some(ResourceKind::Pipelines),
        "/streams" => Some(ResourceKind::Streams),
        "/tables" => Some(ResourceKind::Tables),
        "/schemas" => Some(ResourceKind::Schemas),
        "/mqtt/clients" => Some(ResourceKind::SharedMqttClients),
        "/memory/topics" => Some(ResourceKind::MemoryTopics),
        #[cfg(feature = "wasm_udf")]
        "/udfs" => Some(ResourceKind::Udfs),
        _ => None,
    }
}

/// Unversioned requests keep their existing handler contract. Versioned requests
/// use the same final-state validation and atomic metadata commit as Apply.
pub(crate) async fn handler(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(query): Query<DeleteQuery>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let Some(kind) = route_kind(uri.path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if let Some(revision) = query.revision {
        if let Err(error) = crate::revision::validate_revision(revision) {
            return (StatusCode::BAD_REQUEST, error).into_response();
        }
        let _metadata = match crate::storage_apply::acquire_exclusive_metadata(&state) {
            Ok(guard) => guard,
            Err(response) => return *response,
        };
        let _storage = match state.try_acquire_storage_operation() {
            Ok(guard) => guard,
            Err(_) => {
                return (
                    StatusCode::CONFLICT,
                    "another storage operation is in progress",
                )
                    .into_response();
            }
        };
        let work = match tempfile::Builder::new()
            .prefix(".delete.")
            .tempdir_in(state.storage.base_dir())
        {
            Ok(work) => work,
            Err(error) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
            }
        };
        let manifest = crate::export::ResourceManifestV1 {
            format_version: crate::export::RESOURCE_DIRECTORY_FORMAT_VERSION,
            bundle_version: "resource_delete".to_string(),
            resources: Default::default(),
            deletions: crate::export::ExportDeletions::from_tombstones(vec![StoredTombstone {
                kind,
                identity,
                revision,
            }]),
        };
        return match crate::storage_apply::apply_manifest(&state, manifest, work.path(), false)
            .await
        {
            Ok(result) => Json(result).into_response(),
            Err((status, code, message)) => crate::storage_apply::failure(status, code, message),
        };
    }
    match kind {
        ResourceKind::Pipelines => {
            crate::pipeline::delete_pipeline_handler(State(state), Path(identity))
                .await
                .into_response()
        }
        ResourceKind::Streams => crate::stream::delete_stream_handler(State(state), Path(identity))
            .await
            .into_response(),
        ResourceKind::Tables => crate::table::delete_table_handler(State(state), Path(identity))
            .await
            .into_response(),
        ResourceKind::Schemas => {
            crate::schema::handler::delete_schema_handler(State(state), Path(identity))
                .await
                .into_response()
        }
        ResourceKind::SharedMqttClients => {
            crate::mqtt_client::delete_shared_mqtt_client_handler(State(state), Path(identity))
                .await
                .into_response()
        }
        ResourceKind::MemoryTopics => {
            let _metadata = match crate::storage_apply::acquire_exclusive_metadata(&state) {
                Ok(guard) => guard,
                Err(response) => return *response,
            };
            delete_memory_topic(&state, &identity).await
        }
        #[cfg(feature = "wasm_udf")]
        ResourceKind::Udfs => crate::udf_handler::delete_udf_handler(State(state), Path(identity))
            .await
            .into_response(),
        #[cfg(not(feature = "wasm_udf"))]
        ResourceKind::Udfs => StatusCode::NOT_FOUND.into_response(),
        ResourceKind::Models => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_memory_topic(state: &AppState, identity: &str) -> Response {
    if let Err(error) = crate::resource_id::validate_resource_id(
        crate::resource_id::ResourceIdKind::MemoryTopic,
        identity,
    ) {
        return (StatusCode::BAD_REQUEST, error).into_response();
    }
    let marker = StoredTombstone {
        kind: ResourceKind::MemoryTopics,
        identity: identity.to_string(),
        revision: 1,
    };
    let resources = match crate::export::build_export_resources(&state.storage) {
        Ok(resources) => resources,
        Err(error) => return (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    };
    match blockers(&resources, std::slice::from_ref(&marker)) {
        Ok(blocked) if !blocked.is_empty() => {
            return (
                StatusCode::CONFLICT,
                blocked.into_values().next().unwrap_or_default(),
            )
                .into_response();
        }
        Err(error) => return (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
        _ => {}
    }
    match state.storage.get_memory_topic(identity) {
        Ok(Some(_)) => {}
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    }
    let instances = state.instances.instances_snapshot();
    for (_, instance) in &instances {
        if let Err(error) = instance.ensure_memory_topic_unused(identity) {
            return (StatusCode::CONFLICT, error.to_string()).into_response();
        }
    }
    // Direct runtime callers can acquire handles after preflight. Recheck during
    // removal and restore earlier instances if any subsequent removal fails.
    let mut removed = Vec::new();
    for (_, instance) in &instances {
        let previous = instance
            .memory_topic_kind(identity)
            .zip(instance.memory_topic_capacity(identity));
        if let Err(error) = instance.drop_memory_topic(identity) {
            return restore_memory_topics(
                identity,
                &removed,
                StatusCode::CONFLICT,
                error.to_string(),
            );
        }
        if let Some((kind, capacity)) = previous {
            removed.push((std::sync::Arc::clone(instance), kind, capacity));
        }
    }
    if let Err(error) = state.storage.delete_memory_topic(identity) {
        return restore_memory_topics(
            identity,
            &removed,
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        );
    }
    StatusCode::NO_CONTENT.into_response()
}

fn restore_memory_topics(
    identity: &str,
    removed: &[(
        std::sync::Arc<flow::FlowInstance>,
        flow::connector::MemoryTopicKind,
        usize,
    )],
    status: StatusCode,
    mut message: String,
) -> Response {
    let mut status = status;
    for (instance, kind, capacity) in removed {
        if let Err(error) = instance.declare_memory_topic(identity, *kind, *capacity) {
            message.push_str(&format!("; failed to restore runtime topic: {error}"));
            status = StatusCode::INTERNAL_SERVER_ERROR;
        }
    }
    (status, message).into_response()
}

/// Shared cleanup primitives. The caller coordinates metadata and validates
/// retained references before invoking these operations.
pub(crate) async fn cleanup(
    state: &AppState,
    kind: ResourceKind,
    identity: &str,
) -> Result<(), String> {
    for (_, instance) in state.instances.instances_snapshot() {
        match kind {
            ResourceKind::Pipelines => match instance.delete_pipeline(identity).await {
                Ok(()) | Err(flow::pipeline::PipelineError::NotFound(_)) => {}
                Err(error) => return Err(error.to_string()),
            },
            ResourceKind::Streams => match instance.delete_stream(identity).await {
                Ok(())
                | Err(flow::FlowInstanceError::Catalog(flow::catalog::CatalogError::NotFound(_))) =>
                    {}
                Err(error) => return Err(error.to_string()),
            },
            ResourceKind::Tables => match instance.delete_table(identity).await {
                Ok(())
                | Err(flow::FlowInstanceError::Catalog(
                    flow::catalog::CatalogError::TableNotFound(_),
                )) => {}
                Err(error) => return Err(error.to_string()),
            },
            ResourceKind::SharedMqttClients => match instance.drop_shared_mqtt_client(identity) {
                Ok(())
                | Err(flow::FlowInstanceError::Connector(
                    flow::connector::ConnectorError::NotFound(_),
                )) => {}
                Err(error) => return Err(error.to_string()),
            },
            ResourceKind::MemoryTopics => {
                instance
                    .drop_memory_topic(identity)
                    .map_err(|error| error.to_string())?;
            }
            // UDF DELETE keeps already bound functions until restart, matching
            // the ordinary endpoint's runtime contract.
            ResourceKind::Udfs | ResourceKind::Schemas | ResourceKind::Models => {}
        }
    }
    // Bulk deletion reclaims WASM once for the final accepted metadata state.
    if kind != ResourceKind::Udfs {
        cleanup_persisted(&state.storage, kind, identity)?;
    }
    if kind == ResourceKind::Udfs
        && state
            .instances
            .instances_snapshot()
            .iter()
            .any(|(_, instance)| instance.custom_func_registry().is_registered(identity))
    {
        return Err("UDF metadata and attachment deleted; registered runtime function is retained until restart".to_string());
    }
    if kind == ResourceKind::Schemas {
        crate::stream::named_schema_store().remove(identity);
    }
    Ok(())
}

pub(crate) fn cleanup_persisted(
    storage: &storage::StorageManager,
    kind: ResourceKind,
    identity: &str,
) -> Result<(), String> {
    match kind {
        ResourceKind::Pipelines => storage
            .checkpoint_storage()
            .and_then(|store| store.clear_pipeline_records(identity))
            .map_err(|error| error.to_string()),
        ResourceKind::Schemas => {
            for entry in
                std::fs::read_dir(storage.schemas_dir()).map_err(|error| error.to_string())?
            {
                let entry = entry.map_err(|error| error.to_string())?;
                if !entry
                    .file_type()
                    .map_err(|error| error.to_string())?
                    .is_dir()
                {
                    continue;
                }
                let path = entry.path().join(identity);
                match std::fs::remove_dir_all(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok(())
        }
        ResourceKind::Udfs => {
            let referenced = storage
                .list_udfs()
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|udf| format!("{}.wasm", udf.wasm_sha256))
                .collect::<BTreeSet<_>>();
            for entry in
                std::fs::read_dir(storage.wasm_files_dir()).map_err(|error| error.to_string())?
            {
                let entry = entry.map_err(|error| error.to_string())?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if entry
                    .file_type()
                    .map_err(|error| error.to_string())?
                    .is_file()
                    && name.ends_with(".wasm")
                    && !referenced.contains(&name)
                {
                    std::fs::remove_file(entry.path()).map_err(|error| error.to_string())?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_topic_state(temp_dir: &tempfile::TempDir, multiple: bool) -> AppState {
        let mut specs = vec![crate::FlowInstanceSpec {
            id: "default".into(),
            ..Default::default()
        }];
        if multiple {
            specs.push(crate::FlowInstanceSpec {
                id: "second".into(),
                ..Default::default()
            });
        }
        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage::StorageManager::new(temp_dir.path()).unwrap(),
            specs,
            0,
        )
        .unwrap();
        state
            .storage
            .create_memory_topic(storage::StoredMemoryTopic {
                topic: "events".into(),
                revision: 1,
                kind: storage::StoredMemoryTopicKind::Bytes,
                capacity: 16,
            })
            .unwrap();
        for (_, instance) in state.instances.instances_snapshot() {
            instance
                .declare_memory_topic("events", flow::connector::MemoryTopicKind::Bytes, 16)
                .unwrap();
        }
        state
    }

    #[tokio::test]
    async fn ordinary_memory_delete_preserves_topics_with_active_handles() {
        for subscriber in [false, true] {
            let temp_dir = tempfile::tempdir().unwrap();
            let state = memory_topic_state(&temp_dir, true);
            let instance = state.local_instance("second").unwrap();
            let publisher = if subscriber {
                None
            } else {
                Some(instance.open_memory_publisher_bytes("events").unwrap())
            };
            let receiver = if subscriber {
                Some(instance.open_memory_subscribe_bytes("events").unwrap())
            } else {
                None
            };
            let response = delete_memory_topic(&state, "events").await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            assert!(state.storage.get_memory_topic("events").unwrap().is_some());
            for (_, instance) in state.instances.instances_snapshot() {
                assert_eq!(instance.memory_topic_capacity("events"), Some(16));
            }
            drop(publisher);
            drop(receiver);
            assert_eq!(
                delete_memory_topic(&state, "events").await.status(),
                StatusCode::NO_CONTENT
            );
            assert!(state.storage.get_memory_topic("events").unwrap().is_none());
            for (_, instance) in state.instances.instances_snapshot() {
                assert_eq!(instance.memory_topic_kind("events"), None);
            }
        }
    }

    #[tokio::test]
    async fn ordinary_memory_delete_restores_earlier_instances_after_late_use() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = memory_topic_state(&temp_dir, true);
        let first = state.local_instance("default").unwrap();
        let second = state.local_instance("second").unwrap();
        first.ensure_memory_topic_unused("events").unwrap();
        second.ensure_memory_topic_unused("events").unwrap();
        first.drop_memory_topic("events").unwrap();
        // A direct runtime caller acquires a handle after the all-instance check.
        let publisher = second.open_memory_publisher_bytes("events").unwrap();
        let error = second.drop_memory_topic("events").unwrap_err();
        let response = restore_memory_topics(
            "events",
            &[(first.clone(), flow::connector::MemoryTopicKind::Bytes, 16)],
            StatusCode::CONFLICT,
            error.to_string(),
        );
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(state.storage.get_memory_topic("events").unwrap().is_some());
        assert_eq!(first.memory_topic_capacity("events"), Some(16));
        assert_eq!(second.memory_topic_capacity("events"), Some(16));
        drop(publisher);
        assert_eq!(
            delete_memory_topic(&state, "events").await.status(),
            StatusCode::NO_CONTENT
        );
    }

    #[test]
    fn deletion_dependencies_include_named_schema_connectors_sink_and_udf_ast() {
        let manifest: crate::export::ResourceManifestV1 = serde_json::from_value(serde_json::json!({
            "format_version":1,"bundle_version":"dependencies","resources":{
                "streams":[{"name":"source","revision":1,"type":"mqtt","schema":{"ref":" payload "},"props":{"connector_key":" broker "}}],
                "tables":[{"name":"history","revision":1,"type":"memory","schema":{"ref":"payload"},"props":{}}],
                "pipelines":[{"id":"consumer","revision":1,"sql":"SELECT transform(value) FROM source","sinks":[{"type":"memory","props":{"topic":"output"}}]}]
            },"deletions":{
                "schemas":[{"name":"payload","revision":2}],"shared_mqtt_clients":[{"key":"broker","revision":2}],
                "streams":[{"name":"source","revision":2}],"memory_topics":[{"topic":"output","revision":2}],
                "udfs":[{"name":"TRANSFORM","revision":2}]
            }
        })).unwrap();
        let blocked = blockers(&manifest.resources, &manifest.deletions.tombstones()).unwrap();
        assert_eq!(blocked.len(), 5);
        assert!(blocked.contains_key(&(ResourceKind::Udfs, "transform".into())));
    }
}
