use axum::{
    Json,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Serialize;
use serde_yaml::Value as YamlValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use storage::StoredModel;
use tokio::sync::TryAcquireError;

use crate::audit::ResourceMutationLog;
use crate::pipeline::AppState;
use crate::streaming_upload::{TemporaryUpload, is_zip_filename};

const MODEL_FILE: &str = "model.onnx";
const MODEL_CARD_FILE: &str = "model.yaml";
const TEST_VECTOR_DIR: &str = "test_vectors";
const MIN_TEST_VECTOR_COUNT: usize = 3;

#[derive(Serialize)]
pub struct ModelInfo {
    pub name: String,
    pub revision: u64,
    pub model_sha256: String,
    pub model_size_bytes: u64,
    pub test_vector_count: u32,
}

/// POST /models
///
/// Accepts one ZIP bundle containing a single top-level directory with
/// `model.onnx`, `model.yaml`, and at least three test-vector pairs.
pub async fn upload_model_handler(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut upload = None;
    let mut revision = 1u64;
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
        };
        match field.name() {
            Some("file") if upload.is_none() && is_zip_filename(field.file_name()) => {
                upload = match TemporaryUpload::receive(&state.storage, field, Some("zip")).await {
                    Ok(upload) => Some(upload),
                    Err(error) => return (error.status, error.message).into_response(),
                };
            }
            Some("revision") => {
                let value = match field.text().await {
                    Ok(value) => value,
                    Err(error) => {
                        return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
                    }
                };
                revision = match value.parse::<u64>() {
                    Ok(value) => match crate::revision::validate_revision(value) {
                        Ok(()) => value,
                        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
                    },
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            "field 'revision' must be a positive JSON safe integer",
                        )
                            .into_response();
                    }
                };
            }
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    "expected one ZIP field named 'file' and an optional 'revision' field",
                )
                    .into_response();
            }
        }
    }
    let Some(upload) = upload else {
        return (
            StatusCode::BAD_REQUEST,
            "field 'file' is required and must be a ZIP archive",
        )
            .into_response();
    };

    let work = match tempfile::Builder::new()
        .prefix(".model-upload.")
        .tempdir_in(state.storage.base_dir())
    {
        Ok(work) => work,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    if let Err(error) = crate::import::extract_zip_file(upload.path(), work.path()) {
        return (StatusCode::BAD_REQUEST, error).into_response();
    }
    let bundle_root = match single_bundle_root(work.path()) {
        Ok(path) => path,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let (name, model_card_yaml, model_bytes, test_vector_count) =
        match validate_bundle(&bundle_root) {
            Ok(value) => value,
            Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
        };
    let _metadata_permit = match crate::storage_apply::acquire_exclusive_metadata(&state) {
        Ok(permit) => permit,
        Err(response) => return *response,
    };
    let _model_operation_permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another storage operation is in progress".to_string(),
            )
                .into_response();
        }
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let audit = ResourceMutationLog::new("model", "upload", &name, Some(revision));

    if state.storage.get_model(&name).ok().flatten().is_some() {
        let error = format!("model '{name}' already exists; model versions are immutable");
        audit.log_failure(&error);
        return (StatusCode::CONFLICT, error).into_response();
    }

    let model_sha256 = sha256_hex(&model_bytes);
    let stored = StoredModel {
        name: name.clone(),
        revision,
        model_sha256: model_sha256.clone(),
        model_size_bytes: model_bytes.len() as u64,
        model_card_yaml,
        test_vector_count: test_vector_count as u32,
    };
    if let Err(error) =
        crate::attachments::publish_model_upload(&state.storage, stored, &bundle_root)
    {
        audit.log_failure(&error.to_string());
        return (StatusCode::INTERNAL_SERVER_ERROR, error).into_response();
    }
    audit.log_success();
    (
        StatusCode::OK,
        Json(ModelInfo {
            name,
            revision,
            model_sha256,
            model_size_bytes: model_bytes.len() as u64,
            test_vector_count: test_vector_count as u32,
        }),
    )
        .into_response()
}

/// GET /models
pub async fn list_models_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_models() {
        Ok(models) => Json(models.into_iter().map(model_info).collect::<Vec<_>>()).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// GET /models/:name
pub async fn get_model_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(error) = validate_model_name(&name) {
        return (StatusCode::BAD_REQUEST, error).into_response();
    }
    match state.storage.get_model(&name) {
        Ok(Some(model)) => Json(model_info(model)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("model '{name}' not found")).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// DELETE /models/:name
pub async fn delete_model_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(error) = validate_model_name(&name) {
        return (StatusCode::BAD_REQUEST, error).into_response();
    }
    let _metadata_permit = match crate::storage_apply::acquire_exclusive_metadata(&state) {
        Ok(permit) => permit,
        Err(response) => return *response,
    };
    let _model_operation_permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another storage operation is in progress".to_string(),
            )
                .into_response();
        }
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let model = match state.storage.get_model(&name) {
        Ok(Some(model)) => model,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, format!("model '{name}' not found")).into_response();
        }
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    let audit = ResourceMutationLog::new("model", "delete", &name, Some(model.revision));
    if let Err(error) = crate::attachments::delete_model(&state.storage, &name) {
        audit.log_failure(&error);
        return (StatusCode::INTERNAL_SERVER_ERROR, error).into_response();
    }
    audit.log_success();
    Json(serde_json::json!({"deleted": name})).into_response()
}

fn model_info(model: StoredModel) -> ModelInfo {
    ModelInfo {
        name: model.name,
        revision: model.revision,
        model_sha256: model.model_sha256,
        model_size_bytes: model.model_size_bytes,
        test_vector_count: model.test_vector_count,
    }
}

fn validate_bundle(root: &FsPath) -> Result<(String, String, Vec<u8>, usize), String> {
    let model_path = root.join(MODEL_FILE);
    let card_path = root.join(MODEL_CARD_FILE);
    let model_bytes = fs::read(&model_path).map_err(|error| format!("read model.onnx: {error}"))?;
    if model_bytes.is_empty() {
        return Err("model.onnx must not be empty".to_string());
    }
    let model_card_yaml =
        fs::read_to_string(&card_path).map_err(|error| format!("read model.yaml: {error}"))?;
    let card: YamlValue = serde_yaml::from_str(&model_card_yaml)
        .map_err(|error| format!("parse model.yaml: {error}"))?;
    let mapping = card
        .as_mapping()
        .ok_or_else(|| "model.yaml must contain a mapping".to_string())?;
    let name = yaml_string(mapping, "name")?;
    validate_model_name(&name)?;
    if !mapping.contains_key(&YamlValue::String("inputs".to_string()))
        || !mapping.contains_key(&YamlValue::String("outputs".to_string()))
    {
        return Err("model.yaml must define inputs and outputs".to_string());
    }
    let test_vector_count = validate_test_vectors(&root.join(TEST_VECTOR_DIR))?;
    Ok((name, model_card_yaml, model_bytes, test_vector_count))
}

fn validate_test_vectors(path: &FsPath) -> Result<usize, String> {
    let entries =
        fs::read_dir(path).map_err(|error| format!("read test_vectors directory: {error}"))?;
    let mut inputs = BTreeSet::new();
    let mut outputs = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("read test vector entry: {error}"))?;
        if !entry
            .file_type()
            .map_err(|error| format!("inspect test vector entry: {error}"))?
            .is_file()
        {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(stem) = name
            .strip_prefix("input-")
            .and_then(|v| v.strip_suffix(".json"))
        {
            inputs.insert(stem.to_string());
        }
        if let Some(stem) = name
            .strip_prefix("expected_output-")
            .and_then(|v| v.strip_suffix(".json"))
        {
            outputs.insert(stem.to_string());
        }
    }
    if inputs.len() < MIN_TEST_VECTOR_COUNT {
        return Err(format!(
            "test_vectors must contain at least {MIN_TEST_VECTOR_COUNT} input/expected-output pairs"
        ));
    }
    if inputs != outputs {
        return Err("test_vectors input and expected_output pairs do not match".to_string());
    }
    Ok(inputs.len())
}

fn single_bundle_root(work: &FsPath) -> Result<PathBuf, String> {
    let mut entries =
        fs::read_dir(work).map_err(|error| format!("read extracted model bundle: {error}"))?;
    let first = entries
        .next()
        .transpose()
        .map_err(|error| format!("read extracted model bundle: {error}"))?
        .ok_or_else(|| "model bundle is empty".to_string())?;
    if entries.next().is_some()
        || !first
            .file_type()
            .map_err(|error| error.to_string())?
            .is_dir()
    {
        return Err("model bundle must contain exactly one top-level directory".to_string());
    }
    Ok(first.path())
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header::CONTENT_TYPE};
    use std::io::{Cursor, Write};
    use tempfile::tempdir;
    use tower::ServiceExt;
    use zip::ZipWriter;

    fn sample_default_instance_spec() -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: crate::DEFAULT_FLOW_INSTANCE_ID.to_string(),
            ..crate::FlowInstanceSpec::default()
        }
    }

    fn model_bundle() -> Vec<u8> {
        let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        let entries = [
            ("http_model/model.onnx", b"not-a-real-model".as_slice()),
            (
                "http_model/model.yaml",
                b"name: http_model\ninputs: []\noutputs: []\n".as_slice(),
            ),
            ("http_model/test_vectors/input-1.json", b"{}".as_slice()),
            (
                "http_model/test_vectors/expected_output-1.json",
                b"{}".as_slice(),
            ),
            ("http_model/test_vectors/input-2.json", b"{}".as_slice()),
            (
                "http_model/test_vectors/expected_output-2.json",
                b"{}".as_slice(),
            ),
            ("http_model/test_vectors/input-3.json", b"{}".as_slice()),
            (
                "http_model/test_vectors/expected_output-3.json",
                b"{}".as_slice(),
            ),
        ];
        for (name, contents) in entries {
            archive.start_file(name, options).unwrap();
            archive.write_all(contents).unwrap();
        }
        archive.finish().unwrap().into_inner()
    }

    fn upload_request(bundle: Vec<u8>) -> Request<Body> {
        let boundary = "model_test_boundary";
        let mut body = Vec::new();
        write!(
            body,
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"model.zip\"\r\nContent-Type: application/zip\r\n\r\n"
        )
        .unwrap();
        body.extend(bundle);
        write!(body, "\r\n--{boundary}--\r\n").unwrap();
        Request::builder()
            .method("POST")
            .uri("/models")
            .header(
                CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn model_upload_and_delete_work_through_application_router() {
        let dir = tempdir().unwrap();
        let storage = storage::StorageManager::new(dir.path()).unwrap();
        let state = crate::pipeline::AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
            0,
        )
        .unwrap();

        let response = crate::build_app(state.clone())
            .oneshot(upload_request(model_bundle()))
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));

        let response = crate::build_app(state)
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/models/http_model")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

fn yaml_string(mapping: &serde_yaml::Mapping, key: &str) -> Result<String, String> {
    mapping
        .get(&YamlValue::String(key.to_string()))
        .and_then(YamlValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("model.yaml field '{key}' must be a string"))
}

fn validate_model_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic())
    {
        return Err("model name must start with a letter and be at most 128 bytes".to_string());
    }
    if name.ends_with('.') || name.contains("..") {
        return Err("model name must not end with a dot or contain consecutive dots".to_string());
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
    {
        return Err(
            "model name may contain only ASCII letters, digits, underscores, and dots".to_string(),
        );
    }
    Ok(())
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}
