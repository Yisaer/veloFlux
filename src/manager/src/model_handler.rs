use axum::{
    Json,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use storage::{ModelSchemaColumn, StoredModel};
use tokio::sync::TryAcquireError;

use crate::audit::ResourceMutationLog;
use crate::pipeline::AppState;
use crate::stream::StreamColumnRequest;
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
    pub inputs: Vec<ModelSchemaColumn>,
    pub outputs: Vec<ModelSchemaColumn>,
    pub test_vector_count: u32,
}

#[derive(Serialize)]
pub struct ModelRuntimeStatusInfo {
    pub name: String,
    pub revision: u64,
    pub model_sha256: String,
    pub prepared: bool,
    pub prepare_count: u64,
}

#[derive(Deserialize)]
struct ModelCard {
    name: String,
    inputs: Option<Vec<ModelSchemaColumnRequest>>,
    outputs: Option<Vec<ModelSchemaColumnRequest>>,
}

#[derive(Deserialize)]
struct ModelSchemaColumnRequest {
    name: String,
    data_type: String,
    #[serde(default)]
    cardinality: Option<usize>,
    #[serde(default)]
    fields: Option<Vec<ModelSchemaColumnRequest>>,
    #[serde(default)]
    element: Option<Box<ModelSchemaColumnRequest>>,
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
    let (name, model_card_yaml, model_bytes, inputs, outputs, test_vector_count) =
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
        inputs: inputs.clone(),
        outputs: outputs.clone(),
        test_vector_count: test_vector_count as u32,
    };
    if let Err(error) =
        crate::attachments::publish_model_upload(&state.storage, stored.clone(), &bundle_root)
    {
        audit.log_failure(&error.to_string());
        return (StatusCode::INTERNAL_SERVER_ERROR, error).into_response();
    }
    if let Err(error) = state.model_registry.upsert(stored) {
        audit.log_failure(&error);
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
            inputs,
            outputs,
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

/// GET /models/:name/status
pub async fn get_model_status_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(error) = validate_model_name(&name) {
        return (StatusCode::BAD_REQUEST, error).into_response();
    }
    match state.model_registry.status(&name) {
        Ok(Some(status)) => Json(ModelRuntimeStatusInfo {
            name,
            revision: status.revision,
            model_sha256: status.model_sha256,
            prepared: status.prepared,
            prepare_count: status.prepare_count,
        })
        .into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("model '{name}' not found")).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
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
    let pipelines = match state.storage.list_pipelines() {
        Ok(pipelines) => pipelines,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    for pipeline in pipelines {
        let request = match crate::storage_bridge::pipeline_request_from_stored(&pipeline) {
            Ok(request) => request,
            Err(error) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to decode pipeline '{}': {error}", pipeline.id),
                )
                    .into_response();
            }
        };
        let instance = request
            .flow_instance_id
            .as_deref()
            .and_then(|id| state.instances.get(id))
            .unwrap_or_else(|| state.instances.default_instance());
        let referenced_models =
            match crate::pipeline::referenced_models_from_pipeline_sql(&request, instance.as_ref())
            {
                Ok(models) => models,
                Err(error) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to inspect pipeline '{}': {error}", pipeline.id),
                    )
                        .into_response();
                }
            };
        if referenced_models.contains(&name) {
            return (
                StatusCode::CONFLICT,
                format!("model '{name}' is referenced by pipeline '{}'", pipeline.id),
            )
                .into_response();
        }
    }
    let audit = ResourceMutationLog::new("model", "delete", &name, Some(model.revision));
    if let Err(error) = crate::attachments::delete_model(&state.storage, &name) {
        audit.log_failure(&error);
        return (StatusCode::INTERNAL_SERVER_ERROR, error).into_response();
    }
    if let Err(error) = state.model_registry.remove(&name) {
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
        inputs: model.inputs,
        outputs: model.outputs,
        test_vector_count: model.test_vector_count,
    }
}

fn validate_bundle(
    root: &FsPath,
) -> Result<
    (
        String,
        String,
        Vec<u8>,
        Vec<ModelSchemaColumn>,
        Vec<ModelSchemaColumn>,
        usize,
    ),
    String,
> {
    let model_path = root.join(MODEL_FILE);
    let card_path = root.join(MODEL_CARD_FILE);
    let model_bytes = fs::read(&model_path).map_err(|error| format!("read model.onnx: {error}"))?;
    if model_bytes.is_empty() {
        return Err("model.onnx must not be empty".to_string());
    }
    let model_card_yaml =
        fs::read_to_string(&card_path).map_err(|error| format!("read model.yaml: {error}"))?;
    let card: ModelCard = serde_yaml::from_str(&model_card_yaml)
        .map_err(|error| format!("parse model.yaml: {error}"))?;
    let name = card.name;
    validate_model_name(&name)?;
    let inputs = convert_model_schema(
        card.inputs
            .ok_or_else(|| "model.yaml must define inputs".to_string())?,
        "inputs",
    )?;
    let outputs = convert_model_schema(
        card.outputs
            .ok_or_else(|| "model.yaml must define outputs".to_string())?,
        "outputs",
    )?;
    let test_vector_count = validate_test_vectors(&root.join(TEST_VECTOR_DIR))?;
    Ok((
        name,
        model_card_yaml,
        model_bytes,
        inputs,
        outputs,
        test_vector_count,
    ))
}

fn convert_model_schema(
    columns: Vec<ModelSchemaColumnRequest>,
    direction: &str,
) -> Result<Vec<ModelSchemaColumn>, String> {
    let mut names = BTreeSet::new();
    columns
        .into_iter()
        .map(|column| {
            if !names.insert(column.name.clone()) {
                return Err(format!(
                    "model {direction} contains duplicate column '{}'",
                    column.name
                ));
            }
            convert_model_column(column)
                .map_err(|error| format!("invalid model {direction}: {error}"))
        })
        .collect()
}

fn convert_model_column(column: ModelSchemaColumnRequest) -> Result<ModelSchemaColumn, String> {
    validate_model_schema_contract(&column, &column.name)?;
    if column.cardinality == Some(0) {
        return Err(format!(
            "column '{}' cardinality must be greater than zero",
            column.name
        ));
    }
    if column.cardinality.is_some() && !column.data_type.eq_ignore_ascii_case("list") {
        return Err(format!(
            "column '{}' cardinality is only valid for list columns",
            column.name
        ));
    }
    crate::stream::parse_datatype(&stream_column_request(&column))?;
    let fields = match column.fields {
        Some(fields) => {
            let mut names = BTreeSet::new();
            Some(
                fields
                    .into_iter()
                    .map(|field| {
                        if !names.insert(field.name.clone()) {
                            return Err(format!("duplicate struct field '{}'", field.name));
                        }
                        convert_model_column(field)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        None => None,
    };
    let element = column
        .element
        .map(|element| convert_model_column(*element).map(Box::new))
        .transpose()?;
    Ok(ModelSchemaColumn {
        name: column.name,
        data_type: column.data_type,
        cardinality: column.cardinality,
        fields,
        element,
    })
}

fn validate_model_schema_contract(
    column: &ModelSchemaColumnRequest,
    path: &str,
) -> Result<(), String> {
    let data_type = column.data_type.to_ascii_lowercase();
    match data_type.as_str() {
        "list" => {
            let element = column
                .element
                .as_deref()
                .ok_or_else(|| format!("list column '{path}' must define an element"))?;
            let element_type = element.data_type.to_ascii_lowercase();
            if matches!(element_type.as_str(), "list" | "struct") && column.cardinality.is_none() {
                return Err(format!(
                    "list column '{path}' with a composite element must define cardinality"
                ));
            }
            validate_model_schema_contract(element, &format!("{path}[]"))?;
        }
        "struct" => {
            let fields = column
                .fields
                .as_ref()
                .ok_or_else(|| format!("struct column '{path}' must define fields"))?;
            for field in fields {
                let field_path = format!("{path}.{}", field.name);
                if field.data_type.eq_ignore_ascii_case("list") && field.cardinality.is_none() {
                    return Err(format!(
                        "list field '{field_path}' inside a struct must define cardinality"
                    ));
                }
                validate_model_schema_contract(field, &field_path)?;
            }
        }
        _ => {
            if column.fields.is_some() || column.element.is_some() {
                return Err(format!(
                    "scalar column '{path}' must not define fields or element"
                ));
            }
        }
    }
    Ok(())
}

fn stream_column_request(column: &ModelSchemaColumnRequest) -> StreamColumnRequest {
    StreamColumnRequest {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        fields: column
            .fields
            .as_ref()
            .map(|fields| fields.iter().map(stream_column_request).collect()),
        element: column
            .element
            .as_deref()
            .map(stream_column_request)
            .map(Box::new),
    }
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
    use super::{ModelSchemaColumnRequest, convert_model_column};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header::CONTENT_TYPE};
    use std::io::{Cursor, Write};
    use tempfile::tempdir;
    use tower::ServiceExt;
    use zip::ZipWriter;

    const MNIST_MODEL: &[u8] = include_bytes!("model_testdata/mnist-8.onnx");

    fn sample_default_instance_spec() -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: crate::DEFAULT_FLOW_INSTANCE_ID.to_string(),
            ..crate::FlowInstanceSpec::default()
        }
    }

    fn model_bundle() -> Vec<u8> {
        let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        archive.start_file("mnist_8/model.onnx", options).unwrap();
        archive.write_all(MNIST_MODEL).unwrap();
        let entries = [
            (
                "mnist_8/model.yaml",
                b"name: mnist_8\ninputs:\n  - name: image\n    data_type: list\n    element:\n      name: element\n      data_type: float32\noutputs:\n  - name: probabilities\n    data_type: list\n    element:\n      name: element\n      data_type: float32\n".as_slice(),
            ),
            ("mnist_8/test_vectors/input-1.json", b"{}".as_slice()),
            (
                "mnist_8/test_vectors/expected_output-1.json",
                b"{}".as_slice(),
            ),
            ("mnist_8/test_vectors/input-2.json", b"{}".as_slice()),
            (
                "mnist_8/test_vectors/expected_output-2.json",
                b"{}".as_slice(),
            ),
            ("mnist_8/test_vectors/input-3.json", b"{}".as_slice()),
            (
                "mnist_8/test_vectors/expected_output-3.json",
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

    fn model_column_request(name: &str, data_type: &str) -> ModelSchemaColumnRequest {
        ModelSchemaColumnRequest {
            name: name.to_string(),
            data_type: data_type.to_string(),
            cardinality: None,
            fields: None,
            element: None,
        }
    }

    #[tokio::test]
    async fn mnist_model_upload_succeeds_through_application_router() {
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
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(response["name"], "mnist_8");
        assert_eq!(response["test_vector_count"], 3);
        assert_eq!(response["inputs"][0]["data_type"], "list");
        assert_eq!(response["outputs"][0]["data_type"], "list");
        let stored = state.storage.get_model("mnist_8").unwrap().unwrap();
        assert_eq!(stored.model_size_bytes, MNIST_MODEL.len() as u64);
        assert_eq!(stored.inputs[0].name, "image");
        assert_eq!(stored.outputs[0].name, "probabilities");
        let prepared = state.model_registry.load("mnist_8").unwrap();
        assert_eq!(prepared.metadata().model.name, "mnist_8");
        assert_eq!(
            prepared.metadata().model.model_sha256,
            "2f06e72de813a8635c9bc0397ac447a601bdbfa7df4bebc278723b958831c9bf"
        );
    }

    #[tokio::test]
    async fn deleting_model_referenced_by_stored_pipeline_returns_conflict() {
        let dir = tempdir().unwrap();
        let storage = storage::StorageManager::new(dir.path()).unwrap();
        let state = crate::pipeline::AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
            0,
        )
        .unwrap();

        let upload = crate::build_app(state.clone())
            .oneshot(upload_request(model_bundle()))
            .await
            .unwrap();
        assert_eq!(upload.status(), StatusCode::OK);

        state
            .storage
            .create_pipeline(storage::StoredPipeline {
                id: "pipeline_using_mnist".to_string(),
                revision: 1,
                raw_json: serde_json::json!({
                    "id": "pipeline_using_mnist",
                    "sql": "SELECT onnx('mnist_8', image) FROM telemetry"
                })
                .to_string(),
            })
            .unwrap();

        let response = crate::build_app(state.clone())
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/models/mnist_8")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(state.storage.get_model("mnist_8").unwrap().is_some());
    }

    #[test]
    fn model_schema_accepts_composite_list_with_cardinality() {
        let mut element = model_column_request("detection", "struct");
        element.fields = Some(vec![
            model_column_request("class_id", "int32"),
            model_column_request("score", "float32"),
        ]);
        let mut column = model_column_request("detections", "list");
        column.cardinality = Some(2);
        column.element = Some(Box::new(element));

        let converted = convert_model_column(column).unwrap();
        assert_eq!(converted.cardinality, Some(2));
        assert_eq!(
            serde_json::to_value(converted).unwrap()["cardinality"],
            serde_json::json!(2)
        );
    }

    #[test]
    fn model_schema_rejects_composite_list_without_cardinality() {
        let mut element = model_column_request("detection", "struct");
        element.fields = Some(vec![model_column_request("class_id", "int32")]);
        let mut column = model_column_request("detections", "list");
        column.element = Some(Box::new(element));

        let error = convert_model_column(column).unwrap_err();
        assert!(error.contains("must define cardinality"), "{error}");
    }

    #[test]
    fn model_schema_rejects_struct_list_field_without_cardinality() {
        let mut column = model_column_request("record", "struct");
        column.fields = Some(vec![model_column_request("values", "list")]);

        let error = convert_model_column(column).unwrap_err();
        assert!(
            error.contains("inside a struct must define cardinality"),
            "{error}"
        );
    }
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
