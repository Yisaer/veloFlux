use axum::{Json, extract::Path, http::StatusCode, response::IntoResponse};
use flow::catalog::{describe_function_def, list_function_defs};

pub async fn list_functions_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(list_function_defs())).into_response()
}

pub async fn describe_function_handler(Path(name): Path<String>) -> impl IntoResponse {
    match describe_function_def(&name) {
        Some(def) => (StatusCode::OK, Json(def)).into_response(),
        None => (StatusCode::NOT_FOUND, format!("function {name} not found")).into_response(),
    }
}
