pub mod handler;
pub mod proto;
pub(crate) mod source;

/// Serialize reference acquisition with cascading updates and deletion of a schema.
/// Permits are tried rather than awaited, so resource lock ordering cannot deadlock.
pub(crate) async fn operation_guard(
    state: &crate::pipeline::AppState,
    name: &str,
    exclusive: bool,
) -> Result<tokio::sync::OwnedSemaphorePermit, Box<axum::response::Response>> {
    use axum::response::IntoResponse;
    state
        .try_acquire_schema_operation(name, exclusive)
        .await
        .map_err(|error| {
            Box::new(match error {
                tokio::sync::TryAcquireError::NoPermits => (
                    axum::http::StatusCode::CONFLICT,
                    format!("schema {name} is busy processing another command"),
                )
                    .into_response(),
                tokio::sync::TryAcquireError::Closed => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "schema operation guard closed",
                )
                    .into_response(),
            })
        })
}
