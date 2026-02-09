mod capabilities;
mod function;
mod instances;
mod memory_topic;
mod pipeline;
pub mod storage_bridge;
mod stream;
mod worker;

use axum::Router;
use axum::routing::{delete, get, post};
use pipeline::AppState;
use std::net::SocketAddr;
use storage::StorageManager;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

pub use instances::FlowInstanceSpec;
pub use instances::new_default_flow_instance;
pub use stream::{SchemaParser, register_schema, schema_registry};
pub use worker::FlowWorkerClient;
pub use worker::{FlowWorkerState, build_worker_app};
pub use worker::{
    WorkerApplyPipelineRequest, WorkerApplyPipelineResponse, WorkerDesiredState,
    WorkerMemoryTopicSpec, WorkerPipelineListItem, WorkerPlanCacheResult,
};

pub(crate) static MQTT_QOS: u8 = 0;

fn build_app(state: AppState) -> Router {
    Router::new()
        .route(
            "/pipelines",
            post(pipeline::create_pipeline_handler).get(pipeline::list_pipelines),
        )
        .route(
            "/pipelines/:id/start",
            post(pipeline::start_pipeline_handler),
        )
        .route("/pipelines/:id/stop", post(pipeline::stop_pipeline_handler))
        .route(
            "/pipelines/:id",
            get(pipeline::get_pipeline_handler)
                .put(pipeline::upsert_pipeline_handler)
                .delete(pipeline::delete_pipeline_handler),
        )
        .route(
            "/pipelines/:id/explain",
            get(pipeline::explain_pipeline_handler),
        )
        .route(
            "/pipelines/:id/stats",
            get(pipeline::collect_pipeline_stats_handler),
        )
        .route(
            "/pipelines/:id/buildContext",
            get(pipeline::build_pipeline_context_handler),
        )
        .route(
            "/streams",
            post(stream::create_stream_handler).get(stream::list_streams),
        )
        .route(
            "/streams/describe/:name",
            axum::routing::get(stream::describe_stream_handler),
        )
        .route(
            "/functions",
            axum::routing::get(function::list_functions_handler),
        )
        .route(
            "/functions/describe/:name",
            axum::routing::get(function::describe_function_handler),
        )
        .route(
            "/capabilities/syntax",
            axum::routing::get(capabilities::get_syntax_capabilities_handler),
        )
        .route("/streams/:name", delete(stream::delete_stream_handler))
        .route(
            "/memory/topics",
            post(memory_topic::create_memory_topic_handler)
                .get(memory_topic::list_memory_topics_handler),
        )
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn start_server_with_listener(
    listener: TcpListener,
    instance: flow::FlowInstance,
    storage: StorageManager,
    extra_flow_instances: Vec<FlowInstanceSpec>,
    extra_flow_worker_endpoints: Vec<(String, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = AppState::new(
        instance,
        storage,
        extra_flow_instances,
        extra_flow_worker_endpoints,
    )
    .map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid config: {err}"),
        )
    })?;
    if let Err(err) = state.bootstrap_from_storage().await {
        return Err(format!("failed to bootstrap from storage: {err}").into());
    }
    let app = build_app(state);
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

pub async fn start_server(
    addr: String,
    instance: flow::FlowInstance,
    storage: StorageManager,
    extra_flow_instances: Vec<FlowInstanceSpec>,
    extra_flow_worker_endpoints: Vec<(String, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = addr.parse()?;
    tracing::info!(manager_addr = %addr, "manager listening");
    let listener = TcpListener::bind(addr).await?;
    start_server_with_listener(
        listener,
        instance,
        storage,
        extra_flow_instances,
        extra_flow_worker_endpoints,
    )
    .await
}

pub async fn start_flow_worker_with_listener(
    listener: TcpListener,
    instance: flow::FlowInstance,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = FlowWorkerState::new(instance);
    let app = build_worker_app(state);
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
