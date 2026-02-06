use crate::aggregation::AggregateFunctionRegistry;
use crate::catalog::{Catalog, CatalogError, StreamDefinition};
use crate::codec::{CodecError, DecoderRegistry, EncoderRegistry, MergerRegistry};
use crate::connector::{
    ConnectorError, ConnectorRegistry, MemoryData, MemoryPubSubError, MemoryPubSubRegistry,
    MemoryPublisher, MemoryTopicKind, MqttClientManager, SharedMqttClientConfig,
};
use crate::eventtime::EventtimeTypeRegistry;
use crate::expr::custom_func::CustomFuncRegistry;
use crate::pipeline::PipelineManager;
use crate::shared_stream::{SharedStreamError, SharedStreamInfo, SharedStreamRegistry};
use crate::stateful::StatefulFunctionRegistry;
use crate::PipelineRegistries;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

use parking_lot::Mutex;

mod mqtt;
mod pipelines;
mod registries;
mod streams;

/// Registries that can be shared across multiple [`FlowInstance`]s.
///
/// Instance-scoped runtime resources (shared stream runtime, MQTT clients, memory pub/sub) remain
/// per-instance. These registries only hold definitions/factories and are safe to share to keep
/// custom registrations consistent across instances.
#[derive(Clone)]
pub struct FlowInstanceSharedRegistries {
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    stateful_registry: Arc<StatefulFunctionRegistry>,
    custom_func_registry: Arc<CustomFuncRegistry>,
    eventtime_type_registry: Arc<EventtimeTypeRegistry>,
    merger_registry: Arc<MergerRegistry>,
}

/// Runtime container that manages all Flow resources (streams, pipelines, shared clients).
#[derive(Clone)]
pub struct FlowInstance {
    id: String,
    catalog: Arc<Catalog>,
    spawner: crate::runtime::TaskSpawner,
    shared_stream_registry: Arc<SharedStreamRegistry>,
    pipeline_manager: Arc<PipelineManager>,
    memory_pubsub_registry: MemoryPubSubRegistry,
    // In-memory registry of shared MQTT client configs for discovery/listing.
    shared_mqtt_client_configs: Arc<Mutex<HashMap<String, SharedMqttClientConfig>>>,
    mqtt_client_manager: MqttClientManager,
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    stateful_registry: Arc<StatefulFunctionRegistry>,
    custom_func_registry: Arc<CustomFuncRegistry>,
    eventtime_type_registry: Arc<EventtimeTypeRegistry>,
    merger_registry: Arc<MergerRegistry>,
}

impl FlowInstance {
    fn dedicated_spawner(instance_id: &str) -> crate::runtime::TaskSpawner {
        crate::runtime::TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .thread_name(format!("flow-instance-{instance_id}"))
                .enable_all()
                .build()
                .expect("build flow instance tokio runtime"),
        )
    }

    fn build(
        id: String,
        catalog: Arc<Catalog>,
        spawner: crate::runtime::TaskSpawner,
        shared_registries: Option<FlowInstanceSharedRegistries>,
    ) -> Self {
        let shared_stream_registry = Arc::new(SharedStreamRegistry::new(spawner.clone()));
        let mqtt_client_manager = MqttClientManager::new(spawner.clone());
        let memory_pubsub_registry = MemoryPubSubRegistry::new();
        let connector_registry =
            ConnectorRegistry::with_builtin_sinks(memory_pubsub_registry.clone());

        let registries = shared_registries.unwrap_or_else(|| FlowInstanceSharedRegistries {
            encoder_registry: EncoderRegistry::with_builtin_encoders(),
            decoder_registry: DecoderRegistry::with_builtin_decoders(),
            aggregate_registry: AggregateFunctionRegistry::with_builtins(),
            stateful_registry: StatefulFunctionRegistry::with_builtins(),
            custom_func_registry: CustomFuncRegistry::with_builtins(),
            eventtime_type_registry: EventtimeTypeRegistry::with_builtin_types(),
            merger_registry: Arc::new(MergerRegistry::new()),
        });

        let registries_bundle = PipelineRegistries::new(
            Arc::clone(&connector_registry),
            Arc::clone(&registries.encoder_registry),
            Arc::clone(&registries.decoder_registry),
            Arc::clone(&registries.aggregate_registry),
            Arc::clone(&registries.stateful_registry),
            Arc::clone(&registries.custom_func_registry),
            Arc::clone(&registries.eventtime_type_registry),
            Arc::clone(&registries.merger_registry),
        );
        let context = crate::pipeline::PipelineContext::new(
            Arc::clone(&shared_stream_registry),
            mqtt_client_manager.clone(),
            memory_pubsub_registry.clone(),
            spawner.clone(),
        );
        let pipeline_manager = Arc::new(PipelineManager::new(
            Arc::clone(&catalog),
            context,
            registries_bundle,
        ));

        Self {
            id,
            catalog,
            spawner,
            shared_stream_registry,
            pipeline_manager,
            memory_pubsub_registry,
            shared_mqtt_client_configs: Arc::new(Mutex::new(HashMap::new())),
            mqtt_client_manager,
            connector_registry,
            encoder_registry: registries.encoder_registry,
            decoder_registry: registries.decoder_registry,
            aggregate_registry: registries.aggregate_registry,
            stateful_registry: registries.stateful_registry,
            custom_func_registry: registries.custom_func_registry,
            eventtime_type_registry: registries.eventtime_type_registry,
            merger_registry: registries.merger_registry,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn shared_registries(&self) -> FlowInstanceSharedRegistries {
        FlowInstanceSharedRegistries {
            encoder_registry: Arc::clone(&self.encoder_registry),
            decoder_registry: Arc::clone(&self.decoder_registry),
            aggregate_registry: Arc::clone(&self.aggregate_registry),
            stateful_registry: Arc::clone(&self.stateful_registry),
            custom_func_registry: Arc::clone(&self.custom_func_registry),
            eventtime_type_registry: Arc::clone(&self.eventtime_type_registry),
            merger_registry: Arc::clone(&self.merger_registry),
        }
    }

    /// Create the default Flow instance (`id=default`) that shares the current Tokio runtime.
    ///
    /// This requires a running Tokio runtime; it will panic if called outside a runtime context.
    pub fn new_default() -> Self {
        let handle = tokio::runtime::Handle::current();
        let spawner = crate::runtime::TaskSpawner::from_handle(handle);
        Self::build(
            "default".to_string(),
            Arc::new(Catalog::new()),
            spawner,
            None,
        )
    }

    /// Create a non-default Flow instance with a dedicated Tokio runtime.
    pub fn new_with_id(id: &str, shared_registries: Option<FlowInstanceSharedRegistries>) -> Self {
        let id = id.trim();
        assert!(!id.is_empty(), "flow instance id must not be empty");
        assert!(id != "default", "use new_default for id=default");
        let spawner = Self::dedicated_spawner(id);
        Self::build(
            id.to_string(),
            Arc::new(Catalog::new()),
            spawner,
            shared_registries,
        )
    }

    pub fn merger_registry(&self) -> Arc<MergerRegistry> {
        Arc::clone(&self.merger_registry)
    }

    pub fn declare_memory_topic(
        &self,
        topic: &str,
        kind: MemoryTopicKind,
        capacity: usize,
    ) -> Result<(), MemoryPubSubError> {
        self.memory_pubsub_registry
            .declare_topic(topic, kind, capacity)
    }

    pub fn open_memory_publisher_bytes(
        &self,
        topic: &str,
    ) -> Result<MemoryPublisher, MemoryPubSubError> {
        self.memory_pubsub_registry.open_publisher_bytes(topic)
    }

    pub fn open_memory_publisher_collection(
        &self,
        topic: &str,
    ) -> Result<MemoryPublisher, MemoryPubSubError> {
        self.memory_pubsub_registry.open_publisher_collection(topic)
    }

    pub fn memory_topic_kind(&self, topic: &str) -> Option<MemoryTopicKind> {
        self.memory_pubsub_registry.topic_kind(topic)
    }

    pub fn memory_topic_capacity(&self, topic: &str) -> Option<usize> {
        self.memory_pubsub_registry.topic_capacity(topic)
    }

    pub fn open_memory_subscribe_bytes(
        &self,
        topic: &str,
    ) -> Result<broadcast::Receiver<MemoryData>, MemoryPubSubError> {
        self.memory_pubsub_registry.open_subscribe_bytes(topic)
    }

    pub fn open_memory_subscribe_collection(
        &self,
        topic: &str,
    ) -> Result<broadcast::Receiver<MemoryData>, MemoryPubSubError> {
        self.memory_pubsub_registry.open_subscribe_collection(topic)
    }

    pub async fn wait_for_memory_subscribers(
        &self,
        topic: &str,
        kind: MemoryTopicKind,
        min: usize,
        timeout: std::time::Duration,
    ) -> Result<(), MemoryPubSubError> {
        self.memory_pubsub_registry
            .wait_for_subscribers(topic, kind, min, timeout)
            .await
    }
}

/// Combined runtime view for a catalog stream and its shared stream state.
#[derive(Clone)]
pub struct StreamRuntimeInfo {
    pub definition: Arc<StreamDefinition>,
    pub shared_info: Option<SharedStreamInfo>,
}

/// Errors surfaced by FlowInstance APIs.
#[derive(thiserror::Error, Debug)]
pub enum FlowInstanceError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    SharedStream(#[from] SharedStreamError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("stream {stream} still referenced by pipelines: {pipelines}")]
    StreamInUse { stream: String, pipelines: String },
    #[error("{0}")]
    Invalid(String),
}
