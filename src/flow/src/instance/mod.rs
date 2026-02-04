use crate::aggregation::AggregateFunctionRegistry;
use crate::catalog::{Catalog, CatalogError, StreamDefinition};
use crate::codec::{CodecError, DecoderRegistry, EncoderRegistry, MergerRegistry};
use crate::connector::{
    ConnectorError, ConnectorRegistry, MemoryPubSubRegistry, MqttClientManager,
    SharedMqttClientConfig,
};
use crate::eventtime::EventtimeTypeRegistry;
use crate::expr::custom_func::CustomFuncRegistry;
use crate::pipeline::PipelineManager;
use crate::shared_stream::{SharedStreamError, SharedStreamInfo, SharedStreamRegistry};
use crate::stateful::StatefulFunctionRegistry;
use crate::PipelineRegistries;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

mod mqtt;
mod pipelines;
mod registries;
mod streams;

/// Runtime container that manages all Flow resources (streams, pipelines, shared clients).
#[derive(Clone)]
pub struct FlowInstance {
    catalog: Arc<Catalog>,
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
    /// Create a new Flow instance with instance-scoped resources.
    pub fn new() -> Self {
        crate::deadlock::start_deadlock_detector_once();

        let catalog = Arc::new(Catalog::new());
        let shared_stream_registry = Arc::new(SharedStreamRegistry::new());
        let mqtt_client_manager = MqttClientManager::new();
        let memory_pubsub_registry = MemoryPubSubRegistry::new();
        let connector_registry =
            ConnectorRegistry::with_builtin_sinks(memory_pubsub_registry.clone());
        let encoder_registry = EncoderRegistry::with_builtin_encoders();
        let decoder_registry = DecoderRegistry::with_builtin_decoders();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let stateful_registry = StatefulFunctionRegistry::with_builtins();
        let custom_func_registry = CustomFuncRegistry::with_builtins();

        let eventtime_type_registry = EventtimeTypeRegistry::with_builtin_types();
        let merger_registry = Arc::new(MergerRegistry::new());
        let registries = PipelineRegistries::new(
            Arc::clone(&connector_registry),
            Arc::clone(&encoder_registry),
            Arc::clone(&decoder_registry),
            Arc::clone(&aggregate_registry),
            Arc::clone(&stateful_registry),
            Arc::clone(&custom_func_registry),
            Arc::clone(&eventtime_type_registry),
            Arc::clone(&merger_registry),
        );
        let context = crate::pipeline::PipelineContext::new(
            Arc::clone(&shared_stream_registry),
            mqtt_client_manager.clone(),
            memory_pubsub_registry.clone(),
        );
        let pipeline_manager = Arc::new(PipelineManager::new(
            Arc::clone(&catalog),
            context,
            registries,
        ));
        Self {
            catalog,
            shared_stream_registry,
            pipeline_manager,
            memory_pubsub_registry,
            shared_mqtt_client_configs: Arc::new(Mutex::new(HashMap::new())),
            mqtt_client_manager,
            connector_registry,
            encoder_registry,
            decoder_registry,
            aggregate_registry,
            stateful_registry,
            custom_func_registry,
            eventtime_type_registry,
            merger_registry,
        }
    }

    pub fn merger_registry(&self) -> Arc<MergerRegistry> {
        Arc::clone(&self.merger_registry)
    }

    pub fn memory_pubsub_registry(&self) -> MemoryPubSubRegistry {
        self.memory_pubsub_registry.clone()
    }

    pub fn shared_stream_registry(&self) -> Arc<SharedStreamRegistry> {
        Arc::clone(&self.shared_stream_registry)
    }
}

impl Default for FlowInstance {
    fn default() -> Self {
        Self::new()
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
    #[error("{0}")]
    Invalid(String),
}
