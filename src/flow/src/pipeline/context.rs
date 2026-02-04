use crate::connector::{MemoryPubSubRegistry, MqttClientManager};
use crate::shared_stream::SharedStreamRegistry;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct PipelineContext {
    shared_stream_registry: Arc<SharedStreamRegistry>,
    mqtt_client_manager: MqttClientManager,
    memory_pubsub_registry: MemoryPubSubRegistry,
}

impl PipelineContext {
    pub(crate) fn new(
        shared_stream_registry: Arc<SharedStreamRegistry>,
        mqtt_client_manager: MqttClientManager,
        memory_pubsub_registry: MemoryPubSubRegistry,
    ) -> Self {
        Self {
            shared_stream_registry,
            mqtt_client_manager,
            memory_pubsub_registry,
        }
    }

    pub(crate) fn shared_stream_registry(&self) -> Arc<SharedStreamRegistry> {
        Arc::clone(&self.shared_stream_registry)
    }

    pub(crate) fn mqtt_client_manager(&self) -> &MqttClientManager {
        &self.mqtt_client_manager
    }

    pub(crate) fn memory_pubsub_registry(&self) -> &MemoryPubSubRegistry {
        &self.memory_pubsub_registry
    }
}
