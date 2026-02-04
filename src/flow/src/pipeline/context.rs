use crate::connector::{MemoryPubSubRegistry, MockSourceHandle, MqttClientManager};
use crate::shared_stream::SharedStreamRegistry;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct MockSourceHandleRegistry {
    handles: Arc<RwLock<HashMap<String, MockSourceHandle>>>,
}

impl MockSourceHandleRegistry {
    pub(crate) fn register(&self, key: impl Into<String>, handle: MockSourceHandle) {
        self.handles.write().insert(key.into(), handle);
    }
}

#[derive(Clone)]
pub(crate) struct PipelineContext {
    shared_stream_registry: Arc<SharedStreamRegistry>,
    mqtt_client_manager: MqttClientManager,
    memory_pubsub_registry: MemoryPubSubRegistry,
    mock_source_handle_registry: MockSourceHandleRegistry,
    spawner: crate::runtime::TaskSpawner,
}

impl PipelineContext {
    pub(crate) fn new(
        shared_stream_registry: Arc<SharedStreamRegistry>,
        mqtt_client_manager: MqttClientManager,
        memory_pubsub_registry: MemoryPubSubRegistry,
        spawner: crate::runtime::TaskSpawner,
    ) -> Self {
        Self {
            shared_stream_registry,
            mqtt_client_manager,
            memory_pubsub_registry,
            mock_source_handle_registry: MockSourceHandleRegistry::default(),
            spawner,
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

    pub(crate) fn mock_source_handle_registry(&self) -> &MockSourceHandleRegistry {
        &self.mock_source_handle_registry
    }

    pub(crate) fn spawner(&self) -> &crate::runtime::TaskSpawner {
        &self.spawner
    }
}
