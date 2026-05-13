use std::sync::Arc;

use crate::aggregation::AggregateFunction;
use crate::aggregation::AggregateFunctionRegistry;
use crate::codec::{DecoderRegistry, EncoderRegistry};
use crate::connector::ConnectorRegistry;
use crate::eventtime::EventtimeTypeRegistry;
use crate::expr::custom_func::CustomFuncRegistry;
use crate::stateful::{StatefulFunction, StatefulFunctionRegistry, StatefulRegistryError};
use crate::PipelineRegistries;

use super::FlowInstance;

impl FlowInstance {
    pub fn stateful_registry(&self) -> Arc<StatefulFunctionRegistry> {
        Arc::clone(&self.stateful_registry)
    }

    pub fn eventtime_type_registry(&self) -> Arc<EventtimeTypeRegistry> {
        Arc::clone(&self.eventtime_type_registry)
    }

    pub fn register_stateful_function(
        &self,
        function: Arc<dyn StatefulFunction>,
    ) -> Result<(), StatefulRegistryError> {
        self.stateful_registry.register_function(function)
    }

    pub fn register_aggregate_function(&self, function: Arc<dyn AggregateFunction>) {
        self.aggregate_registry.register_function(function);
    }

    pub fn connector_registry(&self) -> Arc<ConnectorRegistry> {
        Arc::clone(&self.connector_registry)
    }

    pub fn encoder_registry(&self) -> Arc<EncoderRegistry> {
        Arc::clone(&self.encoder_registry)
    }

    pub fn decoder_registry(&self) -> Arc<DecoderRegistry> {
        Arc::clone(&self.decoder_registry)
    }

    pub fn aggregate_registry(&self) -> Arc<AggregateFunctionRegistry> {
        Arc::clone(&self.aggregate_registry)
    }

    /// Get the current custom function registry.
    pub fn custom_func_registry(&self) -> Arc<CustomFuncRegistry> {
        Arc::clone(&self.custom_func_registry.read())
    }

    /// Replace the custom function registry (e.g. to inject WASM UDFs at startup).
    ///
    /// This must be called before any pipeline is created.
    pub fn set_custom_func_registry(&self, registry: Arc<CustomFuncRegistry>) {
        *self.custom_func_registry.write() = registry;
    }

    pub(super) fn pipeline_registries(&self) -> PipelineRegistries {
        PipelineRegistries::new(
            Arc::clone(&self.connector_registry),
            Arc::clone(&self.encoder_registry),
            Arc::clone(&self.decoder_registry),
            Arc::clone(&self.aggregate_registry),
            Arc::clone(&self.stateful_registry),
            Arc::clone(&self.custom_func_registry.read()),
            Arc::clone(&self.eventtime_type_registry),
            Arc::clone(&self.merger_registry),
        )
    }
}
