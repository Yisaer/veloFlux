//! Synchronous semantic stages and channel-based execution boundaries.
//! Regions execute stage chains; source, sink, lookup, shared-stream and
//! coordination drivers retain the external data/control channels.

pub mod aggregation_processor;
pub mod barrier;
pub mod barrier_processor;
pub mod base;
pub mod batch_function_compute_processor;
pub(crate) mod batch_policy;
#[cfg(test)]
#[path = "region_processor/batch_tests.rs"]
mod batch_tests;
pub mod byte_stage;
pub(crate) mod checkpoint_coordinator;
pub mod collection_layout_normalize_processor;
#[cfg(test)]
#[path = "region_processor/compress_tests.rs"]
mod compress_tests;
pub mod compute_processor;
pub mod control_source_processor;
pub(crate) mod count_window_stage;
pub(crate) mod data_metrics;
pub mod datasource_processor;
pub mod decoder_processor;
pub(crate) mod delivery_transform;
pub mod empty_suppress_processor;
pub(crate) mod encoder_stage;
#[cfg(test)]
#[path = "region_processor/encoder_tests.rs"]
mod encoder_tests;
#[cfg(test)]
#[path = "region_processor/encrypt_tests.rs"]
mod encrypt_tests;
pub mod eos_window_processor;
pub mod eventtime;
pub mod filter_processor;
pub mod lookup_join_processor;
pub mod memory_collection_materialize_processor;
pub(crate) mod native_batch_stage;
pub mod order_processor;
pub(crate) mod output_row_accessor;
pub(crate) mod pipeline_state_runtime;
pub mod processor_builder;
pub mod processor_state;
pub mod project_processor;
pub(crate) mod region_processor;
pub mod result_collect_processor;
pub mod row_diff_processor;
pub mod sampler_processor;
pub mod shared_stream_processor;
pub mod sink_processor;
pub(crate) mod sliding_window_runtime;
pub(crate) mod sliding_window_stage;
#[cfg(test)]
#[path = "region_processor/sliding_window_tests.rs"]
mod sliding_window_tests;
pub mod source_change_gate_processor;
pub(crate) mod stage_clock;
mod stage_metrics;
pub mod state_window_processor;
pub mod stateful_function_processor;
pub mod stats;
pub mod stream_data;
pub mod streaming_aggregation_processor;
pub mod table_scan_processor;
mod timed_batch_stage;
pub mod tumbling_window_processor;
pub mod watermark_processor;
pub(crate) mod window_metadata;
pub(crate) mod window_partition;

pub use base::ProcessorError;
pub(crate) use base::{Processor, ProcessorStart};
pub(crate) use checkpoint_coordinator::{CheckpointCoordinator, CheckpointTrigger};
pub use control_source_processor::{ControlSourceProcessor, Ingress, IngressTarget};
pub use datasource_processor::DataSourceProcessor;
pub use eventtime::EventtimePipelineContext;
pub use lookup_join_processor::LookupJoinProcessor;
pub use processor_builder::ProcessorPipeline;
pub(crate) use processor_builder::{
    create_processor_pipeline, ProcessorPipelineDependencies, ProcessorPipelineOptions,
};
pub use processor_state::ProcessorState;
pub use result_collect_processor::ResultCollectProcessor;
pub use sampler_processor::{SamplerConfig, SamplingStrategy};
pub use shared_stream_processor::SharedStreamProcessor;
pub use sink_processor::SinkProcessor;
pub use stats::{
    set_stage_handling_time_enabled, CounterHandle, GaugeHandle, MetricKind, MetricSpec,
    ProcessorStats, ProcessorStatsEntry, ProcessorStatsHandle, ProcessorStatsSnapshot,
};
pub use stream_data::{
    BarrierControlSignal, BarrierControlSignalKind, ControlSignal, EncodedDeliveryFlags,
    InstantControlSignal, StreamData, StreamError,
};
pub use table_scan_processor::TableScanProcessor;

#[cfg(test)]
mod test_support;
