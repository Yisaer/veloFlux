//! Pipeline module.
//!
//! This module intentionally separates the public API surface (types you should construct/call)
//! from runtime implementation details.
//!
//! **Public API**
//! - Definition types: `PipelineDefinition`, `SinkDefinition`, `PipelineOptions`
//! - Runtime lifecycle is managed via `FlowInstance` (create/start/stop/delete/list/explain)
//!
//! **Internal**
//! - Runtime build plumbing lives in `internal` and is not part of the public API.

mod api;
mod context;
mod internal;

pub(crate) use api::PipelineManager;
pub use api::{
    CreatePipelineRequest, CreatePipelineResult, EventtimeOptions, ExplainPipelineTarget,
    KuksaSinkProps, MemorySinkProps, MqttSinkProps, NopSinkProps, PipelineDefinition,
    PipelineError, PipelineOptions, PipelineSnapshot, PipelineStatus, PipelineStopMode,
    SinkDefinition, SinkProps, SinkType,
};
pub(crate) use context::PipelineContext;

#[cfg(test)]
mod tests;
