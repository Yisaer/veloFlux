//! Physical plan builder - converts logical plans to physical plans

use crate::expr::sql_conversion::{
    convert_expr_to_scalar_with_bindings, SchemaBinding, SchemaBindingEntry, SourceBindingKind,
};
use crate::planner::logical::{
    DataSinkPlan, DataSource as LogicalDataSource, Filter as LogicalFilter, LogicalPlan,
    Project as LogicalProject,
};
use crate::planner::physical::physical_project::PhysicalProjectField;
use crate::planner::physical::{
    PhysicalBatch, PhysicalDataSink, PhysicalDataSource, PhysicalEncoder, PhysicalFilter,
    PhysicalPlan, PhysicalProject, PhysicalSharedStream, PhysicalSinkConnector,
    PhysicalStreamingEncoder, PhysicalTail,
};
use crate::planner::sink::{PipelineSink, PipelineSinkConnector};
use std::sync::Arc;

/// Create a physical plan from a logical plan
///
/// This function walks through the logical plan tree and creates corresponding physical plan nodes
/// by pattern matching on the logical plan enum.
pub fn create_physical_plan(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    match logical_plan.as_ref() {
        LogicalPlan::DataSource(logical_ds) => {
            create_physical_data_source(logical_ds, logical_plan.get_plan_index(), bindings)
        }
        LogicalPlan::Filter(logical_filter) => create_physical_filter(
            logical_filter,
            &logical_plan,
            logical_plan.get_plan_index(),
            bindings,
        ),
        LogicalPlan::Project(logical_project) => create_physical_project(
            logical_project,
            &logical_plan,
            logical_plan.get_plan_index(),
            bindings,
        ),
        LogicalPlan::DataSink(logical_sink) => create_physical_data_sink(
            logical_sink,
            &logical_plan,
            logical_plan.get_plan_index(),
            bindings,
        ),
        LogicalPlan::Tail(logical_tail) => create_physical_tail(
            logical_tail,
            &logical_plan,
            logical_plan.get_plan_index(),
            bindings,
        ),
    }
}

/// Create a PhysicalTail from a TailPlan
fn create_physical_tail(
    logical_tail: &crate::planner::logical::TailPlan,
    _logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert all sink children to physical plans
    let mut physical_children = Vec::new();
    for child in logical_tail.base.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
        physical_children.push(physical_child);
    }

    // Create PhysicalTail to hold the physical sink children
    let physical_tail = PhysicalTail::new(physical_children, index);
    Ok(Arc::new(PhysicalPlan::Tail(physical_tail)))
}

/// Create a PhysicalDataSource from a LogicalDataSource
fn create_physical_data_source(
    logical_ds: &LogicalDataSource,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    let entry = find_binding_entry(logical_ds, bindings)?;
    let schema = entry.schema.clone();
    match entry.kind {
        SourceBindingKind::Regular => {
            let physical_ds = PhysicalDataSource::new(
                logical_ds.source_name.clone(),
                logical_ds.alias.clone(),
                schema,
                index,
            );
            Ok(Arc::new(PhysicalPlan::DataSource(physical_ds)))
        }
        SourceBindingKind::Shared => {
            let physical_shared = PhysicalSharedStream::new(
                logical_ds.source_name.clone(),
                logical_ds.alias.clone(),
                schema,
                index,
            );
            Ok(Arc::new(PhysicalPlan::SharedStream(physical_shared)))
        }
    }
}

/// Create a PhysicalFilter from a LogicalFilter
fn create_physical_filter(
    logical_filter: &LogicalFilter,
    logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
        physical_children.push(physical_child);
    }

    // Convert SQL Expr to ScalarExpr
    let scalar_predicate =
        convert_expr_to_scalar_with_bindings(&logical_filter.predicate, bindings).map_err(|e| {
            format!(
                "Failed to convert filter predicate to scalar expression: {}",
                e
            )
        })?;

    let physical_filter = PhysicalFilter::new(
        logical_filter.predicate.clone(),
        scalar_predicate,
        physical_children,
        index,
    );
    Ok(Arc::new(PhysicalPlan::Filter(physical_filter)))
}

/// Create a PhysicalProject from a LogicalProject
fn create_physical_project(
    logical_project: &LogicalProject,
    logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
        physical_children.push(physical_child);
    }

    // Convert logical fields to physical fields
    let mut physical_fields = Vec::new();
    for logical_field in &logical_project.fields {
        let physical_field = PhysicalProjectField::from_logical(
            logical_field.field_name.clone(),
            logical_field.expr.clone(),
            bindings,
        )?;
        physical_fields.push(physical_field);
    }

    let physical_project = PhysicalProject::new(physical_fields, physical_children, index);
    Ok(Arc::new(PhysicalPlan::Project(physical_project)))
}

fn create_physical_data_sink(
    logical_sink: &DataSinkPlan,
    logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
        physical_children.push(physical_child);
    }
    if physical_children.len() != 1 {
        return Err("DataSink plan must have exactly one child".to_string());
    }

    let input_child = Arc::clone(&physical_children[0]);
    let sinks = vec![logical_sink.sink.clone()];
    create_physical_sink_node(&sinks, input_child, index)
}

fn create_physical_sink_node(
    sinks: &[PipelineSink],
    input_child: Arc<PhysicalPlan>,
    index: i64,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut connectors = Vec::new();
    let mut encoder_children = Vec::new();
    let mut next_index = index + 1;
    for sink in sinks {
        let (mut sink_encoders, mut sink_connectors) =
            build_sink_encoders_for_sink(sink, &input_child, &mut next_index)?;
        encoder_children.append(&mut sink_encoders);
        connectors.append(&mut sink_connectors);
    }
    if connectors.is_empty() {
        return Err("DataSink plan must define at least one connector".to_string());
    }
    let child = encoder_children.remove(0);
    let connector = connectors.remove(0);
    let physical_sink = PhysicalDataSink::new(child, index, connector);
    Ok(Arc::new(PhysicalPlan::DataSink(physical_sink)))
}

fn build_sink_encoders_for_sink(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    next_index: &mut i64,
) -> Result<(Vec<Arc<PhysicalPlan>>, Vec<PhysicalSinkConnector>), String> {
    let mut encoder_children = Vec::new();
    let mut connectors = Vec::new();
    let shared_batch = create_shared_batch_if_needed(sink, input_child, next_index);

    let connector = &sink.connector;
    if should_use_streaming_encoder(sink, connector) {
        add_streaming_encoder(
            sink,
            connector,
            0, // connector_idx is always 0 for single connector
            input_child,
            next_index,
            &mut encoder_children,
            &mut connectors,
        );
    } else {
        let encoder_input = shared_batch
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::clone(input_child));
        add_regular_encoder(
            sink,
            connector,
            0, // connector_idx is always 0 for single connector
            encoder_input,
            next_index,
            &mut encoder_children,
            &mut connectors,
        );
    }

    if connectors.is_empty() {
        return Err(format!(
            "Sink {} must define at least one connector",
            sink.sink_id
        ));
    }

    Ok((encoder_children, connectors))
}

fn should_use_streaming_encoder(sink: &PipelineSink, connector: &PipelineSinkConnector) -> bool {
    sink.common.is_batching_enabled() && connector.encoder.supports_streaming()
}

fn create_shared_batch_if_needed(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    next_index: &mut i64,
) -> Option<Arc<PhysicalPlan>> {
    let needs_batch =
        sink.common.is_batching_enabled() && !sink.connector.encoder.supports_streaming();

    if !needs_batch {
        return None;
    }

    let batch_plan = PhysicalBatch::new(
        vec![Arc::clone(input_child)],
        *next_index,
        sink.sink_id.clone(),
        sink.common.clone(),
    );
    *next_index += 1;
    Some(Arc::new(PhysicalPlan::Batch(batch_plan)))
}

fn add_streaming_encoder(
    sink: &PipelineSink,
    connector: &PipelineSinkConnector,
    _connector_idx: usize,
    input_child: &Arc<PhysicalPlan>,
    next_index: &mut i64,
    encoder_children: &mut Vec<Arc<PhysicalPlan>>,
    connectors: &mut Vec<PhysicalSinkConnector>,
) {
    let streaming = PhysicalStreamingEncoder::new(
        vec![Arc::clone(input_child)],
        *next_index,
        sink.sink_id.clone(),
        connector.connector_id.clone(),
        connector.encoder.clone(),
        sink.common.clone(),
    );
    encoder_children.push(Arc::new(PhysicalPlan::StreamingEncoder(streaming)));
    connectors.push(PhysicalSinkConnector::new(
        sink.sink_id.clone(),
        sink.forward_to_result, // Always forward if sink is configured to do so (single connector)
        connector.connector_id.clone(),
        connector.connector.clone(),
        *next_index,
    ));
    *next_index += 1;
}

fn add_regular_encoder(
    sink: &PipelineSink,
    connector: &PipelineSinkConnector,
    _connector_idx: usize,
    encoder_input: Arc<PhysicalPlan>,
    next_index: &mut i64,
    encoder_children: &mut Vec<Arc<PhysicalPlan>>,
    connectors: &mut Vec<PhysicalSinkConnector>,
) {
    let encoder = PhysicalEncoder::new(
        vec![encoder_input],
        *next_index,
        sink.sink_id.clone(),
        connector.connector_id.clone(),
        connector.encoder.clone(),
    );
    encoder_children.push(Arc::new(PhysicalPlan::Encoder(encoder)));
    connectors.push(PhysicalSinkConnector::new(
        sink.sink_id.clone(),
        sink.forward_to_result, // Always forward if sink is configured to do so (single connector)
        connector.connector_id.clone(),
        connector.connector.clone(),
        *next_index,
    ));
    *next_index += 1;
}

fn find_binding_entry<'a>(
    logical_ds: &LogicalDataSource,
    bindings: &'a SchemaBinding,
) -> Result<&'a SchemaBindingEntry, String> {
    if let Some(alias) = logical_ds.alias.as_deref() {
        if let Some(entry) = bindings
            .entries()
            .iter()
            .find(|entry| entry.alias.as_ref().map(|a| a == alias).unwrap_or(false))
        {
            return Ok(entry);
        }
    }
    bindings
        .entries()
        .iter()
        .find(|entry| entry.source_name == logical_ds.source_name)
        .ok_or_else(|| {
            format!(
                "Schema binding not found for source {}",
                logical_ds.source_name
            )
        })
}
