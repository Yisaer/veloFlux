use crate::aggregation::AggregateFunctionRegistry;
use crate::codec::EncoderRegistry;
use crate::planner::physical::{
    output_layout::OutputLayout, PhysicalBarrier, PhysicalPlan, PhysicalStreamingAggregation,
    StreamingWindowSpec,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A physical optimization rule.
trait PhysicalOptRule {
    fn name(&self) -> &str;
    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan>;
}

/// Apply physical plan optimizations using the provided registries.
pub fn optimize_physical_plan(
    physical_plan: Arc<PhysicalPlan>,
    encoder_registry: &EncoderRegistry,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
) -> Arc<PhysicalPlan> {
    let rules: Vec<Box<dyn PhysicalOptRule>> = vec![
        Box::new(StreamingAggregationRewrite {
            aggregate_registry: Arc::clone(&aggregate_registry),
        }),
        Box::new(StreamingEncoderRewrite),
        Box::new(AttachEncoderOutputLayout),
        Box::new(RemovePlannerColumnFilter),
        Box::new(FuseStatelessTransform),
        Box::new(InsertBarrierForFanIn),
    ];
    let mut current = physical_plan;
    for rule in rules {
        let _ = rule.name();
        current = rule.optimize(current, encoder_registry);
    }
    current
}

/// Rule: fuse Window -> Aggregation into StreamingAggregation when all calls are incremental.
struct StreamingAggregationRewrite {
    aggregate_registry: Arc<AggregateFunctionRegistry>,
}

/// Rule: insert a barrier node between a fan-in plan and its children.
///
/// A node is considered fan-in when it has more than one child. The inserted barrier node keeps
/// the original children, and the parent is rewritten to have a single barrier child.
///
/// This is a topology rewrite rule and is intentionally applied as the last physical optimization.
struct InsertBarrierForFanIn;

/// Rule: fuse `PhysicalBatch -> PhysicalSinkEncoder` into `PhysicalIncSinkEncoder`.
///
/// This eliminates one data-pass between BatchProcessor and SinkEncoderProcessor
/// for registered `SinkEncoder` implementations.
struct StreamingEncoderRewrite;

/// Rule: fuse the pure transform chain on a sink branch
/// (`[Order?] -> [Filter?] -> Project -> [EmptySuppress?] -> [Batch?]
/// -> [Encode?] -> [Compress?] -> [Encrypt?]`) into `FusedTransform`,
/// leaving the sink connector (external I/O, like lookup joins) standalone.
///
/// This eliminates one channel hop plus wakeup per fused stage on the hot
/// path. Anything outside the exact shape keeps the unfused plan:
/// row-diff / column-filter nodes inside the span (anything else peels).
/// Pipeline-state counters ride along untouched: the fused task threads
/// the same state handles into the same stage functions.
struct FuseStatelessTransform;

impl PhysicalOptRule for FuseStatelessTransform {
    fn name(&self) -> &str {
        "fuse_stateless_transform"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        // Count edges in the DAG, not Arc owners: previous rules rebuild
        // shared nodes while preserving their plan indices.
        fn count_consumers(
            plan: &PhysicalPlan,
            visited: &mut HashSet<i64>,
            consumers: &mut HashMap<i64, usize>,
        ) {
            if !visited.insert(plan.get_plan_index()) {
                return;
            }
            for child in plan.children() {
                *consumers.entry(child.get_plan_index()).or_default() += 1;
                count_consumers(child, visited, consumers);
            }
        }
        let mut consumers = HashMap::new();
        count_consumers(&plan, &mut HashSet::new(), &mut consumers);
        fuse_stateless_transform(plan, &consumers)
    }
}

fn fuse_stateless_transform(
    plan: Arc<PhysicalPlan>,
    consumers: &HashMap<i64, usize>,
) -> Arc<PhysicalPlan> {
    let children: Vec<Arc<PhysicalPlan>> = plan
        .children()
        .iter()
        .map(|child| fuse_stateless_transform(Arc::clone(child), consumers))
        .collect();

    match plan.as_ref() {
        PhysicalPlan::SinkConnector(_) | PhysicalPlan::DataSink(_) => {
            if let Some(fused) = try_fuse_stateless_transform(plan.as_ref(), &children, consumers) {
                return fused;
            }
            rebuild_with_children(plan.as_ref(), children)
        }
        _ => rebuild_with_children(plan.as_ref(), children),
    }
}

fn try_fuse_stateless_transform(
    plan: &PhysicalPlan,
    children: &[Arc<PhysicalPlan>],
    consumers: &HashMap<i64, usize>,
) -> Option<Arc<PhysicalPlan>> {
    let sink = match plan {
        PhysicalPlan::SinkConnector(sink) | PhysicalPlan::DataSink(sink) => sink,
        _ => return None,
    };
    if children.len() != 1 {
        return None;
    }
    // These connectors can impose a delivery-size limit. Keep their byte
    // stream visible to SinkProcessor so it checks each piece before the
    // batch completes; custom connector limits are unknown to the planner.
    if matches!(
        sink.connector.connector,
        crate::planner::sink::SinkConnectorConfig::Http(_)
            | crate::planner::sink::SinkConnectorConfig::Mqtt(_)
            | crate::planner::sink::SinkConnectorConfig::Custom(_)
    ) {
        return None;
    }
    // Walk down from the connector, collecting one optional stage of each
    // kind. Shapes rejected upstream (e.g. file without an encoder) never
    // reach this rule. Anything else bails.
    let mut node = children[0].as_ref();
    let mut encrypt = None;
    if let PhysicalPlan::SinkEncrypt(encrypt_node) = node {
        if encrypt_node.base.children.len() != 1 {
            return None;
        }
        encrypt = Some(crate::codec::encrypt::SinkEncryptionConfig {
            algorithm: encrypt_node.algorithm,
            key_id: encrypt_node.key_id.clone(),
            key_bits: encrypt_node.key_bits,
            key: Arc::clone(&encrypt_node.key),
        });
        node = encrypt_node.base.children.first()?.as_ref();
    }
    let mut compress = None;
    if let PhysicalPlan::SinkCompress(compress_node) = node {
        if compress_node.base.children.len() != 1 {
            return None;
        }
        compress = Some(compress_node.codec.clone());
        node = compress_node.base.children.first()?.as_ref();
    }
    // Optional Immediate encoder below the optional transforms. Batching is
    // supported in both pre-fusion shapes: standalone `Batch` (encoder none)
    // and `Batch` already fused into `IncSinkEncoder` by
    // `StreamingEncoderRewrite` (this rule runs after it).
    //
    // `peel_post_project` accepts the optional Order / EmptySuppress nodes
    // sitting between the projection and the encoder (in either order, at
    // most once each) and returns the projection underneath.
    fn peel_post_project(
        node: &PhysicalPlan,
    ) -> Option<(bool, crate::planner::physical::PhysicalProject)> {
        let mut node = node;
        let mut empty_suppress_omit = false;
        // EmptySuppress sits directly above the projection (sink-level
        // omit-if-empty); anything else below the encoder must be Project.
        if let PhysicalPlan::EmptySuppress(suppress_node) = node {
            if suppress_node.base.children.len() != 1 {
                return None;
            }
            empty_suppress_omit = suppress_node.omit_if_empty;
            node = suppress_node.base.children.first()?.as_ref();
        }
        let PhysicalPlan::Project(project) = node else {
            return None;
        };
        Some((empty_suppress_omit, project.clone()))
    }
    let (encoder, encoder_output_layout, batch, project_node, empty_suppress_omit) = match node {
        PhysicalPlan::SinkEncoder(encoder) => {
            if encoder.base.children.len() != 1 {
                return None;
            }
            let grandchild = encoder.base.children.first()?;
            let (empty_suppress_omit, project) = peel_post_project(grandchild.as_ref())?;
            (
                Some(encoder.encoder.clone()),
                encoder.output_layout.clone(),
                None,
                project,
                empty_suppress_omit,
            )
        }
        PhysicalPlan::IncSinkEncoder(encoder) => {
            if encoder.base.children.len() != 1 {
                return None;
            }
            let grandchild = encoder.base.children.first()?;
            let (empty_suppress_omit, project) = peel_post_project(grandchild.as_ref())?;
            (
                Some(encoder.encoder.clone()),
                encoder.output_layout.clone(),
                Some((encoder.common.batch_count, encoder.common.batch_duration)),
                project,
                empty_suppress_omit,
            )
        }
        PhysicalPlan::Batch(batch) => {
            if batch.base.children.len() != 1 {
                return None;
            }
            let grandchild = batch.base.children.first()?;
            let (empty_suppress_omit, project) = peel_post_project(grandchild.as_ref())?;
            (
                None,
                None,
                Some((batch.common.batch_count, batch.common.batch_duration)),
                project,
                empty_suppress_omit,
            )
        }
        PhysicalPlan::Project(project) => (None, None, None, project.clone(), false),
        _ => return None,
    };
    let project_children = project_node.base.children.clone();
    if project_children.len() != 1 {
        return None;
    }
    // Optional Order / Filter below the projection (at most one each,
    // in either order -- ORDER BY sorts source rows, WHERE filters them).
    // The fused node takes over the innermost input as its own child.
    // Pipeline-state counters ride along untouched: the fused task threads
    // the same state handles into the same stage functions.
    let mut node = project_children[0].as_ref();
    let mut order = None;
    let mut filter = None;
    let mut filter_before_order = true;
    for _ in 0..2 {
        match node {
            PhysicalPlan::Order(order_node) => {
                if order.is_some() || order_node.base.children.len() != 1 {
                    return None;
                }
                order = Some(order_node.clone());
                // Peel [.., Filter, Order]: Order is deeper, so it runs first.
                if filter.is_some() {
                    filter_before_order = false;
                }
                node = order_node.base.children.first()?.as_ref();
            }
            PhysicalPlan::Filter(filter_node) => {
                if filter.is_some() || filter_node.base.children.len() != 1 {
                    return None;
                }
                filter = Some(filter_node.clone());
                // Peel [.., Order, Filter]: Filter is deeper, so it runs first.
                if order.is_some() {
                    filter_before_order = true;
                }
                node = filter_node.base.children.first()?.as_ref();
            }
            _ => break,
        }
    }
    // Never duplicate a shared stage into per-sink tasks. Besides extra
    // work, cloned pipeline-state handles would advance once per sink.
    let mut consumed = children[0].as_ref();
    while consumed.get_plan_index() != node.get_plan_index() {
        if consumers
            .get(&consumed.get_plan_index())
            .copied()
            .unwrap_or(0)
            > 1
        {
            return None;
        }
        consumed = consumed.children().first()?.as_ref();
    }
    let input = Arc::new(node.clone());
    // The fused node takes the index of the outermost consumed stage; the
    // sink connector node above it is untouched (external I/O never fuses).
    let fused_index = children[0].get_plan_index();
    let (batch_count, batch_duration) = batch.unwrap_or((None, None));
    let fused = crate::planner::physical::PhysicalFusedTransform::new(
        input,
        fused_index,
        sink.connector.sink_id.clone(),
        filter,
        project_node,
        order,
        filter_before_order,
        empty_suppress_omit,
        encoder,
        encoder_output_layout,
        compress,
        encrypt,
        batch_count,
        batch_duration,
    );
    let mut new_sink = sink.clone();
    new_sink.base.children = vec![Arc::new(PhysicalPlan::FusedTransform(fused))];
    Some(match plan {
        PhysicalPlan::SinkConnector(_) => Arc::new(PhysicalPlan::SinkConnector(new_sink)),
        _ => Arc::new(PhysicalPlan::DataSink(new_sink)),
    })
}

/// Rule: preserve final encoder output schema before rewrites delay materialization.
struct AttachEncoderOutputLayout;

/// Remove planner-only column filter nodes after all sink consumers captured their layouts.
struct RemovePlannerColumnFilter;

impl PhysicalOptRule for StreamingAggregationRewrite {
    fn name(&self) -> &str {
        "streaming_aggregation_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        self.optimize_node(plan, encoder_registry)
    }
}

impl StreamingAggregationRewrite {
    fn optimize_node(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        let optimized_children = self.optimize_children(plan.children(), encoder_registry);

        match plan.as_ref() {
            PhysicalPlan::Aggregation(agg) => {
                if let Some(child) = optimized_children.first() {
                    if let Some(streaming) =
                        self.try_fuse_streaming_agg(agg, child, encoder_registry)
                    {
                        return streaming;
                    }
                }
                rebuild_with_children(plan.as_ref(), optimized_children)
            }
            _ => rebuild_with_children(plan.as_ref(), optimized_children),
        }
    }

    fn optimize_children(
        &self,
        children: &[Arc<PhysicalPlan>],
        encoder_registry: &EncoderRegistry,
    ) -> Vec<Arc<PhysicalPlan>> {
        children
            .iter()
            .map(|child| self.optimize_node(Arc::clone(child), encoder_registry))
            .collect()
    }

    fn try_fuse_streaming_agg(
        &self,
        agg: &crate::planner::physical::PhysicalAggregation,
        child: &Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Option<Arc<PhysicalPlan>> {
        if !crate::planner::physical::PhysicalAggregation::all_calls_incremental(
            &agg.aggregate_calls,
            &self.aggregate_registry,
        ) {
            return None;
        }

        let (window_spec, upstream_child) = match child.as_ref() {
            PhysicalPlan::TumblingWindow(window) => {
                let spec = StreamingWindowSpec::Tumbling {
                    time_unit: window.time_unit,
                    length: window.length,
                    partition_by_exprs: window.partition_by_exprs.clone(),
                    partition_by_scalars: window.partition_by_scalars.clone(),
                };
                let upstream = window.base.children.first()?.clone();
                (spec, upstream)
            }
            PhysicalPlan::CountWindow(window) => {
                let spec = StreamingWindowSpec::Count {
                    count: window.count,
                    partition_by_exprs: window.partition_by_exprs.clone(),
                    partition_by_scalars: window.partition_by_scalars.clone(),
                };
                let upstream = window.base.children.first()?.clone();
                (spec, upstream)
            }
            PhysicalPlan::SlidingWindow(window) => {
                let spec = StreamingWindowSpec::Sliding {
                    time_unit: window.time_unit,
                    lookback: window.lookback,
                    lookahead: window.lookahead,
                    partition_by_exprs: window.partition_by_exprs.clone(),
                    partition_by_scalars: window.partition_by_scalars.clone(),
                    trigger_condition_expr: window.trigger_condition_expr.clone(),
                    trigger_condition_scalar: window.trigger_condition_scalar.clone(),
                    trigger_processor_state: window.trigger_processor_state.clone(),
                    trigger_state_usage: window.trigger_state_usage,
                };
                let upstream = window.base.children.first()?.clone();
                (spec, upstream)
            }
            PhysicalPlan::StateWindow(window) => {
                let spec = StreamingWindowSpec::State {
                    open_expr: window.open_expr.clone(),
                    emit_expr: window.emit_expr.clone(),
                    partition_by_exprs: window.partition_by_exprs.clone(),
                    open_scalar: window.open_scalar.clone(),
                    emit_scalar: window.emit_scalar.clone(),
                    partition_by_scalars: window.partition_by_scalars.clone(),
                };
                let upstream = window.base.children.first()?.clone();
                (spec, upstream)
            }
            PhysicalPlan::EosWindow(window) => {
                let upstream = window.base.children.first()?.clone();
                (StreamingWindowSpec::Eos, upstream)
            }
            _ => return None,
        };

        let streaming = PhysicalStreamingAggregation::new(
            window_spec,
            agg.aggregate_mappings.clone(),
            agg.group_by_exprs.clone(),
            agg.aggregate_calls.clone(),
            agg.group_by_scalars.clone(),
            vec![upstream_child],
            agg.base.index(),
        );
        Some(Arc::new(PhysicalPlan::StreamingAggregation(streaming)))
    }
}

impl PhysicalOptRule for StreamingEncoderRewrite {
    fn name(&self) -> &str {
        "streaming_encoder_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        fuse_streaming_encoder(plan)
    }
}

impl PhysicalOptRule for AttachEncoderOutputLayout {
    fn name(&self) -> &str {
        "attach_encoder_output_layout"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        attach_encoder_output_layout(plan)
    }
}

impl PhysicalOptRule for RemovePlannerColumnFilter {
    fn name(&self) -> &str {
        "remove_planner_column_filter"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        remove_planner_column_filter(plan)
    }
}

fn remove_planner_column_filter(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let children = plan
        .children()
        .iter()
        .map(|child| remove_planner_column_filter(Arc::clone(child)))
        .collect::<Vec<_>>();

    if matches!(plan.as_ref(), PhysicalPlan::ColumnFilter(_)) && children.len() == 1 {
        return Arc::clone(&children[0]);
    }
    rebuild_with_children(plan.as_ref(), children)
}

fn fuse_streaming_encoder(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let children: Vec<Arc<PhysicalPlan>> = plan
        .children()
        .iter()
        .map(|child| fuse_streaming_encoder(Arc::clone(child)))
        .collect();

    match plan.as_ref() {
        PhysicalPlan::SinkEncoder(encoder) => {
            // Check if the unique child is PhysicalBatch
            if children.len() != 1 {
                return rebuild_with_children(plan.as_ref(), children);
            }
            let first_child = &children[0];
            if !matches!(first_child.as_ref(), PhysicalPlan::Batch(_)) {
                return rebuild_with_children(plan.as_ref(), children);
            }

            // Fuse PhysicalBatch into PhysicalIncSinkEncoder:
            // - take PhysicalBatch's children (skip the batch node)
            // - take PhysicalBatch's common (batch params)
            // - create PhysicalIncSinkEncoder with same sink_id, encoder, index
            if let PhysicalPlan::Batch(batch) = first_child.as_ref() {
                let batch_children = first_child.children().to_vec();
                let fused_index = encoder.base.index();
                let mut fused = crate::planner::physical::PhysicalIncSinkEncoder::new(
                    batch_children,
                    fused_index,
                    encoder.sink_id.clone(),
                    encoder.encoder.clone(),
                    batch.common.clone(),
                );
                fused.output_layout = encoder.output_layout.clone();
                Arc::new(PhysicalPlan::IncSinkEncoder(fused))
            } else {
                rebuild_with_children(plan.as_ref(), children)
            }
        }
        _ => rebuild_with_children(plan.as_ref(), children),
    }
}

fn attach_encoder_output_layout(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let children = plan
        .children()
        .iter()
        .map(|child| attach_encoder_output_layout(Arc::clone(child)))
        .collect::<Vec<_>>();

    match plan.as_ref() {
        PhysicalPlan::SinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            new.output_layout =
                encoder_output_layout_from_single_child("PhysicalSinkEncoder", &new.base.children);
            Arc::new(PhysicalPlan::SinkEncoder(new))
        }
        PhysicalPlan::IncSinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            new.output_layout = encoder_output_layout_from_single_child(
                "PhysicalIncSinkEncoder",
                &new.base.children,
            );
            Arc::new(PhysicalPlan::IncSinkEncoder(new))
        }
        _ => rebuild_with_children(plan.as_ref(), children),
    }
}

fn encoder_output_layout_from_single_child(
    encoder_plan_type: &str,
    children: &[Arc<PhysicalPlan>],
) -> Option<Arc<OutputLayout>> {
    let [child] = children else {
        return None;
    };
    match child.output_layout() {
        Ok(output_layout) => Some(Arc::new(output_layout)),
        Err(err) => {
            tracing::warn!(
                encoder_plan_type,
                child_plan_type = child.get_plan_type(),
                child_plan_index = child.get_plan_index(),
                error = %err,
                "failed to derive encoder output schema from child plan"
            );
            None
        }
    }
}

impl PhysicalOptRule for InsertBarrierForFanIn {
    fn name(&self) -> &str {
        "insert_barrier_for_fan_in"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        let mut next_index = max_physical_index(&plan) + 1;
        insert_barrier_for_fan_in(plan, &mut next_index)
    }
}

fn insert_barrier_for_fan_in(plan: Arc<PhysicalPlan>, next_index: &mut i64) -> Arc<PhysicalPlan> {
    let optimized_children = plan
        .children()
        .iter()
        .map(|child| insert_barrier_for_fan_in(Arc::clone(child), next_index))
        .collect::<Vec<_>>();

    let rebuilt = rebuild_with_children(plan.as_ref(), optimized_children);
    if matches!(rebuilt.as_ref(), PhysicalPlan::Barrier(_)) {
        return rebuilt;
    }

    if rebuilt.children().len() <= 1 {
        return rebuilt;
    }

    let barrier_index = allocate_index(next_index);
    let barrier_children = rebuilt.children().to_vec();
    let barrier = PhysicalBarrier::new(barrier_children, barrier_index);
    let barrier_node = Arc::new(PhysicalPlan::Barrier(barrier));
    rebuild_with_children(rebuilt.as_ref(), vec![barrier_node])
}

fn max_physical_index(plan: &Arc<PhysicalPlan>) -> i64 {
    let mut max_index = plan.get_plan_index();
    for child in plan.children() {
        max_index = max_index.max(max_physical_index(child));
    }
    max_index
}

fn allocate_index(next: &mut i64) -> i64 {
    let index = *next;
    *next += 1;
    index
}

fn rebuild_with_children(
    plan: &PhysicalPlan,
    children: Vec<Arc<PhysicalPlan>>,
) -> Arc<PhysicalPlan> {
    match plan {
        PhysicalPlan::DataSource(ds) => {
            let mut new = ds.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::DataSource(new))
        }
        PhysicalPlan::TableScan(scan) => {
            let mut new = scan.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::TableScan(new))
        }
        PhysicalPlan::Decoder(decoder) => {
            let mut new = decoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Decoder(new))
        }
        PhysicalPlan::CollectionLayoutNormalize(normalize) => {
            let mut new = normalize.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::CollectionLayoutNormalize(new))
        }
        PhysicalPlan::MemoryCollectionMaterialize(materialize) => {
            let mut new = materialize.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::MemoryCollectionMaterialize(new))
        }
        PhysicalPlan::StatefulFunction(stateful) => {
            let mut new = stateful.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StatefulFunction(new))
        }
        PhysicalPlan::SharedStream(stream) => {
            let mut new = stream.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SharedStream(new))
        }
        PhysicalPlan::SourceChangeGate(gate) => {
            let mut new = gate.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SourceChangeGate(new))
        }
        PhysicalPlan::Filter(filter) => {
            let mut new = filter.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Filter(new))
        }
        PhysicalPlan::LookupJoin(join) => {
            let mut new = join.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::LookupJoin(new))
        }
        PhysicalPlan::Compute(compute) => {
            let mut new = compute.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Compute(new))
        }
        PhysicalPlan::BatchFunctionCompute(compute) => {
            let mut new = compute.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::BatchFunctionCompute(new))
        }
        PhysicalPlan::Order(order) => {
            let mut new = order.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Order(new))
        }
        PhysicalPlan::Project(project) => {
            let mut new = project.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Project(new))
        }
        PhysicalPlan::RowDiff(row_diff) => {
            let mut new = row_diff.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::RowDiff(new))
        }
        PhysicalPlan::ColumnFilter(filter) => {
            let mut new = filter.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::ColumnFilter(new))
        }
        PhysicalPlan::EmptySuppress(empty_suppress) => {
            let mut new = empty_suppress.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::EmptySuppress(new))
        }
        PhysicalPlan::Aggregation(agg) => {
            let mut new = agg.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Aggregation(new))
        }
        PhysicalPlan::Batch(batch) => {
            let mut new = batch.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Batch(new))
        }
        PhysicalPlan::SinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SinkEncoder(new))
        }

        PhysicalPlan::IncSinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::IncSinkEncoder(new))
        }
        PhysicalPlan::SinkCompress(compress) => {
            let mut new = compress.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SinkCompress(new))
        }
        PhysicalPlan::SinkEncrypt(encrypt) => {
            let mut new = encrypt.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SinkEncrypt(new))
        }
        PhysicalPlan::StreamingAggregation(agg) => {
            let mut new = agg.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StreamingAggregation(new))
        }
        PhysicalPlan::ResultCollect(collect) => {
            let mut new = collect.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::ResultCollect(new))
        }
        PhysicalPlan::Barrier(barrier) => {
            let mut new = barrier.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Barrier(new))
        }
        PhysicalPlan::TumblingWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::TumblingWindow(new))
        }
        PhysicalPlan::ProcessTimeWatermark(watermark) => {
            let mut new = watermark.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::ProcessTimeWatermark(new))
        }
        PhysicalPlan::EventtimeWatermark(watermark) => {
            let mut new = watermark.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::EventtimeWatermark(new))
        }
        PhysicalPlan::Watermark(watermark) => {
            let mut new = watermark.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Watermark(new))
        }
        PhysicalPlan::CountWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::CountWindow(new))
        }
        PhysicalPlan::SlidingWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SlidingWindow(new))
        }
        PhysicalPlan::StateWindow(window) => {
            let mut new = window.as_ref().clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StateWindow(Box::new(new)))
        }
        PhysicalPlan::EosWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::EosWindow(new))
        }
        PhysicalPlan::DataSink(sink) => {
            let mut new = sink.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::DataSink(new))
        }
        PhysicalPlan::SinkConnector(sink) => {
            let mut new = sink.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SinkConnector(new))
        }
        PhysicalPlan::FusedTransform(fused) => {
            let mut new = fused.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::FusedTransform(new))
        }
        PhysicalPlan::Sampler(sampler) => {
            let mut new = sampler.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Sampler(new))
        }
    }
}
