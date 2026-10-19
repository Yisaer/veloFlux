//! Fixed task placement, independent of physical semantic rewrites.
//!
//! Regions contain ordered stage identities, not copies of physical subtrees.
//! Missing adapters retain local boundaries while other paths still fuse.

use super::physical::PhysicalPlan;
use crate::expr::ScalarExpr;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const EXECUTION_STRATEGY_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionClass {
    Engine,
    Source,
    Sink,
    ExternalLookup,
    OpaqueFunction,
    SharedRuntime,
    Coordination,
    PlannerOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryReason {
    PolicyExternalIo,
    PolicySinkConnector,
    PolicyOpaqueUdf,
    ExternalOwner,
    FanOut,
    FanIn,
    Coordination,
    StageAdapterMissing,
    PlannerOnly,
}

impl BoundaryReason {
    pub fn category(self) -> &'static str {
        match self {
            Self::PolicyExternalIo | Self::PolicySinkConnector | Self::PolicyOpaqueUdf => "policy",
            Self::StageAdapterMissing => "implementation",
            Self::ExternalOwner
            | Self::FanOut
            | Self::FanIn
            | Self::Coordination
            | Self::PlannerOnly => "contract",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionStage {
    pub index: i64,
    pub name: String,
    pub class: ExecutionClass,
    pub inline_adapter: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionRegion {
    /// The output stage's identity also identifies a singleton region.
    pub output: i64,
    /// Input-to-output execution order.
    pub stages: Vec<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionLink {
    pub upstream: i64,
    pub downstream: i64,
    pub input_port: usize,
    pub reason: BoundaryReason,
    pub category: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionPlan {
    pub strategy_version: u32,
    pub stages: Vec<ExecutionStage>,
    pub regions: Vec<ExecutionRegion>,
    pub links: Vec<ExecutionLink>,
    #[serde(skip)]
    stage_regions: BTreeMap<i64, usize>,
}

fn collect_definitions(
    root: &Arc<PhysicalPlan>,
) -> Result<BTreeMap<i64, Arc<PhysicalPlan>>, String> {
    fn collect(
        node: &Arc<PhysicalPlan>,
        nodes: &mut BTreeMap<i64, Arc<PhysicalPlan>>,
        active: &mut BTreeSet<i64>,
    ) -> Result<(), String> {
        let index = node.get_plan_index();
        if active.contains(&index) {
            return Err(format!("cycle in execution graph at stage {index}"));
        }
        if let Some(previous) = nodes.get(&index) {
            // One physical identity has one immutable definition. Rewrites
            // preserve shared Arcs. Even apparently equivalent independent
            // definitions are ambiguous (custom runtime/state/key ownership)
            // and must never be silently merged by index or Debug output.
            if !Arc::ptr_eq(previous, node) {
                return Err(format!(
                    "duplicate execution graph definition for stage {index}; reuse its Arc"
                ));
            }
            return Ok(());
        }
        nodes.insert(index, Arc::clone(node));
        active.insert(index);
        for child in node.children() {
            collect(child, nodes, active)?;
        }
        active.remove(&index);
        Ok(())
    }
    let mut nodes = BTreeMap::new();
    collect(root, &mut nodes, &mut BTreeSet::new())?;
    Ok(nodes)
}

pub(crate) fn validate_graph_definitions(root: &Arc<PhysicalPlan>) -> Result<(), String> {
    collect_definitions(root).map(|_| ())
}

impl ExecutionPlan {
    /// Derive deterministic placement from the actual DAG, counting edges rather
    /// than Arc owners. External source/sink ownership is never fused away.
    pub fn build(root: &Arc<PhysicalPlan>) -> Result<Self, String> {
        let nodes = collect_definitions(root)?;
        let stages: Vec<_> = nodes
            .values()
            .map(|node| ExecutionStage {
                index: node.get_plan_index(),
                name: node.get_plan_name(),
                class: classify_stage(node),
                inline_adapter: has_inline_adapter(node),
            })
            .collect();
        let by_index: BTreeMap<_, _> = stages.iter().map(|s| (s.index, s)).collect();
        let mut consumers = BTreeMap::<i64, usize>::new();
        for node in nodes.values() {
            for child in node.children() {
                *consumers.entry(child.get_plan_index()).or_default() += 1;
            }
        }
        let mut next = BTreeMap::new();
        let mut previous = BTreeMap::new();
        let mut links = Vec::new();
        for node in nodes.values() {
            let downstream = node.get_plan_index();
            for (input_port, child) in node.children().iter().enumerate() {
                let upstream = child.get_plan_index();
                let from = by_index[&upstream];
                let to = by_index[&downstream];
                let reason = policy_boundary(from.class)
                    .or_else(|| policy_boundary(to.class))
                    .or_else(|| (consumers[&upstream] > 1).then_some(BoundaryReason::FanOut))
                    .or_else(|| {
                        (node.children().len() > 1
                            || matches!(node.as_ref(), PhysicalPlan::Barrier(_)))
                        .then_some(BoundaryReason::FanIn)
                    })
                    .or_else(|| {
                        (!from.inline_adapter || !to.inline_adapter)
                            .then_some(BoundaryReason::StageAdapterMissing)
                    });
                if let Some(reason) = reason {
                    links.push(ExecutionLink {
                        upstream,
                        downstream,
                        input_port,
                        reason,
                        category: reason.category(),
                    });
                } else {
                    next.insert(upstream, downstream);
                    previous.insert(downstream, upstream);
                }
            }
        }
        let mut regions = Vec::new();
        let mut stage_regions = BTreeMap::new();
        for &index in nodes.keys() {
            if previous.contains_key(&index) {
                continue;
            }
            let mut path = vec![index];
            while let Some(&following) = next.get(path.last().expect("nonempty region")) {
                path.push(following);
            }
            for &stage in &path {
                if stage_regions.insert(stage, regions.len()).is_some() {
                    return Err(format!("stage {stage} belongs to several regions"));
                }
            }
            regions.push(ExecutionRegion {
                output: *path.last().expect("nonempty region"),
                stages: path,
            });
        }
        if stage_regions.len() != nodes.len() {
            return Err("execution regions do not cover the physical graph".into());
        }
        Ok(Self {
            strategy_version: EXECUTION_STRATEGY_VERSION,
            stages,
            regions,
            links,
            stage_regions,
        })
    }

    pub fn region_for(&self, stage: i64) -> &ExecutionRegion {
        &self.regions[self.stage_regions[&stage]]
    }
}

fn policy_boundary(class: ExecutionClass) -> Option<BoundaryReason> {
    match class {
        ExecutionClass::Engine => None,
        ExecutionClass::Source | ExecutionClass::ExternalLookup => {
            Some(BoundaryReason::PolicyExternalIo)
        }
        ExecutionClass::Sink => Some(BoundaryReason::PolicySinkConnector),
        ExecutionClass::OpaqueFunction => Some(BoundaryReason::PolicyOpaqueUdf),
        ExecutionClass::SharedRuntime => Some(BoundaryReason::ExternalOwner),
        ExecutionClass::Coordination => Some(BoundaryReason::Coordination),
        ExecutionClass::PlannerOnly => Some(BoundaryReason::PlannerOnly),
    }
}

/// Adapter availability is separate from policy: a missing window adapter is
/// implementation debt, not a claim that state requires a separate task.
pub(crate) fn has_inline_adapter(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Sampler(_)
            | PhysicalPlan::SinkEncoder(_)
            | PhysicalPlan::IncSinkEncoder(_)
            | PhysicalPlan::SinkCompress(_)
            | PhysicalPlan::SinkEncrypt(_)
            | PhysicalPlan::Batch(_)
            | PhysicalPlan::Decoder(_)
            | PhysicalPlan::Barrier(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::Project(_)
            | PhysicalPlan::Compute(_)
            | PhysicalPlan::Order(_)
            | PhysicalPlan::EmptySuppress(_)
            | PhysicalPlan::Watermark(_)
            | PhysicalPlan::ProcessTimeWatermark(_)
            | PhysicalPlan::EventtimeWatermark(_)
            | PhysicalPlan::TumblingWindow(_)
            | PhysicalPlan::CountWindow(_)
            | PhysicalPlan::Aggregation(_)
            | PhysicalPlan::StatefulFunction(_)
            | PhysicalPlan::EosWindow(_)
            | PhysicalPlan::StateWindow(_)
            | PhysicalPlan::SlidingWindow(_)
            | PhysicalPlan::StreamingAggregation(_)
            | PhysicalPlan::CollectionLayoutNormalize(_)
            | PhysicalPlan::MemoryCollectionMaterialize(_)
            | PhysicalPlan::RowDiff(_)
            | PhysicalPlan::SourceChangeGate(_)
            | PhysicalPlan::BatchFunctionCompute(_)
    )
}

pub(crate) fn classify_stage(plan: &PhysicalPlan) -> ExecutionClass {
    if contains_isolated_function(plan) {
        return ExecutionClass::OpaqueFunction;
    }
    match plan {
        PhysicalPlan::DataSource(_) | PhysicalPlan::TableScan(_) => ExecutionClass::Source,
        PhysicalPlan::DataSink(_) | PhysicalPlan::SinkConnector(_) => ExecutionClass::Sink,
        PhysicalPlan::LookupJoin(_) => ExecutionClass::ExternalLookup,
        PhysicalPlan::SharedStream(_) => ExecutionClass::SharedRuntime,
        PhysicalPlan::ResultCollect(_) => ExecutionClass::Coordination,
        PhysicalPlan::ColumnFilter(_) => ExecutionClass::PlannerOnly,
        PhysicalPlan::Decoder(_)
        | PhysicalPlan::CollectionLayoutNormalize(_)
        | PhysicalPlan::MemoryCollectionMaterialize(_)
        | PhysicalPlan::StatefulFunction(_)
        | PhysicalPlan::Filter(_)
        | PhysicalPlan::Compute(_)
        | PhysicalPlan::BatchFunctionCompute(_)
        | PhysicalPlan::Order(_)
        | PhysicalPlan::Project(_)
        | PhysicalPlan::RowDiff(_)
        | PhysicalPlan::EmptySuppress(_)
        | PhysicalPlan::Aggregation(_)
        | PhysicalPlan::SourceChangeGate(_)
        | PhysicalPlan::Batch(_)
        | PhysicalPlan::SinkEncoder(_)
        | PhysicalPlan::IncSinkEncoder(_)
        | PhysicalPlan::SinkCompress(_)
        | PhysicalPlan::SinkEncrypt(_)
        | PhysicalPlan::StreamingAggregation(_)
        | PhysicalPlan::Barrier(_)
        | PhysicalPlan::TumblingWindow(_)
        | PhysicalPlan::CountWindow(_)
        | PhysicalPlan::SlidingWindow(_)
        | PhysicalPlan::StateWindow(_)
        | PhysicalPlan::EosWindow(_)
        | PhysicalPlan::ProcessTimeWatermark(_)
        | PhysicalPlan::EventtimeWatermark(_)
        | PhysicalPlan::Watermark(_)
        | PhysicalPlan::Sampler(_) => ExecutionClass::Engine,
    }
}

/// Preserve guarded evaluation by isolating the owning expression stage. No
/// function call is extracted or executed while classifying the plan.
pub(crate) fn contains_isolated_function(plan: &PhysicalPlan) -> bool {
    let isolated = ScalarExpr::requires_execution_isolation;
    match plan {
        PhysicalPlan::SinkEncoder(p) => !p.supports_inline_execution,
        PhysicalPlan::IncSinkEncoder(p) => !p.supports_inline_execution,
        PhysicalPlan::Sampler(p) => !p.supports_inline_execution,
        PhysicalPlan::Decoder(p) => !p.supports_inline_execution,
        PhysicalPlan::Filter(p) => isolated(&p.scalar_predicate),
        PhysicalPlan::Project(p) => p.fields.iter().any(|f| isolated(&f.compiled_expr)),
        PhysicalPlan::Compute(p) => p.fields.iter().any(|f| isolated(&f.compiled_expr)),
        PhysicalPlan::Order(p) => p.keys.iter().any(|k| isolated(&k.compiled_expr)),
        PhysicalPlan::BatchFunctionCompute(p) => p
            .evaluations
            .iter()
            .any(|e| !e.function.supports_inline_execution() || e.args.iter().any(isolated)),
        PhysicalPlan::Aggregation(p) => {
            p.group_by_scalars.iter().any(isolated)
                || p.aggregate_calls
                    .iter()
                    .any(|c| !c.supports_inline_execution || c.args.iter().any(isolated))
        }
        PhysicalPlan::StatefulFunction(p) => p.calls.iter().any(|c| {
            !c.supports_inline_execution
                || c.arg_scalars
                    .iter()
                    .chain(&c.partition_by_scalars)
                    .any(isolated)
                || c.when_scalar.as_ref().is_some_and(isolated)
                || c.until_scalar.as_ref().is_some_and(isolated)
        }),
        PhysicalPlan::TumblingWindow(p) => p.partition_by_scalars.iter().any(isolated),
        PhysicalPlan::CountWindow(p) => p.partition_by_scalars.iter().any(isolated),
        PhysicalPlan::SlidingWindow(p) => {
            p.partition_by_scalars.iter().any(isolated)
                || p.trigger_condition_scalar.as_ref().is_some_and(isolated)
        }
        PhysicalPlan::StateWindow(p) => {
            p.partition_by_scalars.iter().any(isolated)
                || isolated(&p.open_scalar)
                || isolated(&p.emit_scalar)
        }
        PhysicalPlan::StreamingAggregation(p) => {
            use super::physical::StreamingWindowSpec;
            p.group_by_scalars.iter().any(isolated)
                || p.aggregate_calls
                    .iter()
                    .any(|c| !c.supports_inline_execution || c.args.iter().any(isolated))
                || match &p.window {
                    StreamingWindowSpec::Tumbling {
                        partition_by_scalars,
                        ..
                    }
                    | StreamingWindowSpec::Count {
                        partition_by_scalars,
                        ..
                    } => partition_by_scalars.iter().any(isolated),
                    StreamingWindowSpec::Sliding {
                        partition_by_scalars,
                        trigger_condition_scalar,
                        ..
                    } => {
                        partition_by_scalars.iter().any(isolated)
                            || trigger_condition_scalar.as_ref().is_some_and(isolated)
                    }
                    StreamingWindowSpec::State {
                        partition_by_scalars,
                        open_scalar,
                        emit_scalar,
                        ..
                    } => {
                        partition_by_scalars.iter().any(isolated)
                            || isolated(open_scalar)
                            || isolated(emit_scalar)
                    }
                    StreamingWindowSpec::Eos => false,
                }
        }
        PhysicalPlan::DataSource(_)
        | PhysicalPlan::TableScan(_)
        | PhysicalPlan::CollectionLayoutNormalize(_)
        | PhysicalPlan::MemoryCollectionMaterialize(_)
        | PhysicalPlan::LookupJoin(_)
        | PhysicalPlan::RowDiff(_)
        | PhysicalPlan::ColumnFilter(_)
        | PhysicalPlan::EmptySuppress(_)
        | PhysicalPlan::SharedStream(_)
        | PhysicalPlan::SourceChangeGate(_)
        | PhysicalPlan::Batch(_)
        | PhysicalPlan::DataSink(_)
        | PhysicalPlan::SinkConnector(_)
        | PhysicalPlan::SinkCompress(_)
        | PhysicalPlan::SinkEncrypt(_)
        | PhysicalPlan::ResultCollect(_)
        | PhysicalPlan::Barrier(_)
        | PhysicalPlan::EosWindow(_)
        | PhysicalPlan::ProcessTimeWatermark(_)
        | PhysicalPlan::EventtimeWatermark(_)
        | PhysicalPlan::Watermark(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::physical::{
        PhysicalCompute, PhysicalDataSource, PhysicalFilter, PhysicalSampler,
    };
    use datatypes::{BooleanType, ConcreteDatatype, Schema, Value};
    use sqlparser::ast::{Expr, Value as SqlValue};

    fn source() -> Arc<PhysicalPlan> {
        Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "input".into(),
            Arc::new(Schema::new(vec![])),
            None,
            0,
        )))
    }

    fn filter(child: Arc<PhysicalPlan>, index: i64) -> Arc<PhysicalPlan> {
        Arc::new(PhysicalPlan::Filter(PhysicalFilter::new(
            Expr::Value(SqlValue::Boolean(true)),
            ScalarExpr::Literal(Value::Bool(true), ConcreteDatatype::Bool(BooleanType)),
            vec![child],
            index,
        )))
    }

    fn compute(children: Vec<Arc<PhysicalPlan>>, index: i64) -> Arc<PhysicalPlan> {
        Arc::new(PhysicalPlan::Compute(PhysicalCompute::new(
            vec![],
            children,
            index,
        )))
    }

    #[test]
    fn timed_stages_fuse_with_computation_on_both_sides() {
        for sampler in [false, true] {
            let prefix = compute(vec![filter(source(), 1)], 2);
            let boundary = Arc::new(if sampler {
                PhysicalPlan::Sampler(PhysicalSampler::new(
                    std::time::Duration::from_secs(1),
                    crate::processor::SamplingStrategy::Latest,
                    None,
                    vec![prefix],
                    3,
                ))
            } else {
                PhysicalPlan::Batch(crate::planner::physical::PhysicalBatch::new(
                    vec![prefix],
                    3,
                    "batch".into(),
                    crate::planner::sink::CommonSinkProps {
                        batch_count: Some(10),
                        batch_duration: Some(std::time::Duration::from_secs(1)),
                        ..Default::default()
                    },
                ))
            });
            let root = compute(vec![filter(boundary, 4)], 5);
            let plan = ExecutionPlan::build(&root).unwrap();
            assert_eq!(plan.region_for(3).stages, vec![1, 2, 3, 4, 5]);
            assert!(plan
                .links
                .iter()
                .all(|link| link.reason != BoundaryReason::StageAdapterMissing));
            assert_eq!(
                serde_json::to_value(&plan).unwrap(),
                serde_json::to_value(ExecutionPlan::build(&root).unwrap()).unwrap()
            );
        }
    }

    #[test]
    fn shared_prefix_executes_once_and_topology_cuts_are_local() {
        let shared = filter(filter(source(), 1), 2);
        let root = compute(vec![filter(Arc::clone(&shared), 3), filter(shared, 4)], 5);
        let plan = ExecutionPlan::build(&root).unwrap();
        assert_eq!(plan.stages.len(), 6);
        assert_eq!(plan.region_for(1).stages, vec![1, 2]);
        assert_eq!(
            plan.links
                .iter()
                .filter(|l| l.reason == BoundaryReason::FanOut)
                .count(),
            2
        );
        assert_eq!(
            plan.links
                .iter()
                .filter(|l| l.reason == BoundaryReason::FanIn)
                .count(),
            2
        );
        assert_eq!(
            plan.regions
                .iter()
                .flat_map(|r| &r.stages)
                .filter(|&&i| i == 2)
                .count(),
            1
        );
    }

    #[test]
    fn duplicate_definitions_are_rejected_before_or_after_optimization() {
        // Same type/topology/index is insufficient, even when the definitions
        // currently happen to look equivalent. Sharing means reusing the Arc.
        for changed in [false, true] {
            let shared = filter(source(), 1);
            let mut duplicate = shared.as_ref().clone();
            if changed {
                let PhysicalPlan::Filter(filter) = &mut duplicate else {
                    unreachable!()
                };
                filter.scalar_predicate =
                    ScalarExpr::Literal(Value::Bool(false), ConcreteDatatype::Bool(BooleanType));
            }
            let root = compute(vec![shared, Arc::new(duplicate)], 2);
            let optimized = crate::planner::optimize_physical_plan(
                Arc::clone(&root),
                &crate::codec::EncoderRegistry::with_builtin_encoders(),
                crate::aggregation::AggregateFunctionRegistry::with_builtins(),
            );
            for plan in [root, optimized] {
                let error = ExecutionPlan::build(&plan).unwrap_err();
                assert!(error.contains("definition for stage 1"));
                assert!(!error.contains("Bool"));
            }
        }
    }

    #[test]
    fn optimizer_preserves_shared_fan_in_definition_and_barrier() {
        let left = source();
        let right = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "right".into(),
            Arc::new(Schema::new(vec![])),
            None,
            1,
        )));
        let shared = compute(vec![left, right], 2);
        let root = compute(vec![filter(Arc::clone(&shared), 3), filter(shared, 4)], 5);
        let optimized = crate::planner::optimize_physical_plan(
            root,
            &crate::codec::EncoderRegistry::with_builtin_encoders(),
            crate::aggregation::AggregateFunctionRegistry::with_builtins(),
        );
        let barrier = &optimized.children()[0];
        let first = &barrier.children()[0].children()[0];
        let second = &barrier.children()[1].children()[0];
        assert!(Arc::ptr_eq(first, second));
        let execution = ExecutionPlan::build(&optimized).unwrap();
        assert_eq!(
            execution
                .stages
                .iter()
                .filter(|stage| stage.index == 2)
                .count(),
            1
        );
        assert_eq!(
            execution
                .stages
                .iter()
                .filter(|stage| stage.name.starts_with("PhysicalBarrier_"))
                .count(),
            2
        );
    }

    #[test]
    fn semantic_rewrites_keep_shared_window_and_batch_owners() {
        use crate::planner::physical::{
            PhysicalAggregation, PhysicalBatch, PhysicalCountWindow, PhysicalSinkEncoder,
        };
        use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
        for encoder in [false, true] {
            let shared = Arc::new(if encoder {
                PhysicalPlan::Batch(PhysicalBatch::new(
                    vec![source()],
                    1,
                    "sink".into(),
                    CommonSinkProps {
                        batch_count: Some(2),
                        batch_duration: None,
                    },
                ))
            } else {
                PhysicalPlan::CountWindow(PhysicalCountWindow::new(2, vec![source()], 1))
            });
            let owner = Arc::new(if encoder {
                PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
                    vec![Arc::clone(&shared)],
                    2,
                    "sink".into(),
                    SinkEncoderConfig::json(),
                    CommonSinkProps::default(),
                ))
            } else {
                PhysicalPlan::Aggregation(PhysicalAggregation {
                    base: crate::planner::physical::BasePhysicalPlan::new(
                        vec![Arc::clone(&shared)],
                        2,
                    ),
                    aggregate_mappings: Default::default(),
                    aggregate_calls: vec![],
                    group_by_exprs: vec![],
                    group_by_scalars: vec![],
                })
            });
            let root = compute(vec![owner, compute(vec![shared], 3)], 4);
            let optimized = crate::planner::optimize_physical_plan(
                root,
                &crate::codec::EncoderRegistry::with_builtin_encoders(),
                crate::aggregation::AggregateFunctionRegistry::with_builtins(),
            );
            let barrier = &optimized.children()[0];
            let owner = &barrier.children()[0];
            assert!(matches!(
                owner.as_ref(),
                PhysicalPlan::SinkEncoder(_) | PhysicalPlan::Aggregation(_)
            ));
            assert_eq!(owner.children()[0].get_plan_index(), 1);
            assert!(Arc::ptr_eq(
                &owner.children()[0],
                &barrier.children()[1].children()[0]
            ));
            ExecutionPlan::build(&optimized).unwrap();
        }
    }

    #[test]
    fn opaque_function_isolates_the_owning_guarded_expression() {
        let registry = crate::expr::custom_func::CustomFuncRegistry::default();
        assert!(registry.get("abs").unwrap().supports_inline_execution());
        let delay = registry.get("delay").unwrap();
        assert!(!delay.supports_inline_execution());
        let mut root = filter(source(), 1).as_ref().clone();
        let PhysicalPlan::Filter(p) = &mut root else {
            unreachable!()
        };
        p.scalar_predicate = ScalarExpr::Case {
            operand: None,
            when_then: vec![(
                ScalarExpr::Literal(Value::Bool(false), ConcreteDatatype::Bool(BooleanType)),
                ScalarExpr::CallFunc {
                    func: delay,
                    args: vec![],
                },
            )],
            else_expr: None,
        };
        assert_eq!(classify_stage(&root), ExecutionClass::OpaqueFunction);
        let root = compute(vec![Arc::new(root)], 2);
        let plan = ExecutionPlan::build(&root).unwrap();
        assert_eq!(plan.regions.len(), 3);
        assert!(plan
            .links
            .iter()
            .any(|l| l.reason == BoundaryReason::PolicyOpaqueUdf));
    }
}
