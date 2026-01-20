use crate::planner::physical::PhysicalPlan;
use crate::shared_stream::SharedStreamRegistry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(crate) fn shared_stream_decode_applied_snapshot(
    plan: &Arc<PhysicalPlan>,
    shared_stream_registry: &SharedStreamRegistry,
) -> HashMap<String, Vec<String>> {
    let mut names = HashSet::new();
    collect_shared_stream_names(plan, &mut names);

    let mut name_list: Vec<String> = names.into_iter().collect();
    name_list.sort();

    let mut snapshot = HashMap::new();
    for name in name_list {
        if let Ok(info) = futures::executor::block_on(shared_stream_registry.get_stream(&name)) {
            snapshot.insert(name, info.decoding_columns);
        }
    }
    snapshot
}

fn collect_shared_stream_names(plan: &Arc<PhysicalPlan>, out: &mut HashSet<String>) {
    if let PhysicalPlan::SharedStream(shared) = plan.as_ref() {
        out.insert(shared.stream_name().to_string());
        if let Some(ingest) = shared.explain_ingest_plan() {
            collect_shared_stream_names(&ingest, out);
        }
    }

    for child in plan.children() {
        collect_shared_stream_names(child, out);
    }
}
