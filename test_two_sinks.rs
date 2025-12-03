use flow::planner::logical::create_logical_plan;
use flow::planner::physical::create_physical_plan;
use flow::planner::sink::{PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig};
use parser::parse_sql;
use std::sync::Arc;

fn main() {
    // Parse SQL
    let sql = "SELECT * FROM stream";
    let select_stmt = parse_sql(sql).unwrap();

    // Create two different sinks
    let sink1 = PipelineSink::new(
        "sink1",
        PipelineSinkConnector::new(
            "conn1",
            SinkConnectorConfig::Nop(Default::default()),
            SinkEncoderConfig::Json { encoder_id: "json1".to_string() },
        ),
    );

    let sink2 = PipelineSink::new(
        "sink2",
        PipelineSinkConnector::new(
            "conn2",
            SinkConnectorConfig::Nop(Default::default()),
            SinkEncoderConfig::Json { encoder_id: "json2".to_string() },
        ),
    );

    // Create logical plan with two sinks
    let logical_plan = create_logical_plan(select_stmt, vec![sink1, sink2]).unwrap();
    
    println!("=== Logical Plan Topology ===");
    flow::planner::logical::print_logical_plan(&logical_plan, 0);
    println!("=============================");

    // Create physical plan
    let physical_plan = create_physical_plan(logical_plan, &Default::default()).unwrap();
    
    println!("\n=== Physical Plan Topology ===");
    print_physical_plan_topology(&physical_plan, 0);
    println!("==============================");

    // Analyze plan names
    analyze_plan_names(&physical_plan);
}

fn print_physical_plan_topology(plan: &Arc<flow::planner::physical::PhysicalPlan>, indent: usize) {
    let spacing = "  ".repeat(indent);
    println!("{}{} (index: {})", spacing, plan.get_plan_type(), plan.get_plan_index());
    
    for child in plan.children() {
        print_physical_plan_topology(child, indent + 1);
    }
}

fn analyze_plan_names(plan: &Arc<flow::planner::physical::PhysicalPlan>) {
    let mut plan_names = Vec::new();
    collect_plan_names(plan, &mut plan_names);
    
    println!("\n=== Plan Names Analysis ===");
    println!("All plan names: {:?}", plan_names);
    
    // Count PhysicalDataSink occurrences
    let data_sink_count = plan_names.iter()
        .filter(|name| name.starts_with("PhysicalDataSink"))
        .count();
    println!("PhysicalDataSink count: {}", data_sink_count);
    
    // Get PhysicalDataSink names
    let data_sink_names: Vec<String> = plan_names.iter()
        .filter(|name| name.starts_with("PhysicalDataSink"))
        .cloned()
        .collect();
    
    println!("PhysicalDataSink names: {:?}", data_sink_names);
    
    if data_sink_names.len() >= 2 {
        println!("Are the two PhysicalDataSink names different? {}", 
                 data_sink_names[0] != data_sink_names[1]);
    }
    
    // Check for duplicate names
    let mut name_counts = std::collections::HashMap::new();
    for name in &plan_names {
        *name_counts.entry(name.clone()).or_insert(0) += 1;
    }
    
    println!("\nName frequency:");
    for (name, count) in &name_counts {
        if *count > 1 {
            println!("  {}: {} times (DUPLICATE!)", name, count);
        } else {
            println!("  {}: {} times", name, count);
        }
    }
}

fn collect_plan_names(plan: &Arc<flow::planner::physical::PhysicalPlan>, names: &mut Vec<String>) {
    names.push(plan.get_plan_name());
    for child in plan.children() {
        collect_plan_names(child, names);
    }
}
