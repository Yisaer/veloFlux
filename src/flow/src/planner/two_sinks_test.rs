#[cfg(test)]
mod two_sinks_test {
    use super::*;
    use crate::planner::logical::create_logical_plan;
    use crate::planner::physical::create_physical_plan;
    use crate::planner::sink::{PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig};
    use parser::parse_sql;
    use std::sync::Arc;

    /// Helper function to collect all plan names in the physical plan tree
    fn collect_plan_names(plan: &Arc<crate::planner::physical::PhysicalPlan>, names: &mut Vec<String>) {
        names.push(plan.get_plan_name());
        for child in plan.children() {
            collect_plan_names(child, names);
        }
    }

    /// Helper function to print physical plan topology for debugging
    fn print_physical_plan_topology(plan: &Arc<crate::planner::physical::PhysicalPlan>, indent: usize) {
        let spacing = "  ".repeat(indent);
        println!("{}{} (index: {})", spacing, plan.get_plan_type(), plan.get_plan_index());
        
        for child in plan.children() {
            print_physical_plan_topology(child, indent + 1);
        }
    }

    #[test]
    fn test_two_sinks_physical_plan_topology() {
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
        crate::planner::logical::print_logical_plan(&logical_plan, 0);
        println!("=============================");

        // Create physical plan
        let physical_plan = create_physical_plan(logical_plan, &Default::default()).unwrap();
        
        println!("\n=== Physical Plan Topology ===");
        print_physical_plan_topology(&physical_plan, 0);
        println!("==============================");

        // Collect all plan names
        let mut plan_names = Vec::new();
        collect_plan_names(&physical_plan, &mut plan_names);
        
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
            
            // This is the key assertion - they should have different indices/names
            assert_ne!(data_sink_names[0], data_sink_names[1], 
                      "Two different sinks should have different PhysicalDataSink plan names");
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
        
        // Verify that we have the expected structure
        assert!(plan_names.contains(&"PhysicalResultCollect_3".to_string()), 
                "Should have PhysicalResultCollect node");
        
        assert_eq!(data_sink_count, 2, "Should have exactly 2 PhysicalDataSink nodes");
        
        // Each sink should contribute unique encoder and sink nodes
        let mut unique_names = std::collections::HashSet::new();
        for name in &plan_names {
            if name.starts_with("PhysicalDataSink") || name.starts_with("PhysicalEncoder") {
                unique_names.insert(name.clone());
            }
        }
        
        let expected_unique_sink_related = 4; // 2 sinks * (1 DataSink + 1 Encoder)
        assert_eq!(unique_names.len(), expected_unique_sink_related, 
                  "Should have unique names for sink-related nodes");
    }
}