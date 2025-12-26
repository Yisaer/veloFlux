use datatypes::{
    ColumnSchema, ConcreteDatatype, Int64Type, Schema, StringType, StructField, StructType,
};
use flow::planner::logical::create_logical_plan;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use flow::{
    MqttStreamProps, PipelineRegistries, StreamDecoderConfig, StreamDefinition, StreamProps,
};
use parser::parse_sql;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn physical_explain_reflects_pruned_struct_schema() {
    let user_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("c".to_string(), ConcreteDatatype::Int64(Int64Type), false),
        StructField::new("d".to_string(), ConcreteDatatype::String(StringType), false),
    ])));

    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_2".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new("stream_2".to_string(), "b".to_string(), user_struct),
    ]));

    let definition = StreamDefinition::new(
        "stream_2",
        Arc::clone(&schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    let mut stream_defs = HashMap::new();
    stream_defs.insert("stream_2".to_string(), Arc::new(definition));

    let select_stmt =
        parse_sql("SELECT stream_2.a, stream_2.b->c FROM stream_2").expect("parse sql");
    let logical_plan = create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical");

    let bindings = SchemaBinding::new(vec![SchemaBindingEntry {
        source_name: "stream_2".to_string(),
        alias: None,
        schema: Arc::clone(&schema),
        kind: SourceBindingKind::Regular,
    }]);

    let (optimized_logical, pruned_binding) =
        flow::optimize_logical_plan(Arc::clone(&logical_plan), &bindings);

    let registries = PipelineRegistries::new_with_builtin();

    let physical =
        flow::create_physical_plan(Arc::clone(&optimized_logical), &pruned_binding, &registries)
            .expect("physical plan");
    let report = flow::ExplainReport::from_physical(physical);
    let topology = report.topology_string();
    assert!(topology.contains("schema=[a, b{c}]"));
}
