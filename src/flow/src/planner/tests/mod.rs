use crate::catalog::{MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use crate::codec::{DecoderRegistry, EncoderRegistry};
use crate::connector::ConnectorRegistry;
use crate::planner::explain::PipelineExplain;
use crate::planner::logical::create_logical_plan;
use crate::stateful::StatefulFunctionRegistry;
use crate::{AggregateFunctionRegistry, PipelineRegistries};
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use parser::parse_sql_with_registries;
use std::collections::HashMap;
use std::sync::Arc;

fn setup_single_stream(
    cols: Vec<&str>,
) -> (
    HashMap<String, Arc<StreamDefinition>>,
    crate::expr::sql_conversion::SchemaBinding,
) {
    let schema = Arc::new(Schema::new(
        cols.into_iter()
            .map(|name| {
                ColumnSchema::new(
                    "stream".to_string(),
                    name.to_string(),
                    ConcreteDatatype::Int64(Int64Type),
                )
            })
            .collect(),
    ));
    let definition = StreamDefinition::new(
        "stream",
        Arc::clone(&schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    let mut stream_defs = HashMap::new();
    stream_defs.insert("stream".to_string(), Arc::new(definition));

    let bindings = crate::expr::sql_conversion::SchemaBinding::new(vec![
        crate::expr::sql_conversion::SchemaBindingEntry {
            source_name: "stream".to_string(),
            alias: None,
            schema,
            kind: crate::expr::sql_conversion::SourceBindingKind::Regular,
        },
    ]);

    (stream_defs, bindings)
}

fn build_registries() -> PipelineRegistries {
    let encoder_registry = EncoderRegistry::with_builtin_encoders();
    PipelineRegistries::new_with_stateful_registry(
        ConnectorRegistry::with_builtin_sinks(),
        Arc::clone(&encoder_registry),
        DecoderRegistry::with_builtin_decoders(),
        AggregateFunctionRegistry::with_builtins(),
        StatefulFunctionRegistry::with_builtins(),
    )
}

fn explain_from_sql(sql: &str, cols: Vec<&str>) -> serde_json::Value {
    let registries = build_registries();
    let (stream_defs, bindings) = setup_single_stream(cols);
    let select_stmt = parse_sql_with_registries(
        sql,
        registries.aggregate_registry(),
        registries.stateful_registry(),
    )
    .expect("parse sql");
    let logical_plan = create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical");
    let physical_plan =
        crate::planner::create_physical_plan(Arc::clone(&logical_plan), &bindings, &registries)
            .expect("physical");

    PipelineExplain::new(logical_plan, physical_plan).to_json()
}

#[test]
fn plan_explain_stateful_select_only() {
    let got = explain_from_sql("SELECT lag(a) FROM stream", vec!["a"]).to_string();
    let expected = r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1]"],"operator":"Project"},"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1]"],"operator":"PhysicalProject"}}"##;
    assert_eq!(got, expected);
}

#[test]
fn plan_explain_stateful_where_before_project() {
    let got = explain_from_sql("SELECT a FROM stream WHERE lag(a) > 0", vec!["a"]).to_string();
    let expected = r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Filter_2","info":["predicate=col_1 > 0"],"operator":"Filter"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalFilter_3","info":["predicate=col_1 > 0"],"operator":"PhysicalFilter"}],"id":"PhysicalProject_4","info":["fields=[a]"],"operator":"PhysicalProject"}}"##;
    assert_eq!(got, expected);
}

#[test]
fn plan_explain_stateful_before_window_and_aggregation() {
    let got = explain_from_sql(
        "SELECT sum(a), lag(a) FROM stream GROUP BY tumblingwindow('ss', 10)",
        vec!["a"],
    )
    .to_string();

    let expected = r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Window_2","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_3","info":["aggregates=[sum(a) -> col_2]"],"operator":"Aggregation"}],"id":"Project_4","info":["fields=[col_2; col_1]"],"operator":"Project"},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalWatermark_3","info":["window=tumbling","unit=Seconds","length=10","mode=processing_time","interval=10"],"operator":"PhysicalWatermark"}],"id":"PhysicalTumblingWindow_4","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"PhysicalTumblingWindow"}],"id":"PhysicalAggregation_5","info":["calls=[sum(a) -> col_2]"],"operator":"PhysicalAggregation"}],"id":"PhysicalProject_6","info":["fields=[col_2; col_1]"],"operator":"PhysicalProject"}}"##;
    assert_eq!(got, expected);
}
