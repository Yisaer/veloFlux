use super::plan_explain_table_driven::semantic_plan_json;
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{Catalog, MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::planner::sink::{
    NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
};
use flow::{pipeline::PipelineOptions, PipelineRegistries};
use std::sync::Arc;

fn sink() -> PipelineSink {
    PipelineSink::new(
        "output",
        PipelineSinkConnector::new(
            "nop",
            SinkConnectorConfig::Nop(NopSinkConfig::default()),
            SinkEncoderConfig::json(),
        ),
    )
}

#[test]
fn multi_source_projection_explain_cases() {
    let catalog = Catalog::new();
    for (name, used) in [("faults", "fault_code"), ("messages", "payload")] {
        let mut columns = vec!["device_id".to_string(), used.to_string()];
        columns.extend((0..512).map(|index| format!("unused_{index}")));
        let schema = Arc::new(Schema::new(
            columns
                .into_iter()
                .map(|column| {
                    ColumnSchema::new(name.to_string(), column, ConcreteDatatype::Int64(Int64Type))
                })
                .collect(),
        ));
        catalog
            .insert(
                StreamDefinition::new(
                    name,
                    schema,
                    StreamProps::Mock(MockStreamProps::default()),
                    StreamDecoderConfig::json(),
                )
                .with_eventtime(flow::EventtimeDefinition::new(
                    "device_id",
                    "unixtimestamp_ms",
                )),
            )
            .unwrap();
    }
    struct Case {
        name: &'static str,
        sql: &'static str,
        options: PipelineOptions,
        expected: Result<&'static str, &'static str>,
    }
    let cases = [
        Case {
            name: "qualified_fields",
            sql: "SELECT faults.device_id AS fault_device, messages.device_id AS message_device FROM faults, messages",
            options: PipelineOptions::default(),
            expected: Ok(r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=faults","decoder=json","schema=[device_id]"],"operator":"DataSource"},{"children":[],"id":"DataSource_1","info":["source=messages","decoder=json","schema=[device_id]"],"operator":"DataSource"}],"id":"Project_2","info":["fields=[faults.device_id as fault_device; messages.device_id as message_device]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=output","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=1"],"operator":"Tail"},"options":{"eventtime_enabled":false,"eventtime_late_tolerance_ms":0},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=faults","schema=[device_id]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[device_id]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_4","info":["source=faults","mode=source_alignment, sources=[faults, messages]","slots=[faults.device_id@0:0, messages.device_id@1:0]","schema=[device_id]"],"operator":"PhysicalCollectionLayoutNormalize"},{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_2","info":["source=messages","schema=[device_id]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_3","info":["decoder=json","schema=[device_id]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_5","info":["source=messages","mode=source_alignment, sources=[faults, messages]","slots=[faults.device_id@0:0, messages.device_id@1:0]","schema=[device_id]"],"operator":"PhysicalCollectionLayoutNormalize"}],"id":"PhysicalBarrier_6","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalProject_7","info":["fields=[faults.device_id as fault_device; messages.device_id as message_device]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_9","info":["sink_id=output","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_8","info":["sink_id=output","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_10","info":[],"operator":"PhysicalResultCollect"}}"##),
        },
        Case {
            name: "unique_fields_with_filter",
            sql: "SELECT payload FROM faults, messages WHERE fault_code IS NULL",
            options: PipelineOptions::default(),
            expected: Ok(r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=faults","decoder=json","schema=[fault_code]"],"operator":"DataSource"},{"children":[],"id":"DataSource_1","info":["source=messages","decoder=json","schema=[payload]"],"operator":"DataSource"}],"id":"Filter_2","info":["predicate=isnull(fault_code)"],"operator":"Filter"}],"id":"Project_3","info":["fields=[payload]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=output","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":{"eventtime_enabled":false,"eventtime_late_tolerance_ms":0},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=faults","schema=[fault_code]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[fault_code]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_4","info":["source=faults","mode=source_alignment, sources=[faults, messages]","slots=[faults.fault_code@0:0, messages.payload@1:0]","schema=[fault_code]"],"operator":"PhysicalCollectionLayoutNormalize"},{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_2","info":["source=messages","schema=[payload]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_3","info":["decoder=json","schema=[payload]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_5","info":["source=messages","mode=source_alignment, sources=[faults, messages]","slots=[faults.fault_code@0:0, messages.payload@1:0]","schema=[payload]"],"operator":"PhysicalCollectionLayoutNormalize"}],"id":"PhysicalBarrier_6","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalFilter_7","info":["predicate=isnull(fault_code)"],"operator":"PhysicalFilter"}],"id":"PhysicalProject_8","info":["fields=[payload]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_10","info":["sink_id=output","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_9","info":["sink_id=output","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_11","info":[],"operator":"PhysicalResultCollect"}}"##),
        },
        Case {
            name: "fault_trigger_and_partition_dependencies",
            sql: "SELECT payload FROM faults, messages GROUP BY slidingwindow('ss', 0, 120) OVER (WHEN fault_code = 7 PARTITION BY coalesce(faults.device_id, messages.device_id))",
            options: PipelineOptions::default(),
            expected: Ok(r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=faults","decoder=json","schema=[device_id, fault_code]"],"operator":"DataSource"},{"children":[],"id":"DataSource_1","info":["source=messages","decoder=json","schema=[device_id, payload]"],"operator":"DataSource"}],"id":"Window_2","info":["kind=sliding","unit=Seconds","lookback=0","lookahead=120","partition_by=coalesce(faults.device_id, messages.device_id)","trigger=fault_code = 7"],"operator":"Window"}],"id":"Project_3","info":["fields=[payload]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=output","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":{"eventtime_enabled":false,"eventtime_late_tolerance_ms":0},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=faults","schema=[device_id, fault_code]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[device_id, fault_code]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_4","info":["source=faults","mode=source_alignment, sources=[faults, messages]","slots=[faults.device_id@0:0, faults.fault_code@0:1, messages.device_id@1:0, messages.payload@1:1]","schema=[device_id, fault_code]"],"operator":"PhysicalCollectionLayoutNormalize"},{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_2","info":["source=messages","schema=[device_id, payload]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_3","info":["decoder=json","schema=[device_id, payload]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_5","info":["source=messages","mode=source_alignment, sources=[faults, messages]","slots=[faults.device_id@0:0, faults.fault_code@0:1, messages.device_id@1:0, messages.payload@1:1]","schema=[device_id, payload]"],"operator":"PhysicalCollectionLayoutNormalize"}],"id":"PhysicalBarrier_6","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalProcessTimeWatermark_7","info":["window=sliding","unit=Seconds","lookback=0","lookahead=120","mode=processing_time","interval=1"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalSlidingWindow_8","info":["kind=sliding","unit=Seconds","lookback=0","lookahead=120","partition_by=coalesce(faults.device_id, messages.device_id)","trigger=fault_code = 7"],"operator":"PhysicalSlidingWindow"}],"id":"PhysicalProject_9","info":["fields=[payload]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_11","info":["sink_id=output","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_10","info":["sink_id=output","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_12","info":[],"operator":"PhysicalResultCollect"}}"##),
        },
        Case {
            name: "ambiguous_column",
            sql: "SELECT device_id FROM faults, messages",
            options: PipelineOptions::default(),
            expected: Err("Ambiguous"),
        },
        Case {
            name: "unknown_column",
            sql: "SELECT faults.missing FROM faults, messages",
            options: PipelineOptions::default(),
            expected: Err("Column not found"),
        },
        Case {
            name: "unknown_source",
            sql: "SELECT unknown.device_id FROM faults, messages",
            options: PipelineOptions::default(),
            expected: Err("Column not found"),
        },
        Case {
            name: "multi_source_eventtime_is_rejected",
            sql: "SELECT payload FROM faults, messages",
            options: PipelineOptions {
                eventtime: flow::pipeline::EventtimeOptions { enabled: true, ..Default::default() },
                ..PipelineOptions::default()
            },
            expected: Err("multi-source FROM currently supports processing time only"),
        },
    ];
    let registries = PipelineRegistries::new_with_builtin();
    for case in cases {
        let result = flow::explain_pipeline_with_options(
            case.sql,
            vec![sink()],
            &catalog,
            None,
            &registries,
            &case.options,
        );
        match case.expected {
            Ok(expected) => {
                let explain = result.unwrap_or_else(|error| panic!("case={}: {error}", case.name));
                println!(
                    "case={} SQL: {}\n{}",
                    case.name,
                    case.sql,
                    explain.to_pretty_string()
                );
                let actual = semantic_plan_json(&explain);
                let expected: serde_json::Value =
                    serde_json::from_str(expected).expect("valid expected EXPLAIN JSON");
                assert_eq!(actual, expected, "case={} SQL: {}", case.name, case.sql);
            }
            Err(expected) => {
                let message = result.unwrap_err().to_string();
                println!("case={} SQL: {}\nError: {message}", case.name, case.sql);
                assert!(
                    message.contains(expected),
                    "case={}: unexpected error: {message}",
                    case.name
                );
            }
        }
    }
}

#[test]
fn query_input_wildcard_slots_and_single_input_fusion() {
    let catalog = Catalog::new();
    for (name, fields) in [
        ("a", vec!["a2", "a1"]),
        ("b", vec!["b1"]),
        ("c", vec!["c1"]),
    ] {
        let schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|field| {
                    ColumnSchema::new(
                        name.to_string(),
                        field.to_string(),
                        ConcreteDatatype::Int64(Int64Type),
                    )
                })
                .collect(),
        ));
        catalog
            .insert(StreamDefinition::new(
                name,
                schema,
                StreamProps::Mock(MockStreamProps::default()),
                StreamDecoderConfig::json(),
            ))
            .unwrap();
    }
    struct Case {
        name: &'static str,
        sql: &'static str,
        expected: &'static str,
        region_suffix: &'static [&'static str],
    }
    let cases = [
        Case {
            name: "single_source_identity_layout_and_fusion",
            sql: "SELECT * FROM a",
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=a","decoder=json","schema=[a2, a1]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[*]"],"operator":"Project"},"options":{"eventtime_enabled":false,"eventtime_late_tolerance_ms":0},"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=a","schema=[a2, a1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a2, a1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalBarrier_2","info":["upstream_count=1"],"operator":"PhysicalBarrier"}],"id":"PhysicalProject_3","info":["fields=[*]"],"operator":"PhysicalProject"}}"##,
            region_suffix: &["PhysicalDecoder", "PhysicalBarrier", "PhysicalProject"],
        },
        Case {
            name: "two_source_wildcard_slots",
            sql: "SELECT * FROM a, b",
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=a","decoder=json","schema=[a2, a1]"],"operator":"DataSource"},{"children":[],"id":"DataSource_1","info":["source=b","decoder=json","schema=[b1]"],"operator":"DataSource"}],"id":"Project_2","info":["fields=[*]"],"operator":"Project"},"options":{"eventtime_enabled":false,"eventtime_late_tolerance_ms":0},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=a","schema=[a2, a1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a2, a1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_4","info":["source=a","mode=source_alignment, sources=[a, b]","slots=[a.a2@0:0, a.a1@0:1, b.b1@1:0]","schema=[a2, a1]"],"operator":"PhysicalCollectionLayoutNormalize"},{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_2","info":["source=b","schema=[b1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_3","info":["decoder=json","schema=[b1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_5","info":["source=b","mode=source_alignment, sources=[a, b]","slots=[a.a2@0:0, a.a1@0:1, b.b1@1:0]","schema=[b1]"],"operator":"PhysicalCollectionLayoutNormalize"}],"id":"PhysicalBarrier_6","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalProject_7","info":["fields=[*]"],"operator":"PhysicalProject"}}"##,
            region_suffix: &[],
        },
        Case {
            name: "three_source_wildcard_slots",
            sql: "SELECT * FROM a, b, c",
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=a","decoder=json","schema=[a2, a1]"],"operator":"DataSource"},{"children":[],"id":"DataSource_1","info":["source=b","decoder=json","schema=[b1]"],"operator":"DataSource"},{"children":[],"id":"DataSource_2","info":["source=c","decoder=json","schema=[c1]"],"operator":"DataSource"}],"id":"Project_3","info":["fields=[*]"],"operator":"Project"},"options":{"eventtime_enabled":false,"eventtime_late_tolerance_ms":0},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=a","schema=[a2, a1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a2, a1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_6","info":["source=a","mode=source_alignment, sources=[a, b, c]","slots=[a.a2@0:0, a.a1@0:1, b.b1@1:0, c.c1@2:0]","schema=[a2, a1]"],"operator":"PhysicalCollectionLayoutNormalize"},{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_2","info":["source=b","schema=[b1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_3","info":["decoder=json","schema=[b1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_7","info":["source=b","mode=source_alignment, sources=[a, b, c]","slots=[a.a2@0:0, a.a1@0:1, b.b1@1:0, c.c1@2:0]","schema=[b1]"],"operator":"PhysicalCollectionLayoutNormalize"},{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_4","info":["source=c","schema=[c1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_5","info":["decoder=json","schema=[c1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCollectionLayoutNormalize_8","info":["source=c","mode=source_alignment, sources=[a, b, c]","slots=[a.a2@0:0, a.a1@0:1, b.b1@1:0, c.c1@2:0]","schema=[c1]"],"operator":"PhysicalCollectionLayoutNormalize"}],"id":"PhysicalBarrier_9","info":["upstream_count=3"],"operator":"PhysicalBarrier"}],"id":"PhysicalProject_10","info":["fields=[*]"],"operator":"PhysicalProject"}}"##,
            region_suffix: &[],
        },
    ];
    let registries = PipelineRegistries::new_with_builtin();
    for case in cases {
        let explain = flow::explain_pipeline_with_options(
            case.sql,
            vec![],
            &catalog,
            None,
            &registries,
            &PipelineOptions::default(),
        )
        .unwrap_or_else(|error| panic!("case={}: {error}", case.name));
        println!(
            "case={} SQL: {}\n{}",
            case.name,
            case.sql,
            explain.to_pretty_string()
        );
        let actual = semantic_plan_json(&explain);
        let expected: serde_json::Value =
            serde_json::from_str(case.expected).expect("valid expected EXPLAIN JSON");
        assert_eq!(actual, expected, "case={} SQL: {}", case.name, case.sql);
        if !case.region_suffix.is_empty() {
            super::assert_region_suffix(&explain.execution_to_json(), case.region_suffix);
        }
    }
}
