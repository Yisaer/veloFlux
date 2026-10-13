use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::codec::CompressionCodec;
use flow::connector::sink::file::FileRetentionConfig;
use flow::connector::FileSinkConfig;
use flow::planner::logical::create_logical_plan;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use flow::{
    ConnectorString, NopSinkConfig, PipelineRegistries, PipelineSink, PipelineSinkConnector,
    SinkConnectorConfig, SinkEncoderConfig,
};
use parser::parse_sql;
use std::collections::HashMap;
use std::sync::Arc;

fn setup_registry() -> PipelineRegistries {
    PipelineRegistries::new_with_builtin()
}

fn stream_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![ColumnSchema::new(
        "stream".to_string(),
        "v".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]))
}

fn build_sink_with_compression(compression: Option<CompressionCodec>) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    )
    .with_compression(compression);
    PipelineSink::new("test_sink", connector)
}

fn explain_json(sink: PipelineSink) -> serde_json::Value {
    let schema = stream_schema();
    let stream = StreamDefinition::new(
        "stream",
        Arc::clone(&schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    let stream_defs = HashMap::from([("stream".to_string(), Arc::new(stream))]);
    let sql = "SELECT v FROM stream";
    let select_stmt = parse_sql(sql).expect("parse sql");
    let bindings = SchemaBinding::new(vec![SchemaBindingEntry {
        source_name: "stream".to_string(),
        alias: None,
        schema: Arc::clone(&schema),
        kind: SourceBindingKind::Regular,
    }]);
    let registries = setup_registry();

    let logical = create_logical_plan(select_stmt, vec![sink], &stream_defs).expect("logical plan");
    let (logical, bindings) = flow::optimize_logical_plan(logical, &bindings);
    let physical = flow::create_physical_plan(Arc::clone(&logical), &bindings, &registries)
        .expect("physical plan");
    let physical = flow::optimize_physical_plan(
        physical,
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    let report = flow::ExplainReport::from_physical(physical);
    report.to_json()
}

/// Collect all operator IDs from an explain JSON tree.
fn collect_operators(v: &serde_json::Value, out: &mut Vec<String>) {
    if let Some(op) = v.get("operator").and_then(|o| o.as_str()) {
        out.push(op.to_string());
    }
    if let Some(children) = v.get("children").and_then(|c| c.as_array()) {
        for child in children {
            collect_operators(child, out);
        }
    }
}

// coverage-covers: planner.physical.sink_compress_insertion
#[test]
fn no_compression_does_not_insert_sink_compress() {
    let json = explain_json(build_sink_with_compression(None));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(
        !operators.contains(&"PhysicalSinkCompress".to_string()),
        "unexpected PhysicalSinkCompress in explain: {operators:?}"
    );
    assert!(
        operators.contains(&"PhysicalSinkEncoder".to_string())
            || operators.contains(&"PhysicalFusedTransform".to_string()),
        "expected an encoder stage (standalone or fused) in explain: {operators:?}"
    );
}

// coverage-covers: planner.physical.sink_compress_insertion
// coverage-covers: sink.fused_stateless_sink
#[test]
fn gzip_compression_fuses_compress_with_codec_gzip() {
    let json = explain_json(build_sink_with_compression(Some(CompressionCodec::gzip())));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(
        operators.contains(&"PhysicalFusedTransform".to_string()),
        "expected PhysicalFusedTransform in explain: {operators:?}"
    );
    let json_str = serde_json::to_string(&json).unwrap();
    assert!(
        json_str.contains("codec=gzip"),
        "expected codec=gzip info: {json_str}"
    );
    assert!(
        json_str.contains("fused=project+encoder+compress"),
        "expected fused compress stage: {json_str}"
    );
}

// coverage-covers: planner.physical.sink_compress_insertion
// coverage-covers: sink.fused_stateless_sink
#[test]
fn zstd_compression_with_level_shows_level_in_explain() {
    let json = explain_json(build_sink_with_compression(Some(
        CompressionCodec::zstd_with_level(3),
    )));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(
        operators.contains(&"PhysicalFusedTransform".to_string()),
        "expected PhysicalFusedTransform in explain: {operators:?}"
    );
    let json_str = serde_json::to_string(&json).unwrap();
    assert!(
        json_str.contains("codec=zstd"),
        "expected codec=zstd info: {json_str}"
    );
    assert!(
        json_str.contains("level=3"),
        "expected level=3 info: {json_str}"
    );
}

// coverage-covers: planner.physical.sink_compress_insertion
// coverage-covers: sink.fused_stateless_sink
#[test]
fn file_sink_with_gzip_fuses_delivery_chain() {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::File(FileSinkConfig {
            sink_name: "test_sink".to_string(),
            pipeline_id: "test_pipeline".to_string(),
            path: "/tmp/fused-file-sink-test".to_string(),
            filename_pattern: ConnectorString::plain("{seq}.json"),
            retention: FileRetentionConfig::default(),
        }),
        SinkEncoderConfig::json(),
    )
    .with_compression(Some(CompressionCodec::gzip()));
    let json = explain_json(PipelineSink::new("test_sink", connector));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(
        operators.contains(&"PhysicalFusedTransform".to_string()),
        "expected PhysicalFusedTransform in explain: {operators:?}"
    );
    assert!(
        !operators.contains(&"PhysicalSinkCompress".to_string()),
        "compress stage should fuse, not stand alone: {operators:?}"
    );
    let json_str = serde_json::to_string(&json).unwrap();
    assert!(
        json_str.contains("connector=file"),
        "expected connector=file info: {json_str}"
    );
    assert!(
        json_str.contains("codec=gzip"),
        "expected codec=gzip info: {json_str}"
    );
    assert!(
        json_str.contains("fused=project+encoder+compress"),
        "expected fused compress chain: {json_str}"
    );
}

// coverage-covers: planner.physical.sink_compress_insertion
// coverage-covers: sink.fused_stateless_sink
#[test]
fn sink_compress_is_fused_between_encoder_and_connector() {
    let json = explain_json(build_sink_with_compression(Some(CompressionCodec::gzip())));

    fn find_fused_stages(v: &serde_json::Value) -> Option<String> {
        if v.get("operator").and_then(|o| o.as_str()) == Some("PhysicalFusedTransform") {
            return v.get("info").and_then(|i| i.as_array()).and_then(|info| {
                info.iter().find_map(|entry| {
                    entry
                        .as_str()
                        .and_then(|s| s.strip_prefix("fused=").map(str::to_string))
                })
            });
        }
        v.get("children")
            .and_then(|c| c.as_array())
            .and_then(|children| children.iter().find_map(|child| find_fused_stages(child)))
    }

    assert_eq!(
        find_fused_stages(&json).as_deref(),
        Some("project+encoder+compress"),
        "fused compress chain mismatch: {}",
        serde_json::to_string(&json).unwrap(),
    );
}
