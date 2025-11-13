use std::env;
use std::process;
use std::sync::Arc;

use flow::codec::JsonDecoder;
use flow::connector::{MqttSinkConfig, MqttSinkConnector, MqttSourceConfig, MqttSourceConnector};
use flow::encoder::JsonEncoder;
use flow::processor::processor_builder::PlanProcessor;
use flow::processor::{ProcessorPipeline, SinkProcessor};
use flow::Processor;

const DEFAULT_BROKER_URL: &str = "tcp://127.0.0.1:1833";
const SOURCE_TOPIC: &str = "/yisa/data";
const SINK_TOPIC: &str = "/yisa/data2";
const MQTT_QOS: u8 = 1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sql = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: synapse-flow \"<SQL query>\"");
        process::exit(1);
    });

    let mut sink = SinkProcessor::new("mqtt_sink");
    let sink_config = MqttSinkConfig::new("mqtt_sink", DEFAULT_BROKER_URL, SINK_TOPIC, MQTT_QOS);
    let sink_connector = MqttSinkConnector::new("mqtt_sink_connector", sink_config);
    sink.add_connector(
        Box::new(sink_connector),
        Arc::new(JsonEncoder::new("mqtt_sink_encoder")),
    );

    let mut pipeline = flow::create_pipeline(&sql, vec![sink])?;
    attach_mqtt_sources(&mut pipeline, DEFAULT_BROKER_URL, SOURCE_TOPIC, MQTT_QOS)?;

    pipeline.start();
    println!("Pipeline running between MQTT topics {SOURCE_TOPIC} -> {SINK_TOPIC}.");
    println!("Press Ctrl+C to terminate.");

    tokio::signal::ctrl_c().await?;
    println!("Stopping pipeline...");
    pipeline.close().await?;
    Ok(())
}

fn attach_mqtt_sources(
    pipeline: &mut ProcessorPipeline,
    broker_url: &str,
    topic: &str,
    qos: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut attached = false;
    for processor in pipeline.middle_processors.iter_mut() {
        if let PlanProcessor::DataSource(ds) = processor {
            let source_id = ds.id().to_string();
            let config = MqttSourceConfig::new(
                source_id.clone(),
                broker_url.to_string(),
                topic.to_string(),
                qos,
            );
            let connector =
                MqttSourceConnector::new(format!("{source_id}_source_connector"), config);
            let decoder = Arc::new(JsonDecoder::new(source_id));
            ds.add_connector(Box::new(connector), decoder);
            attached = true;
        }
    }

    if attached {
        Ok(())
    } else {
        Err("no datasource processors available to attach MQTT source".into())
    }
}
