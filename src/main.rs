use std::env;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;

use flow::codec::JsonDecoder;
use flow::connector::{MqttSinkConfig, MqttSinkConnector, MqttSourceConfig, MqttSourceConnector};
use flow::processor::processor_builder::PlanProcessor;
use flow::processor::{ProcessorPipeline, SinkProcessor};
use flow::JsonEncoder;
use flow::Processor;
use once_cell::sync::Lazy;
use prometheus::{register_counter, register_gauge, Counter, Gauge};
use sysinfo::System;
use tokio::time::{sleep, Duration};

const DEFAULT_BROKER_URL: &str = "tcp://127.0.0.1:1883";
const SOURCE_TOPIC: &str = "/yisa/data";
const SINK_TOPIC: &str = "/yisa/data2";
const MQTT_QOS: u8 = 1;
const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9898";
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 5;

static CPU_TIME_COUNTER: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "process_cpu_seconds_total",
        "Total CPU time consumed by the synapse-flow process in seconds"
    )
    .expect("create cpu counter")
});

static MEMORY_USAGE_GAUGE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "synapse_memory_used_bytes",
        "Resident memory used by the process in bytes"
    )
    .expect("create memory gauge")
});

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_metrics_exporter().await?;

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
    println!("Pipeline running between MQTT topics {SOURCE_TOPIC} -> {SINK_TOPIC} WITH SQL {sql}.");
    println!("Press Ctrl+C to terminate.");

    tokio::signal::ctrl_c().await?;
    println!("Stopping pipeline...");
    pipeline.close().await?;
    Ok(())
}

async fn init_metrics_exporter() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = env::var("METRICS_ADDR")
        .unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_string())
        .parse()?;
    let exporter = prometheus_exporter::start(addr)?;
    // Leak exporter handle so the HTTP endpoint stays alive for the duration of the process.
    Box::leak(Box::new(exporter));

    let poll_interval = env::var("METRICS_POLL_INTERVAL_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_METRICS_INTERVAL_SECS);

    tokio::spawn(async move {
        let mut system = System::new();
        let pid = sysinfo::Pid::from_u32(process::id());
        loop {
            system.refresh_process(pid);
            if let Some(proc_info) = system.process(pid) {
                let delta_secs = (proc_info.cpu_usage() as f64 / 100.0) * poll_interval as f64;
                if delta_secs.is_finite() && delta_secs >= 0.0 {
                    CPU_TIME_COUNTER.inc_by(delta_secs);
                }

                let rss_bytes = proc_info.memory() as f64 * 1024.0;
                MEMORY_USAGE_GAUGE.set(rss_bytes);
            } else {
                MEMORY_USAGE_GAUGE.set(0.0);
            }

            sleep(Duration::from_secs(poll_interval)).await;
        }
    });

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
