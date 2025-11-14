use std::env;
use std::fs;
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
use procfs::process::Process;
use prometheus::{register_gauge, Gauge, Registry};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

const DEFAULT_BROKER_URL: &str = "tcp://127.0.0.1:1883";
const SOURCE_TOPIC: &str = "/yisa/data";
const SINK_TOPIC: &str = "/yisa/data2";
const MQTT_QOS: u8 = 1;
const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9898";
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 5;

#[derive(Clone)]
struct ProcessCpuTimeMetrics {
    total: prometheus::Counter,
    user: prometheus::Counter,
    system: prometheus::Counter,
    last_user_ticks: Arc<AtomicU64>,
    last_system_ticks: Arc<AtomicU64>,
}

impl ProcessCpuTimeMetrics {
    fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let total = prometheus::Counter::new(
            "process_cpu_seconds_total",
            "Total CPU time spent by the process in seconds",
        )?;
        let user = prometheus::Counter::new(
            "process_cpu_user_seconds_total",
            "Total user CPU time spent by the process in seconds",
        )?;
        let system = prometheus::Counter::new(
            "process_cpu_system_seconds_total",
            "Total system CPU time spent by the process in seconds",
        )?;

        registry.register(Box::new(total.clone()))?;
        registry.register(Box::new(user.clone()))?;
        registry.register(Box::new(system.clone()))?;

        Ok(Self {
            total,
            user,
            system,
            last_user_ticks: Arc::new(AtomicU64::new(0)),
            last_system_ticks: Arc::new(AtomicU64::new(0)),
        })
    }

    fn update(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let process = Process::myself()?;
        let stat = process.stat()?;
        let clock_ticks_per_second = procfs::ticks_per_second() as f64;

        let current_user_ticks = stat.utime;
        let current_system_ticks = stat.stime;

        let last_user = self.last_user_ticks.load(Ordering::Relaxed);
        let last_system = self.last_system_ticks.load(Ordering::Relaxed);

        let user_delta = if current_user_ticks >= last_user {
            current_user_ticks - last_user
        } else {
            current_user_ticks
        };

        let system_delta = if current_system_ticks >= last_system {
            current_system_ticks - last_system
        } else {
            current_system_ticks
        };

        let user_secs = user_delta as f64 / clock_ticks_per_second;
        let system_secs = system_delta as f64 / clock_ticks_per_second;
        let total_secs = user_secs + system_secs;

        if user_delta > 0 {
            self.user.inc_by(user_secs);
        }
        if system_delta > 0 {
            self.system.inc_by(system_secs);
        }
        if total_secs > 0.0 {
            self.total.inc_by(total_secs);
        }

        self.last_user_ticks
            .store(current_user_ticks, Ordering::Relaxed);
        self.last_system_ticks
            .store(current_system_ticks, Ordering::Relaxed);

        let rss_bytes = (stat.rss.max(0) as f64) * procfs::page_size() as f64;
        Ok(rss_bytes)
    }
}

static MEMORY_USAGE_GAUGE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "synapse_memory_used_bytes",
        "Resident memory used by the process in bytes (RSS from /proc/self/stat)"
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

    let cpu_metrics = ProcessCpuTimeMetrics::new(prometheus::default_registry())?;
    let memory_gauge = MEMORY_USAGE_GAUGE.clone();

    tokio::spawn(async move {
        loop {
            match cpu_metrics.update() {
                Ok(rss_bytes) => memory_gauge.set(rss_bytes),
                Err(_) => memory_gauge.set(0.0),
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
