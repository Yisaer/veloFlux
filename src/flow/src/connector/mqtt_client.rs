use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use rumqttc::{
    AsyncClient, ConnectionError, Event, EventLoop, MqttOptions, Packet, QoS, Transport,
};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;

use crate::connector::ConnectorError;
use crate::runtime::TaskSpawner;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedMqttClientConfig {
    pub key: String,
    pub broker_url: String,
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
    pub max_packet_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum SharedMqttEvent {
    Payload(Vec<u8>),
    EndOfStream,
}

fn mqtt_topic_matches(filter: &str, topic: &str) -> bool {
    let filter_levels = filter.split('/').collect::<Vec<_>>();
    let topic_levels = topic.split('/').collect::<Vec<_>>();
    let mut filter_idx = 0usize;
    let mut topic_idx = 0usize;

    while filter_idx < filter_levels.len() {
        match filter_levels[filter_idx] {
            "#" => return filter_idx == filter_levels.len() - 1,
            "+" => {
                if topic_idx >= topic_levels.len() {
                    return false;
                }
                filter_idx += 1;
                topic_idx += 1;
            }
            level => {
                if topic_idx >= topic_levels.len() || topic_levels[topic_idx] != level {
                    return false;
                }
                filter_idx += 1;
                topic_idx += 1;
            }
        }
    }

    topic_idx == topic_levels.len()
}

pub struct SharedMqttClient {
    entry: Arc<MqttClientEntry>,
    manager: MqttClientManager,
}

impl SharedMqttClient {
    fn from_acquired(entry: Arc<MqttClientEntry>, manager: MqttClientManager) -> Self {
        Self { entry, manager }
    }

    pub fn client(&self) -> AsyncClient {
        self.entry.client.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Result<SharedMqttEvent, ConnectorError>> {
        self.entry.events_tx.subscribe()
    }

    pub fn is_connected(&self) -> bool {
        self.entry.connected.load(Ordering::Acquire)
    }

    pub async fn publish(
        &self,
        topic: String,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> Result<(), ConnectorError> {
        if !self.is_connected() {
            let last_error = self.entry.last_error.read().clone();
            let message = last_error
                .map(|err| format!("mqtt not connected: {err}"))
                .unwrap_or_else(|| "mqtt not connected".to_string());
            return Err(ConnectorError::Connection(message));
        }

        self.entry
            .client
            .publish(topic, qos, retain, payload)
            .await
            .map_err(|err| ConnectorError::Connection(err.to_string()))
    }
}

impl Drop for SharedMqttClient {
    fn drop(&mut self) {
        self.manager.release(&self.entry);
    }
}

struct MqttClientEntry {
    client: AsyncClient,
    events_tx: broadcast::Sender<Result<SharedMqttEvent, ConnectorError>>,
    shutdown_tx: watch::Sender<bool>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    ref_count: AtomicUsize,
    closing: AtomicBool,
    topic: String,
    qos: QoS,
    connected: AtomicBool,
    last_error: RwLock<Option<String>>,
}

impl MqttClientEntry {
    async fn new(config: &SharedMqttClientConfig) -> Result<(Self, EventLoop), ConnectorError> {
        let mut options = build_mqtt_options(config)?;
        options.set_keep_alive(Duration::from_secs(30));

        let qos = map_qos(config.qos)?;
        let topic = config.topic.clone();

        let (client, event_loop) = AsyncClient::new(options, 64);
        client
            .subscribe(topic.clone(), qos)
            .await
            .map_err(|err| ConnectorError::Connection(err.to_string()))?;

        let (events_tx, _) = broadcast::channel(1024);
        let (shutdown_tx, _) = watch::channel(false);

        Ok((
            Self {
                client,
                events_tx,
                shutdown_tx,
                join_handle: Mutex::new(None),
                ref_count: AtomicUsize::new(0),
                closing: AtomicBool::new(false),
                topic,
                qos,
                connected: AtomicBool::new(false),
                last_error: RwLock::new(None),
            },
            event_loop,
        ))
    }

    fn start_event_loop(
        spawner: &TaskSpawner,
        entry: &Arc<MqttClientEntry>,
        mut event_loop: EventLoop,
    ) {
        let entry_for_task = Arc::clone(entry);
        let mut shutdown_rx = entry.shutdown_tx.subscribe();
        let events_tx = entry.events_tx.clone();
        let topic = entry.topic.clone();
        let qos = entry.qos;
        let client = entry.client.clone();

        let handle = spawner.spawn(async move {
            let mut backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(5);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    event = event_loop.poll() => match event {
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            entry_for_task.connected.store(true, Ordering::Release);
                            backoff = Duration::from_millis(100);
                            if mqtt_topic_matches(&topic, &publish.topic) {
                                let _ = events_tx.send(Ok(SharedMqttEvent::Payload(publish.payload.to_vec())));
                            }
                        }
                        Ok(Event::Incoming(Packet::Disconnect)) => {
                            entry_for_task.connected.store(false, Ordering::Release);
                            *entry_for_task.last_error.write() = Some("disconnect".to_string());
                            tracing::warn!("shared mqtt client disconnected; reconnecting");
                            event_loop.clean();
                        }
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            entry_for_task.connected.store(true, Ordering::Release);
                            *entry_for_task.last_error.write() = None;
                            backoff = Duration::from_millis(100);
                            let _ = client.subscribe(topic.clone(), qos).await;
                        }
                        Ok(_) => {}
                        Err(ConnectionError::RequestsDone) => {
                            entry_for_task.connected.store(false, Ordering::Release);
                            let _ = events_tx.send(Ok(SharedMqttEvent::EndOfStream));
                            break;
                        }
                        Err(err) => {
                            entry_for_task.connected.store(false, Ordering::Release);
                            *entry_for_task.last_error.write() = Some(err.to_string());
                            let _ = events_tx.send(Err(ConnectorError::Connection(err.to_string())));
                            sleep(backoff).await;
                            backoff = std::cmp::min(backoff * 2, max_backoff);
                        }
                    }
                }
            }
        });

        *entry.join_handle.lock() = Some(handle);
    }

    fn spawn_shutdown(spawner: &TaskSpawner, entry: Arc<Self>) {
        spawner.spawn(async move {
            let _ = entry.shutdown_tx.send(true);
            let _ = entry.client.disconnect().await;
            if let Some(handle) = entry.join_handle.lock().take() {
                handle.abort();
            }
            let _ = entry.events_tx.send(Ok(SharedMqttEvent::EndOfStream));
        });
    }
}

#[derive(Clone)]
pub(crate) struct MqttClientManager {
    entries: Arc<Mutex<HashMap<String, Arc<MqttClientEntry>>>>,
    spawner: TaskSpawner,
}

impl MqttClientManager {
    pub(crate) fn new(spawner: TaskSpawner) -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            spawner,
        }
    }

    pub(crate) async fn create_client(
        &self,
        config: SharedMqttClientConfig,
    ) -> Result<(), ConnectorError> {
        let key = config.key.clone();
        {
            if self.entries.lock().contains_key(&key) {
                return Err(ConnectorError::AlreadyExists(key));
            }
        }

        let (entry, event_loop) = MqttClientEntry::new(&config).await?;
        let entry = Arc::new(entry);
        MqttClientEntry::start_event_loop(&self.spawner, &entry, event_loop);

        self.entries.lock().insert(key, entry);
        Ok(())
    }

    pub(crate) async fn acquire_client(
        &self,
        key: &str,
    ) -> Result<SharedMqttClient, ConnectorError> {
        let entry = {
            let guard = self.entries.lock();
            let entry = guard
                .get(key)
                .cloned()
                .ok_or_else(|| ConnectorError::NotFound(key.to_string()))?;
            entry.ref_count.fetch_add(1, Ordering::AcqRel);
            entry
        };
        Ok(SharedMqttClient::from_acquired(entry, self.clone()))
    }

    pub(crate) fn drop_client(&self, key: &str) -> Result<(), ConnectorError> {
        let entry = self
            .entries
            .lock()
            .remove(key)
            .ok_or_else(|| ConnectorError::NotFound(key.to_string()))?;

        entry.closing.store(true, Ordering::Release);
        if entry.ref_count.load(Ordering::Acquire) == 0 {
            MqttClientEntry::spawn_shutdown(&self.spawner, entry);
        }
        Ok(())
    }

    fn release(&self, entry: &Arc<MqttClientEntry>) {
        if entry.ref_count.fetch_sub(1, Ordering::AcqRel) == 1
            && entry.closing.load(Ordering::Acquire)
        {
            MqttClientEntry::spawn_shutdown(&self.spawner, Arc::clone(entry));
        }
    }
}

fn normalize_broker_url(url: &str) -> String {
    if url.contains("://") {
        url.to_owned()
    } else {
        format!("tcp://{url}")
    }
}

fn build_mqtt_options(config: &SharedMqttClientConfig) -> Result<MqttOptions, ConnectorError> {
    let endpoint = Url::parse(&normalize_broker_url(&config.broker_url)).map_err(|err| {
        ConnectorError::Connection(format!("invalid broker URL `{}`: {err}", config.broker_url))
    })?;

    let scheme = endpoint.scheme();

    let host = endpoint.host_str().ok_or_else(|| {
        ConnectorError::Connection(format!("broker URL `{}` missing host", config.broker_url))
    })?;

    let port = endpoint
        .port()
        .or_else(|| default_port_for_scheme(scheme))
        .ok_or_else(|| {
            ConnectorError::Connection(format!("broker URL `{}` missing port", config.broker_url))
        })?;

    let mut options = MqttOptions::new(config.client_id.clone(), host, port);
    let max_packet_size = config.max_packet_size.unwrap_or(64 * 1024 * 1024);
    options.set_max_packet_size(max_packet_size, max_packet_size);

    if is_tls_scheme(scheme) {
        options.set_transport(Transport::tls_with_default_config());
    }

    Ok(options)
}

fn map_qos(qos: u8) -> Result<QoS, ConnectorError> {
    match qos {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        other => Err(ConnectorError::Other(format!(
            "unsupported MQTT QoS level: {other}"
        ))),
    }
}

fn default_port_for_scheme(scheme: &str) -> Option<u16> {
    match scheme {
        "mqtt" | "tcp" => Some(1883),
        "mqtts" | "ssl" | "tcps" => Some(8883),
        _ => None,
    }
}

fn is_tls_scheme(scheme: &str) -> bool {
    matches!(scheme, "mqtts" | "ssl" | "tcps")
}

#[cfg(test)]
mod tests {
    use super::{mqtt_topic_matches, MqttClientManager, SharedMqttClientConfig};
    use crate::connector::ConnectorError;
    use crate::runtime::TaskSpawner;
    use tokio::runtime::Handle;

    #[test]
    fn mqtt_topic_match_exact() {
        assert!(mqtt_topic_matches("topic/123", "topic/123"));
        assert!(!mqtt_topic_matches("topic/123", "topic/456"));
    }

    #[test]
    fn mqtt_topic_match_single_level_wildcard() {
        assert!(mqtt_topic_matches("topic/+", "topic/123"));
        assert!(!mqtt_topic_matches("topic/+", "topic/123/456"));
        assert!(!mqtt_topic_matches("topic/+", "topic"));
    }

    #[test]
    fn mqtt_topic_match_multi_level_wildcard() {
        assert!(mqtt_topic_matches("topic/#", "topic"));
        assert!(mqtt_topic_matches("topic/#", "topic/123"));
        assert!(mqtt_topic_matches("topic/#", "topic/123/456"));
        assert!(!mqtt_topic_matches("topic/#", "other/123"));
    }

    #[test]
    fn mqtt_topic_match_mixed_wildcards() {
        assert!(mqtt_topic_matches(
            "fleet/+/telemetry/#",
            "fleet/car_a/telemetry"
        ));
        assert!(mqtt_topic_matches(
            "fleet/+/telemetry/#",
            "fleet/car_a/telemetry/speed"
        ));
        assert!(!mqtt_topic_matches(
            "fleet/+/telemetry/#",
            "fleet/car_a/state/speed"
        ));
    }

    #[tokio::test]
    async fn drop_client_removes_busy_client_from_registry() {
        let manager = MqttClientManager::new(TaskSpawner::from_handle(Handle::current()));
        let cfg = SharedMqttClientConfig {
            key: "shared".to_string(),
            broker_url: "tcp://127.0.0.1:1883".to_string(),
            topic: "fleet/+/telemetry".to_string(),
            client_id: "client_shared".to_string(),
            qos: 0,
            max_packet_size: None,
        };

        manager
            .create_client(cfg)
            .await
            .expect("create shared mqtt client");
        let held = manager
            .acquire_client("shared")
            .await
            .expect("acquire shared mqtt client");

        manager
            .drop_client("shared")
            .expect("best-effort drop busy shared mqtt client");

        assert!(matches!(
            manager.acquire_client("shared").await,
            Err(ConnectorError::NotFound(_))
        ));

        drop(held);
    }
}
