use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS, Transport};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;

use crate::backpressure_hub::BackpressureHub;
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

    pub async fn subscribe(
        &self,
    ) -> Result<mpsc::Receiver<Result<SharedMqttEvent, ConnectorError>>, ConnectorError> {
        self.entry.subscribe().await
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
            .client()
            .await?
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

struct MqttClientRuntime {
    client: AsyncClient,
    events_hub: Arc<BackpressureHub<Result<SharedMqttEvent, ConnectorError>>>,
    shutdown_tx: watch::Sender<bool>,
    join_handle: JoinHandle<()>,
}

struct MqttClientEntry {
    config: SharedMqttClientConfig,
    runtime: AsyncMutex<Option<MqttClientRuntime>>,
    ref_count: AtomicUsize,
    closing: AtomicBool,
    connected: AtomicBool,
    last_error: RwLock<Option<String>>,
    spawner: TaskSpawner,
}

impl MqttClientEntry {
    fn new(config: SharedMqttClientConfig, spawner: TaskSpawner) -> Self {
        Self {
            config,
            runtime: AsyncMutex::new(None),
            ref_count: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            last_error: RwLock::new(None),
            spawner,
        }
    }

    async fn subscribe(
        self: &Arc<Self>,
    ) -> Result<mpsc::Receiver<Result<SharedMqttEvent, ConnectorError>>, ConnectorError> {
        self.ensure_runtime_started().await?;
        let hub = {
            let runtime = self.runtime.lock().await;
            runtime
                .as_ref()
                .map(|runtime| Arc::clone(&runtime.events_hub))
                .ok_or_else(|| {
                    ConnectorError::ResourceBusy(
                        "shared mqtt client runtime is unavailable".to_string(),
                    )
                })?
        };
        hub.subscribe()
            .await
            .map_err(|_| ConnectorError::ResourceBusy("shared mqtt client is closed".to_string()))
    }

    async fn client(self: &Arc<Self>) -> Result<AsyncClient, ConnectorError> {
        self.ensure_runtime_started().await?;
        let runtime = self.runtime.lock().await;
        runtime
            .as_ref()
            .map(|runtime| runtime.client.clone())
            .ok_or_else(|| {
                ConnectorError::ResourceBusy(
                    "shared mqtt client runtime is unavailable".to_string(),
                )
            })
    }

    async fn ensure_runtime_started(self: &Arc<Self>) -> Result<(), ConnectorError> {
        if self.runtime.lock().await.is_some() {
            return Ok(());
        }

        let config = self.config.clone();
        let mut options = build_mqtt_options(&config)?;
        options.set_keep_alive(Duration::from_secs(30));

        let qos = map_qos(config.qos)?;
        let topic = config.topic.clone();

        let (client, mut event_loop) = AsyncClient::new(options, 64);
        client
            .subscribe(topic.clone(), qos)
            .await
            .map_err(|err| ConnectorError::Connection(err.to_string()))?;

        let events_hub = Arc::new(BackpressureHub::new(1024));
        let (shutdown_tx, _) = watch::channel(false);
        let mut runtime = self.runtime.lock().await;
        if runtime.is_some() {
            return Ok(());
        }

        let entry_for_task = Arc::clone(self);
        let mut shutdown_rx = shutdown_tx.subscribe();
        let events_hub_for_task = Arc::clone(&events_hub);
        let client_for_task = client.clone();
        let handle = self.spawner.spawn(async move {
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
                                let _ = events_hub_for_task
                                    .send(Ok(SharedMqttEvent::Payload(publish.payload.to_vec())))
                                    .await;
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
                            let _ = client_for_task.subscribe(topic.clone(), qos).await;
                        }
                        Ok(_) => {}
                        Err(ConnectionError::RequestsDone) => {
                            entry_for_task.connected.store(false, Ordering::Release);
                            if entry_for_task.closing.load(Ordering::Acquire) {
                                break;
                            }
                            let message = "mqtt requests done".to_string();
                            *entry_for_task.last_error.write() = Some(message.clone());
                            let _ = events_hub_for_task
                                .send(Err(ConnectorError::Connection(message)))
                                .await;
                            sleep(backoff).await;
                            backoff = std::cmp::min(backoff * 2, max_backoff);
                        }
                        Err(err) => {
                            entry_for_task.connected.store(false, Ordering::Release);
                            *entry_for_task.last_error.write() = Some(err.to_string());
                            let _ = events_hub_for_task
                                .send(Err(ConnectorError::Connection(err.to_string())))
                                .await;
                            sleep(backoff).await;
                            backoff = std::cmp::min(backoff * 2, max_backoff);
                        }
                    }
                }
            }
        });

        *runtime = Some(MqttClientRuntime {
            client,
            events_hub,
            shutdown_tx,
            join_handle: handle,
        });
        Ok(())
    }

    async fn shutdown_runtime(self: &Arc<Self>, emit_terminal: bool) {
        let runtime = {
            let mut runtime = self.runtime.lock().await;
            runtime.take()
        };
        let Some(runtime) = runtime else {
            return;
        };

        let _ = runtime.shutdown_tx.send(true);
        let _ = runtime.client.disconnect().await;
        runtime.join_handle.abort();
        let _ = runtime.join_handle.await;
        self.connected.store(false, Ordering::Release);
        if emit_terminal {
            runtime
                .events_hub
                .close(Some(Ok(SharedMqttEvent::EndOfStream)))
                .await;
        } else {
            runtime.events_hub.close(None).await;
        }
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
        validate_shared_mqtt_client_config(&config)?;

        let key = config.key.clone();
        {
            if self.entries.lock().contains_key(&key) {
                return Err(ConnectorError::AlreadyExists(key));
            }
        }

        let entry = Arc::new(MqttClientEntry::new(config, self.spawner.clone()));

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
        if let Err(err) = entry.ensure_runtime_started().await {
            entry.ref_count.fetch_sub(1, Ordering::AcqRel);
            return Err(err);
        }
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
            let shutdown_entry = Arc::clone(&entry);
            self.spawner.spawn(async move {
                shutdown_entry.shutdown_runtime(true).await;
            });
        }
        Ok(())
    }

    fn release(&self, entry: &Arc<MqttClientEntry>) {
        if entry.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            let emit_terminal = entry.closing.load(Ordering::Acquire);
            let shutdown_entry = Arc::clone(entry);
            self.spawner.spawn(async move {
                shutdown_entry.shutdown_runtime(emit_terminal).await;
            });
        }
    }

    #[cfg(test)]
    pub(crate) async fn test_runtime_started(&self, key: &str) -> bool {
        let entry = {
            let guard = self.entries.lock();
            guard.get(key).cloned()
        };
        let Some(entry) = entry else {
            return false;
        };
        let started = entry.runtime.lock().await.is_some();
        started
    }
}

fn validate_shared_mqtt_client_config(
    config: &SharedMqttClientConfig,
) -> Result<(), ConnectorError> {
    let _ = build_mqtt_options(config)?;
    let _ = map_qos(config.qos)?;
    Ok(())
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
    use super::{
        build_mqtt_options, mqtt_topic_matches, MqttClientManager, SharedMqttClientConfig,
        SharedMqttEvent,
    };
    use crate::connector::ConnectorError;
    use crate::runtime::TaskSpawner;
    use crate::test_support::EmbeddedMqttBroker;
    use rumqttc::{QoS, Transport};
    use tokio::runtime::Handle;
    use tokio::time::{sleep, timeout, Duration};

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
        let broker = EmbeddedMqttBroker::start().await;
        let manager = MqttClientManager::new(TaskSpawner::from_handle(Handle::current()));
        let cfg = SharedMqttClientConfig {
            key: "shared".to_string(),
            broker_url: broker.broker_url(),
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

    #[tokio::test]
    async fn shared_mqtt_runtime_is_lazy_started_and_reclaimed_when_last_reference_drops() {
        let broker = EmbeddedMqttBroker::start().await;
        let manager = MqttClientManager::new(TaskSpawner::from_handle(Handle::current()));
        let cfg = SharedMqttClientConfig {
            key: "lazy".to_string(),
            broker_url: broker.broker_url(),
            topic: "fleet/+/telemetry".to_string(),
            client_id: "client_lazy".to_string(),
            qos: 0,
            max_packet_size: None,
        };

        manager
            .create_client(cfg)
            .await
            .expect("create shared mqtt client");
        assert!(
            !manager.test_runtime_started("lazy").await,
            "runtime should not start before first acquisition"
        );

        let held = manager
            .acquire_client("lazy")
            .await
            .expect("acquire shared mqtt client");
        assert!(
            manager.test_runtime_started("lazy").await,
            "runtime should start on first acquisition"
        );

        drop(held);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if !manager.test_runtime_started("lazy").await {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "shared mqtt runtime did not stop after last reference dropped"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn shared_mqtt_subscribers_receive_payloads_from_embedded_broker() {
        let broker = EmbeddedMqttBroker::start().await;
        let manager = MqttClientManager::new(TaskSpawner::from_handle(Handle::current()));
        let cfg = SharedMqttClientConfig {
            key: "embedded".to_string(),
            broker_url: broker.broker_url(),
            topic: "fleet/+/telemetry".to_string(),
            client_id: "client_embedded".to_string(),
            qos: 0,
            max_packet_size: None,
        };

        manager
            .create_client(cfg)
            .await
            .expect("create shared mqtt client");
        let held = manager
            .acquire_client("embedded")
            .await
            .expect("acquire shared mqtt client");
        let mut events = held
            .subscribe()
            .await
            .expect("subscribe shared mqtt events");

        broker
            .publish("fleet/device_a/telemetry", b"first-payload".to_vec())
            .await
            .expect("publish first embedded mqtt payload");
        broker
            .publish("fleet/device_b/telemetry", b"second-payload".to_vec())
            .await
            .expect("publish second embedded mqtt payload");

        let first = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("recv first payload timeout")
            .expect("first payload event should exist");
        let second = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("recv second payload timeout")
            .expect("second payload event should exist");

        match first {
            Ok(SharedMqttEvent::Payload(payload)) => assert_eq!(payload, b"first-payload".to_vec()),
            other => panic!("expected first payload event, got {other:?}"),
        }
        match second {
            Ok(SharedMqttEvent::Payload(payload)) => {
                assert_eq!(payload, b"second-payload".to_vec());
            }
            other => panic!("expected second payload event, got {other:?}"),
        }

        held.publish(
            "fleet/device_c/telemetry".to_string(),
            QoS::AtMostOnce,
            false,
            b"published-through-shared-client".to_vec(),
        )
        .await
        .expect("publish through shared mqtt client");

        drop(held);
    }

    #[test]
    fn shared_mqtt_build_mqtt_options_uses_shared_client_packet_limit() {
        let config = SharedMqttClientConfig {
            key: "shared".to_string(),
            broker_url: "shared.example.com".to_string(),
            topic: "fleet/+/telemetry".to_string(),
            client_id: "shared_client".to_string(),
            qos: 1,
            max_packet_size: Some(16384),
        };

        let options = build_mqtt_options(&config).expect("build shared mqtt options");

        assert_eq!(
            options.broker_address(),
            ("shared.example.com".to_string(), 1883)
        );
        assert_eq!(options.client_id(), "shared_client");
        assert_eq!(options.max_packet_size(), 16384);
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn shared_mqtt_build_mqtt_options_enables_tls_and_secure_default_port() {
        let config = SharedMqttClientConfig {
            key: "shared_tls".to_string(),
            broker_url: "mqtts://secure-shared.example.com".to_string(),
            topic: "fleet/+/telemetry".to_string(),
            client_id: "shared_tls_client".to_string(),
            qos: 1,
            max_packet_size: None,
        };

        let options = build_mqtt_options(&config).expect("build secure shared mqtt options");

        assert_eq!(
            options.broker_address(),
            ("secure-shared.example.com".to_string(), 8883)
        );
        assert_eq!(options.client_id(), "shared_tls_client");
        assert_eq!(options.max_packet_size(), 64 * 1024 * 1024);
        assert!(matches!(options.transport(), Transport::Tls(_)));
    }
}
