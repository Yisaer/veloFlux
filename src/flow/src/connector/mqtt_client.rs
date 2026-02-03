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
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedMqttClientConfig {
    pub key: String,
    pub broker_url: String,
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
}

#[derive(Debug, Clone)]
pub enum SharedMqttEvent {
    Payload(Vec<u8>),
    EndOfStream,
}

pub struct SharedMqttClient {
    key: String,
    entry: Arc<MqttClientEntry>,
    manager: MqttClientManager,
}

impl SharedMqttClient {
    fn new(key: String, entry: Arc<MqttClientEntry>, manager: MqttClientManager) -> Self {
        entry.ref_count.fetch_add(1, Ordering::AcqRel);
        Self {
            key,
            entry,
            manager,
        }
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
        self.manager.release(&self.key);
    }
}

struct MqttClientEntry {
    client: AsyncClient,
    events_tx: broadcast::Sender<Result<SharedMqttEvent, ConnectorError>>,
    shutdown_tx: watch::Sender<bool>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    ref_count: AtomicUsize,
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
                topic,
                qos,
                connected: AtomicBool::new(false),
                last_error: RwLock::new(None),
            },
            event_loop,
        ))
    }

    fn start_event_loop(entry: &Arc<MqttClientEntry>, mut event_loop: EventLoop) {
        let entry_for_task = Arc::clone(entry);
        let mut shutdown_rx = entry.shutdown_tx.subscribe();
        let events_tx = entry.events_tx.clone();
        let topic = entry.topic.clone();
        let qos = entry.qos;
        let client = entry.client.clone();

        let handle = tokio::spawn(async move {
            let mut backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(5);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    event = event_loop.poll() => match event {
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            entry_for_task.connected.store(true, Ordering::Release);
                            backoff = Duration::from_millis(100);
                            if publish.topic == topic {
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

    fn spawn_shutdown(entry: Arc<Self>) {
        tokio::spawn(async move {
            let _ = entry.shutdown_tx.send(true);
            let _ = entry.client.disconnect().await;
            if let Some(handle) = entry.join_handle.lock().take() {
                handle.abort();
            }
            let _ = entry.events_tx.send(Ok(SharedMqttEvent::EndOfStream));
        });
    }
}

#[derive(Clone, Default)]
pub struct MqttClientManager {
    entries: Arc<Mutex<HashMap<String, Arc<MqttClientEntry>>>>,
}

impl MqttClientManager {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn create_client(
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
        MqttClientEntry::start_event_loop(&entry, event_loop);

        self.entries.lock().insert(key, entry);
        Ok(())
    }

    pub async fn acquire_client(&self, key: &str) -> Result<SharedMqttClient, ConnectorError> {
        if let Some(entry) = self.entries.lock().get(key).cloned() {
            return Ok(SharedMqttClient::new(key.to_string(), entry, self.clone()));
        }
        Err(ConnectorError::NotFound(key.to_string()))
    }

    pub fn drop_client(&self, key: &str) -> Result<(), ConnectorError> {
        let mut guard = self.entries.lock();
        let entry = guard
            .get(key)
            .cloned()
            .ok_or_else(|| ConnectorError::NotFound(key.to_string()))?;

        if entry.ref_count.load(Ordering::Acquire) > 0 {
            return Err(ConnectorError::ResourceBusy(key.to_string()));
        }

        guard.remove(key);
        MqttClientEntry::spawn_shutdown(entry);
        Ok(())
    }

    pub fn release(&self, key: &str) {
        if let Some(entry) = self.entries.lock().get(key) {
            entry.ref_count.fetch_sub(1, Ordering::AcqRel);
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
