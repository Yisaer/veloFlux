//! MQTT sink connector implemented with the `rumqttc` async client.

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop, MqttOptions, QoS, Transport};
use tokio::task::JoinHandle;
use url::Url;

/// Basic MQTT configuration for sinks.
#[derive(Debug, Clone)]
pub struct MqttSinkConfig {
    /// Logical identifier for this sink.
    pub sink_name: String,
    /// Broker endpoint (e.g. `tcp://localhost:1883`).
    pub broker_url: String,
    /// Topic to publish results to.
    pub topic: String,
    /// Requested QoS level (0, 1, or 2).
    pub qos: u8,
    /// Whether MQTT retain flag should be set.
    pub retain: bool,
    /// Optional MQTT client id. Defaults to `sink_name`.
    pub client_id: Option<String>,
}

impl MqttSinkConfig {
    pub fn new(
        sink_name: impl Into<String>,
        broker_url: impl Into<String>,
        topic: impl Into<String>,
        qos: u8,
    ) -> Self {
        Self {
            sink_name: sink_name.into(),
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            retain: false,
            client_id: None,
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    fn client_id(&self) -> String {
        self.client_id
            .clone()
            .unwrap_or_else(|| self.sink_name.clone())
    }
}

/// Connector that publishes encoded payloads to an MQTT topic.
pub struct MqttSinkConnector {
    id: String,
    config: MqttSinkConfig,
    qos: QoS,
    client: Option<AsyncClient>,
    event_loop_handle: Option<JoinHandle<()>>,
}

impl MqttSinkConnector {
    /// Create a connector; the underlying MQTT session is lazily established on
    /// the first `send` call.
    pub fn new(id: impl Into<String>, config: MqttSinkConfig) -> Result<Self, SinkConnectorError> {
        let qos = Self::map_qos(config.qos)?;
        Ok(Self {
            id: id.into(),
            config,
            qos,
            client: None,
            event_loop_handle: None,
        })
    }

    async fn ensure_connection(&mut self) -> Result<(), SinkConnectorError> {
        if self.client.is_some() {
            return Ok(());
        }

        let options = Self::build_mqtt_options(&self.config)?;
        let (client, event_loop) = AsyncClient::new(options, 32);
        self.event_loop_handle = Some(Self::spawn_event_loop(event_loop));
        self.client = Some(client);
        Ok(())
    }

    fn spawn_event_loop(mut event_loop: EventLoop) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) => {}
                    Err(ConnectionError::RequestsDone) => break,
                    Err(err) => {
                        eprintln!("[mqtt_sink] event loop error: {err}");
                        break;
                    }
                }
            }
        })
    }

    fn build_mqtt_options(config: &MqttSinkConfig) -> Result<MqttOptions, SinkConnectorError> {
        let normalized = Self::normalize_broker_url(&config.broker_url);
        let endpoint = Url::parse(&normalized).map_err(|err| {
            SinkConnectorError::Other(format!("invalid broker URL `{}`: {err}", config.broker_url))
        })?;
        let scheme = endpoint.scheme();

        let host = endpoint.host_str().ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "broker URL `{}` is missing a host",
                config.broker_url
            ))
        })?;

        let port = endpoint
            .port()
            .or_else(|| Self::default_port_for_scheme(scheme))
            .ok_or_else(|| {
                SinkConnectorError::Other(format!(
                    "broker URL `{}` is missing a port",
                    config.broker_url
                ))
            })?;

        let mut options = MqttOptions::new(config.client_id(), host, port);

        if Self::is_tls_scheme(scheme) {
            options.set_transport(Transport::tls_with_default_config());
        }

        Ok(options)
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

    fn normalize_broker_url(url: &str) -> String {
        if url.contains("://") {
            url.to_owned()
        } else {
            format!("tcp://{url}")
        }
    }

    fn map_qos(qos: u8) -> Result<QoS, SinkConnectorError> {
        match qos {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            other => Err(SinkConnectorError::Other(format!(
                "unsupported MQTT QoS level: {other}"
            ))),
        }
    }
}

#[async_trait]
impl SinkConnector for MqttSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        self.ensure_connection().await?;

        let client = self.client.as_ref().ok_or_else(|| {
            SinkConnectorError::Unavailable(format!("mqtt sink `{}` not connected", self.id))
        })?;

        client
            .publish(
                self.config.topic.clone(),
                self.qos,
                self.config.retain,
                payload.to_vec(),
            )
            .await
            .map_err(|err| SinkConnectorError::Other(format!("mqtt publish error: {err}")))
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        if let Some(client) = &self.client {
            client.disconnect().await.map_err(|err| {
                SinkConnectorError::Other(format!("mqtt disconnect error: {err}"))
            })?;
        }
        self.client = None;
        if let Some(handle) = self.event_loop_handle.take() {
            handle.abort();
        }
        Ok(())
    }
}
