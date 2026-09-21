//! MQTT sink connector supporting shared or standalone clients.

use super::{DeliveryResult, SinkConnector, SinkConnectorError};
use crate::connector::mqtt_protocol::{
    MqttClient, MqttConnectionError, MqttEvent, MqttEventLoop, MqttOptions,
    MqttOutgoingPacketLimit, MqttProtocolVersion, MqttQos, DEFAULT_MQTT_MAX_PACKET_SIZE,
};
use async_trait::async_trait;
use parking_lot::RwLock;
use rumqttc::Transport;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;

use crate::connector::mask_url_userinfo;
use crate::connector::mqtt_client::{MqttClientManager, SharedMqttClient};
use crate::runtime::TaskSpawner;
use crate::template::ConnectorString;

/// Basic MQTT configuration for sinks.
#[derive(Debug, Clone)]
pub struct MqttSinkConfig {
    pub sink_name: String,
    pub broker_url: String,
    pub topic: ConnectorString,
    pub qos: u8,
    pub retain: bool,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
    pub max_packet_size: Option<usize>,
    pub protocol_version: Option<crate::connector::MqttProtocolVersion>,
    pub user_properties: Vec<crate::connector::MqttUserProperty>,
}

impl MqttSinkConfig {
    pub fn new(
        sink_name: impl Into<String>,
        broker_url: impl Into<String>,
        topic: impl Into<ConnectorString>,
        qos: u8,
    ) -> Self {
        Self {
            sink_name: sink_name.into(),
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            retain: false,
            client_id: None,
            connector_key: None,
            max_packet_size: None,
            protocol_version: None,
            user_properties: Vec::new(),
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

    pub fn with_connector_key(mut self, connector_key: impl Into<String>) -> Self {
        self.connector_key = Some(connector_key.into());
        self
    }

    pub fn with_max_packet_size(mut self, max_packet_size: usize) -> Self {
        self.max_packet_size = Some(max_packet_size);
        self
    }

    pub fn with_protocol_version(
        mut self,
        protocol_version: crate::connector::MqttProtocolVersion,
    ) -> Self {
        self.protocol_version = Some(protocol_version);
        self
    }

    pub fn with_user_properties(
        mut self,
        user_properties: Vec<crate::connector::MqttUserProperty>,
    ) -> Self {
        self.user_properties = user_properties;
        self
    }

    fn client_id(&self) -> String {
        self.client_id
            .clone()
            .unwrap_or_else(|| self.sink_name.clone())
    }
}

pub(crate) struct MqttSinkConnector {
    id: String,
    flow_instance_id: Arc<str>,
    config: MqttSinkConfig,
    client: Option<SinkClient>,
    buffer: Option<Vec<u8>>,
    delivery_topic: Option<ConnectorString>,
    delivery_user_properties: Vec<crate::connector::MqttUserProperty>,
    mqtt_clients: MqttClientManager,
    spawner: TaskSpawner,
}

enum SinkClient {
    Shared(SharedMqttClient),
    Standalone(StandaloneMqttClient),
}

impl SinkClient {
    fn protocol_version(&self) -> MqttProtocolVersion {
        match self {
            Self::Shared(shared) => shared.protocol_version(),
            Self::Standalone(standalone) => standalone.protocol_version,
        }
    }

    fn outgoing_packet_limit(&self) -> MqttOutgoingPacketLimit {
        match self {
            Self::Shared(shared) => shared.outgoing_packet_limit(),
            Self::Standalone(standalone) => standalone.outgoing_packet_limit(),
        }
    }

    async fn publish(
        &self,
        topic: &str,
        qos: MqttQos,
        retain: bool,
        payload: Vec<u8>,
        user_properties: &[crate::connector::MqttUserProperty],
    ) -> Result<(), SinkConnectorError> {
        match self {
            SinkClient::Shared(shared) => shared
                .publish_with_user_properties(
                    topic.to_string(),
                    qos,
                    retain,
                    payload,
                    user_properties,
                )
                .await
                .map_err(|err| SinkConnectorError::Transient(format!("mqtt publish error: {err}"))),
            SinkClient::Standalone(standalone) => {
                standalone
                    .publish(topic, qos, retain, payload, user_properties)
                    .await
            }
        }
    }

    async fn shutdown(self) -> Result<(), SinkConnectorError> {
        match self {
            SinkClient::Shared(_) => Ok(()),
            SinkClient::Standalone(standalone) => standalone.shutdown().await,
        }
    }
}

struct StandaloneMqttClient {
    client: MqttClient,
    event_loop_handle: JoinHandle<()>,
    state: MqttConnectionState,
    protocol_version: MqttProtocolVersion,
    v3_max_packet_size: usize,
}

#[derive(Clone, Default)]
struct MqttConnectionState {
    connected: Arc<AtomicBool>,
    outgoing_packet_limit: Arc<RwLock<MqttOutgoingPacketLimit>>,
    last_error: Arc<RwLock<Option<String>>>,
}

impl MqttConnectionState {
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn set_connected(&self, max_outgoing_packet_size: Option<usize>) {
        *self.outgoing_packet_limit.write() =
            MqttOutgoingPacketLimit::from_connack(max_outgoing_packet_size);
        *self.last_error.write() = None;
        self.connected.store(true, Ordering::Release);
    }

    fn set_error(&self, err: impl Into<String>) {
        self.connected.store(false, Ordering::Release);
        *self.outgoing_packet_limit.write() = MqttOutgoingPacketLimit::Unknown;
        *self.last_error.write() = Some(err.into());
    }

    fn last_error(&self) -> Option<String> {
        self.last_error.read().clone()
    }
}

impl StandaloneMqttClient {
    async fn new(
        config: &MqttSinkConfig,
        spawner: &TaskSpawner,
    ) -> Result<Self, SinkConnectorError> {
        let options = build_mqtt_options(config)?;
        let (client, event_loop) = MqttClient::new(options, 32);
        let state = MqttConnectionState::default();
        let event_loop_handle = spawner.spawn(run_event_loop(event_loop, state.clone()));
        Ok(Self {
            client,
            event_loop_handle,
            state,
            protocol_version: config.protocol_version.unwrap_or_default(),
            v3_max_packet_size: config
                .max_packet_size
                .unwrap_or(DEFAULT_MQTT_MAX_PACKET_SIZE),
        })
    }

    fn outgoing_packet_limit(&self) -> MqttOutgoingPacketLimit {
        match self.protocol_version {
            MqttProtocolVersion::V3 => MqttOutgoingPacketLimit::Limited(self.v3_max_packet_size),
            MqttProtocolVersion::V5 => *self.state.outgoing_packet_limit.read(),
        }
    }

    async fn publish(
        &self,
        topic: &str,
        qos: MqttQos,
        retain: bool,
        payload: Vec<u8>,
        user_properties: &[crate::connector::MqttUserProperty],
    ) -> Result<(), SinkConnectorError> {
        if !self.state.is_connected() {
            let message = self
                .state
                .last_error()
                .map(|err| format!("mqtt not connected: {err}"))
                .unwrap_or_else(|| "mqtt not connected".to_string());
            // Connection is not established — transient, may recover.
            return Err(SinkConnectorError::Transient(message));
        }

        let result = if user_properties.is_empty() {
            self.client
                .publish(topic.to_string(), qos, retain, payload)
                .await
        } else {
            self.client
                .publish_with_user_properties(
                    topic.to_string(),
                    qos,
                    retain,
                    payload,
                    user_properties,
                )
                .await
        };
        result.map_err(|err| SinkConnectorError::Transient(format!("mqtt publish error: {err}")))
    }

    async fn shutdown(self) -> Result<(), SinkConnectorError> {
        if self.client.disconnect().await.is_err() {
            tracing::debug!("mqtt sink disconnect skipped: eventloop not available");
        }
        self.event_loop_handle.abort();
        Ok(())
    }
}

async fn run_event_loop(mut event_loop: MqttEventLoop, state: MqttConnectionState) {
    let mut backoff = std::time::Duration::from_millis(100);
    let max_backoff = std::time::Duration::from_secs(5);
    loop {
        match event_loop.poll().await {
            Ok(MqttEvent::Connected {
                max_outgoing_packet_size,
            }) => {
                state.set_connected(max_outgoing_packet_size);
                backoff = std::time::Duration::from_millis(100);
            }
            Ok(MqttEvent::Disconnected) => {
                tracing::warn!("mqtt sink disconnected; reconnecting");
                state.set_error("disconnect");
                event_loop.clean();
                backoff = std::time::Duration::from_millis(100);
            }
            Ok(MqttEvent::Publish { .. }) | Ok(MqttEvent::Other) => {
                backoff = std::time::Duration::from_millis(100);
            }
            Err(MqttConnectionError::RequestsDone) => break,
            Err(MqttConnectionError::Connection(err)) => {
                tracing::warn!(error = %err, "mqtt sink event loop error; reconnecting");
                state.set_error(err);
                sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
        }
    }
}

impl MqttSinkConnector {
    pub fn new(
        id: impl Into<String>,
        config: MqttSinkConfig,
        flow_instance_id: impl Into<Arc<str>>,
        mqtt_clients: MqttClientManager,
        spawner: TaskSpawner,
    ) -> Self {
        Self {
            id: id.into(),
            flow_instance_id: flow_instance_id.into(),
            config,
            client: None,
            buffer: None,
            delivery_topic: None,
            delivery_user_properties: Vec::new(),
            mqtt_clients,
            spawner,
        }
    }

    async fn ensure_client(&mut self) -> Result<(), SinkConnectorError> {
        if self.client.is_some() {
            return Ok(());
        }

        crate::connector::validate_mqtt_user_properties(&self.config.user_properties)
            .map_err(SinkConnectorError::Permanent)?;
        if self.config.connector_key.is_some() && self.config.protocol_version.is_some() {
            return Err(SinkConnectorError::Permanent(
                "mqtt sink protocol_version is owned by connector_key and must not be set locally"
                    .to_string(),
            ));
        }

        if let Some(connector_key) = self.config.connector_key.clone() {
            let client = self
                .mqtt_clients
                .acquire_client(&connector_key)
                .await
                .map_err(|err| SinkConnectorError::Other(err.to_string()))?;
            if client.protocol_version() == crate::connector::MqttProtocolVersion::V3
                && !self.config.user_properties.is_empty()
            {
                return Err(SinkConnectorError::Permanent(
                    "mqtt sink user_properties require effective protocol_version `v5`".to_string(),
                ));
            }
            tracing::info!(
                connector_id = %self.id,
                connector_key = %connector_key,
                "mqtt sink starting with shared client"
            );
            self.client = Some(SinkClient::Shared(client));
        } else {
            if self.config.protocol_version.unwrap_or_default()
                == crate::connector::MqttProtocolVersion::V3
                && !self.config.user_properties.is_empty()
            {
                return Err(SinkConnectorError::Permanent(
                    "mqtt sink user_properties require protocol_version `v5`".to_string(),
                ));
            }
            let standalone = StandaloneMqttClient::new(&self.config, &self.spawner).await?;
            tracing::info!(connector_id = %self.id, "mqtt sink starting standalone client");
            self.client = Some(SinkClient::Standalone(standalone));
        }
        Ok(())
    }

    fn publish_qos(&self) -> Result<MqttQos, SinkConnectorError> {
        MqttQos::try_from(self.config.qos).map_err(SinkConnectorError::Permanent)
    }
}

#[async_trait]
impl SinkConnector for MqttSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn record_message_in(&self) {
        veloflux_metrics::mqtt_sink_messages_in_total()
            .with_label_values(&[self.flow_instance_id.as_ref(), self.id.as_str()])
            .inc();
    }

    fn record_message_out(&self) {
        veloflux_metrics::mqtt_sink_messages_out_total()
            .with_label_values(&[self.flow_instance_id.as_ref(), self.id.as_str()])
            .inc();
    }

    fn max_delivery_bytes(&self) -> Option<usize> {
        if self.config.connector_key.is_none()
            && self.config.protocol_version.unwrap_or_default() == MqttProtocolVersion::V3
        {
            Some(
                self.config
                    .max_packet_size
                    .unwrap_or(DEFAULT_MQTT_MAX_PACKET_SIZE),
            )
        } else {
            None
        }
    }

    async fn start_delivery(
        &mut self,
        properties: &crate::PropertyContext,
    ) -> Result<(), SinkConnectorError> {
        self.ensure_client().await?;
        let topic = self
            .config
            .topic
            .resolve(properties)
            .map_err(|err| SinkConnectorError::Permanent(err.to_string()))?;
        crate::validate_mqtt_publish_topic(&topic).map_err(SinkConnectorError::Permanent)?;
        let user_properties = self
            .config
            .user_properties
            .iter()
            .map(|property| {
                property
                    .value
                    .resolve(properties)
                    .map(|value| crate::connector::MqttUserProperty::new(&property.key, value))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| SinkConnectorError::Permanent(err.to_string()))?;
        crate::connector::validate_mqtt_user_properties(&user_properties)
            .map_err(SinkConnectorError::Permanent)?;
        self.delivery_topic = Some(topic);
        self.delivery_user_properties = user_properties;
        self.buffer = Some(Vec::new());
        Ok(())
    }

    async fn write_chunk(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        let Some(buffer) = self.buffer.as_mut() else {
            return Err(SinkConnectorError::Other(format!(
                "mqtt sink `{}` received chunk without active delivery",
                self.id
            )));
        };
        buffer.extend_from_slice(payload);
        Ok(())
    }

    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        let payload = self.buffer.take().ok_or_else(|| {
            SinkConnectorError::Permanent(format!(
                "mqtt sink `{}` finished without active delivery",
                self.id
            ))
        })?;
        let bytes_written = payload.len() as u64;
        let qos = self.publish_qos()?;
        let topic = self.delivery_topic.take().ok_or_else(|| {
            SinkConnectorError::Permanent(format!(
                "mqtt sink `{}` has no topic for active delivery",
                self.id
            ))
        })?;
        let user_properties = std::mem::take(&mut self.delivery_user_properties);
        if let Some(client) = &self.client {
            let packet_size = mqtt_publish_packet_size(
                client.protocol_version(),
                topic.expose(),
                qos,
                payload.len(),
                &user_properties,
            )?;
            match client.outgoing_packet_limit() {
                MqttOutgoingPacketLimit::Unknown => {
                    return Err(SinkConnectorError::Transient(
                        "mqtt not connected: outgoing packet limit unavailable before CONNACK"
                            .to_string(),
                    ));
                }
                MqttOutgoingPacketLimit::Limited(limit) if packet_size > limit => {
                    return Err(SinkConnectorError::Permanent(format!(
                        "mqtt publish packet size {packet_size} exceeds connection limit {limit}"
                    )));
                }
                MqttOutgoingPacketLimit::Unlimited | MqttOutgoingPacketLimit::Limited(_) => {}
            }
            client
                .publish(
                    topic.expose(),
                    qos,
                    self.config.retain,
                    payload,
                    &user_properties,
                )
                .await?;
            Ok(DeliveryResult { bytes_written })
        } else {
            Err(SinkConnectorError::Transient(format!(
                "mqtt sink `{}` not connected",
                self.id
            )))
        }
    }

    async fn abort_delivery(&mut self) {
        self.buffer = None;
        self.delivery_topic = None;
        self.delivery_user_properties.clear();
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_client().await
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        if let Some(client) = self.client.take() {
            client.shutdown().await?;
            tracing::info!(connector_id = %self.id, "mqtt sink closed");
        }
        Ok(())
    }
}

const MQTT_MAX_REMAINING_LENGTH: usize = 268_435_455;

fn mqtt_variable_byte_integer_len(value: usize) -> Result<usize, SinkConnectorError> {
    if value > MQTT_MAX_REMAINING_LENGTH {
        return Err(SinkConnectorError::Permanent(
            "mqtt publish packet exceeds protocol size limit".to_string(),
        ));
    }
    Ok(match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    })
}

fn mqtt_publish_packet_size(
    protocol_version: MqttProtocolVersion,
    topic: &str,
    qos: MqttQos,
    payload_len: usize,
    user_properties: &[crate::connector::MqttUserProperty],
) -> Result<usize, SinkConnectorError> {
    let mut remaining_len = 2usize
        .saturating_add(topic.len())
        .saturating_add(if qos == MqttQos::Qos0 { 0 } else { 2 })
        .saturating_add(payload_len);
    if protocol_version == MqttProtocolVersion::V5 {
        let properties_len = user_properties.iter().fold(0usize, |len, property| {
            len.saturating_add(1)
                .saturating_add(2)
                .saturating_add(property.key.len())
                .saturating_add(2)
                .saturating_add(property.value.expose().len())
        });
        remaining_len = remaining_len
            .saturating_add(mqtt_variable_byte_integer_len(properties_len)?)
            .saturating_add(properties_len);
    }
    let remaining_len_bytes = mqtt_variable_byte_integer_len(remaining_len)?;
    Ok(1 + remaining_len_bytes + remaining_len)
}

fn build_mqtt_options(config: &MqttSinkConfig) -> Result<MqttOptions, SinkConnectorError> {
    let normalized = normalize_broker_url(&config.broker_url);
    let endpoint = Url::parse(&normalized).map_err(|err| {
        SinkConnectorError::Other(format!(
            "invalid broker URL `{}`: {err}",
            mask_url_userinfo(&config.broker_url)
        ))
    })?;
    let scheme = endpoint.scheme();

    let host = endpoint.host_str().ok_or_else(|| {
        SinkConnectorError::Other(format!(
            "broker URL `{}` is missing a host",
            mask_url_userinfo(&config.broker_url)
        ))
    })?;

    let port = endpoint
        .port()
        .or_else(|| default_port_for_scheme(scheme))
        .ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "broker URL `{}` is missing a port",
                mask_url_userinfo(&config.broker_url)
            ))
        })?;

    let max_packet_size = config
        .max_packet_size
        .unwrap_or(DEFAULT_MQTT_MAX_PACKET_SIZE);
    let mut options = MqttOptions::new(
        config.protocol_version.unwrap_or_default(),
        config.client_id(),
        host,
        port,
        max_packet_size,
    )
    .map_err(SinkConnectorError::Other)?;
    if is_tls_scheme(scheme) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::mqtt_client::SharedMqttClientConfig;
    use crate::connector::SinkConnector;
    use crate::model::batch_from_columns_simple;
    use crate::runtime::TaskSpawner;
    use crate::test_support::EmbeddedMqttBroker;
    use datatypes::Value;
    use rumqttc::mqttbytes::v4::Publish as V3Publish;
    use rumqttc::v5::mqttbytes::v5::Packet as V5Packet;
    use rumqttc::v5::mqttbytes::v5::{Publish as V5Publish, PublishProperties};
    use rumqttc::v5::mqttbytes::QoS as V5Qos;
    use rumqttc::v5::{AsyncClient as V5Client, Event as V5Event, MqttOptions as V5Options};
    use rumqttc::{
        AsyncClient as V3Client, Event as V3Event, MqttOptions as V3Options, Packet as V3Packet,
        QoS as V3Qos,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::runtime::Handle;
    use tokio::sync::{mpsc, oneshot, watch};
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    #[test]
    fn mqtt_publish_packet_size_matches_rumqttc_for_qos_and_properties() {
        let topic = "fleet/vehicle/telemetry";
        let payload = vec![b'x'; 127];
        for (qos, v3_qos, v5_qos) in [
            (MqttQos::Qos0, V3Qos::AtMostOnce, V5Qos::AtMostOnce),
            (MqttQos::Qos1, V3Qos::AtLeastOnce, V5Qos::AtLeastOnce),
        ] {
            let mut v3 = V3Publish::new(topic, v3_qos, payload.clone());
            let mut v5 = V5Publish::new(topic, v5_qos, payload.clone(), None);
            if qos != MqttQos::Qos0 {
                v3.pkid = 1;
                v5.pkid = 1;
            }
            assert_eq!(
                mqtt_publish_packet_size(MqttProtocolVersion::V3, topic, qos, payload.len(), &[])
                    .expect("v3 packet size"),
                v3.size()
            );
            assert_eq!(
                mqtt_publish_packet_size(MqttProtocolVersion::V5, topic, qos, payload.len(), &[])
                    .expect("v5 packet size"),
                v5.size()
            );
        }

        let properties = vec![
            crate::connector::MqttUserProperty::new("tag", "first"),
            crate::connector::MqttUserProperty::new("tag", "second"),
        ];
        let v5 = V5Publish::new(
            topic,
            V5Qos::AtMostOnce,
            payload.clone(),
            Some(PublishProperties {
                user_properties: properties
                    .iter()
                    .map(|property| (property.key.clone(), property.value.expose().to_string()))
                    .collect(),
                ..PublishProperties::default()
            }),
        );
        assert_eq!(
            mqtt_publish_packet_size(
                MqttProtocolVersion::V5,
                topic,
                MqttQos::Qos0,
                payload.len(),
                &properties
            )
            .expect("v5 packet size with user properties"),
            v5.size()
        );
        assert!(mqtt_publish_packet_size(
            MqttProtocolVersion::V3,
            topic,
            MqttQos::Qos0,
            MQTT_MAX_REMAINING_LENGTH,
            &[]
        )
        .is_err());
    }

    #[tokio::test]
    async fn mqtt_sink_rejects_oversized_packets_for_standalone_and_shared_v3() {
        let broker = EmbeddedMqttBroker::start().await;
        let spawner = TaskSpawner::from_handle(Handle::current());
        let manager = MqttClientManager::new("default", spawner.clone());
        let topic = broker.scoped_topic("sink/packet-limit");
        manager
            .create_client(SharedMqttClientConfig {
                key: "packet-limit".to_string(),
                broker_url: broker.broker_url(),
                topic: broker.scoped_filter("sink/#"),
                client_id: "mqtt_shared_packet_limit".to_string(),
                qos: 0,
                max_packet_size: Some(256),
                protocol_version: MqttProtocolVersion::V3,
                username: None,
                password: None,
                resolved_password: None,
            })
            .await
            .expect("create shared client");

        for config in [
            MqttSinkConfig::new("standalone", broker.broker_url(), topic.clone(), 1)
                .with_max_packet_size(256),
            MqttSinkConfig::new("shared", broker.broker_url(), topic.clone(), 1)
                .with_connector_key("packet-limit")
                .with_max_packet_size(8),
        ] {
            let mut connector = MqttSinkConnector::new(
                config.sink_name.clone(),
                config,
                Arc::<str>::from("default"),
                manager.clone(),
                spawner.clone(),
            );
            connector.ready().await.expect("mqtt sink ready");
            let expected_payload_limit = if connector.config.connector_key.is_some() {
                None
            } else {
                Some(256)
            };
            assert_eq!(connector.max_delivery_bytes(), expected_payload_limit);
            connector
                .start_delivery(&crate::PropertyContext::default())
                .await
                .expect("start oversized delivery");
            connector
                .write_chunk(&vec![b'x'; 256])
                .await
                .expect("write oversized delivery");
            let error = connector
                .finish_delivery()
                .await
                .expect_err("full publish packet exceeds connection limit");
            assert!(matches!(error, SinkConnectorError::Permanent(_)));
            assert!(error.to_string().contains("exceeds connection limit 256"));
            connector.close().await.expect("close mqtt sink");
        }
    }

    #[tokio::test]
    async fn mqtt_sink_checks_resolved_topic_size_for_each_delivery() {
        let broker = EmbeddedMqttBroker::start().await;
        let registry = crate::PropertyRegistry::new(
            std::collections::BTreeMap::from([(
                "vin".to_string(),
                crate::secret::SecretString::new("short"),
            )]),
            true,
        );
        let topic = registry
            .bind(r#"out/{{ prop("vin") }}"#)
            .expect("bind dynamic MQTT topic");
        let spawner = TaskSpawner::from_handle(Handle::current());
        let mut connector = MqttSinkConnector::new(
            "dynamic-topic-limit",
            MqttSinkConfig::new("dynamic-topic-limit", broker.broker_url(), topic, 0)
                .with_max_packet_size(64),
            Arc::<str>::from("default"),
            MqttClientManager::new("default", spawner.clone()),
            spawner,
        );
        connector.ready().await.expect("mqtt sink ready");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !matches!(
            connector.client.as_ref(),
            Some(SinkClient::Standalone(client)) if client.state.is_connected()
        ) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "MQTT did not connect"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        connector
            .start_delivery(&registry.snapshot())
            .await
            .expect("start short-topic delivery");
        connector
            .write_chunk(&vec![b'x'; 40])
            .await
            .expect("write short-topic delivery");
        connector
            .finish_delivery()
            .await
            .expect("short-topic packet fits limit");

        registry
            .apply_override(
                "vin",
                crate::secret::SecretString::new("this-is-a-very-long-vin"),
            )
            .expect("update dynamic topic property");
        connector
            .start_delivery(&registry.snapshot())
            .await
            .expect("start long-topic delivery");
        connector
            .write_chunk(&vec![b'x'; 40])
            .await
            .expect("write long-topic delivery");
        let error = connector
            .finish_delivery()
            .await
            .expect_err("longer resolved topic exceeds limit");
        assert!(matches!(error, SinkConnectorError::Permanent(_)));
        assert!(error.to_string().contains("exceeds connection limit 64"));
        connector.close().await.expect("close mqtt sink");
    }

    #[tokio::test]
    async fn mqtt_v5_sink_uses_broker_outgoing_limit_for_standalone_and_shared_clients() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind MQTT 5 test broker");
        let port = listener.local_addr().expect("broker address").port();
        let (connack_tx, connack_rx) = watch::channel(false);
        let broker = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept MQTT client");
                let mut connack_rx = connack_rx.clone();
                tokio::spawn(async move {
                    let mut buffer = [0u8; 512];
                    let mut header = [0u8; 1];
                    stream
                        .read_exact(&mut header)
                        .await
                        .expect("read CONNECT header");
                    assert_eq!(header[0], 0x10, "expected MQTT CONNECT packet");
                    let mut remaining_len = 0usize;
                    for index in 0..4 {
                        let mut byte = [0u8; 1];
                        stream
                            .read_exact(&mut byte)
                            .await
                            .expect("read CONNECT remaining length");
                        remaining_len |= usize::from(byte[0] & 0x7f) << (7 * index);
                        if byte[0] & 0x80 == 0 {
                            break;
                        }
                        assert!(index < 3, "invalid MQTT remaining length");
                    }
                    let mut connect_body = vec![0u8; remaining_len];
                    stream
                        .read_exact(&mut connect_body)
                        .await
                        .expect("read complete CONNECT body");
                    while !*connack_rx.borrow() {
                        connack_rx.changed().await.expect("release CONNACK");
                    }
                    // Successful CONNACK with Maximum Packet Size = 128 bytes.
                    stream
                        .write_all(&[0x20, 0x08, 0x00, 0x00, 0x05, 0x27, 0x00, 0x00, 0x00, 0x80])
                        .await
                        .expect("send CONNACK");
                    while stream.read(&mut buffer).await.unwrap_or(0) != 0 {}
                });
            }
        });

        let broker_url = format!("tcp://127.0.0.1:{port}");
        let spawner = TaskSpawner::from_handle(Handle::current());
        let manager = MqttClientManager::new("default", spawner.clone());
        manager
            .create_client(SharedMqttClientConfig {
                key: "v5-packet-limit".to_string(),
                broker_url: broker_url.clone(),
                topic: "tests/#".to_string(),
                client_id: "mqtt_shared_v5_packet_limit".to_string(),
                qos: 0,
                max_packet_size: Some(8192),
                protocol_version: MqttProtocolVersion::V5,
                username: None,
                password: None,
                resolved_password: None,
            })
            .await
            .expect("create shared MQTT 5 client");

        let user_properties = vec![crate::connector::MqttUserProperty::new(
            "tag",
            "property-value-that-makes-the-packet-too-large",
        )];
        assert!(
            mqtt_publish_packet_size(
                MqttProtocolVersion::V5,
                "tests/packet-limit",
                MqttQos::Qos1,
                90,
                &[],
            )
            .expect("bare MQTT 5 packet size")
                <= 128
        );

        for config in [
            MqttSinkConfig::new("v5-standalone", &broker_url, "tests/packet-limit", 1)
                .with_protocol_version(MqttProtocolVersion::V5)
                .with_max_packet_size(8192)
                .with_user_properties(user_properties.clone()),
            MqttSinkConfig::new("v5-shared", &broker_url, "tests/packet-limit", 1)
                .with_connector_key("v5-packet-limit")
                .with_user_properties(user_properties.clone()),
        ] {
            connack_tx.send(false).expect("hold CONNACK");
            let mut connector = MqttSinkConnector::new(
                config.sink_name.clone(),
                config,
                Arc::<str>::from("default"),
                manager.clone(),
                spawner.clone(),
            );
            connector.ready().await.expect("MQTT 5 sink ready");
            assert_eq!(connector.max_delivery_bytes(), None);

            connector
                .start_delivery(&crate::PropertyContext::default())
                .await
                .expect("start delivery before CONNACK");
            connector
                .write_chunk(&vec![b'x'; 90])
                .await
                .expect("write delivery before CONNACK");
            let error = connector
                .finish_delivery()
                .await
                .expect_err("limit is unknown before CONNACK");
            assert!(matches!(error, SinkConnectorError::Transient(_)));
            connector.abort_delivery().await;

            connack_tx.send(true).expect("send CONNACK");

            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let connected = match connector.client.as_ref().expect("MQTT client") {
                    SinkClient::Shared(shared) => shared.is_connected(),
                    SinkClient::Standalone(standalone) => standalone.state.is_connected(),
                };
                if connected {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "MQTT 5 did not connect"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            connector
                .start_delivery(&crate::PropertyContext::default())
                .await
                .expect("start oversized MQTT 5 delivery");
            connector
                .write_chunk(&vec![b'x'; 90])
                .await
                .expect("write oversized MQTT 5 delivery");
            let error = connector
                .finish_delivery()
                .await
                .expect_err("packet exceeds broker limit");
            assert!(matches!(error, SinkConnectorError::Permanent(_)));
            assert!(error.to_string().contains("exceeds connection limit 128"));
            connector.close().await.expect("close MQTT 5 sink");
        }
        broker.abort();
    }

    #[tokio::test]
    async fn mqtt_sink_connector_rejects_collection_payloads() {
        let spawner = test_spawner();
        let mut connector = MqttSinkConnector::new(
            "mqtt_sink_collection_contract",
            MqttSinkConfig::new(
                "mqtt_sink_collection_contract",
                "mqtt://localhost:1883",
                "tests/mqtt/out",
                0,
            ),
            Arc::<str>::from("default"),
            MqttClientManager::new("default", spawner.clone()),
            spawner,
        );

        let batch = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "value".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("build collection payload");

        let err = connector
            .send_collection(&batch)
            .await
            .expect_err("mqtt sink should reject collection payloads");
        assert!(
            err.to_string()
                .contains("does not support collection payloads"),
            "unexpected error: {err}"
        );
    }

    // coverage-covers: sink.connector.mqtt_output
    #[tokio::test]
    async fn mqtt_sink_connector_publishes_to_dynamic_topic() {
        let broker = EmbeddedMqttBroker::start().await;
        let expected_topic = broker.scoped_topic("sink/VIN-UPDATED");
        let registry = crate::PropertyRegistry::new(
            std::collections::BTreeMap::from([(
                "vin".to_string(),
                crate::secret::SecretString::new("VIN-INITIAL"),
            )]),
            true,
        );
        let topic = registry
            .bind(&broker.scoped_topic(r#"sink/{{ prop("vin") }}"#))
            .expect("bind MQTT topic");

        let mut options = V3Options::new(
            "mqtt_sink_output_subscriber",
            "127.0.0.1",
            Url::parse(&broker.broker_url())
                .expect("parse embedded broker url")
                .port()
                .expect("embedded broker url should include port"),
        );
        options.set_keep_alive(Duration::from_secs(5));
        let (subscriber, mut event_loop) = V3Client::new(options, 8);
        let (connack_tx, connack_rx) = oneshot::channel();
        let (suback_tx, suback_rx) = oneshot::channel();
        let (publish_tx, mut publish_rx) = mpsc::channel::<(String, Vec<u8>)>(1);
        let subscriber_task = tokio::spawn(async move {
            let mut connack_tx = Some(connack_tx);
            let mut suback_tx = Some(suback_tx);
            loop {
                match event_loop.poll().await {
                    Ok(V3Event::Incoming(V3Packet::ConnAck(_))) => {
                        if let Some(tx) = connack_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(V3Event::Incoming(V3Packet::SubAck(_))) => {
                        if let Some(tx) = suback_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(V3Event::Incoming(V3Packet::Publish(publish))) => {
                        if publish_tx
                            .send((publish.topic, publish.payload.to_vec()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        timeout(Duration::from_secs(5), connack_rx)
            .await
            .expect("subscriber connack timeout")
            .expect("subscriber connack channel");
        subscriber
            .subscribe(expected_topic.clone(), V3Qos::AtLeastOnce)
            .await
            .expect("subscribe embedded broker topic");
        timeout(Duration::from_secs(5), suback_rx)
            .await
            .expect("subscriber suback timeout")
            .expect("subscriber suback channel");

        let spawner = TaskSpawner::from_handle(Handle::current());
        let mut connector = MqttSinkConnector::new(
            "mqtt_sink_output",
            MqttSinkConfig::new("mqtt_sink_output", broker.broker_url(), topic, 1),
            Arc::<str>::from("default"),
            MqttClientManager::new("default", spawner.clone()),
            spawner,
        );
        connector.ready().await.expect("mqtt sink ready");
        registry
            .apply_override("vin", crate::secret::SecretString::new("VIN-UPDATED"))
            .expect("update vin");
        let properties = registry.snapshot();

        let expected_payload = br#"{"a":1}"#;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match async {
                connector.start_delivery(&properties).await?;
                connector.write_chunk(expected_payload).await?;
                connector.finish_delivery().await.map(|_| ())
            }
            .await
            {
                Ok(()) => break,
                Err(err)
                    if err.to_string().contains("mqtt not connected")
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("mqtt sink publish failed: {err}"),
            }
        }

        let (received_topic, received_payload) = timeout(Duration::from_secs(5), publish_rx.recv())
            .await
            .expect("mqtt sink output payload timeout")
            .expect("mqtt sink output payload");
        assert_eq!(received_topic, expected_topic);
        assert_eq!(received_payload, expected_payload);

        connector.close().await.expect("close mqtt sink connector");
        let _ = subscriber.disconnect().await;
        subscriber_task.abort();
        let _ = subscriber_task.await;
    }

    // coverage-covers: sink.connector.mqtt_v5_user_properties
    #[tokio::test]
    async fn mqtt_v5_sink_publishes_static_user_properties_in_order() {
        let broker = EmbeddedMqttBroker::start().await;
        let topic = broker.scoped_topic("sink/v5/out");
        let expected_topic = topic.clone();
        let port = Url::parse(&broker.broker_url_v5())
            .expect("parse embedded MQTT 5 broker URL")
            .port()
            .expect("embedded MQTT 5 broker URL should include port");

        let mut options = V5Options::new("mqtt_v5_sink_subscriber", "127.0.0.1", port);
        options.set_keep_alive(Duration::from_secs(5));
        let (subscriber, mut event_loop) = V5Client::new(options, 8);
        let (connack_tx, connack_rx) = oneshot::channel();
        let (suback_tx, suback_rx) = oneshot::channel();
        let (publish_tx, mut publish_rx) =
            mpsc::channel::<(String, Vec<u8>, Vec<(String, String)>)>(1);
        let subscriber_task = tokio::spawn(async move {
            let mut connack_tx = Some(connack_tx);
            let mut suback_tx = Some(suback_tx);
            loop {
                match event_loop.poll().await {
                    Ok(V5Event::Incoming(V5Packet::ConnAck(_))) => {
                        if let Some(tx) = connack_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(V5Event::Incoming(V5Packet::SubAck(_))) => {
                        if let Some(tx) = suback_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(V5Event::Incoming(V5Packet::Publish(publish))) => {
                        let topic = String::from_utf8(publish.topic.to_vec())
                            .expect("MQTT 5 topic should be UTF-8");
                        let user_properties = publish
                            .properties
                            .map(|properties| properties.user_properties)
                            .unwrap_or_default();
                        if publish_tx
                            .send((topic, publish.payload.to_vec(), user_properties))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        timeout(Duration::from_secs(5), connack_rx)
            .await
            .expect("MQTT 5 subscriber connack timeout")
            .expect("MQTT 5 subscriber connack channel");
        subscriber
            .subscribe(topic.clone(), V5Qos::AtLeastOnce)
            .await
            .expect("subscribe MQTT 5 embedded broker topic");
        timeout(Duration::from_secs(5), suback_rx)
            .await
            .expect("MQTT 5 subscriber suback timeout")
            .expect("MQTT 5 subscriber suback channel");

        let expected_properties = vec![
            crate::connector::MqttUserProperty::new("source", "veloflux"),
            crate::connector::MqttUserProperty::new("tag", "primary"),
            crate::connector::MqttUserProperty::new("tag", "edge"),
        ];
        let spawner = TaskSpawner::from_handle(Handle::current());
        let config = MqttSinkConfig::new(
            "mqtt_v5_sink_output",
            broker.broker_url_v5(),
            ConnectorString::sensitive(topic),
            1,
        )
        .with_protocol_version(crate::connector::MqttProtocolVersion::V5)
        .with_user_properties(expected_properties.clone());
        let mut connector = MqttSinkConnector::new(
            "mqtt_v5_sink_output",
            config,
            Arc::<str>::from("default"),
            MqttClientManager::new("default", spawner.clone()),
            spawner,
        );
        connector.ready().await.expect("MQTT 5 sink ready");

        let expected_payload = br#"{"version":5}"#;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match async {
                connector
                    .start_delivery(&crate::PropertyContext::default())
                    .await?;
                connector.write_chunk(expected_payload).await?;
                connector.finish_delivery().await.map(|_| ())
            }
            .await
            {
                Ok(()) => break,
                Err(err)
                    if err.to_string().contains("mqtt not connected")
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("MQTT 5 sink publish failed: {err}"),
            }
        }

        let (received_topic, received_payload, received_properties) =
            timeout(Duration::from_secs(5), publish_rx.recv())
                .await
                .expect("MQTT 5 sink output timeout")
                .expect("MQTT 5 sink output");
        assert_eq!(received_topic, expected_topic);
        assert_eq!(received_payload, expected_payload);
        assert_eq!(
            received_properties,
            expected_properties
                .iter()
                .map(|property| { (property.key.clone(), property.value.expose().to_string()) })
                .collect::<Vec<_>>()
        );

        connector
            .close()
            .await
            .expect("close MQTT 5 sink connector");
        let _ = subscriber.disconnect().await;
        subscriber_task.abort();
        let _ = subscriber_task.await;
    }

    #[test]
    fn mqtt_sink_build_mqtt_options_uses_sink_local_client_and_packet_limit() {
        let config = MqttSinkConfig::new("mqtt_sink", "sink.example.com", "tests/mqtt/out", 1)
            .with_client_id("sink_client")
            .with_max_packet_size(4096);

        let options = build_mqtt_options(&config).expect("build mqtt sink options");

        assert_eq!(
            options.broker_address(),
            ("sink.example.com".to_string(), 1883)
        );
        assert_eq!(options.client_id(), "sink_client");
        assert_eq!(options.max_packet_size(), 4096);
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn mqtt_sink_build_mqtt_options_enables_tls_and_secure_default_port() {
        let config = MqttSinkConfig::new(
            "mqtt_sink",
            "mqtts://secure-sink.example.com",
            "tests/mqtt/out",
            1,
        )
        .with_max_packet_size(8192);

        let options = build_mqtt_options(&config).expect("build secure mqtt sink options");

        assert_eq!(
            options.broker_address(),
            ("secure-sink.example.com".to_string(), 8883)
        );
        assert_eq!(options.client_id(), "mqtt_sink");
        assert_eq!(options.max_packet_size(), 8192);
        assert!(matches!(options.transport(), Transport::Tls(_)));
    }
}
