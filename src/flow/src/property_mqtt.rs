//! Process-level local MQTT provider for runtime property overrides.

use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::connector::mqtt_protocol::{
    MqttClient, MqttConnectionError, MqttEvent, MqttOptions, MqttProtocolVersion, MqttQos,
};
use crate::property::{
    validate_property_key, validate_property_subscription_topic, PropertyRegistry,
    PropertyUpdateResult, MAX_PROPERTY_KEY_BYTES, MAX_PROPERTY_SUBSCRIPTION_TOPIC_BYTES,
};
use crate::secret::SecretString;

const BROKER_HOST: &str = "127.0.0.1";
const BROKER_PORT: u16 = 1883;
const CLIENT_ID: &str = "VeloFluxIntPropRegistry";
const MAX_PAYLOAD_BYTES: usize = 4096;
const MAX_PACKET_BYTES: usize = 8192;
const MQTT_PACKET_OVERHEAD_BYTES: usize = 16;
const REQUEST_CAPACITY: usize = 32;

const _: () = assert!(
    MAX_PACKET_BYTES
        >= MAX_PAYLOAD_BYTES
            + MAX_PROPERTY_SUBSCRIPTION_TOPIC_BYTES
            + MAX_PROPERTY_KEY_BYTES
            + MQTT_PACKET_OVERHEAD_BYTES
);

/// Handle for the single process-level local MQTT property provider.
pub struct PropertyMqttProvider {
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

impl PropertyMqttProvider {
    /// Start the provider without waiting for the broker or retained messages.
    pub fn start(registry: PropertyRegistry, topic: String) -> Result<Self, String> {
        validate_property_subscription_topic(&topic)?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(run_provider(
            registry,
            topic,
            BROKER_HOST.to_string(),
            BROKER_PORT,
            shutdown_rx,
        ));
        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            handle,
        })
    }

    #[cfg(test)]
    fn start_with_endpoint(
        registry: PropertyRegistry,
        topic: String,
        host: String,
        port: u16,
    ) -> Result<Self, String> {
        validate_property_subscription_topic(&topic)?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(run_provider(registry, topic, host, port, shutdown_rx));
        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            handle,
        })
    }

    /// Stop the provider and wait for its event loop task to exit.
    pub async fn shutdown(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        let _ = self.handle.await;
    }
}

async fn run_provider(
    registry: PropertyRegistry,
    topic: String,
    host: String,
    port: u16,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let mut options = match MqttOptions::new(
        MqttProtocolVersion::V3,
        CLIENT_ID.to_string(),
        &host,
        port,
        MAX_PACKET_BYTES,
    ) {
        Ok(options) => options,
        Err(err) => {
            record_error("connection_error");
            tracing::error!(
                provider = "property_registry_mqtt",
                error = %err,
                "failed to construct local MQTT property provider"
            );
            return;
        }
    };
    options.set_keep_alive(Duration::from_secs(30));
    let (client, mut event_loop) = MqttClient::new(options, REQUEST_CAPACITY);
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);
    let mut seen_connected = false;
    let mut connected = false;

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            event = event_loop.poll() => match event {
                Ok(MqttEvent::Connected { .. }) => {
                    if seen_connected && !connected {
                        veloflux_metrics::property_registry_mqtt_reconnects_total().inc();
                    }
                    seen_connected = true;
                    connected = true;
                    veloflux_metrics::property_registry_mqtt_connected().set(1);
                    backoff = Duration::from_millis(100);
                    if let Err(err) = client.subscribe(topic.clone(), MqttQos::Qos1).await {
                        record_error("connection_error");
                        tracing::warn!(
                            provider = "property_registry_mqtt",
                            error = %err,
                            "failed to subscribe to local MQTT property updates"
                        );
                    }
                }
                Ok(MqttEvent::Publish {
                    topic: publish_topic,
                    payload,
                }) => {
                    connected = true;
                    veloflux_metrics::property_registry_mqtt_connected().set(1);
                    backoff = Duration::from_millis(100);
                    apply_publish(&registry, &publish_topic, payload.as_ref());
                }
                Ok(MqttEvent::Disconnected) => {
                    connected = false;
                    veloflux_metrics::property_registry_mqtt_connected().set(0);
                    record_error("connection_error");
                    tracing::warn!(
                        provider = "property_registry_mqtt",
                        "local MQTT property provider disconnected; reconnecting"
                    );
                    event_loop.clean();
                }
                Ok(MqttEvent::Other) => {}
                Err(MqttConnectionError::RequestsDone) => {
                    connected = false;
                    veloflux_metrics::property_registry_mqtt_connected().set(0);
                    record_error("connection_error");
                    if !wait_for_backoff_or_shutdown(backoff, &mut shutdown_rx).await {
                        break;
                    }
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
                Err(MqttConnectionError::Connection(err)) => {
                    connected = false;
                    veloflux_metrics::property_registry_mqtt_connected().set(0);
                    record_error("connection_error");
                    tracing::warn!(
                        provider = "property_registry_mqtt",
                        error = %err,
                        "local MQTT property provider connection error; reconnecting"
                    );
                    if !wait_for_backoff_or_shutdown(backoff, &mut shutdown_rx).await {
                        break;
                    }
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
            }
        }
    }

    veloflux_metrics::property_registry_mqtt_connected().set(0);
    let _ = client.disconnect().await;
}

async fn wait_for_backoff_or_shutdown(
    backoff: Duration,
    shutdown_rx: &mut oneshot::Receiver<()>,
) -> bool {
    tokio::select! {
        _ = shutdown_rx => false,
        _ = sleep(backoff) => true,
    }
}

fn apply_publish(registry: &PropertyRegistry, topic: &str, payload: &[u8]) {
    let Some(key) = topic.rsplit('/').next() else {
        record_rejection("invalid_topic_key");
        return;
    };
    if validate_property_key(key).is_err() {
        record_rejection("invalid_topic_key");
        tracing::warn!(
            provider = "property_registry_mqtt",
            error_kind = "invalid_topic_key",
            "rejected MQTT property update"
        );
        return;
    }
    if payload.is_empty() {
        record_rejection("empty_payload");
        tracing::warn!(
            provider = "property_registry_mqtt",
            property_key = key,
            error_kind = "empty_payload",
            "rejected MQTT property update"
        );
        return;
    }
    if payload.len() > MAX_PAYLOAD_BYTES {
        record_rejection("payload_too_large");
        tracing::warn!(
            provider = "property_registry_mqtt",
            property_key = key,
            error_kind = "payload_too_large",
            "rejected MQTT property update"
        );
        return;
    }
    let value = match std::str::from_utf8(payload) {
        Ok(value) => value,
        Err(_) => {
            record_rejection("invalid_utf8");
            tracing::warn!(
                provider = "property_registry_mqtt",
                property_key = key,
                error_kind = "invalid_utf8",
                "rejected MQTT property update"
            );
            return;
        }
    };

    match registry.apply_override(key, SecretString::new(value.to_string())) {
        Ok(PropertyUpdateResult::Changed { revision }) => {
            veloflux_metrics::property_registry_mqtt_updates_total()
                .with_label_values(&["changed"])
                .inc();
            tracing::info!(
                provider = "property_registry_mqtt",
                property_key = key,
                revision,
                "applied MQTT property update"
            );
        }
        Ok(PropertyUpdateResult::Noop { .. }) => {
            veloflux_metrics::property_registry_mqtt_updates_total()
                .with_label_values(&["noop"])
                .inc();
        }
        Err(_) => {
            record_rejection("unknown_property");
            tracing::warn!(
                provider = "property_registry_mqtt",
                property_key = key,
                error_kind = "unknown_property",
                "rejected MQTT property update"
            );
        }
    }
}

fn record_rejection(kind: &'static str) {
    veloflux_metrics::property_registry_mqtt_updates_total()
        .with_label_values(&["rejected"])
        .inc();
    record_error(kind);
}

fn record_error(kind: &'static str) {
    veloflux_metrics::property_registry_mqtt_errors_total()
        .with_label_values(&[kind])
        .inc();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::EmbeddedMqttBroker;
    use rumqttc::{AsyncClient, Event, MqttOptions as RumqttOptions, Packet};
    use std::collections::BTreeMap;
    use tokio::time::{sleep, timeout, Instant};

    fn registry() -> PropertyRegistry {
        PropertyRegistry::new(
            BTreeMap::from([("vin".to_string(), SecretString::new("VIN-STATIC"))]),
            true,
        )
    }

    #[test]
    fn publish_updates_only_valid_declared_properties() {
        let registry = registry();
        apply_publish(&registry, "$local/notification/vin", b"VIN-DYNAMIC");
        assert_eq!(
            registry
                .snapshot()
                .get("vin")
                .expect("configured vin")
                .expose(),
            "VIN-DYNAMIC"
        );
        assert_eq!(registry.snapshot().revision(), 1);

        apply_publish(&registry, "$local/notification/unknown", b"secret-value");
        apply_publish(&registry, "$local/notification/vin", b"");
        apply_publish(&registry, "$local/notification/vin", &[0xff]);
        apply_publish(
            &registry,
            "$local/notification/vin",
            &vec![b'x'; MAX_PAYLOAD_BYTES + 1],
        );
        assert_eq!(registry.snapshot().revision(), 1);
    }

    #[tokio::test]
    async fn backoff_wait_stops_promptly_on_shutdown() {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            shutdown_tx.send(()).expect("send shutdown");
        });

        let completed_backoff = timeout(
            Duration::from_secs(1),
            wait_for_backoff_or_shutdown(Duration::from_secs(5), &mut shutdown_rx),
        )
        .await
        .expect("shutdown-aware backoff should not wait for the timer");

        assert!(!completed_backoff);
    }

    #[tokio::test]
    async fn provider_applies_retained_updates_after_reconnect() {
        let broker = EmbeddedMqttBroker::start().await;
        broker
            .publish_retained("notification/vin", b"VIN-RETAINED".to_vec())
            .await
            .expect("publish retained property");

        let registry = registry();
        let provider = PropertyMqttProvider::start_with_endpoint(
            registry.clone(),
            broker.scoped_filter("notification/+"),
            "127.0.0.1".to_string(),
            broker.port(),
        )
        .expect("start property MQTT provider");
        wait_for_property(&registry, "VIN-RETAINED").await;

        broker
            .publish_retained("notification/vin", b"VIN-LIVE".to_vec())
            .await
            .expect("publish latest retained property");
        wait_for_property(&registry, "VIN-LIVE").await;

        interrupt_provider_connection(broker.port()).await;
        let deadline = Instant::now() + Duration::from_secs(5);
        while registry
            .snapshot()
            .get("vin")
            .expect("configured vin")
            .expose()
            != "VIN-RECOVERED"
        {
            broker
                .publish_retained("notification/vin", b"VIN-RECOVERED".to_vec())
                .await
                .expect("publish property after reconnect");
            assert!(
                Instant::now() < deadline,
                "property provider did not recover after disconnect"
            );
            sleep(Duration::from_millis(100)).await;
        }

        provider.shutdown().await;
    }

    async fn wait_for_property(registry: &PropertyRegistry, expected: &str) {
        timeout(Duration::from_secs(5), async {
            loop {
                if registry
                    .snapshot()
                    .get("vin")
                    .expect("configured vin")
                    .expose()
                    == expected
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("property update timeout");
    }

    async fn interrupt_provider_connection(port: u16) {
        let mut options = RumqttOptions::new(CLIENT_ID, "127.0.0.1", port);
        options.set_keep_alive(Duration::from_secs(5));
        let (client, mut event_loop) = AsyncClient::new(options, 8);
        timeout(Duration::from_secs(5), async {
            loop {
                if matches!(
                    event_loop.poll().await,
                    Ok(Event::Incoming(Packet::ConnAck(_)))
                ) {
                    break;
                }
            }
        })
        .await
        .expect("duplicate client connection timeout");
        let _ = client.disconnect().await;
    }
}
