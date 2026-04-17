use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, OnceCell};
use tokio::time::sleep;
use uuid::Uuid;

const EMBEDDED_MQTT_BROKER_START_RETRIES: usize = 16;

static SHARED_EMBEDDED_MQTT_BROKER: OnceCell<SharedEmbeddedMqttBroker> = OnceCell::const_new();

struct SharedEmbeddedMqttBroker {
    port: u16,
}

pub(crate) struct EmbeddedMqttBroker {
    port: u16,
    topic_prefix: String,
}

impl EmbeddedMqttBroker {
    pub(crate) async fn start() -> Self {
        let shared = SHARED_EMBEDDED_MQTT_BROKER
            .get_or_init(start_shared_broker)
            .await;

        Self {
            port: shared.port,
            topic_prefix: format!("tests/{}", Uuid::new_v4()),
        }
    }

    pub(crate) fn broker_url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.port)
    }

    pub(crate) fn scoped_filter(&self, filter: &str) -> String {
        self.scoped_topic(filter)
    }

    pub(crate) fn scoped_topic(&self, topic: &str) -> String {
        let topic = topic.trim_start_matches('/');
        format!("{}/{}", self.topic_prefix, topic)
    }

    pub(crate) async fn publish(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), String> {
        let scoped_topic = self.scoped_topic(topic);
        let mut options = MqttOptions::new(
            format!("publisher-{}", Uuid::new_v4()),
            "127.0.0.1",
            self.port,
        );
        options.set_keep_alive(Duration::from_secs(5));

        let (client, mut event_loop) = AsyncClient::new(options, 8);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (publish_tx, publish_rx) = oneshot::channel();
        let pump = tokio::spawn(async move {
            let mut ready_tx = Some(ready_tx);
            let mut publish_tx = Some(publish_tx);
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(Ok(()));
                        }
                    }
                    Ok(Event::Incoming(Packet::PubAck(_))) => {
                        if let Some(publish_tx) = publish_tx.take() {
                            let _ = publish_tx.send(Ok(()));
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(Err(err.to_string()));
                        }
                        if let Some(publish_tx) = publish_tx.take() {
                            let _ = publish_tx.send(Err(err.to_string()));
                        }
                        break;
                    }
                }
            }
        });

        let publish_result = async {
            match tokio::time::timeout(Duration::from_secs(5), ready_rx).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(err))) => return Err(err),
                Ok(Err(_)) => {
                    return Err("embedded mqtt publisher readiness channel closed".to_string())
                }
                Err(_) => {
                    return Err(
                        "timed out waiting for embedded mqtt publisher connection".to_string()
                    )
                }
            }

            client
                .publish(scoped_topic, QoS::AtLeastOnce, false, payload.into())
                .await
                .map_err(|err| err.to_string())?;

            match tokio::time::timeout(Duration::from_secs(5), publish_rx).await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(err))) => Err(err),
                Ok(Err(_)) => Err("embedded mqtt publisher ack channel closed".to_string()),
                Err(_) => Err("timed out waiting for embedded mqtt publish ack".to_string()),
            }
        }
        .await;

        let _ = client.disconnect().await;
        pump.abort();
        let _ = pump.await;
        publish_result
    }
}

async fn start_shared_broker() -> SharedEmbeddedMqttBroker {
    for attempt in 1..=EMBEDDED_MQTT_BROKER_START_RETRIES {
        let port = reserve_local_port();
        let startup_rx = spawn_embedded_broker_thread(port);
        match wait_for_tcp_listener(port, startup_rx).await {
            Ok(()) => return SharedEmbeddedMqttBroker { port },
            Err(_) if attempt < EMBEDDED_MQTT_BROKER_START_RETRIES => continue,
            Err(err) => panic!("start shared embedded mqtt broker after retries: {err}"),
        }
    }

    unreachable!("embedded mqtt broker retry loop should return or panic")
}

fn spawn_embedded_broker_thread(port: u16) -> std_mpsc::Receiver<String> {
    let config = embedded_broker_config(port);
    let (startup_tx, startup_rx) = std_mpsc::channel();

    thread::Builder::new()
        .name(format!("rumqttd-test-{port}"))
        .spawn(move || {
            let mut broker = Broker::new(config);
            let result = broker.start();
            let message = match result {
                Ok(()) => format!("embedded mqtt broker exited unexpectedly on port {port}"),
                Err(err) => format!("start embedded mqtt broker on port {port}: {err}"),
            };
            let _ = startup_tx.send(message);
        })
        .expect("spawn embedded mqtt broker thread");

    startup_rx
}

fn embedded_broker_config(port: u16) -> Config {
    Config {
        id: 0,
        router: RouterConfig {
            max_connections: 32,
            max_outgoing_packet_count: 64,
            max_segment_size: 1024,
            max_segment_count: 8,
            custom_segment: None,
            initialized_filters: None,
            shared_subscriptions_strategy: Default::default(),
        },
        v4: Some(HashMap::from([(
            "test".to_string(),
            ServerSettings {
                name: "mqtt-test".to_string(),
                listen: SocketAddr::from(([127, 0, 0, 1], port)),
                tls: None,
                next_connection_delay_ms: 1,
                connections: ConnectionSettings {
                    connection_timeout_ms: 5_000,
                    max_payload_size: 1024 * 1024,
                    max_inflight_count: 32,
                    auth: None,
                    external_auth: None,
                    dynamic_filters: true,
                },
            },
        )])),
        ..Config::default()
    }
}

fn reserve_local_port() -> u16 {
    let listener =
        TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral tcp listener for mqtt test");
    let port = listener
        .local_addr()
        .expect("read ephemeral tcp listener address")
        .port();
    drop(listener);
    port
}

async fn wait_for_tcp_listener(
    port: u16,
    startup_rx: std_mpsc::Receiver<String>,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }

        match startup_rx.try_recv() {
            Ok(err) => return Err(err),
            Err(std_mpsc::TryRecvError::Disconnected) => {
                return Err(format!(
                    "embedded mqtt broker thread exited before listening on port {port}"
                ))
            }
            Err(std_mpsc::TryRecvError::Empty) => {}
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "embedded mqtt broker did not listen on port {port} within timeout"
            ));
        }

        sleep(Duration::from_millis(20)).await;
    }
}
