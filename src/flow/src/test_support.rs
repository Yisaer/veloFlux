use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use tokio::sync::oneshot;
use tokio::time::sleep;
use uuid::Uuid;

pub(crate) struct EmbeddedMqttBroker {
    port: u16,
}

impl EmbeddedMqttBroker {
    pub(crate) async fn start() -> Self {
        let port = reserve_local_port();
        let config = Config {
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
        };

        thread::Builder::new()
            .name(format!("rumqttd-test-{port}"))
            .spawn(move || {
                let mut broker = Broker::new(config);
                broker.start().expect("start embedded mqtt broker");
            })
            .expect("spawn embedded mqtt broker thread");

        wait_for_tcp_listener(port).await;

        Self { port }
    }

    pub(crate) fn broker_url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.port)
    }

    pub(crate) async fn publish(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), String> {
        let mut options = MqttOptions::new(
            format!("publisher-{}", Uuid::new_v4()),
            "127.0.0.1",
            self.port,
        );
        options.set_keep_alive(Duration::from_secs(5));

        let (client, mut event_loop) = AsyncClient::new(options, 8);
        let (ready_tx, ready_rx) = oneshot::channel();
        let pump = tokio::spawn(async move {
            let mut ready_tx = Some(ready_tx);
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(Ok(()));
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(Err(err.to_string()));
                        }
                        break;
                    }
                }
            }
        });

        match tokio::time::timeout(Duration::from_secs(5), ready_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(err))) => return Err(err),
            Ok(Err(_)) => {
                return Err("embedded mqtt publisher readiness channel closed".to_string())
            }
            Err(_) => {
                return Err("timed out waiting for embedded mqtt publisher connection".to_string())
            }
        }

        client
            .publish(topic, QoS::AtMostOnce, false, payload.into())
            .await
            .map_err(|err| err.to_string())?;
        sleep(Duration::from_millis(200)).await;
        let _ = client.disconnect().await;
        pump.abort();
        let _ = pump.await;
        Ok(())
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

async fn wait_for_tcp_listener(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "embedded mqtt broker did not listen on port {port} within timeout"
        );

        sleep(Duration::from_millis(20)).await;
    }
}
