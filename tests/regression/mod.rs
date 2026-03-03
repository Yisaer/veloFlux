mod flow_instance_binding;
mod pipeline_build_context;
mod shared_stream_lifecycle;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use sdk::{ClientConfig, ManagerClient};

pub fn make_client(addr: SocketAddr) -> ManagerClient {
    let base_url = format!("http://{}", addr).parse().expect("base_url");
    ManagerClient::new(ClientConfig::new(base_url)).expect("create client")
}

pub fn random_suffix() -> String {
    use rand::{distributions::Alphanumeric, Rng};
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

pub async fn bind_manager_listener_or_skip() -> Option<TcpListener> {
    match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => Some(listener),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping test: binding a local listener is not permitted: {err}");
            None
        }
        Err(err) => panic!("bind manager listener: {err}"),
    }
}
