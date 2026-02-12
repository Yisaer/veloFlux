use std::time::Duration;
use url::Url;

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub base_url: Url,
    pub timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:8080"
                .parse()
                .expect("default base_url must be valid"),
            timeout: Duration::from_secs(30),
        }
    }
}

impl ClientConfig {
    pub fn new(base_url: Url) -> Self {
        Self {
            base_url,
            ..Default::default()
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}
