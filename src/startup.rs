use std::fmt::Display;
use std::time::Instant;

pub struct StartupPhase {
    mode: &'static str,
    flow_instance_id: String,
    phase: &'static str,
    config_path: Option<String>,
    started_at: Instant,
}

impl StartupPhase {
    pub fn new(
        mode: &'static str,
        flow_instance_id: impl Into<String>,
        phase: &'static str,
        config_path: Option<&str>,
    ) -> Self {
        let phase_log = Self {
            mode,
            flow_instance_id: flow_instance_id.into(),
            phase,
            config_path: config_path.map(ToOwned::to_owned),
            started_at: Instant::now(),
        };
        tracing::info!(
            mode = phase_log.mode,
            flow_instance_id = %phase_log.flow_instance_id,
            phase = phase_log.phase,
            config_path = phase_log.config_path(),
            result = "started",
            "startup phase"
        );
        phase_log
    }

    pub fn mode(&self) -> &'static str {
        self.mode
    }

    pub fn flow_instance_id(&self) -> &str {
        &self.flow_instance_id
    }

    pub fn phase(&self) -> &'static str {
        self.phase
    }

    pub fn config_path(&self) -> &str {
        self.config_path.as_deref().unwrap_or("<unset>")
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.started_at.elapsed().as_millis()
    }

    pub fn log_success(&self) {
        tracing::info!(
            mode = self.mode,
            flow_instance_id = %self.flow_instance_id,
            phase = self.phase,
            config_path = self.config_path(),
            result = "succeeded",
            elapsed_ms = self.elapsed_ms(),
            "startup phase"
        );
    }

    pub fn log_failure(&self, error: &(impl Display + ?Sized)) {
        tracing::error!(
            mode = self.mode,
            flow_instance_id = %self.flow_instance_id,
            phase = self.phase,
            config_path = self.config_path(),
            result = "failed",
            elapsed_ms = self.elapsed_ms(),
            error = %error,
            "startup phase"
        );
    }
}
