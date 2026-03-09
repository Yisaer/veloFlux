use std::fmt::Display;
use std::time::Instant;

pub(crate) struct StartupPhase {
    mode: &'static str,
    flow_instance_id: String,
    phase: &'static str,
    started_at: Instant,
}

impl StartupPhase {
    pub(crate) fn new(
        mode: &'static str,
        flow_instance_id: impl Into<String>,
        phase: &'static str,
    ) -> Self {
        let phase_log = Self {
            mode,
            flow_instance_id: flow_instance_id.into(),
            phase,
            started_at: Instant::now(),
        };
        tracing::info!(
            mode = phase_log.mode,
            flow_instance_id = %phase_log.flow_instance_id,
            phase = phase_log.phase,
            result = "started",
            "startup phase"
        );
        phase_log
    }

    pub(crate) fn elapsed_ms(&self) -> u128 {
        self.started_at.elapsed().as_millis()
    }

    pub(crate) fn log_success(&self) {
        tracing::info!(
            mode = self.mode,
            flow_instance_id = %self.flow_instance_id,
            phase = self.phase,
            result = "succeeded",
            elapsed_ms = self.elapsed_ms(),
            "startup phase"
        );
    }

    pub(crate) fn log_failure(&self, error: &(impl Display + ?Sized)) {
        tracing::error!(
            mode = self.mode,
            flow_instance_id = %self.flow_instance_id,
            phase = self.phase,
            result = "failed",
            elapsed_ms = self.elapsed_ms(),
            error = %error,
            "startup phase"
        );
    }
}
