use std::fmt::Display;
use std::time::Instant;

pub(crate) struct ResourceMutationLog {
    resource_kind: &'static str,
    action: &'static str,
    resource_id: String,
    flow_instance_id: Option<String>,
    started_at: Instant,
}

impl ResourceMutationLog {
    pub(crate) fn new(
        resource_kind: &'static str,
        action: &'static str,
        resource_id: impl Into<String>,
        flow_instance_id: Option<&str>,
    ) -> Self {
        let log = Self {
            resource_kind,
            action,
            resource_id: resource_id.into(),
            flow_instance_id: flow_instance_id.map(ToOwned::to_owned),
            started_at: Instant::now(),
        };
        tracing::info!(
            resource_kind = log.resource_kind,
            resource_id = %log.resource_id,
            action = log.action,
            flow_instance_id = log.flow_instance_id(),
            result = "requested",
            "resource mutation"
        );
        log
    }

    pub(crate) fn flow_instance_id(&self) -> &str {
        self.flow_instance_id.as_deref().unwrap_or("<unset>")
    }

    pub(crate) fn set_flow_instance_id(&mut self, flow_instance_id: Option<&str>) {
        self.flow_instance_id = flow_instance_id.map(ToOwned::to_owned);
    }

    pub(crate) fn elapsed_ms(&self) -> u128 {
        self.started_at.elapsed().as_millis()
    }

    pub(crate) fn log_success(&self) {
        tracing::info!(
            resource_kind = self.resource_kind,
            resource_id = %self.resource_id,
            action = self.action,
            flow_instance_id = self.flow_instance_id(),
            result = "succeeded",
            elapsed_ms = self.elapsed_ms(),
            "resource mutation"
        );
    }

    pub(crate) fn log_failure(&self, error: &(impl Display + ?Sized)) {
        tracing::error!(
            resource_kind = self.resource_kind,
            resource_id = %self.resource_id,
            action = self.action,
            flow_instance_id = self.flow_instance_id(),
            result = "failed",
            elapsed_ms = self.elapsed_ms(),
            error = %error,
            "resource mutation"
        );
    }
}
