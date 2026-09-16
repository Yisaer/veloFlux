//! Shared Upon template support for connector and encoder profiles.

use crate::property::{validate_property_key, PropertyContext};
use crate::secret::SecretString;
use parking_lot::Mutex;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A fixed connector string that retains whether it contains sensitive data.
#[derive(Clone)]
pub enum ConnectorString {
    Plain(String),
    Sensitive(SecretString),
    Dynamic(Arc<DynamicConnectorString>),
}

/// A property-backed connector string resolved against a delivery snapshot.
pub struct DynamicConnectorString {
    template: String,
    initial: ConnectorString,
    initial_revision: u64,
    cached: Mutex<Option<(u64, ConnectorString)>>,
}

impl ConnectorString {
    pub fn plain(value: impl Into<String>) -> Self {
        Self::Plain(value.into())
    }

    pub fn sensitive(value: impl Into<String>) -> Self {
        Self::Sensitive(SecretString::new(value))
    }

    pub(crate) fn dynamic(
        template: String,
        initial: ConnectorString,
        initial_revision: u64,
    ) -> Self {
        Self::Dynamic(Arc::new(DynamicConnectorString {
            template,
            initial,
            initial_revision,
            cached: Mutex::new(None),
        }))
    }

    /// Explicitly expose the connector value at validation or wire boundaries.
    pub fn expose(&self) -> &str {
        match self {
            Self::Plain(value) => value,
            Self::Sensitive(value) => value.expose(),
            Self::Dynamic(value) => value.initial.expose(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.expose().is_empty()
    }

    pub fn is_sensitive(&self) -> bool {
        matches!(self, Self::Sensitive(_) | Self::Dynamic(_))
    }

    /// Resolve a dynamic connector string against one immutable property snapshot.
    pub fn resolve(
        &self,
        properties: &PropertyContext,
    ) -> Result<Self, crate::property::PropertyTemplateError> {
        let Self::Dynamic(dynamic) = self else {
            return Ok(self.clone());
        };
        if properties.revision() == dynamic.initial_revision {
            return Ok(dynamic.initial.clone());
        }
        if let Some((_, value)) = dynamic
            .cached
            .lock()
            .as_ref()
            .filter(|(revision, _)| *revision == properties.revision())
        {
            return Ok(value.clone());
        }

        let rendered = properties.render(&dynamic.template)?;
        *dynamic.cached.lock() = Some((properties.revision(), rendered.clone()));
        Ok(rendered)
    }
}

impl PartialEq for ConnectorString {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Plain(left), Self::Plain(right)) => left == right,
            (Self::Sensitive(left), Self::Sensitive(right)) => left == right,
            (Self::Dynamic(left), Self::Dynamic(right)) => {
                left.template == right.template
                    && left.initial == right.initial
                    && left.initial_revision == right.initial_revision
            }
            _ => false,
        }
    }
}

impl Eq for ConnectorString {}

impl From<String> for ConnectorString {
    fn from(value: String) -> Self {
        Self::Plain(value)
    }
}

impl From<&str> for ConnectorString {
    fn from(value: &str) -> Self {
        Self::Plain(value.to_string())
    }
}

impl fmt::Debug for ConnectorString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain(value) => value.fmt(f),
            Self::Sensitive(_) | Self::Dynamic(_) => f.write_str("<redacted>"),
        }
    }
}

impl fmt::Display for ConnectorString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain(value) => value.fmt(f),
            Self::Sensitive(_) | Self::Dynamic(_) => f.write_str("<redacted>"),
        }
    }
}

pub(crate) struct PropertyTemplateProfile {
    used_property: Arc<AtomicBool>,
}

impl PropertyTemplateProfile {
    pub(crate) fn was_property_used(&self) -> bool {
        self.used_property.load(Ordering::Relaxed)
    }
}

/// Create an Upon engine with the process property function installed.
pub(crate) fn engine_with_properties(
    properties: PropertyContext,
) -> (upon::Engine<'static>, PropertyTemplateProfile) {
    let mut engine = upon::Engine::new();
    let used_property = Arc::new(AtomicBool::new(false));
    let used_property_for_function = Arc::clone(&used_property);
    engine.add_function("prop", move |key: &str| -> Result<String, String> {
        validate_property_key(key).map_err(|err| err.to_string())?;
        let value = properties
            .get(key)
            .ok_or_else(|| format!("property `{key}` is not defined"))?;
        used_property_for_function.store(true, Ordering::Relaxed);
        Ok(value.expose().to_string())
    });
    (engine, PropertyTemplateProfile { used_property })
}
