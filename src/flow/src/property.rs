//! Process-wide properties used by connector string templates.

use crate::secret::SecretString;
use crate::template::{engine_with_properties, ConnectorString};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;

/// Maximum encoded length of one canonical property key.
pub(crate) const MAX_PROPERTY_KEY_BYTES: usize = 128;
/// Maximum encoded length of the local MQTT property subscription filter.
pub(crate) const MAX_PROPERTY_SUBSCRIPTION_TOPIC_BYTES: usize = 1024;

/// Process-wide property registry shared by all flow instances.
#[derive(Clone)]
pub struct PropertyRegistry {
    static_base: Arc<BTreeMap<String, SecretString>>,
    state: Arc<RwLock<PropertyRegistryState>>,
    runtime_updates_enabled: bool,
}

struct PropertyRegistryState {
    overrides: BTreeMap<String, SecretString>,
    effective: Arc<BTreeMap<String, SecretString>>,
    revision: u64,
}

/// Result of applying one runtime property override.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyUpdateResult {
    Changed { revision: u64 },
    Noop { revision: u64 },
}

/// Runtime property update failure that never contains the property value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PropertyUpdateError {
    #[error("property `{0}` is not declared")]
    UnknownProperty(String),
    #[error("property `{0}` override must not be empty")]
    EmptyValue(String),
}

impl PropertyRegistry {
    /// Build a registry from the merged config/environment static base.
    pub fn new(static_base: BTreeMap<String, SecretString>, runtime_updates_enabled: bool) -> Self {
        let static_base = Arc::new(static_base);
        Self {
            state: Arc::new(RwLock::new(PropertyRegistryState {
                overrides: BTreeMap::new(),
                effective: Arc::clone(&static_base),
                revision: 0,
            })),
            static_base,
            runtime_updates_enabled,
        }
    }

    /// Build a static-only registry from an existing immutable context.
    pub fn from_context(context: PropertyContext) -> Self {
        Self::new((*context.values).clone(), false)
    }

    /// Return an immutable snapshot of the current effective values.
    pub fn snapshot(&self) -> PropertyContext {
        let state = self.state.read();
        PropertyContext {
            values: Arc::clone(&state.effective),
            revision: state.revision,
        }
    }

    /// Return whether runtime property bindings should be retained in plans.
    pub fn runtime_updates_enabled(&self) -> bool {
        self.runtime_updates_enabled
    }

    /// Bind a connector template, retaining it only when runtime updates can affect it.
    pub fn bind(&self, template: &str) -> Result<ConnectorString, PropertyTemplateError> {
        let snapshot = self.snapshot();
        let rendered = snapshot.render(template)?;
        if self.runtime_updates_enabled && rendered.is_sensitive() {
            Ok(ConnectorString::dynamic(
                template.to_string(),
                rendered,
                snapshot.revision(),
            ))
        } else {
            Ok(rendered)
        }
    }

    /// Apply one non-empty override to an already declared property.
    pub fn apply_override(
        &self,
        key: &str,
        value: SecretString,
    ) -> Result<PropertyUpdateResult, PropertyUpdateError> {
        if !self.static_base.contains_key(key) {
            return Err(PropertyUpdateError::UnknownProperty(key.to_string()));
        }
        if value.is_empty() {
            return Err(PropertyUpdateError::EmptyValue(key.to_string()));
        }

        let mut state = self.state.write();
        if state
            .overrides
            .get(key)
            .is_some_and(|current| current == &value)
        {
            return Ok(PropertyUpdateResult::Noop {
                revision: state.revision,
            });
        }

        state.overrides.insert(key.to_string(), value);
        let mut effective = (*self.static_base).clone();
        for (override_key, override_value) in &state.overrides {
            effective.insert(override_key.clone(), override_value.clone());
        }
        state.revision = state.revision.saturating_add(1);
        state.effective = Arc::new(effective);
        Ok(PropertyUpdateResult::Changed {
            revision: state.revision,
        })
    }
}

impl Default for PropertyRegistry {
    fn default() -> Self {
        Self::new(BTreeMap::new(), false)
    }
}

impl fmt::Debug for PropertyRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.read();
        f.debug_struct("PropertyRegistry")
            .field("keys", &self.static_base.keys().collect::<Vec<_>>())
            .field("count", &self.static_base.len())
            .field("override_count", &state.overrides.len())
            .field("revision", &state.revision)
            .field("runtime_updates_enabled", &self.runtime_updates_enabled)
            .finish()
    }
}

/// Immutable process property snapshot shared by all flow instances.
#[derive(Clone, Default)]
pub struct PropertyContext {
    values: Arc<BTreeMap<String, SecretString>>,
    revision: u64,
}

impl PropertyContext {
    /// Build a property context from already-redacted values.
    pub fn new(values: BTreeMap<String, SecretString>) -> Self {
        Self {
            values: Arc::new(values),
            revision: 0,
        }
    }

    /// Render one connector string using this property snapshot.
    pub fn render(&self, template: &str) -> Result<ConnectorString, PropertyTemplateError> {
        const TEMPLATE_NAME: &str = "connector";
        let (mut engine, profile) = engine_with_properties(self.clone());
        engine
            .add_template(TEMPLATE_NAME, template.to_string())
            .map_err(|err| PropertyTemplateError::Compile(err.to_string()))?;
        let rendered = engine
            .template(TEMPLATE_NAME)
            .render(upon::Value::Map(Default::default()))
            .to_string()
            .map_err(|err| PropertyTemplateError::Render(err.to_string()))?;
        if profile.was_property_used() {
            Ok(ConnectorString::sensitive(rendered))
        } else {
            Ok(ConnectorString::plain(rendered))
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<&SecretString> {
        self.values.get(key)
    }

    /// Return the number of configured properties without exposing their values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Return whether no properties are configured.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Monotonic process-local revision captured by this snapshot.
    pub fn revision(&self) -> u64 {
        self.revision
    }
}

impl fmt::Debug for PropertyContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PropertyContext")
            .field("keys", &self.values.keys().collect::<Vec<_>>())
            .field("count", &self.values.len())
            .field("revision", &self.revision)
            .finish()
    }
}

/// Connector property template compilation or rendering error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PropertyTemplateError {
    #[error("invalid connector template: {0}")]
    Compile(String),
    #[error("connector template render failed: {0}")]
    Render(String),
    #[error("property key `{0}` is invalid; expected 1-128 bytes matching [a-z][a-z0-9_]*")]
    InvalidKey(String),
}

/// Validate the canonical lower-snake-case property key grammar.
pub fn validate_property_key(key: &str) -> Result<(), PropertyTemplateError> {
    if key.len() > MAX_PROPERTY_KEY_BYTES {
        return Err(PropertyTemplateError::InvalidKey(key.to_string()));
    }
    let mut chars = key.chars();
    if !matches!(chars.next(), Some('a'..='z'))
        || !chars.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
    {
        return Err(PropertyTemplateError::InvalidKey(key.to_string()));
    }
    Ok(())
}

/// Validate an MQTT publish topic before a connector is created.
pub fn validate_mqtt_publish_topic(topic: &ConnectorString) -> Result<(), String> {
    let topic = topic.expose();
    if topic.is_empty() {
        return Err("MQTT publish topic must not be empty".to_string());
    }
    if topic.len() > u16::MAX as usize {
        return Err("MQTT publish topic exceeds 65535 bytes".to_string());
    }
    if topic.contains('\0') || !rumqttc::valid_topic(topic) {
        return Err("MQTT publish topic is invalid".to_string());
    }
    Ok(())
}

/// Validate the subscription filter used by the local MQTT property provider.
pub fn validate_property_subscription_topic(topic: &str) -> Result<(), String> {
    if topic.is_empty() {
        return Err("property_registry.mqtt.topic must not be empty".to_string());
    }
    if topic.len() > MAX_PROPERTY_SUBSCRIPTION_TOPIC_BYTES {
        return Err(format!(
            "property_registry.mqtt.topic exceeds {MAX_PROPERTY_SUBSCRIPTION_TOPIC_BYTES} bytes"
        ));
    }
    if topic.contains('\0') || !rumqttc::valid_filter(topic) {
        return Err("property_registry.mqtt.topic must be a valid MQTT topic filter".to_string());
    }
    if topic.rsplit('/').next() != Some("+") {
        return Err(
            "property_registry.mqtt.topic must end with a single-level `+` wildcard".to_string(),
        );
    }
    if topic[..topic.len() - 1]
        .chars()
        .any(|character| character == '+' || character == '#')
    {
        return Err(
            "property_registry.mqtt.topic may use a wildcard only in the final `+` level"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(entries: &[(&str, &str)]) -> PropertyContext {
        PropertyContext::new(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), SecretString::new((*value).to_string())))
                .collect(),
        )
    }

    #[test]
    fn connector_templates_render_with_upon_property_profile() {
        struct Case {
            name: &'static str,
            template: &'static str,
            expected: &'static str,
            sensitive: bool,
        }

        let properties = context(&[
            ("vin", "VIN-123"),
            ("cs", "CS-9"),
            ("empty", ""),
            ("nested", r#"{{ prop("vin") }}"#),
        ]);
        let cases = [
            Case {
                name: "plain literal",
                template: "vehicles/static",
                expected: "vehicles/static",
                sensitive: false,
            },
            Case {
                name: "single property",
                template: r#"vehicles/{{ prop("vin") }}"#,
                expected: "vehicles/VIN-123",
                sensitive: true,
            },
            Case {
                name: "multiple properties and whitespace",
                template: r#"{{  prop( "vin" )  }}/{{ prop("cs") }}"#,
                expected: "VIN-123/CS-9",
                sensitive: true,
            },
            Case {
                name: "upon string expression",
                template: r#"{{ "literal" }}"#,
                expected: "literal",
                sensitive: false,
            },
            Case {
                name: "upon control block",
                template: "{% if true %}enabled{% endif %}",
                expected: "enabled",
                sensitive: false,
            },
            Case {
                name: "property values are not recursively expanded",
                template: r#"{{ prop("nested") }}"#,
                expected: r#"{{ prop("vin") }}"#,
                sensitive: true,
            },
            Case {
                name: "empty property",
                template: r#"prefix{{ prop("empty") }}suffix"#,
                expected: "prefixsuffix",
                sensitive: true,
            },
        ];

        for case in cases {
            let rendered = properties
                .render(case.template)
                .unwrap_or_else(|err| panic!("case `{}` failed: {err}", case.name));
            assert_eq!(rendered.expose(), case.expected, "case `{}`", case.name);
            assert_eq!(
                rendered.is_sensitive(),
                case.sensitive,
                "case `{}`",
                case.name
            );
        }
    }

    #[test]
    fn property_key_and_subscription_topic_enforce_length_limits() {
        let max_key = format!("a{}", "b".repeat(MAX_PROPERTY_KEY_BYTES - 1));
        assert!(validate_property_key(&max_key).is_ok());
        assert!(validate_property_key(&format!("{max_key}b")).is_err());

        let topic_suffix = "/+";
        let max_topic = format!(
            "{}{}",
            "a".repeat(MAX_PROPERTY_SUBSCRIPTION_TOPIC_BYTES - topic_suffix.len()),
            topic_suffix
        );
        assert!(validate_property_subscription_topic(&max_topic).is_ok());
        assert!(validate_property_subscription_topic(&format!("a{max_topic}")).is_err());
    }

    #[test]
    fn connector_templates_reject_invalid_or_unavailable_expressions() {
        struct Case {
            name: &'static str,
            template: &'static str,
            error_contains: &'static str,
        }

        let properties = context(&[("vin", "VIN-123")]);
        let cases = [
            Case {
                name: "uppercase property key",
                template: r#"{{ prop("Vin") }}"#,
                error_contains: "invalid",
            },
            Case {
                name: "numeric property key prefix",
                template: r#"{{ prop("1vin") }}"#,
                error_contains: "invalid",
            },
            Case {
                name: "empty property key",
                template: r#"{{ prop("") }}"#,
                error_contains: "invalid",
            },
            Case {
                name: "missing property",
                template: r#"{{ prop("missing") }}"#,
                error_contains: "not defined",
            },
            Case {
                name: "unclosed expression",
                template: r#"{{ prop("vin") "#,
                error_contains: "invalid connector template",
            },
            Case {
                name: "unknown function",
                template: r#"{{ foo() }}"#,
                error_contains: "function",
            },
            Case {
                name: "row is unavailable",
                template: r#"{{ .row }}"#,
                error_contains: "not found",
            },
        ];

        for case in cases {
            let err = match properties.render(case.template) {
                Ok(_) => panic!("case `{}` should fail", case.name),
                Err(err) => err,
            };
            assert!(
                err.to_string().contains(case.error_contains),
                "case `{}` returned unexpected error: {err}",
                case.name
            );
            assert!(
                !err.to_string().contains("VIN-123"),
                "case `{}` exposed a property value",
                case.name
            );
        }
    }

    #[test]
    fn registry_applies_overrides_without_mutating_existing_snapshots() {
        let registry = PropertyRegistry::new(
            BTreeMap::from([
                ("site".to_string(), SecretString::new("cn")),
                ("vin".to_string(), SecretString::new("VIN-STATIC")),
            ]),
            true,
        );
        let before = registry.snapshot();

        assert_eq!(
            registry
                .apply_override("vin", SecretString::new("VIN-DYNAMIC"))
                .expect("apply override"),
            PropertyUpdateResult::Changed { revision: 1 }
        );
        let after = registry.snapshot();

        assert_eq!(
            before.get("vin").expect("static vin").expose(),
            "VIN-STATIC"
        );
        assert_eq!(before.revision(), 0);
        assert_eq!(
            after.get("vin").expect("dynamic vin").expose(),
            "VIN-DYNAMIC"
        );
        assert_eq!(after.get("site").expect("static site").expose(), "cn");
        assert_eq!(after.revision(), 1);
    }

    #[test]
    fn registry_rejects_invalid_updates_and_treats_duplicates_as_noop() {
        let registry = PropertyRegistry::new(
            BTreeMap::from([("vin".to_string(), SecretString::new("VIN-STATIC"))]),
            true,
        );

        assert_eq!(
            registry.apply_override("unknown", SecretString::new("value")),
            Err(PropertyUpdateError::UnknownProperty("unknown".to_string()))
        );
        assert_eq!(
            registry.apply_override("vin", SecretString::new("")),
            Err(PropertyUpdateError::EmptyValue("vin".to_string()))
        );
        assert_eq!(registry.snapshot().revision(), 0);

        registry
            .apply_override("vin", SecretString::new("VIN-DYNAMIC"))
            .expect("first override");
        assert_eq!(
            registry
                .apply_override("vin", SecretString::new("VIN-DYNAMIC"))
                .expect("duplicate override"),
            PropertyUpdateResult::Noop { revision: 1 }
        );
        assert_eq!(registry.snapshot().revision(), 1);
    }

    #[test]
    fn registry_binds_only_property_templates_dynamically() {
        let registry = PropertyRegistry::new(
            BTreeMap::from([("vin".to_string(), SecretString::new("VIN-STATIC"))]),
            true,
        );
        let dynamic = registry
            .bind(r#"vehicles/{{ prop("vin") }}"#)
            .expect("bind dynamic template");
        let literal = registry.bind("vehicles/static").expect("bind literal");

        registry
            .apply_override("vin", SecretString::new("VIN-DYNAMIC"))
            .expect("apply override");
        let snapshot = registry.snapshot();

        assert!(matches!(dynamic, ConnectorString::Dynamic(_)));
        assert_eq!(
            dynamic.resolve(&snapshot).expect("resolve").expose(),
            "vehicles/VIN-DYNAMIC"
        );
        assert!(matches!(literal, ConnectorString::Plain(_)));
    }

    #[test]
    fn registry_debug_and_update_errors_do_not_expose_values() {
        let registry = PropertyRegistry::new(
            BTreeMap::from([("vin".to_string(), SecretString::new("VIN-SECRET"))]),
            true,
        );
        registry
            .apply_override("vin", SecretString::new("VIN-OVERRIDE-SECRET"))
            .expect("apply override");

        let debug = format!("{registry:?}");
        assert!(!debug.contains("VIN-SECRET"));
        assert!(!debug.contains("VIN-OVERRIDE-SECRET"));
        let error = registry
            .apply_override("missing", SecretString::new("ERROR-SECRET"))
            .expect_err("unknown key must fail")
            .to_string();
        assert!(!error.contains("ERROR-SECRET"));
    }

    #[test]
    fn property_subscription_topic_requires_a_trailing_single_level_wildcard() {
        for topic in ["$local/notification/+"] {
            validate_property_subscription_topic(topic).expect(topic);
        }
        for topic in [
            "",
            "$local/notification/#",
            "$local/notification/vin",
            "$local/+/notification",
            "$local/notification/+/#",
            "+/notification/+",
            "$local/+/+",
        ] {
            assert!(
                validate_property_subscription_topic(topic).is_err(),
                "topic `{topic}` should fail"
            );
        }
    }
}
