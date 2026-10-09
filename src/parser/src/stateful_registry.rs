use std::sync::Arc;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StatefulOutputMode {
    #[default]
    Immediate,
    Deferred,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatefulFunctionCapabilities {
    pub output_mode: StatefulOutputMode,
    pub supports_until: bool,
}

impl StatefulFunctionCapabilities {
    pub const DEFERRED_WITH_UNTIL: Self = Self {
        output_mode: StatefulOutputMode::Deferred,
        supports_until: true,
    };
}

pub trait StatefulRegistry: Send + Sync {
    fn is_stateful_function(&self, name: &str) -> bool;

    fn capabilities(&self, name: &str) -> Option<StatefulFunctionCapabilities> {
        self.is_stateful_function(name)
            .then(StatefulFunctionCapabilities::default)
    }
}

pub const BUILTIN_STATEFUL_FUNCTIONS: [&str; 14] = [
    "acc_avg",
    "acc_count",
    "acc_max",
    "acc_min",
    "acc_sum",
    "change_capture",
    "change_to",
    "changed_col",
    "consecutive_count",
    "consecutive_start",
    "had_changed",
    "lag",
    "lead",
    "latest",
];

pub fn builtin_stateful_function_names() -> &'static [&'static str] {
    &BUILTIN_STATEFUL_FUNCTIONS
}

#[derive(Default)]
pub struct StaticStatefulRegistry {
    functions: std::collections::HashMap<String, StatefulFunctionCapabilities>,
}

impl StaticStatefulRegistry {
    pub fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut registry = Self::default();
        for name in names {
            let name = name.into().to_lowercase();
            let capabilities = if name == "lead" {
                StatefulFunctionCapabilities::DEFERRED_WITH_UNTIL
            } else {
                StatefulFunctionCapabilities::default()
            };
            registry.register(name, capabilities);
        }
        registry
    }

    pub fn register(
        &mut self,
        name: impl Into<String>,
        capabilities: StatefulFunctionCapabilities,
    ) {
        self.functions
            .insert(name.into().to_lowercase(), capabilities);
    }
}

impl StatefulRegistry for StaticStatefulRegistry {
    fn is_stateful_function(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_lowercase())
    }

    fn capabilities(&self, name: &str) -> Option<StatefulFunctionCapabilities> {
        self.functions.get(&name.to_lowercase()).copied()
    }
}

pub fn default_stateful_registry() -> Arc<dyn StatefulRegistry> {
    Arc::new(StaticStatefulRegistry::new(
        builtin_stateful_function_names().iter().copied(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_includes_builtin_stateful_functions() {
        let registry = default_stateful_registry();
        assert!(registry.is_stateful_function("change_capture"));
        assert!(registry.is_stateful_function("change_to"));
        assert!(registry.is_stateful_function("changed_col"));
        assert!(registry.is_stateful_function("consecutive_count"));
        assert!(registry.is_stateful_function("consecutive_start"));
        assert!(registry.is_stateful_function("had_changed"));
        assert!(registry.is_stateful_function("lag"));
        assert!(registry.is_stateful_function("latest"));
        assert!(registry.is_stateful_function("acc_sum"));
        assert!(registry.is_stateful_function("LAG"));
        assert!(!registry.is_stateful_function("missing"));
    }
}
