use std::sync::Arc;

pub trait StatefulRegistry: Send + Sync {
    fn is_stateful_function(&self, name: &str) -> bool;
}

#[derive(Default)]
pub struct StaticStatefulRegistry {
    names: std::collections::HashSet<String>,
}

impl StaticStatefulRegistry {
    pub fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(|s| s.into().to_lowercase()).collect(),
        }
    }
}

impl StatefulRegistry for StaticStatefulRegistry {
    fn is_stateful_function(&self, name: &str) -> bool {
        self.names.contains(&name.to_lowercase())
    }
}

#[derive(Default)]
struct EmptyStatefulRegistry;

impl StatefulRegistry for EmptyStatefulRegistry {
    fn is_stateful_function(&self, _name: &str) -> bool {
        false
    }
}

pub fn default_stateful_registry() -> Arc<dyn StatefulRegistry> {
    Arc::new(EmptyStatefulRegistry)
}

