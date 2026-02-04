use super::{ConcatFunc, CustomFunc};
use std::collections::HashMap;
use std::sync::Arc;

/// Registry for scalar custom functions referenced in SQL (e.g. `concat(a, b)`).
pub struct CustomFuncRegistry {
    functions: HashMap<String, Arc<dyn CustomFunc>>,
}

impl CustomFuncRegistry {
    pub fn with_builtins() -> Arc<Self> {
        Arc::new(Self::builtins())
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn CustomFunc>> {
        self.functions.get(&name.to_lowercase()).cloned()
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_lowercase())
    }

    pub fn list_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.functions.keys().cloned().collect();
        names.sort();
        names
    }

    fn builtins() -> Self {
        let mut functions: HashMap<String, Arc<dyn CustomFunc>> = HashMap::new();
        let concat = Arc::new(ConcatFunc);
        functions.insert(concat.name().to_lowercase(), concat);
        Self { functions }
    }
}

impl Default for CustomFuncRegistry {
    fn default() -> Self {
        Self::builtins()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_include_concat() {
        let registry = CustomFuncRegistry::default();
        assert!(registry.is_registered("concat"));
        assert!(registry.is_registered("CONCAT"));
        assert!(registry.get("concat").is_some());
        assert!(registry.get("CoNcAt").is_some());
        assert_eq!(registry.list_names(), vec!["concat".to_string()]);
    }

    #[test]
    fn registry_rejects_unknown_functions() {
        let registry = CustomFuncRegistry::default();
        assert!(!registry.is_registered("dummy"));
        assert!(registry.get("dummy").is_none());
        assert!(registry.get("missing").is_none());
    }
}
