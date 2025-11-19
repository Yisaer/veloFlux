use datatypes::Schema;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Errors that can occur when mutating the catalog.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CatalogError {
    #[error("schema already exists: {0}")]
    AlreadyExists(String),
    #[error("schema not found: {0}")]
    NotFound(String),
}

#[derive(Default)]
pub struct Catalog {
    schemas: RwLock<HashMap<String, Arc<Schema>>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, source: &str) -> Option<Arc<Schema>> {
        let guard = self.schemas.read().expect("catalog poisoned");
        guard.get(source).cloned()
    }

    pub fn list(&self) -> Vec<(String, Arc<Schema>)> {
        let guard = self.schemas.read().expect("catalog poisoned");
        guard
            .iter()
            .map(|(source, schema)| (source.clone(), schema.clone()))
            .collect()
    }

    pub fn insert(&self, source: impl Into<String>, schema: Schema) -> Result<(), CatalogError> {
        let source = source.into();
        let mut guard = self.schemas.write().expect("catalog poisoned");
        if guard.contains_key(&source) {
            return Err(CatalogError::AlreadyExists(source));
        }
        guard.insert(source, Arc::new(schema));
        Ok(())
    }

    pub fn upsert(&self, source: impl Into<String>, schema: Schema) {
        let source = source.into();
        let mut guard = self.schemas.write().expect("catalog poisoned");
        guard.insert(source, Arc::new(schema));
    }

    pub fn remove(&self, source: &str) -> Result<(), CatalogError> {
        let mut guard = self.schemas.write().expect("catalog poisoned");
        guard
            .remove(source)
            .map(|_| ())
            .ok_or_else(|| CatalogError::NotFound(source.to_string()))
    }
}

static GLOBAL_CATALOG: Lazy<Catalog> = Lazy::new(Catalog::new);

pub fn global_catalog() -> &'static Catalog {
    &GLOBAL_CATALOG
}
