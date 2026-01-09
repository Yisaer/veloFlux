use std::sync::Arc;

/// Encoder-side projection spec for delayed materialization of `ColumnRef::ByIndex`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByIndexProjection {
    pub columns: Arc<[ByIndexProjectionColumn]>,
}

impl ByIndexProjection {
    pub fn new(columns: Vec<ByIndexProjectionColumn>) -> Self {
        Self {
            columns: Arc::from(columns),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn columns(&self) -> &[ByIndexProjectionColumn] {
        &self.columns
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByIndexProjectionColumn {
    pub source_name: String,
    pub column_index: usize,
    pub output_name: String,
}

impl ByIndexProjectionColumn {
    pub fn new(source_name: String, column_index: usize, output_name: String) -> Self {
        Self {
            source_name,
            column_index,
            output_name,
        }
    }
}

