use crate::model::{CollectionError, Column, Tuple};
use datatypes::Value;
use std::collections::{HashMap, HashSet};

/// RecordBatch represents a collection of rows stored purely as tuples.
#[derive(Debug)]
pub struct RecordBatch {
    rows: Vec<Tuple>,
}

impl RecordBatch {
    /// Create a new RecordBatch from row data.
    pub fn new(rows: Vec<Tuple>) -> Result<Self, CollectionError> {
        Ok(Self { rows })
    }

    /// Create an empty RecordBatch.
    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }

    /// Access the stored rows.
    pub fn rows(&self) -> &[Tuple] {
        &self.rows
    }

    /// Mutable access to stored rows (internal use for in-place updates).
    pub fn rows_mut(&mut self) -> &mut [Tuple] {
        &mut self.rows
    }

    /// Number of rows.
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Return a snapshot of the logical columns.
    pub fn columns(&self) -> Vec<Column> {
        let pairs = self.column_pairs();
        build_columns_from_rows(&self.rows, &pairs)
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn column_pairs(&self) -> Vec<(String, String)> {
        collect_column_pairs(&self.rows)
    }
}

impl Clone for RecordBatch {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
        }
    }
}

impl PartialEq for RecordBatch {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}

pub fn rows_from_columns(columns: Vec<Column>) -> Result<Vec<Tuple>, CollectionError> {
    if columns.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen = HashSet::new();
    for column in &columns {
        let key = (column.source_name.clone(), column.name.clone());
        if !seen.insert(key.clone()) {
            return Err(CollectionError::Other(format!(
                "Duplicate column: {}.{}",
                key.0, key.1
            )));
        }
    }

    let num_rows = columns[0].len();
    for (i, column) in columns.iter().enumerate() {
        if column.len() != num_rows {
            return Err(CollectionError::Other(format!(
                "Column {} has {} rows, expected {}",
                i,
                column.len(),
                num_rows
            )));
        }
    }

    let mut index_template = HashMap::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        index_template.insert((column.source_name.clone(), column.name.clone()), idx);
    }

    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(columns.len());
        for column in &columns {
            values.push(column.get(row_idx).cloned().unwrap_or(Value::Null));
        }
        rows.push(Tuple::new(index_template.clone(), values));
    }
    Ok(rows)
}

pub fn batch_from_columns(columns: Vec<Column>) -> Result<RecordBatch, CollectionError> {
    let rows = rows_from_columns(columns)?;
    RecordBatch::new(rows)
}

pub fn collect_column_pairs(rows: &[Tuple]) -> Vec<(String, String)> {
    let mut order = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        for pair in row.column_pairs() {
            if seen.insert(pair.clone()) {
                order.push(pair);
            }
        }
    }
    order
}

pub fn build_columns_from_rows(rows: &[Tuple], column_pairs: &[(String, String)]) -> Vec<Column> {
    let effective_pairs = if column_pairs.is_empty() {
        collect_column_pairs(rows)
    } else {
        column_pairs.to_vec()
    };

    let mut column_values: Vec<Vec<Value>> = effective_pairs
        .iter()
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();

    for row in rows {
        for (idx, (source, name)) in effective_pairs.iter().enumerate() {
            if let Some(value) = row.value_by_name(source, name) {
                column_values[idx].push(value.clone());
            } else {
                column_values[idx].push(Value::Null);
            }
        }
    }

    effective_pairs
        .into_iter()
        .zip(column_values)
        .map(|((source, name), values)| Column::new(source, name, values))
        .collect()
}
