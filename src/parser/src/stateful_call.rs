use serde::{Deserialize, Serialize};
use sqlparser::ast::Expr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatefulCallSpec {
    pub func_name: String,
    pub args: Vec<Expr>,
    pub when: Option<Expr>,
    pub until: Option<Expr>,
    pub partition_by: Vec<Expr>,
    pub original_expr: Expr,
}

impl StatefulCallSpec {
    pub fn display_sql(&self) -> String {
        let args = self
            .args
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let mut result = format!("{}({args})", self.func_name);
        if let Some(filter) = &self.when {
            result.push_str(&format!(" FILTER (WHERE {filter})"));
        }
        if !self.partition_by.is_empty() || self.until.is_some() {
            result.push_str(" OVER (");
            if !self.partition_by.is_empty() {
                result.push_str("PARTITION BY ");
                result.push_str(
                    &self
                        .partition_by
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
            if let Some(until) = &self.until {
                if !self.partition_by.is_empty() {
                    result.push(' ');
                }
                result.push_str(&format!("UNTIL {until}"));
            }
            result.push(')');
        }
        result
    }

    pub fn dedup_key(&self) -> String {
        let args = self
            .args
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let when = self
            .when
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "none".to_string());
        let partition_by = self
            .partition_by
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let until = self
            .until
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "none".to_string());
        format!(
            "{}|args=[{}]|when={}|until={}|partition_by=[{}]",
            self.func_name, args, when, until, partition_by
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatefulMappingEntry {
    pub output_column: String,
    pub spec: StatefulCallSpec,
}
