use serde::{Deserialize, Serialize};
use sqlparser::ast::Expr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatefulCallSpec {
    pub func_name: String,
    pub args: Vec<Expr>,
    pub when: Option<Expr>,
    pub partition_by: Vec<Expr>,
    pub original_expr: Expr,
}

impl StatefulCallSpec {
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
        format!(
            "{}|args=[{}]|when={}|partition_by=[{}]",
            self.func_name, args, when, partition_by
        )
    }
}
