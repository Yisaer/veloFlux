use sqlparser::ast::Expr;

/// Represents a SELECT statement with its fields
/// Currently only maintains the select fields part
#[derive(Debug, Clone)]
pub struct SelectStmt {
    /// The select fields/expressions
    pub select_fields: Vec<SelectField>,
}

/// Represents a single select field/expression
#[derive(Debug, Clone)]
pub struct SelectField {
    /// The expression for this field (from sqlparser AST)
    pub expr: Expr,
    /// Optional alias for this field
    pub alias: Option<String>,
}

impl SelectStmt {
    /// Create a new SelectStmt with empty fields
    pub fn new() -> Self {
        Self {
            select_fields: Vec::new(),
        }
    }

    /// Create a new SelectStmt with given fields
    pub fn with_fields(select_fields: Vec<SelectField>) -> Self {
        Self { select_fields }
    }
}

impl Default for SelectStmt {
    fn default() -> Self {
        Self::new()
    }
}

impl SelectField {
    /// Create a new SelectField
    pub fn new(expr: Expr, alias: Option<String>) -> Self {
        Self { expr, alias }
    }
}
