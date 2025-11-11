//! Visitor pattern implementations for SQL AST traversal

pub mod aggregate_visitor;
pub mod table_visitor;

pub use aggregate_visitor::{
    extract_aggregates_with_visitor,
    contains_aggregates_with_visitor,
    AggregateVisitor
};
pub use table_visitor::TableInfoVisitor;
// SourceInfo is defined in select_stmt, not in table_visitor
pub use crate::select_stmt::SourceInfo;