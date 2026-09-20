use crate::planner::logical::BaseLogicalPlan;
use datatypes::Schema;
use parser::JoinType;
use sqlparser::ast::{BinaryOperator, Expr};
use std::sync::Arc;

/// A logical reference to a relation column before physical message slots are assigned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalColumnRef {
    pub relation: String,
    pub column: String,
}

/// One equality key binding for a lookup join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupKeyBinding {
    pub probe_column: LogicalColumnRef,
    pub table_column: LogicalColumnRef,
}

/// Logical specification for one stream-to-table lookup join.
#[derive(Debug, Clone)]
pub struct LookupJoinSpec {
    pub join_type: JoinType,
    pub table_name: String,
    pub table_schema: Arc<Schema>,
    pub table_projection: Vec<String>,
    pub key_bindings: Vec<LookupKeyBinding>,
}

/// A unary logical lookup join.
#[derive(Debug, Clone)]
pub struct LookupJoin {
    pub base: BaseLogicalPlan,
    pub spec: LookupJoinSpec,
}

impl LookupJoin {
    pub fn new(spec: LookupJoinSpec, child: Arc<super::LogicalPlan>, index: i64) -> Self {
        Self {
            base: BaseLogicalPlan::new(vec![child], index),
            spec,
        }
    }
}

/// Extract equality key bindings from an INNER JOIN ON expression.
pub fn extract_lookup_key_bindings(
    on: &Expr,
    stream_name: &str,
    stream_schema: &Schema,
    table_name: &str,
    table_schema: &Schema,
) -> Result<Vec<LookupKeyBinding>, String> {
    let mut predicates = Vec::new();
    flatten_conjunction(on, &mut predicates);
    let mut bindings = Vec::with_capacity(predicates.len());

    for predicate in predicates {
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = predicate
        else {
            return Err(
                "lookup join ON supports only stream/table equality predicates".to_string(),
            );
        };

        let left = column_ref(left).ok_or_else(|| {
            "lookup join ON equality operands must be qualified columns".to_string()
        })?;
        let right = column_ref(right).ok_or_else(|| {
            "lookup join ON equality operands must be qualified columns".to_string()
        })?;

        let (probe_column, table_column) =
            if left.relation == stream_name && right.relation == table_name {
                (left, right)
            } else if left.relation == table_name && right.relation == stream_name {
                (right, left)
            } else {
                return Err(format!(
                    "lookup join ON must connect stream `{stream_name}` with table `{table_name}`"
                ));
            };

        if !schema_has_column(stream_schema, &probe_column.column) {
            return Err(format!(
                "stream column `{}` not found in `{stream_name}`",
                probe_column.column
            ));
        }
        if !schema_has_column(table_schema, &table_column.column) {
            return Err(format!(
                "table column `{}` not found in `{table_name}`",
                table_column.column
            ));
        }

        bindings.push(LookupKeyBinding {
            probe_column,
            table_column,
        });
    }

    if bindings.is_empty() {
        return Err("lookup join ON requires at least one equality predicate".to_string());
    }
    Ok(bindings)
}

fn flatten_conjunction<'a>(expr: &'a Expr, output: &mut Vec<&'a Expr>) {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        flatten_conjunction(left, output);
        flatten_conjunction(right, output);
    } else {
        output.push(expr);
    }
}

fn column_ref(expr: &Expr) -> Option<LogicalColumnRef> {
    let Expr::CompoundIdentifier(parts) = expr else {
        return None;
    };
    if parts.len() != 2 {
        return None;
    }
    Some(LogicalColumnRef {
        relation: parts[0].value.clone(),
        column: parts[1].value.clone(),
    })
}

fn schema_has_column(schema: &Schema, column: &str) -> bool {
    schema
        .column_schemas()
        .iter()
        .any(|candidate| candidate.name == column)
}
