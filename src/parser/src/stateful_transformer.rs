use crate::col_placeholder_allocator::ColPlaceholderAllocator;
use crate::select_stmt::SelectStmt;
use crate::stateful_registry::StatefulRegistry;
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Ident};
use std::collections::HashMap;
use std::sync::Arc;

pub fn transform_stateful_functions(
    mut select_stmt: SelectStmt,
    stateful_registry: Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
) -> Result<(SelectStmt, HashMap<String, Expr>), String> {
    let mut mappings: HashMap<String, Expr> = HashMap::new();
    let mut seen: HashMap<String, String> = HashMap::new();

    for field in &mut select_stmt.select_fields {
        field.expr = rewrite_expr_stateful(
            &field.expr,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
        )?;
    }

    if let Some(where_expr) = &mut select_stmt.where_condition {
        *where_expr = rewrite_expr_stateful(
            where_expr,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
        )?;
    }

    select_stmt.stateful_mappings = mappings.clone();
    Ok((select_stmt, mappings))
}

fn rewrite_expr_stateful(
    expr: &Expr,
    registry: &Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
    seen: &mut HashMap<String, String>,
    mappings: &mut HashMap<String, Expr>,
) -> Result<Expr, String> {
    match expr {
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_expr_stateful(
                left,
                registry,
                allocator,
                seen,
                mappings,
            )?),
            op: op.clone(),
            right: Box::new(rewrite_expr_stateful(
                right,
                registry,
                allocator,
                seen,
                mappings,
            )?),
        }),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr_stateful(
                expr,
                registry,
                allocator,
                seen,
                mappings,
            )?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_expr_stateful(
            inner,
            registry,
            allocator,
            seen,
            mappings,
        )?))),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(rewrite_expr_stateful(
                expr,
                registry,
                allocator,
                seen,
                mappings,
            )?),
            negated: *negated,
            low: Box::new(rewrite_expr_stateful(
                low,
                registry,
                allocator,
                seen,
                mappings,
            )?),
            high: Box::new(rewrite_expr_stateful(
                high,
                registry,
                allocator,
                seen,
                mappings,
            )?),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let rewritten_expr = rewrite_expr_stateful(expr, registry, allocator, seen, mappings)?;
            let rewritten_list: Result<Vec<_>, _> = list
                .iter()
                .map(|item| rewrite_expr_stateful(item, registry, allocator, seen, mappings))
                .collect();
            Ok(Expr::InList {
                expr: Box::new(rewritten_expr),
                list: rewritten_list?,
                negated: *negated,
            })
        }
        Expr::Function(func) => {
            let func_name = func
                .name
                .0
                .last()
                .map(|ident| ident.value.clone())
                .unwrap_or_default();

            if registry.is_stateful_function(&func_name) {
                let key = expr.to_string();
                if let Some(col) = seen.get(&key) {
                    return Ok(Expr::Identifier(Ident::new(col)));
                }

                let col = allocator.allocate();
                seen.insert(key, col.clone());
                mappings.insert(col.clone(), expr.clone());
                return Ok(Expr::Identifier(Ident::new(col)));
            }

            let mut new_args = Vec::with_capacity(func.args.len());
            for arg in &func.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr)) = arg {
                    let new_inner =
                        rewrite_expr_stateful(inner_expr, registry, allocator, seen, mappings)?;
                    new_args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(new_inner)));
                } else {
                    new_args.push(arg.clone());
                }
            }
            let mut new_func = func.clone();
            new_func.args = new_args;
            Ok(Expr::Function(new_func))
        }
        _ => Ok(expr.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SelectField;
    use crate::stateful_registry::StaticStatefulRegistry;
    use sqlparser::ast::{Function, ObjectName};

    fn lag_expr(arg: Expr) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("lag")]),
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        })
    }

    #[test]
    fn deduplicates_stateful_calls_in_select() {
        let registry = Arc::new(StaticStatefulRegistry::new(["lag"]));
        let mut allocator = ColPlaceholderAllocator::new();
        let expr = lag_expr(Expr::Identifier(Ident::new("a")));
        let select_stmt = SelectStmt::with_fields(vec![
            SelectField::new(expr.clone(), None, expr.to_string()),
            SelectField::new(expr.clone(), None, expr.to_string()),
        ]);

        let (out, mappings) =
            transform_stateful_functions(select_stmt, registry, &mut allocator).unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(out.stateful_mappings.len(), 1);
        assert_eq!(out.select_fields[0].expr.to_string(), "col_1");
        assert_eq!(out.select_fields[1].expr.to_string(), "col_1");
    }

    #[test]
    fn rewrites_stateful_in_where() {
        let registry = Arc::new(StaticStatefulRegistry::new(["lag"]));
        let mut allocator = ColPlaceholderAllocator::new();
        let lag_a = lag_expr(Expr::Identifier(Ident::new("a")));

        let mut select_stmt =
            SelectStmt::with_fields(vec![SelectField::new(Expr::Identifier(Ident::new("a")), None, "a".to_string())]);
        select_stmt.where_condition = Some(Expr::BinaryOp {
            left: Box::new(lag_a),
            op: sqlparser::ast::BinaryOperator::Gt,
            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("0".to_string(), false))),
        });

        let (out, mappings) =
            transform_stateful_functions(select_stmt, registry, &mut allocator).unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(
            out.where_condition.as_ref().unwrap().to_string(),
            "col_1 > 0"
        );
    }
}

