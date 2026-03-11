use crate::aggregate_registry::AggregateRegistry;
use crate::col_placeholder_allocator::ColPlaceholderAllocator;
use crate::select_stmt::SelectStmt;
use crate::stateful_call::StatefulCallSpec;
use crate::stateful_registry::StatefulRegistry;
use crate::stateful_validation::validate_stateful_context_expr;
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, Ident, WindowSpec, WindowType};
use std::collections::HashMap;
use std::sync::Arc;

pub fn transform_stateful_functions(
    mut select_stmt: SelectStmt,
    aggregate_registry: Arc<dyn AggregateRegistry>,
    stateful_registry: Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
) -> Result<(SelectStmt, HashMap<String, StatefulCallSpec>), String> {
    let mut mappings: HashMap<String, StatefulCallSpec> = HashMap::new();
    let mut seen: HashMap<String, String> = HashMap::new();

    for field in &mut select_stmt.select_fields {
        field.expr = rewrite_expr_stateful(
            &field.expr,
            &aggregate_registry,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
        )?;
    }

    if let Some(where_expr) = &mut select_stmt.where_condition {
        *where_expr = rewrite_expr_stateful(
            where_expr,
            &aggregate_registry,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
        )?;
    }

    for item in &mut select_stmt.order_by {
        item.expr = rewrite_expr_stateful(
            &item.expr,
            &aggregate_registry,
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
    aggregate_registry: &Arc<dyn AggregateRegistry>,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
    seen: &mut HashMap<String, String>,
    mappings: &mut HashMap<String, StatefulCallSpec>,
) -> Result<Expr, String> {
    match expr {
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_expr_stateful(
                left,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
            )?),
            op: op.clone(),
            right: Box::new(rewrite_expr_stateful(
                right,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
            )?),
        }),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr_stateful(
                expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
            )?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_expr_stateful(
            inner,
            aggregate_registry,
            stateful_registry,
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
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
            )?),
            negated: *negated,
            low: Box::new(rewrite_expr_stateful(
                low,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
            )?),
            high: Box::new(rewrite_expr_stateful(
                high,
                aggregate_registry,
                stateful_registry,
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
            let rewritten_expr = rewrite_expr_stateful(
                expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
            )?;
            let rewritten_list = list
                .iter()
                .map(|item| {
                    rewrite_expr_stateful(
                        item,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::InList {
                expr: Box::new(rewritten_expr),
                list: rewritten_list,
                negated: *negated,
            })
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let rewritten_operand = match operand.as_ref() {
                Some(inner) => Some(Box::new(rewrite_expr_stateful(
                    inner,
                    aggregate_registry,
                    stateful_registry,
                    allocator,
                    seen,
                    mappings,
                )?)),
                None => None,
            };

            let rewritten_conditions = conditions
                .iter()
                .map(|expr| {
                    rewrite_expr_stateful(
                        expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let rewritten_results = results
                .iter()
                .map(|expr| {
                    rewrite_expr_stateful(
                        expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let rewritten_else = match else_result.as_ref() {
                Some(inner) => Some(Box::new(rewrite_expr_stateful(
                    inner,
                    aggregate_registry,
                    stateful_registry,
                    allocator,
                    seen,
                    mappings,
                )?)),
                None => None,
            };

            Ok(Expr::Case {
                operand: rewritten_operand,
                conditions: rewritten_conditions,
                results: rewritten_results,
                else_result: rewritten_else,
            })
        }
        Expr::Function(func) => {
            let func_name = func
                .name
                .0
                .last()
                .map(|ident| ident.value.to_lowercase())
                .unwrap_or_default();

            if stateful_registry.is_stateful_function(&func_name) {
                let call_spec =
                    parse_stateful_call(func, expr, stateful_registry, aggregate_registry)?;
                let key = call_spec.dedup_key();
                if let Some(col) = seen.get(&key) {
                    return Ok(Expr::Identifier(Ident::new(col)));
                }

                let col = allocator.allocate();
                seen.insert(key, col.clone());
                mappings.insert(col.clone(), call_spec);
                return Ok(Expr::Identifier(Ident::new(col)));
            }

            let mut new_args = Vec::with_capacity(func.args.len());
            for arg in &func.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr)) = arg {
                    let new_inner = rewrite_expr_stateful(
                        inner_expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                    )?;
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

fn parse_stateful_call(
    func: &Function,
    original_expr: &Expr,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
) -> Result<StatefulCallSpec, String> {
    let func_name = func
        .name
        .0
        .last()
        .map(|ident| ident.value.to_lowercase())
        .unwrap_or_default();

    if func.distinct {
        return Err(format!(
            "stateful function '{}' does not support DISTINCT",
            func_name
        ));
    }

    if !func.order_by.is_empty() {
        return Err(format!(
            "stateful function '{}' does not support ORDER BY",
            func_name
        ));
    }

    let mut args = Vec::with_capacity(func.args.len());
    for arg in &func.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                validate_stateful_context_expr(
                    expr,
                    stateful_registry,
                    aggregate_registry,
                    "arguments",
                )?;
                args.push(expr.clone());
            }
            _ => {
                return Err(format!(
                    "unsupported stateful function argument for {}: {}",
                    func_name, arg
                ));
            }
        }
    }

    let when = match func.filter.as_ref() {
        Some(expr) => {
            validate_stateful_context_expr(expr, stateful_registry, aggregate_registry, "FILTER")?;
            Some((**expr).clone())
        }
        None => None,
    };

    let partition_by = parse_stateful_over_partition_by(func, &func_name)?;
    for expr in &partition_by {
        validate_stateful_context_expr(
            expr,
            stateful_registry,
            aggregate_registry,
            "OVER PARTITION BY",
        )?;
    }

    Ok(StatefulCallSpec {
        func_name,
        args,
        when,
        partition_by,
        original_expr: original_expr.clone(),
    })
}

fn parse_stateful_over_partition_by(func: &Function, func_name: &str) -> Result<Vec<Expr>, String> {
    let Some(over) = func.over.as_ref() else {
        return Ok(Vec::new());
    };

    match over {
        WindowType::WindowSpec(spec) => parse_stateful_window_spec_partition_by(spec, func_name),
        WindowType::NamedWindow(name) => Err(format!(
            "stateful function '{}' does not support named windows (got {})",
            func_name, name
        )),
    }
}

fn parse_stateful_window_spec_partition_by(
    spec: &WindowSpec,
    func_name: &str,
) -> Result<Vec<Expr>, String> {
    if !spec.order_by.is_empty() {
        return Err(format!(
            "stateful function '{}' OVER does not support ORDER BY",
            func_name
        ));
    }

    if spec.window_frame.is_some() {
        return Err(format!(
            "stateful function '{}' OVER does not support window frames",
            func_name
        ));
    }

    if spec.partition_by.is_empty() {
        return Err(format!(
            "stateful function '{}' OVER requires PARTITION BY expressions",
            func_name
        ));
    }

    Ok(spec.partition_by.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SelectField;
    use crate::aggregate_registry::default_aggregate_registry;
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

        let (out, mappings) = transform_stateful_functions(
            select_stmt,
            default_aggregate_registry(),
            registry,
            &mut allocator,
        )
        .unwrap();
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

        let mut select_stmt = SelectStmt::with_fields(vec![SelectField::new(
            Expr::Identifier(Ident::new("a")),
            None,
            "a".to_string(),
        )]);
        select_stmt.where_condition = Some(Expr::BinaryOp {
            left: Box::new(lag_a),
            op: sqlparser::ast::BinaryOperator::Gt,
            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                "0".to_string(),
                false,
            ))),
        });

        let (out, mappings) = transform_stateful_functions(
            select_stmt,
            default_aggregate_registry(),
            registry,
            &mut allocator,
        )
        .unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(
            out.where_condition.as_ref().unwrap().to_string(),
            "col_1 > 0"
        );
    }
}
