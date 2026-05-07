use crate::acc_call::{AccCallSpec, AccMappingEntry, is_acc_function_name};
use crate::aggregate_registry::AggregateRegistry;
use crate::col_placeholder_allocator::ColPlaceholderAllocator;
use crate::select_stmt::SelectStmt;
use crate::stateful_registry::StatefulRegistry;
use crate::window::is_supported_window_function;
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, Ident, Visit, Visitor, WindowSpec, WindowType,
};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

pub fn transform_acc_functions(
    mut select_stmt: SelectStmt,
    aggregate_registry: Arc<dyn AggregateRegistry>,
    stateful_registry: Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
) -> Result<(SelectStmt, HashMap<String, AccCallSpec>), String> {
    let mut mappings = HashMap::new();
    let mut ordered_columns = Vec::new();
    let mut seen = HashMap::new();

    for field in &mut select_stmt.select_fields {
        let before_field = ordered_columns.len();
        field.expr = rewrite_expr_acc(
            &field.expr,
            &aggregate_registry,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
            &mut ordered_columns,
        )?;

        if ordered_columns.len() > before_field && field.alias.is_none() {
            field.alias = Some(field.field_name.clone());
        }
    }

    if let Some(where_expr) = &mut select_stmt.where_condition {
        *where_expr = rewrite_expr_acc(
            where_expr,
            &aggregate_registry,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
            &mut ordered_columns,
        )?;
    }

    for item in &mut select_stmt.order_by {
        item.expr = rewrite_expr_acc(
            &item.expr,
            &aggregate_registry,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
            &mut ordered_columns,
        )?;
    }

    select_stmt.acc_mappings = ordered_columns
        .iter()
        .map(|output_column| {
            mappings
                .get(output_column)
                .ok_or_else(|| format!("acc mapping missing for output column '{output_column}'"))
                .map(|spec| AccMappingEntry {
                    output_column: output_column.clone(),
                    spec: spec.clone(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((select_stmt, mappings))
}

fn rewrite_expr_acc(
    expr: &Expr,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
    seen: &mut HashMap<String, String>,
    mappings: &mut HashMap<String, AccCallSpec>,
    ordered_columns: &mut Vec<String>,
) -> Result<Expr, String> {
    match expr {
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_expr_acc(
                left,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
            op: op.clone(),
            right: Box::new(rewrite_expr_acc(
                right,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
        }),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr_acc(
                expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_expr_acc(
            inner,
            aggregate_registry,
            stateful_registry,
            allocator,
            seen,
            mappings,
            ordered_columns,
        )?))),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(rewrite_expr_acc(
                expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
            negated: *negated,
            low: Box::new(rewrite_expr_acc(
                low,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
            high: Box::new(rewrite_expr_acc(
                high,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let rewritten_expr = rewrite_expr_acc(
                expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?;
            let rewritten_list = list
                .iter()
                .map(|item| {
                    rewrite_expr_acc(
                        item,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
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
                Some(inner) => Some(Box::new(rewrite_expr_acc(
                    inner,
                    aggregate_registry,
                    stateful_registry,
                    allocator,
                    seen,
                    mappings,
                    ordered_columns,
                )?)),
                None => None,
            };

            let rewritten_conditions = conditions
                .iter()
                .map(|expr| {
                    rewrite_expr_acc(
                        expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let rewritten_results = results
                .iter()
                .map(|expr| {
                    rewrite_expr_acc(
                        expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let rewritten_else = match else_result.as_ref() {
                Some(inner) => Some(Box::new(rewrite_expr_acc(
                    inner,
                    aggregate_registry,
                    stateful_registry,
                    allocator,
                    seen,
                    mappings,
                    ordered_columns,
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
            let func_name = function_name(func);
            if is_acc_function_name(&func_name) {
                let call_spec = parse_acc_call(func, expr, stateful_registry, aggregate_registry)?;
                let key = call_spec.dedup_key();
                if let Some(col) = seen.get(&key) {
                    return Ok(Expr::Identifier(Ident::new(col)));
                }

                if !mappings.is_empty() {
                    return Err("only one acc function is supported per statement".to_string());
                }

                let col = allocator.allocate();
                seen.insert(key, col.clone());
                mappings.insert(col.clone(), call_spec);
                ordered_columns.push(col.clone());
                return Ok(Expr::Identifier(Ident::new(col)));
            }

            Ok(Expr::Function(rewrite_function_acc_context(
                func,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?))
        }
        _ => Ok(expr.clone()),
    }
}

fn rewrite_function_acc_context(
    func: &Function,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
    seen: &mut HashMap<String, String>,
    mappings: &mut HashMap<String, AccCallSpec>,
    ordered_columns: &mut Vec<String>,
) -> Result<Function, String> {
    let mut new_args = Vec::with_capacity(func.args.len());
    for arg in &func.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr)) => {
                let rewritten = rewrite_expr_acc(
                    inner_expr,
                    aggregate_registry,
                    stateful_registry,
                    allocator,
                    seen,
                    mappings,
                    ordered_columns,
                )?;
                new_args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(rewritten)));
            }
            FunctionArg::Named { name, arg } => {
                let rewritten_arg = match arg {
                    FunctionArgExpr::Expr(inner_expr) => FunctionArgExpr::Expr(rewrite_expr_acc(
                        inner_expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )?),
                    _ => arg.clone(),
                };
                new_args.push(FunctionArg::Named {
                    name: name.clone(),
                    arg: rewritten_arg,
                });
            }
            _ => new_args.push(arg.clone()),
        }
    }

    let new_filter = match func.filter.as_ref() {
        Some(expr) => Some(Box::new(rewrite_expr_acc(
            expr,
            aggregate_registry,
            stateful_registry,
            allocator,
            seen,
            mappings,
            ordered_columns,
        )?)),
        None => None,
    };

    let new_over = match func.over.as_ref() {
        Some(WindowType::WindowSpec(spec)) => Some(WindowType::WindowSpec(WindowSpec {
            partition_by: spec
                .partition_by
                .iter()
                .map(|expr| {
                    rewrite_expr_acc(
                        expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            order_by: spec
                .order_by
                .iter()
                .map(|item| {
                    let mut rewritten = item.clone();
                    rewritten.expr = rewrite_expr_acc(
                        &item.expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )?;
                    Ok(rewritten)
                })
                .collect::<Result<Vec<_>, String>>()?,
            window_frame: spec.window_frame.clone(),
        })),
        Some(WindowType::NamedWindow(name)) => Some(WindowType::NamedWindow(name.clone())),
        None => None,
    };

    let new_order_by = func
        .order_by
        .iter()
        .map(|item| {
            let mut rewritten = item.clone();
            rewritten.expr = rewrite_expr_acc(
                &item.expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?;
            Ok(rewritten)
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut new_func = func.clone();
    new_func.args = new_args;
    new_func.filter = new_filter;
    new_func.over = new_over;
    new_func.order_by = new_order_by;
    Ok(new_func)
}

fn parse_acc_call(
    func: &Function,
    original_expr: &Expr,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
) -> Result<AccCallSpec, String> {
    let func_name = function_name(func);

    if func.distinct {
        return Err(format!(
            "acc function '{}' does not support DISTINCT",
            func_name
        ));
    }

    if !func.order_by.is_empty() {
        return Err(format!(
            "acc function '{}' does not support ORDER BY",
            func_name
        ));
    }

    if func.filter.is_some() {
        return Err(format!(
            "acc function '{}' does not support FILTER",
            func_name
        ));
    }

    if func.over.is_some() {
        return Err(format!(
            "acc function '{}' does not support OVER",
            func_name
        ));
    }

    let mut args = Vec::with_capacity(func.args.len());
    for arg in &func.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                validate_acc_arg_expr(expr, stateful_registry, aggregate_registry, &func_name)?;
                args.push(expr.clone());
            }
            _ => {
                return Err(format!(
                    "unsupported acc function argument for {}: {}",
                    func_name, arg
                ));
            }
        }
    }

    Ok(AccCallSpec {
        func_name,
        args,
        original_expr: original_expr.clone(),
    })
}

fn validate_acc_arg_expr(
    expr: &Expr,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
    acc_func_name: &str,
) -> Result<(), String> {
    let mut visitor = AccArgValidator {
        stateful_registry,
        aggregate_registry,
        acc_func_name,
    };
    match expr.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

struct AccArgValidator<'a> {
    stateful_registry: &'a Arc<dyn StatefulRegistry>,
    aggregate_registry: &'a Arc<dyn AggregateRegistry>,
    acc_func_name: &'a str,
}

impl Visitor for AccArgValidator<'_> {
    type Break = String;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            let func_name = function_name(func);
            if is_acc_function_name(&func_name) {
                return ControlFlow::Break(format!(
                    "acc functions are not supported inside acc function {}: {}",
                    self.acc_func_name, expr
                ));
            }

            if self.stateful_registry.is_stateful_function(&func_name) {
                return ControlFlow::Break(format!(
                    "stateful functions are not supported inside acc function {}: {}",
                    self.acc_func_name, expr
                ));
            }

            if self.aggregate_registry.is_aggregate_function(&func_name) {
                return ControlFlow::Break(format!(
                    "aggregate functions are not supported inside acc function {}: {}",
                    self.acc_func_name, expr
                ));
            }

            if is_supported_window_function(&func_name) {
                return ControlFlow::Break(format!(
                    "window functions are not supported inside acc function {}: {}",
                    self.acc_func_name, expr
                ));
            }
        }

        ControlFlow::Continue(())
    }
}

fn function_name(func: &Function) -> String {
    func.name
        .0
        .last()
        .map(|ident| ident.value.to_lowercase())
        .unwrap_or_default()
}
