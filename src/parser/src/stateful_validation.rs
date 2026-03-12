use crate::aggregate_registry::AggregateRegistry;
use crate::stateful_registry::StatefulRegistry;
use crate::window::is_supported_window_function;
use sqlparser::ast::Visit;
use sqlparser::ast::{Expr, Visitor};
use std::ops::ControlFlow;
use std::sync::Arc;

struct StatefulContextValidator<'a> {
    stateful_registry: &'a Arc<dyn StatefulRegistry>,
    aggregate_registry: &'a Arc<dyn AggregateRegistry>,
    context: &'a str,
}

impl Visitor for StatefulContextValidator<'_> {
    type Break = String;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            let func_name = func
                .name
                .0
                .last()
                .map(|ident| ident.value.to_lowercase())
                .unwrap_or_default();

            if self.stateful_registry.is_stateful_function(&func_name) {
                return ControlFlow::Break(format!(
                    "stateful functions are not supported inside stateful function {}: {}",
                    self.context, expr
                ));
            }

            if self.aggregate_registry.is_aggregate_function(&func_name) {
                return ControlFlow::Break(format!(
                    "aggregate functions are not supported inside stateful function {}: {}",
                    self.context, expr
                ));
            }

            if is_supported_window_function(&func_name) {
                return ControlFlow::Break(format!(
                    "window functions are not supported inside stateful function {}: {}",
                    self.context, expr
                ));
            }
        }

        ControlFlow::Continue(())
    }
}

pub fn validate_stateful_context_expr(
    expr: &Expr,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
    context: &str,
) -> Result<(), String> {
    let mut visitor = StatefulContextValidator {
        stateful_registry,
        aggregate_registry,
        context,
    };
    match expr.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}
