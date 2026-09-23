use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, Ident, JoinConstraint as SqlJoinConstraint,
    JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    UnaryOperator, Visit, VisitMut, VisitorMut,
};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, TokenWithLocation, Tokenizer};

use crate::aggregate_registry::{AggregateRegistry, default_aggregate_registry};
use crate::aggregate_transformer::transform_aggregate_functions;
use crate::col_placeholder_allocator::ColPlaceholderAllocator;
use crate::dialect::StreamDialect;
use crate::select_stmt::{
    JoinConstraint as ParsedJoinConstraint, JoinSpec, JoinType, OrderByItem, SelectField,
    SelectStmt, SourceInfo,
};
use crate::stateful_registry::{StatefulRegistry, default_stateful_registry};
use crate::stateful_transformer::transform_stateful_functions;
use crate::visitor::TableInfoVisitor;
use crate::window::Window;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// SQL Parser based on StreamDialect
pub struct StreamSqlParser {
    dialect: StreamDialect,
    aggregate_registry: Arc<dyn AggregateRegistry>,
    stateful_registry: Arc<dyn StatefulRegistry>,
}

#[derive(Debug, Clone)]
struct RawNamedExpression {
    name: String,
    expr: Expr,
}

impl StreamSqlParser {
    /// Create a new StreamSqlParser
    pub fn new() -> Self {
        Self::with_registries(default_aggregate_registry(), default_stateful_registry())
    }

    /// Create a StreamSqlParser with a specific aggregate registry
    pub fn with_registry(aggregate_registry: Arc<dyn AggregateRegistry>) -> Self {
        Self::with_registries(aggregate_registry, default_stateful_registry())
    }

    pub fn with_registries(
        aggregate_registry: Arc<dyn AggregateRegistry>,
        stateful_registry: Arc<dyn StatefulRegistry>,
    ) -> Self {
        Self {
            dialect: StreamDialect::new(),
            aggregate_registry,
            stateful_registry,
        }
    }

    /// Parse SQL string and return SelectStmt containing select fields and aggregate mappings
    /// This is the main entry point for parsing SQL with StreamDialect
    /// Automatically transforms aggregate functions during parsing
    pub fn parse(&self, sql: &str) -> Result<SelectStmt, String> {
        let (sql_without_named_expressions, named_expressions) =
            self.extract_named_expressions(sql)?;
        let sliding_over =
            preprocess_sliding_window_over(&sql_without_named_expressions, &self.dialect)?;

        // Create a parser with StreamDialect
        let parser = Parser::parse_sql(&self.dialect, &sliding_over.sql)
            .map_err(|e| format!("Parse error: {}", e))?;

        if parser.len() != 1 {
            return Err("Expected exactly one SQL statement".to_string());
        }

        let statement = &parser[0];

        // Collect window + non-window GROUP BY expressions before we move on
        let (mut window, group_by_exprs) =
            crate::dialect::collect_window_and_group_by_exprs(statement)
                .map_err(|e| format!("Dialect processing error: {}", e))?;
        if let Some(trigger_condition) = sliding_over.trigger_condition {
            window = match window {
                Some(sliding @ Window::Sliding { .. }) => {
                    Some(sliding.with_sliding_trigger_condition(Some(Box::new(trigger_condition))))
                }
                Some(other) => {
                    return Err(format!(
                        "slidingwindow OVER WHEN was parsed but no GROUP BY slidingwindow was found; found {:?}",
                        other,
                    ));
                }
                None => {
                    return Err(
                        "slidingwindow OVER WHEN was parsed but no GROUP BY window function was found"
                            .to_string(),
                    );
                }
            };
        }

        // Extract raw select fields from the statement (before transformation)
        let mut select_stmt = self.extract_select_fields(statement)?;
        select_stmt.window = window;
        select_stmt.group_by_exprs = group_by_exprs;

        // Expand named expressions exactly once before any parser transformer runs.
        // All subsequent transformations only receive the rewritten SelectStmt and
        // never inspect the original WITH NAMED EXPRESSIONS text.
        let named_expression_env = self.expand_named_expression_declarations(named_expressions)?;
        validate_named_expression_alias_collisions(&select_stmt, &named_expression_env)?;
        select_stmt.named_expression_names = named_expression_env.keys().cloned().collect();
        rewrite_named_expressions(&mut select_stmt, &named_expression_env);
        lower_null_predicates(&mut select_stmt);

        let mut allocator = ColPlaceholderAllocator::new();

        // Transform stateful functions first (search + replace, with dedup).
        // This enables cases like last_row(lag(a)) where a stateful call appears inside an aggregate.
        let (select_stmt, _stateful_mappings) = transform_stateful_functions(
            select_stmt,
            Arc::clone(&self.aggregate_registry),
            Arc::clone(&self.stateful_registry),
            &mut allocator,
        )?;

        // Transform aggregate functions after stateful rewrite.
        let (transformed_stmt, _aggregate_mappings) = transform_aggregate_functions(
            select_stmt,
            Arc::clone(&self.aggregate_registry),
            &mut allocator,
        )?;

        Ok(transformed_stmt)
    }

    fn extract_named_expressions(
        &self,
        sql: &str,
    ) -> Result<(String, Vec<RawNamedExpression>), String> {
        let leading = sql.len() - sql.trim_start().len();
        let trimmed = &sql[leading..];
        let prefix = "WITH NAMED EXPRESSIONS";
        if !trimmed
            .get(..prefix.len())
            .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
        {
            return Ok((sql.to_string(), Vec::new()));
        }

        let mut cursor = prefix.len();
        let mut declarations = Vec::new();
        loop {
            cursor = Self::skip_ascii_whitespace(trimmed, cursor);
            let name_start = cursor;
            while trimmed
                .as_bytes()
                .get(cursor)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            {
                cursor += 1;
            }
            if name_start == cursor {
                return Err("expected named expression identifier".to_string());
            }
            let name = trimmed[name_start..cursor].to_string();

            cursor = Self::skip_ascii_whitespace(trimmed, cursor);
            cursor = Self::consume_keyword(trimmed, cursor, "AS")
                .ok_or_else(|| format!("expected AS after named expression `{name}`"))?;
            cursor = Self::skip_ascii_whitespace(trimmed, cursor);
            if trimmed.as_bytes().get(cursor) != Some(&b'(') {
                return Err(format!(
                    "expected `(` before named expression `{name}` expression"
                ));
            }

            let expression_start = cursor + 1;
            let expression_end = Self::find_matching_parenthesis(trimmed, cursor)
                .ok_or_else(|| format!("unclosed expression for named expression `{name}`"))?;
            let expression_sql = trimmed[expression_start..expression_end].trim();
            if expression_sql.is_empty() {
                return Err(format!("named expression `{name}` must not be empty"));
            }
            let expr = self.parse_named_expression_expr(expression_sql, &name)?;
            declarations.push(RawNamedExpression { name, expr });

            cursor = Self::skip_ascii_whitespace(trimmed, expression_end + 1);
            if trimmed.as_bytes().get(cursor) == Some(&b',') {
                cursor += 1;
                continue;
            }
            break;
        }

        cursor = Self::skip_ascii_whitespace(trimmed, cursor);
        if Self::consume_keyword(trimmed, cursor, "SELECT").is_none() {
            return Err("WITH NAMED EXPRESSIONS must be followed by SELECT".to_string());
        }

        Ok((trimmed[cursor..].to_string(), declarations))
    }

    fn parse_named_expression_expr(
        &self,
        expression_sql: &str,
        name: &str,
    ) -> Result<Expr, String> {
        let sql = format!("SELECT {expression_sql}");
        let statements = Parser::parse_sql(&self.dialect, &sql)
            .map_err(|err| format!("invalid named expression `{name}`: {err}"))?;
        if statements.len() != 1 {
            return Err(format!(
                "invalid named expression `{name}`: expected one expression"
            ));
        }

        match &statements[0] {
            Statement::Query(query) => match &*query.body {
                SetExpr::Select(select) if select.projection.len() == 1 => {
                    match &select.projection[0] {
                        SelectItem::UnnamedExpr(expr) => Ok(expr.clone()),
                        SelectItem::ExprWithAlias { .. } => Err(format!(
                            "invalid named expression `{name}`: aliases are not allowed"
                        )),
                        _ => Err(format!(
                            "invalid named expression `{name}`: expected a scalar expression"
                        )),
                    }
                }
                _ => Err(format!(
                    "invalid named expression `{name}`: expected a scalar expression"
                )),
            },
            _ => Err(format!(
                "invalid named expression `{name}`: expected a scalar expression"
            )),
        }
    }

    fn expand_named_expression_declarations(
        &self,
        declarations: Vec<RawNamedExpression>,
    ) -> Result<HashMap<String, Expr>, String> {
        let all_names: HashSet<String> = declarations
            .iter()
            .map(|declaration| declaration.name.clone())
            .collect();
        if all_names.len() != declarations.len() {
            return Err("duplicate named expression declaration".to_string());
        }

        let mut environment = HashMap::with_capacity(declarations.len());
        for declaration in declarations {
            let mut expander = NamedExpressionExpander {
                environment: &environment,
                all_names: &all_names,
            };
            let mut expr = declaration.expr;
            if let std::ops::ControlFlow::Break(error) = VisitMut::visit(&mut expr, &mut expander) {
                return Err(error);
            }
            environment.insert(declaration.name, expr);
        }
        Ok(environment)
    }

    /// Extract select fields from a parsed SQL statement
    fn extract_select_fields(&self, statement: &Statement) -> Result<SelectStmt, String> {
        match statement {
            Statement::Query(query) => self.extract_from_query(query),
            _ => Err("Expected a SELECT query".to_string()),
        }
    }

    fn skip_ascii_whitespace(input: &str, mut offset: usize) -> usize {
        while input
            .as_bytes()
            .get(offset)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            offset += 1;
        }
        offset
    }

    fn consume_keyword(input: &str, offset: usize, keyword: &str) -> Option<usize> {
        let end = offset.checked_add(keyword.len())?;
        let value = input.get(offset..end)?;
        if !value.eq_ignore_ascii_case(keyword) {
            return None;
        }
        if input
            .as_bytes()
            .get(end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            return None;
        }
        Some(end)
    }

    fn find_matching_parenthesis(input: &str, opening: usize) -> Option<usize> {
        let bytes = input.as_bytes();
        let mut depth = 0usize;
        let mut quote = None;
        let mut offset = opening;

        while let Some(byte) = bytes.get(offset).copied() {
            if let Some(quote_byte) = quote {
                if byte == quote_byte {
                    if bytes.get(offset + 1) == Some(&quote_byte) {
                        offset += 2;
                        continue;
                    }
                    quote = None;
                }
                offset += 1;
                continue;
            }

            match byte {
                b'\'' | b'"' => quote = Some(byte),
                b'(' => depth += 1,
                b')' => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(offset);
                    }
                }
                _ => {}
            }
            offset += 1;
        }

        None
    }

    /// Extract select fields from a query
    fn extract_from_query(&self, query: &Query) -> Result<SelectStmt, String> {
        let mut select_stmt = match &*query.body {
            SetExpr::Select(select) => self.extract_from_select(select)?,
            _ => return Err("Expected a simple SELECT query".to_string()),
        };

        let mut order_by_items = Vec::with_capacity(query.order_by.len());
        for item in &query.order_by {
            if item.nulls_first.is_some() {
                return Err("NULLS FIRST/LAST in ORDER BY is not supported yet".to_string());
            }
            order_by_items.push(OrderByItem {
                expr: item.expr.clone(),
                asc: item.asc.unwrap_or(true),
            });
        }
        select_stmt.order_by = order_by_items;
        Ok(select_stmt)
    }

    /// Extract select fields from a SELECT statement
    fn extract_from_select(&self, select: &Select) -> Result<SelectStmt, String> {
        let mut select_fields = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let field_name = projection_field_name(expr);
                    select_fields.push(SelectField::new(expr.clone(), None, field_name));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let field_name = alias.value.clone();
                    select_fields.push(SelectField::new(
                        expr.clone(),
                        Some(alias.value.clone()),
                        field_name,
                    ));
                }
                SelectItem::Wildcard(_) => {
                    let expr = Expr::Identifier(Ident::new("*"));
                    let field_name = expr.to_string();
                    select_fields.push(SelectField::new(expr, None, field_name));
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    let mut idents = object_name.0.clone();
                    idents.push(Ident::new("*"));
                    let expr = Expr::CompoundIdentifier(idents);
                    let field_name = expr.to_string();
                    select_fields.push(SelectField::new(expr, None, field_name));
                }
            }
        }

        // Extract WHERE and HAVING clauses if present
        let where_condition = select.selection.clone();
        let having = select.having.clone();

        // Use visitor pattern to extract table (source) information
        let mut table_visitor = TableInfoVisitor::new();
        let _ = select.visit(&mut table_visitor);
        let source_infos = table_visitor.get_sources();

        let mut select_stmt =
            SelectStmt::with_fields_and_conditions(select_fields, where_condition, having);
        select_stmt.source_infos = source_infos;
        let (from, joins) = extract_join_structure(select)?;
        select_stmt.from = from;
        select_stmt.joins = joins;

        Ok(select_stmt)
    }

    // Window validation (e.g. only allowed in GROUP BY) is intentionally not enforced here.
}

fn extract_join_structure(select: &Select) -> Result<(Option<SourceInfo>, Vec<JoinSpec>), String> {
    let Some(from) = select.from.first() else {
        return Ok((None, Vec::new()));
    };

    if select.from.len() > 1 {
        return Ok((None, Vec::new()));
    }

    let TableFactor::Table {
        name: from_name,
        alias: left_alias,
        ..
    } = &from.relation
    else {
        if from.joins.is_empty() {
            return Ok((None, Vec::new()));
        }
        return Err("JOIN requires a named table relation".to_string());
    };
    if left_alias.is_some() && !from.joins.is_empty() {
        return Err("stream/table aliases are not supported for JOIN".to_string());
    }

    let base_relation = SourceInfo {
        name: from_name.to_string(),
        alias: None,
    };

    let mut joins = Vec::with_capacity(from.joins.len());
    for join in &from.joins {
        let TableFactor::Table {
            name,
            alias: right_alias,
            ..
        } = &join.relation
        else {
            return Err("JOIN requires a named table relation".to_string());
        };
        if right_alias.is_some() {
            return Err("stream/table aliases are not supported for JOIN".to_string());
        }

        let (join_type, constraint) = match &join.join_operator {
            JoinOperator::Inner(constraint) => (JoinType::Inner, Some(constraint)),
            JoinOperator::LeftOuter(constraint) => (JoinType::Left, Some(constraint)),
            JoinOperator::RightOuter(constraint) => (JoinType::Right, Some(constraint)),
            JoinOperator::FullOuter(constraint) => (JoinType::Full, Some(constraint)),
            JoinOperator::CrossJoin => (JoinType::Cross, None),
            JoinOperator::LeftSemi(_)
            | JoinOperator::RightSemi(_)
            | JoinOperator::LeftAnti(_)
            | JoinOperator::RightAnti(_)
            | JoinOperator::CrossApply
            | JoinOperator::OuterApply => {
                return Err("unsupported JOIN type".to_string());
            }
        };
        let constraint = match constraint {
            None => ParsedJoinConstraint::None,
            Some(SqlJoinConstraint::On(on)) => ParsedJoinConstraint::On(on.clone()),
            Some(SqlJoinConstraint::None) => ParsedJoinConstraint::None,
            Some(SqlJoinConstraint::Using(_)) => {
                return Err("JOIN USING is not supported".to_string());
            }
            Some(SqlJoinConstraint::Natural) => {
                return Err("NATURAL JOIN is not supported".to_string());
            }
        };

        joins.push(JoinSpec {
            join_type,
            relation: SourceInfo {
                name: name.to_string(),
                alias: None,
            },
            constraint,
        });
    }
    if joins.is_empty() {
        Ok((None, joins))
    } else {
        Ok((Some(base_relation), joins))
    }
}

struct NullPredicateLowerer;

struct NamedExpressionExpander<'a> {
    environment: &'a HashMap<String, Expr>,
    all_names: &'a HashSet<String>,
}

struct NamedExpressionRewriter<'a> {
    environment: &'a HashMap<String, Expr>,
}

impl VisitorMut for NamedExpressionRewriter<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> std::ops::ControlFlow<Self::Break> {
        if let Expr::Identifier(identifier) = expr
            && let Some(replacement) = self.environment.get(identifier.value.as_str())
        {
            *expr = Expr::Nested(Box::new(replacement.clone()));
        }
        std::ops::ControlFlow::Continue(())
    }
}

impl VisitorMut for NamedExpressionExpander<'_> {
    type Break = String;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> std::ops::ControlFlow<Self::Break> {
        let Expr::Identifier(identifier) = expr else {
            return std::ops::ControlFlow::Continue(());
        };

        if let Some(replacement) = self.environment.get(identifier.value.as_str()) {
            *expr = Expr::Nested(Box::new(replacement.clone()));
        } else if self.all_names.contains(identifier.value.as_str()) {
            return std::ops::ControlFlow::Break(format!(
                "forward reference to named expression `{}` is not allowed",
                identifier.value
            ));
        }
        std::ops::ControlFlow::Continue(())
    }
}

impl VisitorMut for NullPredicateLowerer {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> std::ops::ControlFlow<Self::Break> {
        let lowered = match expr {
            Expr::IsNull(inner) => Some(isnull_call((**inner).clone())),
            Expr::IsNotNull(inner) => Some(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(isnull_call((**inner).clone())),
            }),
            _ => None,
        };
        if let Some(lowered) = lowered {
            *expr = lowered;
        }
        std::ops::ControlFlow::Continue(())
    }
}

fn rewrite_named_expressions(select_stmt: &mut SelectStmt, environment: &HashMap<String, Expr>) {
    if environment.is_empty() {
        return;
    }

    let mut rewriter = NamedExpressionRewriter { environment };
    for field in &mut select_stmt.select_fields {
        let _ = VisitMut::visit(&mut field.expr, &mut rewriter);
    }
    let _ = VisitMut::visit(&mut select_stmt.where_condition, &mut rewriter);
    let _ = VisitMut::visit(&mut select_stmt.having, &mut rewriter);
    for item in &mut select_stmt.order_by {
        let _ = VisitMut::visit(&mut item.expr, &mut rewriter);
    }
    let _ = VisitMut::visit(&mut select_stmt.group_by_exprs, &mut rewriter);

    let Some(window) = select_stmt.window.as_mut() else {
        return;
    };
    match window {
        Window::Tumbling {
            partition_by,
            filter,
            ..
        }
        | Window::Count {
            partition_by,
            filter,
            ..
        } => {
            let _ = VisitMut::visit(partition_by, &mut rewriter);
            let _ = VisitMut::visit(filter, &mut rewriter);
        }
        Window::Sliding {
            partition_by,
            trigger_condition,
            filter,
            ..
        } => {
            let _ = VisitMut::visit(partition_by, &mut rewriter);
            let _ = VisitMut::visit(trigger_condition, &mut rewriter);
            let _ = VisitMut::visit(filter, &mut rewriter);
        }
        Window::State {
            open,
            emit,
            partition_by,
            filter,
        } => {
            let _ = VisitMut::visit(open, &mut rewriter);
            let _ = VisitMut::visit(emit, &mut rewriter);
            let _ = VisitMut::visit(partition_by, &mut rewriter);
            let _ = VisitMut::visit(filter, &mut rewriter);
        }
        Window::Eos { filter } => {
            let _ = VisitMut::visit(filter, &mut rewriter);
        }
    }
}

fn validate_named_expression_alias_collisions(
    select_stmt: &SelectStmt,
    environment: &HashMap<String, Expr>,
) -> Result<(), String> {
    for field in &select_stmt.select_fields {
        let Some(alias) = field.alias.as_ref() else {
            continue;
        };

        if environment.contains_key(alias) {
            return Err(format!(
                "SELECT alias `{alias}` collides with named expression `{alias}`"
            ));
        }
    }

    Ok(())
}

fn isnull_call(expr: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![Ident::new("isnull")]),
        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))],
        over: None,
        distinct: false,
        special: false,
        order_by: vec![],
        filter: None,
        null_treatment: None,
    })
}

fn lower_null_predicates(select_stmt: &mut SelectStmt) {
    let mut lowerer = NullPredicateLowerer;
    for field in &mut select_stmt.select_fields {
        let _ = VisitMut::visit(&mut field.expr, &mut lowerer);
    }
    let _ = VisitMut::visit(&mut select_stmt.where_condition, &mut lowerer);
    let _ = VisitMut::visit(&mut select_stmt.having, &mut lowerer);
    for item in &mut select_stmt.order_by {
        let _ = VisitMut::visit(&mut item.expr, &mut lowerer);
    }
    let _ = VisitMut::visit(&mut select_stmt.group_by_exprs, &mut lowerer);

    let Some(window) = select_stmt.window.as_mut() else {
        return;
    };
    match window {
        Window::Tumbling {
            partition_by,
            filter,
            ..
        }
        | Window::Count {
            partition_by,
            filter,
            ..
        } => {
            let _ = VisitMut::visit(partition_by, &mut lowerer);
            let _ = VisitMut::visit(filter, &mut lowerer);
        }
        Window::Sliding {
            partition_by,
            trigger_condition,
            filter,
            ..
        } => {
            let _ = VisitMut::visit(partition_by, &mut lowerer);
            let _ = VisitMut::visit(trigger_condition, &mut lowerer);
            let _ = VisitMut::visit(filter, &mut lowerer);
        }
        Window::State {
            open,
            emit,
            partition_by,
            filter,
        } => {
            let _ = VisitMut::visit(open, &mut lowerer);
            let _ = VisitMut::visit(emit, &mut lowerer);
            let _ = VisitMut::visit(partition_by, &mut lowerer);
            let _ = VisitMut::visit(filter, &mut lowerer);
        }
        Window::Eos { filter } => {
            let _ = VisitMut::visit(filter, &mut lowerer);
        }
    }
}

impl Default for StreamSqlParser {
    fn default() -> Self {
        Self::new()
    }
}

fn projection_field_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|ident| ident.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        _ => expr.to_string(),
    }
}

struct SlidingOverPreprocessResult {
    sql: String,
    trigger_condition: Option<Expr>,
}

struct Replacement {
    start: usize,
    end: usize,
    text: String,
}

/// Preprocess SQL to extract `OVER (WHEN <expr>)` from slidingwindow and remove it
/// from the SQL text so the standard parser can handle the rest.
///
/// Handles three cases:
/// - `slidingwindow(...) OVER (WHEN <expr> PARTITION BY ...)` → extracts WHEN, keeps PARTITION BY
/// - `slidingwindow(...) OVER (WHEN <expr>)` → extracts WHEN, removes entire OVER clause
/// - `slidingwindow(...)` (no OVER, or OVER without WHEN) → no-op
fn preprocess_sliding_window_over(
    sql: &str,
    dialect: &StreamDialect,
) -> Result<SlidingOverPreprocessResult, String> {
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let tokens = tokenizer
        .tokenize_with_location()
        .map_err(|err| format!("Parse error: {err}"))?;
    let non_ws = non_whitespace_token_indexes(&tokens);
    let mut replacements = Vec::new();
    let mut trigger_condition = None;

    for &token_idx in &non_ws {
        if !is_word(&tokens[token_idx].token, "slidingwindow") {
            continue;
        }

        let Some(fn_lparen) = next_non_whitespace(&tokens, token_idx + 1) else {
            continue;
        };
        if !matches!(tokens[fn_lparen].token, Token::LParen) {
            continue;
        }
        let fn_rparen = matching_rparen(&tokens, fn_lparen)?;

        let mut cursor = next_non_whitespace(&tokens, fn_rparen + 1);
        if let Some(filter_idx) = cursor
            && is_word(&tokens[filter_idx].token, "filter")
        {
            let Some(filter_lparen) = next_non_whitespace(&tokens, filter_idx + 1) else {
                return Err("slidingwindow FILTER requires parentheses".to_string());
            };
            if !matches!(tokens[filter_lparen].token, Token::LParen) {
                return Err("slidingwindow FILTER requires parentheses".to_string());
            }
            let filter_rparen = matching_rparen(&tokens, filter_lparen)?;
            cursor = next_non_whitespace(&tokens, filter_rparen + 1);
        }

        let Some(over_idx) = cursor else {
            continue;
        };
        if !is_word(&tokens[over_idx].token, "over") {
            continue;
        }
        let Some(over_lparen) = next_non_whitespace(&tokens, over_idx + 1) else {
            return Err("slidingwindow OVER requires parentheses".to_string());
        };
        if !matches!(tokens[over_lparen].token, Token::LParen) {
            return Err("slidingwindow OVER requires parentheses".to_string());
        }
        let over_rparen = matching_rparen(&tokens, over_lparen)?;
        let Some(first_over_token) = next_non_whitespace(&tokens, over_lparen + 1) else {
            continue;
        };
        if !is_word(&tokens[first_over_token].token, "when") {
            continue;
        }
        if trigger_condition.is_some() {
            return Err("Only one slidingwindow OVER WHEN clause is allowed".to_string());
        }

        let Some(expr_start_token) = next_non_whitespace(&tokens, first_over_token + 1) else {
            return Err("slidingwindow OVER WHEN requires an expression".to_string());
        };
        if expr_start_token >= over_rparen {
            return Err("slidingwindow OVER WHEN requires an expression".to_string());
        }

        let partition_token = find_top_level_partition_by(&tokens, expr_start_token, over_rparen)?;
        let expr_end_token = partition_token.unwrap_or(over_rparen);
        let expr_start = token_start_offset(sql, &tokens[expr_start_token])?;
        let expr_end = token_start_offset(sql, &tokens[expr_end_token])?;
        let expr_sql = sql
            .get(expr_start..expr_end)
            .ok_or_else(|| "failed to slice slidingwindow OVER WHEN expression".to_string())?
            .trim();
        if expr_sql.is_empty() {
            return Err("slidingwindow OVER WHEN requires an expression".to_string());
        }
        trigger_condition = Some(parse_over_when_expr(expr_sql, dialect)?);

        if partition_token.is_some() {
            // Remove `WHEN <expr>` portion (including the WHEN keyword),
            // leaving `OVER (PARTITION BY ...)` intact.
            let when_start = token_start_offset(sql, &tokens[first_over_token])?;
            replacements.push(Replacement {
                start: when_start,
                end: expr_end,
                text: String::new(),
            });
        } else {
            // Remove entire `OVER (WHEN <expr>)`
            let over_start = token_start_offset(sql, &tokens[over_idx])?;
            let over_end = token_start_offset(sql, &tokens[over_rparen])?.saturating_add(1);
            replacements.push(Replacement {
                start: over_start,
                end: over_end,
                text: String::new(),
            });
        }
    }

    let mut normalized = sql.to_string();
    for replacement in replacements.iter().rev() {
        normalized.replace_range(replacement.start..replacement.end, &replacement.text);
    }

    Ok(SlidingOverPreprocessResult {
        sql: normalized,
        trigger_condition,
    })
}

fn parse_over_when_expr(expr_sql: &str, dialect: &StreamDialect) -> Result<Expr, String> {
    let sql = format!("SELECT {expr_sql}");
    let statements =
        Parser::parse_sql(dialect, &sql).map_err(|err| format!("Parse error: {err}"))?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Err("failed to parse slidingwindow OVER WHEN expression".to_string());
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err("failed to parse slidingwindow OVER WHEN expression".to_string());
    };
    let Some(item) = select.projection.first() else {
        return Err("failed to parse slidingwindow OVER WHEN expression".to_string());
    };
    match item {
        SelectItem::UnnamedExpr(expr) => Ok(expr.clone()),
        _ => Err("slidingwindow OVER WHEN must be a scalar expression".to_string()),
    }
}

fn non_whitespace_token_indexes(tokens: &[TokenWithLocation]) -> Vec<usize> {
    tokens
        .iter()
        .enumerate()
        .filter_map(|(idx, token)| (!is_whitespace(&token.token)).then_some(idx))
        .collect()
}

fn next_non_whitespace(tokens: &[TokenWithLocation], start: usize) -> Option<usize> {
    tokens
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(idx, token)| (!is_whitespace(&token.token)).then_some(idx))
}

fn is_whitespace(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

fn is_word(token: &Token, expected: &str) -> bool {
    match token {
        Token::Word(word) => word.value.eq_ignore_ascii_case(expected),
        _ => false,
    }
}

fn matching_rparen(tokens: &[TokenWithLocation], lparen: usize) -> Result<usize, String> {
    let mut depth = 0usize;
    for (idx, token) in tokens.iter().enumerate().skip(lparen) {
        match token.token {
            Token::LParen => depth = depth.saturating_add(1),
            Token::RParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok(idx);
                }
            }
            _ => {}
        }
    }
    Err("unclosed parentheses in slidingwindow clause".to_string())
}

fn find_top_level_partition_by(
    tokens: &[TokenWithLocation],
    start: usize,
    end: usize,
) -> Result<Option<usize>, String> {
    let mut depth = 0usize;
    let mut idx = start;
    while idx < end {
        match tokens[idx].token {
            Token::LParen | Token::LBracket | Token::LBrace => depth = depth.saturating_add(1),
            Token::RParen | Token::RBracket | Token::RBrace => depth = depth.saturating_sub(1),
            _ => {
                if depth == 0 && is_word(&tokens[idx].token, "partition") {
                    let Some(by_idx) = next_non_whitespace(tokens, idx + 1) else {
                        return Ok(None);
                    };
                    if by_idx < end && is_word(&tokens[by_idx].token, "by") {
                        return Ok(Some(idx));
                    }
                }
            }
        }
        idx += 1;
    }
    Ok(None)
}

fn token_start_offset(sql: &str, token: &TokenWithLocation) -> Result<usize, String> {
    location_to_byte_offset(sql, token.location).ok_or_else(|| {
        format!(
            "failed to resolve SQL token location at line {}, column {}",
            token.location.line, token.location.column
        )
    })
}

fn location_to_byte_offset(sql: &str, location: Location) -> Option<usize> {
    let mut line = 1u64;
    let mut column = 1u64;
    for (offset, ch) in sql.char_indices() {
        if line == location.line && column == location.column {
            return Some(offset);
        }
        if ch == '\n' {
            line = line.saturating_add(1);
            column = 1;
        } else {
            column = column.saturating_add(1);
        }
    }
    (line == location.line && column == location.column).then_some(sql.len())
}

/// Convenience function to parse SQL and return SelectStmt
pub fn parse_sql(sql: &str) -> Result<SelectStmt, String> {
    let parser = StreamSqlParser::new();
    parser.parse(sql)
}

pub fn parse_sql_with_registries(
    sql: &str,
    aggregate_registry: Arc<dyn AggregateRegistry>,
    stateful_registry: Arc<dyn StatefulRegistry>,
) -> Result<SelectStmt, String> {
    let parser = StreamSqlParser::with_registries(aggregate_registry, stateful_registry);
    parser.parse(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_agg_replacement_expr_field_name() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT sum(a) + 1");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);

        let field = &select_stmt.select_fields[0];
        assert_eq!(field.alias, Some("sum(a) + 1".to_string()));
        assert_eq!(field.field_name, "sum(a) + 1".to_string());
        assert_eq!(field.expr.to_string(), "col_1 + 1".to_string());
    }

    #[test]
    fn test_parse_simple_select() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a + b");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);

        let field = &select_stmt.select_fields[0];
        assert!(field.alias.is_none());
        assert_eq!(field.field_name, "a + b".to_string());
    }

    #[test]
    fn test_extract_named_expressions_basic() {
        let parser = StreamSqlParser::new();
        let (query_sql, expressions) = parser
            .extract_named_expressions(
                "WITH NAMED EXPRESSIONS b AS (a + 1), c AS (b + 1) SELECT c FROM stream",
            )
            .expect("named expressions should parse");

        assert_eq!(query_sql, "SELECT c FROM stream");
        assert_eq!(expressions.len(), 2);
        assert_eq!(expressions[0].name, "b");
        assert_eq!(expressions[0].expr.to_string(), "a + 1");
        assert_eq!(expressions[1].name, "c");
        assert_eq!(expressions[1].expr.to_string(), "b + 1");
    }

    #[test]
    fn test_extract_named_expressions_is_case_insensitive_and_allows_newlines() {
        let parser = StreamSqlParser::new();
        let (query_sql, expressions) = parser
            .extract_named_expressions(
                "with named expressions\n    b as (\n        a + 1\n    )\nselect b from stream",
            )
            .expect("named expressions should parse");

        assert_eq!(query_sql, "select b from stream");
        assert_eq!(expressions.len(), 1);
        assert_eq!(expressions[0].name, "b");
        assert_eq!(expressions[0].expr.to_string(), "a + 1");
    }

    #[test]
    fn test_extract_named_expressions_ignores_function_argument_commas() {
        let parser = StreamSqlParser::new();
        let (_, expressions) = parser
            .extract_named_expressions(
                "WITH NAMED EXPRESSIONS b AS (coalesce(a, 1)), c AS (concat('x,y', a)) SELECT c FROM stream",
            )
            .expect("function argument commas should not split declarations");

        assert_eq!(expressions.len(), 2);
        assert_eq!(expressions[0].expr.to_string(), "coalesce(a, 1)");
        assert_eq!(expressions[1].expr.to_string(), "concat('x,y', a)");
    }

    #[test]
    fn test_extract_named_expressions_preserves_nested_parentheses() {
        let parser = StreamSqlParser::new();
        let (_, expressions) = parser
            .extract_named_expressions(
                "WITH NAMED EXPRESSIONS b AS (abs((a + 1) * 2)) SELECT b FROM stream",
            )
            .expect("nested parentheses should parse");

        assert_eq!(expressions.len(), 1);
        assert_eq!(expressions[0].expr.to_string(), "abs((a + 1) * 2)");
    }

    #[test]
    fn test_extract_named_expressions_preserves_quoted_parentheses_and_commas() {
        let parser = StreamSqlParser::new();
        let (_, expressions) = parser
            .extract_named_expressions(
                "WITH NAMED EXPRESSIONS b AS (concat('(', a, ')')) SELECT b FROM stream",
            )
            .expect("quoted punctuation should not affect scanning");

        assert_eq!(expressions.len(), 1);
        assert_eq!(expressions[0].expr.to_string(), "concat('(', a, ')')");
    }

    #[test]
    fn test_extract_named_expressions_leaves_regular_sql_unchanged() {
        let parser = StreamSqlParser::new();
        let (query_sql, expressions) = parser
            .extract_named_expressions("SELECT a FROM stream")
            .expect("regular SQL should remain supported");

        assert_eq!(query_sql, "SELECT a FROM stream");
        assert!(expressions.is_empty());
    }

    fn named_expression(
        parser: &StreamSqlParser,
        name: &str,
        expression: &str,
    ) -> RawNamedExpression {
        RawNamedExpression {
            name: name.to_string(),
            expr: parser
                .parse_named_expression_expr(expression, name)
                .expect("named expression should parse"),
        }
    }

    #[test]
    fn test_expand_named_expressions_empty() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(Vec::new())
            .expect("empty declarations should succeed");

        assert!(environment.is_empty());
    }

    #[test]
    fn test_expand_named_expression_single_level() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(vec![named_expression(&parser, "b", "a + 1")])
            .expect("single declaration should succeed");

        assert_eq!(environment["b"].to_string(), "a + 1");
    }

    #[test]
    fn test_expand_named_expressions_two_level_dependency() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(vec![
                named_expression(&parser, "b", "a + 1"),
                named_expression(&parser, "c", "b + 1"),
            ])
            .expect("ordered declarations should succeed");

        assert_eq!(environment["b"].to_string(), "a + 1");
        assert_eq!(environment["c"].to_string(), "(a + 1) + 1");
    }

    #[test]
    fn test_expand_named_expressions_multi_level_dependency() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(vec![
                named_expression(&parser, "b", "a + 1"),
                named_expression(&parser, "c", "b + 1"),
                named_expression(&parser, "d", "c + 1"),
            ])
            .expect("multi-level declarations should succeed");

        assert_eq!(environment["b"].to_string(), "a + 1");
        assert_eq!(environment["c"].to_string(), "(a + 1) + 1");
        assert_eq!(environment["d"].to_string(), "((a + 1) + 1) + 1");
    }

    #[test]
    fn test_expand_named_expression_replaces_multiple_references() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(vec![
                named_expression(&parser, "b", "a + 1"),
                named_expression(&parser, "c", "b + b"),
            ])
            .expect("multiple references should succeed");

        assert_eq!(environment["c"].to_string(), "(a + 1) + (a + 1)");
    }

    #[test]
    fn test_expand_named_expression_replaces_function_arguments() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(vec![
                named_expression(&parser, "b", "a + 1"),
                named_expression(&parser, "c", "coalesce(b, 0)"),
            ])
            .expect("function argument references should succeed");

        assert_eq!(environment["c"].to_string(), "coalesce((a + 1), 0)");
    }

    #[test]
    fn test_expand_named_expression_preserves_operator_precedence() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(vec![
                named_expression(&parser, "b", "a + 1"),
                named_expression(&parser, "c", "b * 2"),
            ])
            .expect("precedence-preserving expansion should succeed");

        assert_eq!(environment["c"].to_string(), "(a + 1) * 2");
    }

    #[test]
    fn test_expand_named_expression_rejects_forward_reference() {
        let parser = StreamSqlParser::new();
        let result = parser.expand_named_expression_declarations(vec![
            named_expression(&parser, "c", "b + 1"),
            named_expression(&parser, "b", "a + 1"),
        ]);

        assert!(matches!(
            result,
            Err(error) if error == "forward reference to named expression `b` is not allowed"
        ));
    }

    #[test]
    fn test_expand_named_expression_rejects_self_reference() {
        let parser = StreamSqlParser::new();
        let result = parser
            .expand_named_expression_declarations(vec![named_expression(&parser, "b", "b + 1")]);

        assert_eq!(
            result,
            Err("forward reference to named expression `b` is not allowed".to_string())
        );
    }

    #[test]
    fn test_expand_named_expressions_rejects_duplicate_declarations() {
        let parser = StreamSqlParser::new();
        let result = parser.expand_named_expression_declarations(vec![
            named_expression(&parser, "b", "a + 1"),
            named_expression(&parser, "b", "a + 2"),
        ]);

        assert_eq!(
            result,
            Err("duplicate named expression declaration".to_string())
        );
    }

    #[test]
    fn test_expand_named_expression_preserves_regular_input_identifiers() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(vec![
                named_expression(&parser, "b", "a + 1"),
                named_expression(&parser, "c", "input_column + 1"),
            ])
            .expect("regular input identifiers should remain valid");

        assert_eq!(environment["c"].to_string(), "input_column + 1");
    }

    #[test]
    fn test_expand_named_expression_preserves_qualified_identifiers() {
        let parser = StreamSqlParser::new();
        let environment = parser
            .expand_named_expression_declarations(vec![
                named_expression(&parser, "b", "a + 1"),
                named_expression(&parser, "c", "stream.b + 1"),
            ])
            .expect("qualified identifiers should remain valid");

        assert_eq!(environment["c"].to_string(), "stream.b + 1");
    }

    #[test]
    fn test_rewrite_named_expression_in_select() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT b FROM stream")
            .expect("named expression should be replaced in SELECT");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "(a + 1)");
    }

    #[test]
    fn test_rewrite_named_expression_in_select_with_nested_dependency() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1), c AS (b * 2) SELECT c FROM stream")
            .expect("nested named expression should be replaced in SELECT");

        assert_eq!(
            select_stmt.select_fields[0].expr.to_string(),
            "((a + 1) * 2)"
        );
    }

    #[test]
    fn test_parse_named_expression_three_level_dependency() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "WITH NAMED EXPRESSIONS b AS (a + 1), c AS (b + 1), d AS (c + 1) SELECT d FROM stream",
            )
            .expect("three-level named expression should parse");

        assert_eq!(
            select_stmt.select_fields[0].expr.to_string(),
            "(((a + 1) + 1) + 1)"
        );
    }

    #[test]
    fn test_parse_named_expression_rejects_forward_reference() {
        let parser = StreamSqlParser::new();
        let result =
            parser.parse("WITH NAMED EXPRESSIONS c AS (b + 1), b AS (a + 1) SELECT c FROM stream");

        assert!(matches!(
            result,
            Err(error) if error == "forward reference to named expression `b` is not allowed"
        ));
    }

    #[test]
    fn test_parse_named_expression_replaces_bare_but_not_qualified_identifier() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS d AS (a + 1) SELECT d, stream.d FROM stream")
            .expect("named expression and qualified input should parse");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "(a + 1)");
        assert_eq!(select_stmt.select_fields[1].expr.to_string(), "stream.d");
    }

    #[test]
    fn test_unused_named_expressions_do_not_trigger_rewrites() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "WITH NAMED EXPRESSIONS unused_stateful AS (lag(a)), unused_aggregate AS (sum(a)) SELECT b FROM stream",
            )
            .expect("unused named expressions should not affect parsing");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "b");
        assert!(select_stmt.stateful_mappings.is_empty());
        assert!(select_stmt.aggregate_mappings.is_empty());
    }

    #[test]
    fn test_rewrite_named_expression_in_multiple_select_fields() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT b, b + 2 FROM stream")
            .expect("named expression should be replaced in every SELECT field");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "(a + 1)");
        assert_eq!(select_stmt.select_fields[1].expr.to_string(), "(a + 1) + 2");
    }

    #[test]
    fn test_rewrite_named_expression_in_where() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT a FROM stream WHERE b > 10")
            .expect("named expression should be replaced in WHERE");

        assert_eq!(
            select_stmt
                .where_condition
                .expect("WHERE should exist")
                .to_string(),
            "(a + 1) > 10"
        );
    }

    #[test]
    fn test_rewrite_named_expression_in_group_by() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT count(a) FROM stream GROUP BY b")
            .expect("named expression should be replaced in GROUP BY");

        assert_eq!(select_stmt.group_by_exprs[0].to_string(), "(a + 1)");
    }

    #[test]
    fn test_rewrite_named_expression_in_having() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT count(a) FROM stream HAVING b > 10")
            .expect("named expression should be replaced in HAVING");

        assert_eq!(
            select_stmt.having.expect("HAVING should exist").to_string(),
            "(a + 1) > 10"
        );
    }

    #[test]
    fn test_rewrite_named_expression_in_order_by() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT a FROM stream ORDER BY b")
            .expect("named expression should be replaced in ORDER BY");

        assert_eq!(select_stmt.order_by[0].expr.to_string(), "(a + 1)");
    }

    #[test]
    fn test_rewrite_named_expression_in_window_partition_by() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "WITH NAMED EXPRESSIONS b AS (a + 1) SELECT * FROM stream GROUP BY tumblingwindow('ss', 10) OVER (PARTITION BY b)",
            )
            .expect("named expression should be replaced in window partition");

        match select_stmt.window.expect("window should exist") {
            Window::Tumbling { partition_by, .. } => {
                assert_eq!(partition_by[0].to_string(), "(a + 1)");
            }
            other => panic!("expected tumbling window, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_named_expression_in_window_filter() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "WITH NAMED EXPRESSIONS b AS (a + 1) SELECT * FROM stream GROUP BY tumblingwindow('ss', 10) FILTER (WHERE b > 0)",
            )
            .expect("named expression should be replaced in window filter");

        match select_stmt.window.expect("window should exist") {
            Window::Tumbling { filter, .. } => {
                assert_eq!(
                    filter.expect("window filter should exist").to_string(),
                    "(a + 1) > 0"
                );
            }
            other => panic!("expected tumbling window, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_named_expression_in_sliding_window_when() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "WITH NAMED EXPRESSIONS b AS (a + 1) SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN b > 0)",
            )
            .expect("named expression should be replaced in sliding window trigger");

        match select_stmt.window.expect("window should exist") {
            Window::Sliding {
                trigger_condition, ..
            } => {
                assert_eq!(
                    trigger_condition
                        .expect("trigger condition should exist")
                        .to_string(),
                    "(a + 1) > 0"
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_named_expression_in_state_window() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "WITH NAMED EXPRESSIONS b AS (a + 1) SELECT * FROM stream GROUP BY statewindow(b > 0, b < 10) OVER (PARTITION BY b)",
            )
            .expect("named expression should be replaced in state window");

        match select_stmt.window.expect("window should exist") {
            Window::State {
                open,
                emit,
                partition_by,
                ..
            } => {
                assert_eq!(open.to_string(), "(a + 1) > 0");
                assert_eq!(emit.to_string(), "(a + 1) < 10");
                assert_eq!(partition_by[0].to_string(), "(a + 1)");
            }
            other => panic!("expected state window, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_named_expression_preserves_qualified_identifier() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT stream.b FROM stream")
            .expect("qualified identifier should remain valid");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "stream.b");
    }

    #[test]
    fn test_rewrite_named_expression_preserves_undeclared_identifier() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT c FROM stream")
            .expect("undeclared input identifier should remain valid");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "c");
    }

    #[test]
    fn test_rejects_named_expression_and_select_alias_collision() {
        let parser = StreamSqlParser::new();
        let result =
            parser.parse("WITH NAMED EXPRESSIONS b AS (a + 1) SELECT a AS b, b FROM stream");

        assert!(matches!(
            result,
            Err(error) if error == "SELECT alias `b` collides with named expression `b`"
        ));
    }

    #[test]
    fn test_rewrite_named_expression_enters_stateful_rewrite() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS previous AS (lag(a)) SELECT previous + 1 FROM stream")
            .expect("named expression should enter stateful rewrite");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "(col_1) + 1");
        assert_eq!(select_stmt.stateful_mappings.len(), 1);
        assert_eq!(
            select_stmt.stateful_mappings[0]
                .spec
                .original_expr
                .to_string(),
            "lag(a)"
        );
    }

    #[test]
    fn test_rewrite_named_expression_enters_aggregate_rewrite() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS total AS (sum(a)) SELECT total FROM stream")
            .expect("named expression should enter aggregate rewrite");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "(col_1)");
        assert_eq!(select_stmt.aggregate_mappings.len(), 1);
        assert_eq!(
            select_stmt.aggregate_mappings["col_1"].to_string(),
            "sum(a)"
        );
    }

    #[test]
    fn test_named_expression_is_expanded_before_null_predicate_lowering() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS nullable AS (a IS NULL) SELECT nullable FROM stream")
            .expect("named expression should be expanded before null predicate lowering");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "(isnull(a))");
    }

    #[test]
    fn test_named_expression_is_expanded_before_analytic_function_rewrite() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "WITH NAMED EXPRESSIONS previous AS (lag(a) OVER (PARTITION BY k)) SELECT previous + 1 FROM stream",
            )
            .expect("named expression should be expanded before analytic rewrite");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "(col_1) + 1");
        assert_eq!(select_stmt.stateful_mappings.len(), 1);
        assert_eq!(
            select_stmt.stateful_mappings[0]
                .spec
                .original_expr
                .to_string(),
            "lag(a) OVER (PARTITION BY k)"
        );
        assert_eq!(
            select_stmt.stateful_mappings[0]
                .spec
                .partition_by
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["k"]
        );
    }

    #[test]
    fn test_rewrite_named_expression_does_not_expand_replacement_again() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("WITH NAMED EXPRESSIONS b AS (a + 1), c AS (b + 1) SELECT c FROM stream")
            .expect("named expression should be expanded once");

        assert_eq!(
            select_stmt.select_fields[0].expr.to_string(),
            "((a + 1) + 1)"
        );
    }

    #[test]
    fn test_rewrite_named_expression_without_environment_preserves_ast() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("SELECT a + 1 FROM stream")
            .expect("regular SQL should remain valid");

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "a + 1");
    }

    #[test]
    fn test_parse_select_with_alias() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a + b AS total");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);

        let field = &select_stmt.select_fields[0];
        assert_eq!(field.alias, Some("total".to_string()));
        assert_eq!(field.field_name, "total".to_string());
    }

    #[test]
    fn test_parse_multiple_fields() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b + c, CONCAT(name, 'test') AS full_name");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 3);

        // First field: a
        assert_eq!(select_stmt.select_fields[0].alias, None);
        assert_eq!(select_stmt.select_fields[0].field_name, "a");
        // Second field: b + c
        assert_eq!(select_stmt.select_fields[1].alias, None);
        assert_eq!(select_stmt.select_fields[1].field_name, "b + c");
        // Third field: CONCAT with alias
        assert_eq!(
            select_stmt.select_fields[2].alias,
            Some("full_name".to_string())
        );
        assert_eq!(select_stmt.select_fields[2].field_name, "full_name");
    }

    #[test]
    fn test_parse_quoted_identifier_projection_names_are_unquoted() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT `TDU_1.TMInletWaterTempFltSts`, `msg`.`sig`");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 2);
        assert_eq!(
            select_stmt.select_fields[0].field_name,
            "TDU_1.TMInletWaterTempFltSts"
        );
        assert_eq!(select_stmt.select_fields[1].field_name, "msg.sig");
        assert_eq!(
            select_stmt.select_fields[0].expr.to_string(),
            "`TDU_1.TMInletWaterTempFltSts`"
        );
    }

    #[test]
    fn test_parse_invalid_sql() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("INVALID SQL");

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_non_select() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("INSERT INTO table VALUES (1)");

        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Parse error"));
    }

    #[test]
    fn test_convenience_function() {
        let result = parse_sql("SELECT a * b");
        assert!(result.is_ok());

        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);
    }

    #[test]
    fn test_parse_lowers_standard_null_predicates() {
        let select_stmt =
            parse_sql("SELECT coalesce(a, b) IS NOT NULL AS has_value FROM s WHERE a IS NULL")
                .expect("standard NULL predicates should parse");

        assert_eq!(
            select_stmt.select_fields[0].expr.to_string(),
            "NOT isnull(coalesce(a, b))"
        );
        assert_eq!(select_stmt.select_fields[0].field_name, "has_value");
        assert_eq!(
            select_stmt
                .where_condition
                .as_ref()
                .expect("WHERE predicate")
                .to_string(),
            "isnull(a)"
        );
    }

    #[test]
    fn test_parse_order_by_basic() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a FROM t ORDER BY b DESC, c");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.order_by.len(), 2);
        assert_eq!(select_stmt.order_by[0].expr.to_string(), "b");
        assert_eq!(select_stmt.order_by[0].asc, false);
        assert_eq!(select_stmt.order_by[1].expr.to_string(), "c");
        assert_eq!(select_stmt.order_by[1].asc, true);
    }

    #[test]
    fn test_parse_order_by_nulls_first_rejected() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a FROM t ORDER BY b NULLS FIRST");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("NULLS FIRST/LAST in ORDER BY is not supported yet")
        );
    }

    #[test]
    fn test_parse_order_by_aggregate_rewrite() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a FROM t ORDER BY sum(b)");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.order_by.len(), 1);

        // Aggregate is rewritten to a placeholder column.
        assert_eq!(select_stmt.order_by[0].expr.to_string(), "col_1");
        assert_eq!(select_stmt.aggregate_mappings.len(), 1);
        assert_eq!(
            select_stmt
                .aggregate_mappings
                .get("col_1")
                .unwrap()
                .to_string(),
            "sum(b)"
        );
    }

    #[test]
    fn test_parse_order_by_stateful_rewrite() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a FROM t ORDER BY lag(a)");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.order_by.len(), 1);
        assert_eq!(select_stmt.order_by[0].expr.to_string(), "col_1");
        assert_eq!(select_stmt.stateful_mappings.len(), 1);
        assert_eq!(
            select_stmt
                .stateful_mappings
                .iter()
                .find(|entry| entry.output_column == "col_1")
                .unwrap()
                .spec
                .original_expr
                .to_string(),
            "lag(a)"
        );
    }

    #[test]
    fn test_parse_acc_rewrite() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT acc_sum(a) + 1 FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();

        assert_eq!(select_stmt.select_fields.len(), 1);
        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "col_1 + 1");
        assert_eq!(select_stmt.select_fields[0].alias, None);
        assert_eq!(select_stmt.acc_mappings.len(), 0);
        assert_eq!(select_stmt.stateful_mappings.len(), 1);

        let mapping = &select_stmt.stateful_mappings[0];
        assert_eq!(mapping.output_column, "col_1");
        assert_eq!(mapping.spec.func_name, "acc_sum");
        assert_eq!(
            mapping
                .spec
                .args
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        assert_eq!(mapping.spec.original_expr.to_string(), "acc_sum(a)");
    }

    #[test]
    fn test_parse_acc_reuses_same_expression() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT acc_count(a), acc_count(a) FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "col_1");
        assert_eq!(select_stmt.select_fields[1].expr.to_string(), "col_1");
        assert_eq!(select_stmt.acc_mappings.len(), 0);
        assert_eq!(select_stmt.stateful_mappings.len(), 1);
    }

    #[test]
    fn test_parse_acc_does_not_alias_unrelated_fields() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT acc_count(a), b FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();

        assert_eq!(select_stmt.select_fields[0].alias, None);
        assert_eq!(select_stmt.select_fields[1].alias, None);
        assert_eq!(select_stmt.select_fields[1].field_name, "b");
    }

    #[test]
    fn test_parse_acc_allows_multiple_distinct_calls() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT acc_count(a), acc_sum(b) FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.stateful_mappings.len(), 2);
        assert_eq!(select_stmt.stateful_mappings[0].spec.func_name, "acc_count");
        assert_eq!(select_stmt.stateful_mappings[1].spec.func_name, "acc_sum");
    }

    #[test]
    fn test_parse_acc_supports_filter_and_over_partition_by() {
        let parser = StreamSqlParser::new();
        let result =
            parser.parse("SELECT acc_sum(a) FILTER (WHERE flag) OVER (PARTITION BY k) FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.stateful_mappings.len(), 1);
        let mapping = &select_stmt.stateful_mappings[0];
        assert_eq!(mapping.spec.func_name, "acc_sum");
        assert_eq!(mapping.spec.when.as_ref().unwrap().to_string(), "flag");
        assert_eq!(mapping.spec.partition_by[0].to_string(), "k");
    }
}

#[cfg(test)]
mod source_info_tests {
    use super::*;
    use crate::Window;
    use crate::select_stmt::JoinConstraint as ParsedJoinConstraint;

    #[test]
    fn test_parse_select_with_single_table() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b FROM users");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.source_infos.len(), 1);

        let source = &select_stmt.source_infos[0];
        assert_eq!(source.name, "users");
        assert_eq!(source.alias, None);
    }

    #[test]
    fn test_parse_select_with_table_alias() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b FROM users AS u");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.source_infos.len(), 1);

        let source = &select_stmt.source_infos[0];
        assert_eq!(source.name, "users");
        assert_eq!(source.alias, Some("u".to_string()));
    }

    #[test]
    fn test_parse_select_with_multiple_tables() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b FROM users AS u, orders AS o");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.source_infos.len(), 2);

        assert_eq!(select_stmt.source_infos[0].name, "users");
        assert_eq!(select_stmt.source_infos[0].alias, Some("u".to_string()));
        assert_eq!(select_stmt.source_infos[1].name, "orders");
        assert_eq!(select_stmt.source_infos[1].alias, Some("o".to_string()));
    }

    #[test]
    fn test_parse_select_with_where_clause() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b FROM users WHERE a > 10");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.source_infos.len(), 1);
        assert_eq!(select_stmt.source_infos[0].name, "users");
        assert!(select_stmt.where_condition.is_some());
    }

    #[test]
    fn test_parse_inner_join_and_normalize_plain_join() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse("SELECT live.id, lookup.name FROM live JOIN lookup ON live.id = lookup.id")
            .unwrap();

        assert_eq!(select_stmt.source_infos.len(), 2);
        assert_eq!(select_stmt.source_infos[0].name, "live");
        assert_eq!(select_stmt.source_infos[0].alias, None);
        assert_eq!(select_stmt.source_infos[1].name, "lookup");
        assert_eq!(select_stmt.source_infos[1].alias, None);
        assert_eq!(select_stmt.joins.len(), 1);
        assert_eq!(select_stmt.joins[0].join_type, JoinType::Inner);
        assert_eq!(select_stmt.from.as_ref().unwrap().name, "live");
        assert_eq!(select_stmt.joins[0].relation.name, "lookup");
        assert!(matches!(
            select_stmt.joins[0].constraint,
            ParsedJoinConstraint::On(ref on) if on.to_string() == "live.id = lookup.id"
        ));
    }

    #[test]
    fn test_parse_inner_join_preserves_complete_on_expression() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "SELECT * FROM live INNER JOIN lookup ON live.id = lookup.id AND live.kind = lookup.kind",
            )
            .unwrap();

        assert_eq!(select_stmt.joins.len(), 1);
        assert!(matches!(
            select_stmt.joins[0].constraint,
            ParsedJoinConstraint::On(ref on)
                if on.to_string() == "live.id = lookup.id AND live.kind = lookup.kind"
        ));
    }

    #[test]
    fn test_parse_inner_join_chain_preserves_relation_order() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser
            .parse(
                "SELECT * FROM live JOIN table_one ON live.id = table_one.id JOIN stream_two ON live.id = stream_two.id",
            )
            .unwrap();

        assert_eq!(select_stmt.from.as_ref().unwrap().name, "live");
        assert_eq!(select_stmt.joins.len(), 2);
        assert_eq!(select_stmt.joins[0].relation.name, "table_one");
        assert_eq!(select_stmt.joins[1].relation.name, "stream_two");
        assert_eq!(select_stmt.joins[0].join_type, JoinType::Inner);
        assert_eq!(select_stmt.joins[1].join_type, JoinType::Inner);
    }

    #[test]
    fn test_parse_join_rejects_relation_aliases() {
        let parser = StreamSqlParser::new();

        let left_alias = parser
            .parse("SELECT * FROM live AS l JOIN lookup ON l.id = lookup.id")
            .unwrap_err();
        assert!(left_alias.contains("stream/table aliases are not supported for JOIN"));

        let right_alias = parser
            .parse("SELECT * FROM live JOIN lookup AS t ON live.id = t.id")
            .unwrap_err();
        assert!(right_alias.contains("stream/table aliases are not supported for JOIN"));
    }

    #[test]
    fn test_parse_join_rejects_unsupported_join_forms() {
        let parser = StreamSqlParser::new();

        let left_join = parser
            .parse("SELECT * FROM live LEFT JOIN lookup ON live.id = lookup.id")
            .unwrap();
        assert_eq!(left_join.joins[0].join_type, JoinType::Left);

        let using_join = parser
            .parse("SELECT * FROM live JOIN lookup USING (id)")
            .unwrap_err();
        assert!(using_join.contains("JOIN USING is not supported"));
    }

    #[test]
    fn test_parse_left_join_normalizes_left_outer_join() {
        let parser = StreamSqlParser::new();

        let left_join = parser
            .parse("SELECT * FROM live LEFT JOIN lookup ON live.id = lookup.id")
            .unwrap();
        let left_outer_join = parser
            .parse("SELECT * FROM live LEFT OUTER JOIN lookup ON live.id = lookup.id")
            .unwrap();

        assert_eq!(left_join.joins[0].join_type, JoinType::Left);
        assert_eq!(left_outer_join.joins[0].join_type, JoinType::Left);
        assert_eq!(
            left_join.joins[0].relation.name,
            left_outer_join.joins[0].relation.name
        );
        assert!(matches!(
            left_join.joins[0].constraint,
            ParsedJoinConstraint::On(ref on) if on.to_string() == "live.id = lookup.id"
        ));
        assert!(matches!(
            left_outer_join.joins[0].constraint,
            ParsedJoinConstraint::On(ref on) if on.to_string() == "live.id = lookup.id"
        ));
    }

    #[test]
    fn test_parse_non_join_query_keeps_join_list_empty() {
        let parser = StreamSqlParser::new();
        let select_stmt = parser.parse("SELECT a, b FROM users AS u").unwrap();

        assert_eq!(select_stmt.source_infos.len(), 1);
        assert_eq!(select_stmt.source_infos[0].alias, Some("u".to_string()));
        assert!(select_stmt.joins.is_empty());
    }

    #[test]
    fn parse_group_by_window() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT * FROM stream GROUP BY tumblingwindow('ss', 10)");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);
        assert!(select_stmt.window.is_some());

        match select_stmt.window {
            Some(Window::Tumbling {
                time_unit, length, ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(length, 10);
            }
            _ => panic!("Expected tumbling window"),
        }
    }

    #[test]
    fn parse_group_by_tumbling_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let sql =
            "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10) OVER (PARTITION BY k1, k2)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::Tumbling {
                time_unit,
                length,
                partition_by,
                ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(length, 10);
                assert_eq!(partition_by.len(), 2);
                assert_eq!(partition_by[0].to_string(), "k1");
                assert_eq!(partition_by[1].to_string(), "k2");
            }
            other => panic!("expected tumbling window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_count_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY countwindow(3) OVER (PARTITION BY k)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::Count {
                count,
                partition_by,
                ..
            }) => {
                assert_eq!(count, 3);
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
            }
            other => panic!("expected count window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_sliding_window() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT * FROM stream GROUP BY slidingwindow('ss', 10)");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);
        assert!(select_stmt.window.is_some());

        match select_stmt.window {
            Some(Window::Sliding {
                time_unit,
                lookback,
                lookahead,
                ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, None);
            }
            other => panic!("Expected sliding window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15) OVER (PARTITION BY k)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::Sliding {
                time_unit,
                lookback,
                lookahead,
                partition_by,
                ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, Some(15));
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
            }
            other => panic!("expected sliding window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_eos_window() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT sum(a) FROM history_table GROUP BY eoswindow()");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);
        assert_eq!(select_stmt.window, Some(Window::eos()));
    }

    #[test]
    fn reject_eos_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let result = parser
            .parse("SELECT sum(a) FROM history_table GROUP BY eoswindow() OVER (PARTITION BY k)");

        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("eoswindow does not support OVER"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn reject_window_over_order_by() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT * FROM stream GROUP BY countwindow(3) OVER (ORDER BY k)");

        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("countwindow OVER does not support ORDER BY"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn select_eos_window_function_does_not_set_group_window() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT eoswindow() FROM history_table");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert!(select_stmt.window.is_none());
        assert_eq!(select_stmt.select_fields.len(), 1);
        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "eoswindow()");
    }

    #[test]
    fn reject_multiple_windows() {
        let parser = StreamSqlParser::new();
        let result =
            parser.parse("SELECT * FROM stream GROUP BY tumblingwindow('ss', 10), countwindow(3)");

        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("Only one window function is allowed"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn parse_group_by_with_column_and_window() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10), b";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        // window should be parsed
        match select_stmt.window {
            Some(Window::Tumbling {
                time_unit, length, ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(length, 10);
            }
            other => panic!("expected tumbling window, got {:?}", other),
        }

        // group_by_exprs should only contain the column, not the window function
        assert_eq!(select_stmt.group_by_exprs.len(), 1);
        let expr = &select_stmt.group_by_exprs[0];
        match expr {
            Expr::Identifier(ident) => assert_eq!(ident.to_string(), "b"),
            other => panic!("expected group by identifier `b`, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_with_column_and_sliding_window() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15), b";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                time_unit,
                lookback,
                lookahead,
                ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, Some(15));
            }
            other => panic!("expected sliding window, got {:?}", other),
        }

        assert_eq!(select_stmt.group_by_exprs.len(), 1);
        let expr = &select_stmt.group_by_exprs[0];
        match expr {
            Expr::Identifier(ident) => assert_eq!(ident.to_string(), "b"),
            other => panic!("expected group by identifier `b`, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_state_window() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY statewindow(a > 0, b = 1)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        assert!(select_stmt.window.is_some());
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::State {
                open,
                emit,
                partition_by,
                ..
            }) => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
                assert!(partition_by.is_empty());
            }
            other => panic!("expected state window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_with_column_and_state_window() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY statewindow(a > 0, b = 1), c";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::State {
                open,
                emit,
                partition_by,
                ..
            }) => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
                assert!(partition_by.is_empty());
            }
            other => panic!("expected state window, got {:?}", other),
        }

        assert_eq!(select_stmt.group_by_exprs.len(), 1);
        let expr = &select_stmt.group_by_exprs[0];
        match expr {
            Expr::Identifier(ident) => assert_eq!(ident.to_string(), "c"),
            other => panic!("expected group by identifier `c`, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_state_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let sql =
            "SELECT * FROM stream GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY k1, k2)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        assert!(select_stmt.window.is_some());
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::State {
                open,
                emit,
                partition_by,
                ..
            }) => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
                assert_eq!(partition_by.len(), 2);
                assert_eq!(partition_by[0].to_string(), "k1");
                assert_eq!(partition_by[1].to_string(), "k2");
            }
            other => panic!("expected state window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN a > 1)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert!(select_stmt.group_by_exprs.is_empty());

        match select_stmt.window {
            Some(Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            }) => {
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, None);
                assert!(partition_by.is_empty());
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
            }
            other => panic!("expected sliding window with trigger condition, got {other:?}"),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when_partition_by() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN a > 1 PARTITION BY k)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert!(select_stmt.group_by_exprs.is_empty());

        match select_stmt.window {
            Some(Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            }) => {
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, None);
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
            }
            other => panic!(
                "expected sliding window with trigger condition and partition, got {other:?}"
            ),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when_complex_expr() {
        let parser = StreamSqlParser::new();
        let sql =
            "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN a > 1 AND b = 0)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                trigger_condition, ..
            }) => {
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1 AND b = 0".to_string()),
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when_last_hit_time() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN a > last_hit_time_unix_ms())";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                trigger_condition, ..
            }) => {
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > last_hit_time_unix_ms()".to_string()),
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when_no_expr() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN)";
        let result = parser.parse(sql);

        assert!(result.is_err(), "expected error for empty WHEN expression");
        assert!(result.unwrap_err().contains("requires an expression"));
    }

    #[test]
    fn parse_sliding_window_over_when_with_lookahead() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15) OVER (WHEN a > 1 PARTITION BY k1, k2)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            }) => {
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, Some(15));
                assert_eq!(partition_by.len(), 2);
                assert_eq!(partition_by[0].to_string(), "k1");
                assert_eq!(partition_by[1].to_string(), "k2");
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
            }
            other => panic!(
                "expected sliding window with lookahead, trigger, and partition, got {other:?}"
            ),
        }
    }

    #[test]
    fn parse_sliding_window_with_filter_and_over_when() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) FILTER (WHERE x > 0) OVER (WHEN a > 1)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                trigger_condition,
                filter,
                ..
            }) => {
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
                assert_eq!(
                    filter.as_ref().map(|e| e.to_string()),
                    Some("x > 0".to_string()),
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_partition_by_without_when() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15) OVER (PARTITION BY k)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            }) => {
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, Some(15));
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
                assert!(trigger_condition.is_none());
            }
            other => panic!("expected sliding window with partition, got {:?}", other),
        }
    }
}
