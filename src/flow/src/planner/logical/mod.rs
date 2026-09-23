use crate::catalog::{StreamDefinition, TableDefinition, TableProps};
use crate::pipeline::SourceInputConfig;
use parser::window as parser_window;
use parser::{JoinConstraint, JoinType, SelectStmt};
use sqlparser::ast::{visit_expressions_mut, Expr, Value as SqlValue};
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use datatypes::ConcreteDatatype;

pub mod aggregation;
pub mod compute;
pub mod datasource;
pub mod filter;
pub mod lookup_join;
pub mod order;
pub mod project;
pub mod sink;
pub mod stateful_function;
pub mod table_scan;
pub mod tail;
pub mod window;

use crate::planner::sink::PipelineSink;
pub use aggregation::Aggregation;
pub use compute::Compute;
pub use datasource::DataSource;
pub use filter::Filter;
pub use lookup_join::{
    extract_lookup_key_bindings, LogicalColumnRef, LookupJoin, LookupJoinSpec, LookupKeyBinding,
};
pub use order::{Order, OrderItem};
pub use project::Project;
pub use sink::DataSinkPlan;
pub use stateful_function::{LogicalStatefulCall, StatefulFunctionPlan};
pub use table_scan::{TableScan, TableScanRequest, TableScanSpec};
pub use tail::TailPlan;
pub use window::{LogicalWindow, LogicalWindowSpec, TimeUnit};

#[derive(Debug, Clone)]
pub struct BaseLogicalPlan {
    pub index: i64,
    pub children: Vec<Arc<LogicalPlan>>,
}

impl BaseLogicalPlan {
    pub fn new(children: Vec<Arc<LogicalPlan>>, index: i64) -> Self {
        Self { children, index }
    }

    pub fn children(&self) -> &[Arc<LogicalPlan>] {
        &self.children
    }

    pub fn index(&self) -> i64 {
        self.index
    }
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    DataSource(DataSource),
    TableScan(TableScan),
    StatefulFunction(StatefulFunctionPlan),
    Filter(Filter),
    LookupJoin(LookupJoin),
    Aggregation(Aggregation),
    Compute(Compute),
    Order(Order),
    Project(Project),
    DataSink(DataSinkPlan),
    Tail(TailPlan),
    Window(LogicalWindow),
}

impl LogicalPlan {
    pub fn children(&self) -> &[Arc<LogicalPlan>] {
        match self {
            LogicalPlan::DataSource(plan) => plan.base.children(),
            LogicalPlan::TableScan(plan) => plan.base.children(),
            LogicalPlan::StatefulFunction(plan) => plan.base.children(),
            LogicalPlan::Filter(plan) => plan.base.children(),
            LogicalPlan::LookupJoin(plan) => plan.base.children(),
            LogicalPlan::Aggregation(plan) => plan.base.children(),
            LogicalPlan::Compute(plan) => plan.base.children(),
            LogicalPlan::Order(plan) => plan.base.children(),
            LogicalPlan::Project(plan) => plan.base.children(),
            LogicalPlan::DataSink(plan) => plan.base.children(),
            LogicalPlan::Tail(plan) => plan.base.children(),
            LogicalPlan::Window(plan) => plan.base.children(),
        }
    }

    pub fn get_plan_type(&self) -> &str {
        match self {
            LogicalPlan::DataSource(_) => "DataSource",
            LogicalPlan::TableScan(_) => "TableScan",
            LogicalPlan::StatefulFunction(_) => "StatefulFunction",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::LookupJoin(_) => "LookupJoin",
            LogicalPlan::Aggregation(_) => "Aggregation",
            LogicalPlan::Compute(_) => "Compute",
            LogicalPlan::Order(_) => "Order",
            LogicalPlan::Project(_) => "Project",
            LogicalPlan::DataSink(_) => "DataSink",
            LogicalPlan::Tail(_) => "Tail",
            LogicalPlan::Window(_) => "Window",
        }
    }

    pub fn get_plan_index(&self) -> i64 {
        match self {
            LogicalPlan::DataSource(plan) => plan.base.index(),
            LogicalPlan::TableScan(plan) => plan.base.index(),
            LogicalPlan::StatefulFunction(plan) => plan.base.index(),
            LogicalPlan::Filter(plan) => plan.base.index(),
            LogicalPlan::LookupJoin(plan) => plan.base.index(),
            LogicalPlan::Aggregation(plan) => plan.base.index(),
            LogicalPlan::Compute(plan) => plan.base.index(),
            LogicalPlan::Order(plan) => plan.base.index(),
            LogicalPlan::Project(plan) => plan.base.index(),
            LogicalPlan::DataSink(plan) => plan.base.index(),
            LogicalPlan::Tail(plan) => plan.base.index(),
            LogicalPlan::Window(plan) => plan.base.index(),
        }
    }

    /// Get the plan name in format: {{plan_type}}_{{plan_index}}
    pub fn get_plan_name(&self) -> String {
        format!("{}_{}", self.get_plan_type(), self.get_plan_index())
    }

    /// Print logical topology (similar to PhysicalPlan::print_topology)
    pub fn print_topology(&self, indent: usize) {
        let spacing = "  ".repeat(indent);
        println!(
            "{}{} (index: {})",
            spacing,
            self.get_plan_type(),
            self.get_plan_index()
        );

        for child in self.children() {
            child.print_topology(indent + 1);
        }
    }
}

/// Create a LogicalPlan from a SelectStmt
///
/// The plan structure will be:
/// - DataSource(s) (from SelectStmt::source_infos, one per source)
/// - StatefulFunction (from SelectStmt::stateful_mappings, if present) - takes DataSources as children
/// - Window (from SelectStmt::window, if present) - takes StatefulFunction/DataSources as children
/// - Aggregation (from SelectStmt::aggregate_mappings, if present) - takes Window or DataSources as children
/// - Filter (from SelectStmt::having, if present) - takes Aggregation as children
/// - Filter (from SelectStmt::where_condition, if present) - takes HAVING/Aggregation/Window/DataSources as children
/// - Order (from SelectStmt::order_by, if present) - takes Filter/HAVING/Aggregation/Window/DataSources as children
/// - Project (from SelectStmt::select_fields) - takes Filter, Aggregation, Window, or DataSources as children
///
/// # Arguments
///
/// * `select_stmt` - The parsed SELECT statement
///
/// # Returns
///
/// Returns the root LogicalPlan node
pub fn create_logical_plan(
    select_stmt: SelectStmt,
    sinks: Vec<PipelineSink>,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
) -> Result<Arc<LogicalPlan>, String> {
    create_logical_plan_with_table_defs_and_source_inputs(
        select_stmt,
        sinks,
        stream_defs,
        &HashMap::new(),
        &HashMap::new(),
    )
}

pub fn create_logical_plan_with_source_inputs(
    select_stmt: SelectStmt,
    sinks: Vec<PipelineSink>,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    source_inputs: &HashMap<String, SourceInputConfig>,
) -> Result<Arc<LogicalPlan>, String> {
    create_logical_plan_with_table_defs_and_source_inputs(
        select_stmt,
        sinks,
        stream_defs,
        &HashMap::new(),
        source_inputs,
    )
}

pub fn create_logical_plan_with_table_defs_and_source_inputs(
    select_stmt: SelectStmt,
    sinks: Vec<PipelineSink>,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
    source_inputs: &HashMap<String, SourceInputConfig>,
) -> Result<Arc<LogicalPlan>, String> {
    let mut select_stmt = select_stmt;

    bind_schema_version(&mut select_stmt, stream_defs, table_defs)?;
    validate_group_by_requires_aggregates(&select_stmt)?;
    validate_window_metadata_function_placement(&select_stmt)?;
    validate_having_constraints(&select_stmt)?;
    validate_last_hit_time_unix_ms_placement(&select_stmt)?;
    validate_last_agg_hit_count_placement(&select_stmt)?;
    validate_relation_aliases_unsupported(&select_stmt)?;
    validate_select_alias_names(&select_stmt, stream_defs, table_defs)?;
    validate_aggregation_projection(&select_stmt)?;
    validate_order_by_constraints(&select_stmt)?;
    validate_expression_types(&select_stmt, stream_defs, table_defs)?;
    validate_window_filter_constraints(&select_stmt)?;

    let start_index = 0i64;
    let mut current_index = start_index;

    // 1. Create DataSource(s) from source_infos
    if select_stmt.source_infos.is_empty() {
        return Err("No data source found in SELECT statement".to_string());
    }

    let mut current_plans: Vec<Arc<LogicalPlan>> = Vec::new();
    let table_sources = select_stmt
        .source_infos
        .iter()
        .filter(|source| table_defs.contains_key(&source.name))
        .count();
    if !select_stmt.joins.is_empty() {
        let from = select_stmt
            .from
            .as_ref()
            .ok_or_else(|| "lookup join is missing its FROM relation".to_string())?;
        let stream_sources = select_stmt
            .source_infos
            .iter()
            .filter(|source| stream_defs.contains_key(&source.name))
            .count();
        if stream_sources != 1 {
            return Err("stream-table lookup join requires exactly one stream source".to_string());
        }
        let stream = stream_defs.get(&from.name).ok_or_else(|| {
            format!(
                "lookup join FROM relation `{}` must be a registered stream",
                from.name
            )
        })?;
        let datasource = DataSource::new(
            from.name.clone(),
            None,
            stream.decoder().clone(),
            current_index,
            stream.schema(),
            stream.eventtime().cloned(),
            stream.sampler().cloned(),
        )
        .with_stream_type(stream.stream_type());
        let mut datasource = datasource;
        datasource.source_input = source_inputs.get(&from.name).cloned().unwrap_or_default();
        let mut current_plan = Arc::new(LogicalPlan::DataSource(datasource));
        current_index += 1;
        for join in &select_stmt.joins {
            match join.join_type {
                JoinType::Inner | JoinType::Left => {}
                _ => {
                    return Err(
                        "stream-table lookup join supports only INNER and LEFT JOIN".to_string()
                    );
                }
            }
            let JoinConstraint::On(on) = &join.constraint else {
                return Err("stream-table lookup join requires an ON clause".to_string());
            };
            let table = table_defs.get(&join.relation.name).ok_or_else(|| {
                format!(
                    "lookup join relation `{}` must be a registered table",
                    join.relation.name
                )
            })?;
            if table.table_type() != crate::catalog::TableType::MemoryMock {
                return Err("only memory tables support lookup joins currently".to_string());
            }
            let Some(lookup_capability) = table.capabilities().lookup.as_ref() else {
                return Err(format!(
                    "table `{}` does not advertise lookup capability",
                    table.id()
                ));
            };
            let key_bindings = extract_lookup_key_bindings(
                on,
                &from.name,
                stream.schema().as_ref(),
                &join.relation.name,
                table.schema().as_ref(),
            )?;
            for binding in &key_bindings {
                let supported = lookup_capability
                    .lookup_key_sets
                    .iter()
                    .any(|key_set| key_set.len() == 1 && key_set[0] == binding.table_column.column);
                if !supported {
                    return Err(format!(
                        "table `{}` does not advertise an index for lookup column `{}`",
                        table.id(),
                        binding.table_column.column
                    ));
                }
            }
            let table_projection = required_lookup_table_projection(
                &select_stmt,
                &join.relation.name,
                table.schema().as_ref(),
                &key_bindings,
            );
            let lookup_join = LookupJoin::new(
                LookupJoinSpec {
                    join_type: join.join_type,
                    table_name: join.relation.name.clone(),
                    table_schema: table.schema(),
                    table_projection,
                    key_bindings,
                },
                current_plan,
                current_index,
            );
            current_plan = Arc::new(LogicalPlan::LookupJoin(lookup_join));
            current_index += 1;
        }
        current_plans.push(current_plan);
    } else if table_sources > 0 {
        validate_eos_window_source_constraints(&select_stmt, table_sources)?;
        if select_stmt.source_infos.len() != 1 {
            return Err("table scan currently supports exactly one relation".to_string());
        }
        if !select_stmt.aggregate_mappings.is_empty() {
            match select_stmt.window.as_ref() {
                Some(parser_window::Window::Eos { .. }) => {}
                Some(_) => {
                    return Err(
                        "table scan aggregate queries only support GROUP BY eoswindow()"
                            .to_string(),
                    );
                }
                None => {
                    return Err(
                        "aggregate functions over table scans require GROUP BY eoswindow()"
                            .to_string(),
                    );
                }
            }
        } else if select_stmt.window.is_some() {
            return Err("windowed table scans require aggregate functions".to_string());
        }
        let source_info = &select_stmt.source_infos[0];
        let definition = table_defs.get(&source_info.name).ok_or_else(|| {
            format!(
                "table {} missing catalog definition for logical planning",
                source_info.name
            )
        })?;
        let request = TableScanRequest {
            batch_size: match definition.props() {
                TableProps::History(props) => props.batch_size,
                TableProps::MemoryMock(_) => None,
            },
        };
        let table_scan = TableScan::new(
            TableScanSpec {
                table_name: source_info.name.clone(),
                table_type: definition.table_type(),
                alias: None,
                decoder: definition.decoder().clone(),
                schema: definition.schema(),
                props: definition.props().clone(),
                request,
            },
            current_index,
        );
        current_plans.push(Arc::new(LogicalPlan::TableScan(table_scan)));
        current_index += 1;
    } else {
        validate_eos_window_source_constraints(&select_stmt, table_sources)?;
        for source_info in &select_stmt.source_infos {
            let definition = stream_defs.get(&source_info.name).ok_or_else(|| {
                format!(
                    "stream {} missing catalog definition for logical planning",
                    source_info.name
                )
            })?;
            let schema = definition.schema();
            let datasource = DataSource::new(
                source_info.name.clone(),
                None,
                definition.decoder().clone(),
                current_index,
                schema,
                definition.eventtime().cloned(),
                definition.sampler().cloned(),
            )
            .with_stream_type(definition.stream_type());
            let mut datasource = datasource;
            datasource.source_input = source_inputs
                .get(&source_info.name)
                .cloned()
                .unwrap_or_default();
            current_plans.push(Arc::new(LogicalPlan::DataSource(datasource)));
            current_index += 1;
        }
    }

    // 2. Create StatefulFunctionPlan if stateful mappings exist.
    if !select_stmt.stateful_mappings.is_empty() {
        let calls = select_stmt
            .stateful_mappings
            .iter()
            .map(|entry| stateful_function::LogicalStatefulCall {
                output_column: entry.output_column.clone(),
                spec: entry.spec.clone(),
            })
            .collect::<Vec<_>>();
        let stateful =
            stateful_function::StatefulFunctionPlan::new(calls, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::StatefulFunction(stateful))];
        current_index += 1;
    }

    // 3. Create Window from window if present
    if let Some(window) = select_stmt.window.clone() {
        let (window, window_filter) = split_window_filter(window);
        if let Some(filter_expr) = window_filter {
            let filter = Filter::new(filter_expr, current_plans, current_index);
            current_plans = vec![Arc::new(LogicalPlan::Filter(filter))];
            current_index += 1;
        }
        let spec = convert_window_spec(window)?;
        let window_plan = LogicalWindow::new(spec, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Window(window_plan))];
        current_index += 1;
    }

    // 4. Create Aggregation if aggregate mappings exist
    if !select_stmt.aggregate_mappings.is_empty() {
        let aggregation = aggregation::Aggregation::new(
            select_stmt.aggregate_mappings.clone(),
            select_stmt.group_by_exprs.clone(),
            current_plans,
            current_index,
        );
        current_plans = vec![Arc::new(LogicalPlan::Aggregation(aggregation))];
        current_index += 1;
    }

    // 5. Create Filter from HAVING if present (post-aggregation)
    if let Some(having_expr) = select_stmt.having.clone() {
        let filter = Filter::new(having_expr, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Filter(filter))];
        current_index += 1;
    }

    // 6. Create Filter from where_condition if present
    if let Some(where_expr) = select_stmt.where_condition.clone() {
        let filter = Filter::new(where_expr, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Filter(filter))];
        current_index += 1;
    }

    // 7. Create Order from order_by (if present)
    if !select_stmt.order_by.is_empty() {
        let items = select_stmt
            .order_by
            .iter()
            .map(|item| OrderItem {
                expr: item.expr.clone(),
                asc: item.asc,
            })
            .collect();
        let order = Order::new(items, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Order(order))];
        current_index += 1;
    }

    // 8. Create Project from select_fields
    let mut project_fields = Vec::new();
    for select_field in select_stmt.select_fields.iter() {
        let field_name = select_field
            .alias
            .clone()
            .unwrap_or_else(|| select_field.field_name.clone());
        project_fields.push(project::ProjectField {
            field_name,
            expr: select_field.expr.clone(), // Keep original sqlparser expression
        });
    }

    let project = Project::new(project_fields, current_plans, current_index);
    let base = Arc::new(LogicalPlan::Project(project));

    let root = if sinks.is_empty() {
        base
    } else {
        // Always create TailPlan for both single and multiple sinks
        // This ensures consistent PhysicalResultCollect creation in physical plan
        let next_index = max_plan_index(&base) + 1;
        let mut sink_children = Vec::new();
        let sink_count = sinks.len();

        for (idx, sink) in sinks.into_iter().enumerate() {
            let sink_index = next_index + idx as i64;
            let sink_plan = DataSinkPlan::new(Arc::clone(&base), sink_index, sink);
            sink_children.push(Arc::new(LogicalPlan::DataSink(sink_plan)));
        }

        // Create TailPlan to hold sink children (1 or more)
        let tail_index = next_index + sink_count as i64;
        let tail_plan = TailPlan::new(sink_children, tail_index);
        Arc::new(LogicalPlan::Tail(tail_plan))
    };

    verify_logical_plan(root.as_ref())?;
    Ok(root)
}

fn bind_schema_version(
    select_stmt: &mut SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
) -> Result<(), String> {
    fn bind_expr(
        expr: &mut Expr,
        source_names: &[String],
        stream_defs: &HashMap<String, Arc<StreamDefinition>>,
        table_defs: &HashMap<String, Arc<TableDefinition>>,
    ) -> Result<(), String> {
        match visit_expressions_mut(expr, |expr| {
            let Expr::Function(function) = expr else {
                return ControlFlow::Continue(());
            };
            if !function
                .name
                .to_string()
                .eq_ignore_ascii_case("schema_version")
            {
                return ControlFlow::Continue(());
            }

            if !function.args.is_empty() {
                return ControlFlow::Break("schema_version() takes zero arguments".to_string());
            }
            if function.filter.is_some() {
                return ControlFlow::Break("schema_version() does not support FILTER".to_string());
            }
            if function.over.is_some() {
                return ControlFlow::Break("schema_version() does not support OVER".to_string());
            }
            if !function.order_by.is_empty() {
                return ControlFlow::Break(
                    "schema_version() does not support ORDER BY".to_string(),
                );
            }
            if function.distinct || function.null_treatment.is_some() || function.special {
                return ControlFlow::Break(
                    "schema_version() does not support function modifiers".to_string(),
                );
            }

            let [source_name] = source_names else {
                return ControlFlow::Break(
                    "schema_version() requires exactly one stream source".to_string(),
                );
            };
            if table_defs.contains_key(source_name) {
                return ControlFlow::Break(
                    "schema_version() is not supported for table sources".to_string(),
                );
            }
            let Some(stream) = stream_defs.get(source_name) else {
                return ControlFlow::Break(format!(
                    "stream {source_name} missing catalog definition for schema_version()"
                ));
            };
            let Some(schema_version) = stream.schema_version() else {
                return ControlFlow::Break(
                    "schema_version() requires a named schema reference".to_string(),
                );
            };
            let Ok(schema_version) = i64::try_from(schema_version) else {
                return ControlFlow::Break(format!(
                    "schema version {schema_version} exceeds the SQL integer range"
                ));
            };
            *expr = Expr::Value(SqlValue::Number(schema_version.to_string(), false));
            ControlFlow::Continue(())
        }) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(err) => Err(err),
        }
    }

    let source_names = select_stmt
        .source_infos
        .iter()
        .map(|source| source.name.clone())
        .collect::<Vec<_>>();
    let bind = |expr: &mut Expr| bind_expr(expr, &source_names, stream_defs, table_defs);

    for field in &mut select_stmt.select_fields {
        bind(&mut field.expr)?;
    }
    if let Some(expr) = &mut select_stmt.where_condition {
        bind(expr)?;
    }
    if let Some(expr) = &mut select_stmt.having {
        bind(expr)?;
    }
    for expr in &mut select_stmt.group_by_exprs {
        bind(expr)?;
    }
    for item in &mut select_stmt.order_by {
        bind(&mut item.expr)?;
    }
    for expr in select_stmt.aggregate_mappings.values_mut() {
        bind(expr)?;
    }
    for entry in &mut select_stmt.stateful_mappings {
        for expr in &mut entry.spec.args {
            bind(expr)?;
        }
        if let Some(expr) = &mut entry.spec.when {
            bind(expr)?;
        }
        for expr in &mut entry.spec.partition_by {
            bind(expr)?;
        }
        bind(&mut entry.spec.original_expr)?;
    }
    for entry in &mut select_stmt.acc_mappings {
        for expr in &mut entry.spec.args {
            bind(expr)?;
        }
        bind(&mut entry.spec.original_expr)?;
    }
    if let Some(window) = &mut select_stmt.window {
        match window {
            parser_window::Window::Tumbling {
                partition_by,
                filter,
                ..
            }
            | parser_window::Window::Count {
                partition_by,
                filter,
                ..
            } => {
                for expr in partition_by {
                    bind(expr)?;
                }
                if let Some(expr) = filter {
                    bind(expr)?;
                }
            }
            parser_window::Window::Sliding {
                partition_by,
                trigger_condition,
                filter,
                ..
            } => {
                for expr in partition_by {
                    bind(expr)?;
                }
                if let Some(expr) = trigger_condition {
                    bind(expr)?;
                }
                if let Some(expr) = filter {
                    bind(expr)?;
                }
            }
            parser_window::Window::State {
                open,
                emit,
                partition_by,
                filter,
            } => {
                bind(open)?;
                bind(emit)?;
                for expr in partition_by {
                    bind(expr)?;
                }
                if let Some(expr) = filter {
                    bind(expr)?;
                }
            }
            parser_window::Window::Eos { filter } => {
                if let Some(expr) = filter {
                    bind(expr)?;
                }
            }
        }
    }

    Ok(())
}

pub fn verify_logical_plan(plan: &LogicalPlan) -> Result<(), String> {
    fn verify_node(plan: &LogicalPlan) -> Result<(), String> {
        match plan {
            LogicalPlan::Project(project) => {
                let mut seen =
                    std::collections::HashSet::<&str>::with_capacity(project.fields.len());
                for field in &project.fields {
                    if !seen.insert(field.field_name.as_str()) {
                        return Err(format!(
                            "duplicate project field name `{}` in {}",
                            field.field_name,
                            plan.get_plan_name()
                        ));
                    }
                }
            }
            LogicalPlan::Compute(compute) => {
                let mut seen =
                    std::collections::HashSet::<&str>::with_capacity(compute.fields.len());
                for field in &compute.fields {
                    if !seen.insert(field.field_name.as_str()) {
                        return Err(format!(
                            "duplicate compute field name `{}` in {}",
                            field.field_name,
                            plan.get_plan_name()
                        ));
                    }
                }
            }
            _ => {}
        }

        for child in plan.children() {
            verify_node(child.as_ref())?;
        }
        Ok(())
    }

    verify_node(plan)
}

#[derive(Debug, Clone)]
struct SourceSchemaEntry {
    source_name: String,
    column_lookup: HashMap<String, ConcreteDatatype>,
}

fn validate_expression_types(
    select_stmt: &SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
) -> Result<(), String> {
    let mut sources = Vec::new();
    for source_info in &select_stmt.source_infos {
        let Some(schema) = relation_schema(&source_info.name, stream_defs, table_defs) else {
            continue;
        };
        let mut column_lookup = HashMap::with_capacity(schema.column_schemas().len());
        for col in schema.column_schemas() {
            column_lookup.insert(col.name.clone(), col.data_type.clone());
        }

        sources.push(SourceSchemaEntry {
            source_name: source_info.name.clone(),
            column_lookup,
        });
    }

    for field in &select_stmt.select_fields {
        validate_expr_against_sources(&field.expr, &sources)?;
    }
    if let Some(expr) = &select_stmt.where_condition {
        validate_expr_against_sources(expr, &sources)?;
    }
    if let Some(expr) = &select_stmt.having {
        validate_expr_against_sources(expr, &sources)?;
    }
    for expr in &select_stmt.group_by_exprs {
        validate_expr_against_sources(expr, &sources)?;
    }
    if let Some(window) = select_stmt.window.as_ref() {
        if let Some(filter) = window.filter() {
            validate_expr_against_sources(filter, &sources)?;
        }
        for expr in parser_window_expression_inputs(window) {
            validate_expr_against_sources(expr, &sources)?;
        }
    }
    for item in &select_stmt.order_by {
        validate_expr_against_sources(&item.expr, &sources)?;
    }

    Ok(())
}

fn parser_window_expression_inputs(window: &parser_window::Window) -> Vec<&Expr> {
    match window {
        parser_window::Window::Tumbling { partition_by, .. }
        | parser_window::Window::Count { partition_by, .. }
        | parser_window::Window::Sliding { partition_by, .. } => partition_by.iter().collect(),
        parser_window::Window::State {
            open,
            emit,
            partition_by,
            ..
        } => {
            let mut exprs = Vec::with_capacity(2 + partition_by.len());
            exprs.push(open.as_ref());
            exprs.push(emit.as_ref());
            exprs.extend(partition_by);
            exprs
        }
        parser_window::Window::Eos { .. } => Vec::new(),
    }
}

fn required_lookup_table_projection(
    select_stmt: &SelectStmt,
    table_name: &str,
    table_schema: &datatypes::Schema,
    key_bindings: &[LookupKeyBinding],
) -> Vec<String> {
    let mut required = HashSet::new();
    let mut all_columns = false;

    fn collect_expr_columns(
        expr: &Expr,
        table_name: &str,
        required: &mut HashSet<String>,
        all_columns: &mut bool,
    ) {
        let mut expr = expr.clone();
        let _ = visit_expressions_mut(&mut expr, |node| {
            if let Expr::CompoundIdentifier(idents) = node {
                if let (Some(relation), Some(column)) = (idents.first(), idents.last()) {
                    if relation.value == table_name {
                        if column.value == "*" {
                            *all_columns = true;
                        } else {
                            required.insert(column.value.clone());
                        }
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }

    for field in &select_stmt.select_fields {
        match &field.expr {
            Expr::Identifier(identifier) if identifier.value == "*" => all_columns = true,
            Expr::CompoundIdentifier(idents)
                if idents
                    .last()
                    .is_some_and(|identifier| identifier.value == "*")
                    && idents
                        .first()
                        .is_some_and(|identifier| identifier.value == table_name) =>
            {
                all_columns = true;
            }
            expr => collect_expr_columns(expr, table_name, &mut required, &mut all_columns),
        }
    }

    if let Some(expr) = &select_stmt.where_condition {
        collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
    }
    if let Some(expr) = &select_stmt.having {
        collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
    }
    for expr in &select_stmt.group_by_exprs {
        collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
    }
    for item in &select_stmt.order_by {
        collect_expr_columns(&item.expr, table_name, &mut required, &mut all_columns);
    }
    for expr in select_stmt.aggregate_mappings.values() {
        collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
    }
    for mapping in &select_stmt.stateful_mappings {
        collect_expr_columns(
            &mapping.spec.original_expr,
            table_name,
            &mut required,
            &mut all_columns,
        );
        for expr in &mapping.spec.args {
            collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
        }
        if let Some(expr) = &mapping.spec.when {
            collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
        }
        for expr in &mapping.spec.partition_by {
            collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
        }
    }
    for mapping in &select_stmt.acc_mappings {
        collect_expr_columns(
            &mapping.spec.original_expr,
            table_name,
            &mut required,
            &mut all_columns,
        );
    }
    if let Some(window) = &select_stmt.window {
        if let Some(expr) = window.filter() {
            collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
        }
        for expr in parser_window_expression_inputs(window) {
            collect_expr_columns(expr, table_name, &mut required, &mut all_columns);
        }
    }

    for binding in key_bindings {
        required.insert(binding.table_column.column.clone());
    }

    table_schema
        .column_schemas()
        .iter()
        .filter(|column| all_columns || required.contains(&column.name))
        .map(|column| column.name.clone())
        .collect()
}

fn validate_select_alias_names(
    select_stmt: &SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
) -> Result<(), String> {
    use crate::expr::internal_columns::is_reserved_user_name;
    use std::collections::HashSet;

    // Collect all source schemas to keep them alive.
    let mut schemas = Vec::with_capacity(select_stmt.source_infos.len());
    for source_info in &select_stmt.source_infos {
        if let Some(schema) = relation_schema(&source_info.name, stream_defs, table_defs) {
            schemas.push(schema);
        }
    }

    // Collect all source column names; alias must not collide with any of them.
    let mut source_column_names = HashSet::<&str>::new();
    for schema in &schemas {
        for col in schema.column_schemas() {
            source_column_names.insert(col.name.as_str());
        }
    }

    for name in &select_stmt.named_expression_names {
        if source_column_names.contains(name.as_str()) {
            return Err(format!(
                "named expression `{name}` collides with an input column name"
            ));
        }
    }

    let mut seen_aliases = HashSet::<String>::new();
    for field in &select_stmt.select_fields {
        let Some(alias) = field.alias.as_ref() else {
            continue;
        };

        if is_reserved_user_name(alias.as_str()) {
            return Err(format!("SELECT alias `{}` is reserved", alias));
        }

        if source_column_names.contains(alias.as_str()) {
            return Err(format!(
                "SELECT alias `{}` collides with an input column name",
                alias
            ));
        }

        if !seen_aliases.insert(alias.clone()) {
            return Err(format!("duplicate SELECT alias `{}`", alias));
        }
    }

    Ok(())
}

fn validate_relation_aliases_unsupported(select_stmt: &SelectStmt) -> Result<(), String> {
    for source_info in &select_stmt.source_infos {
        if let Some(alias) = source_info.alias.as_ref() {
            return Err(format!(
                "relation alias `{alias}` for `{}` is not supported yet",
                source_info.name
            ));
        }
    }
    Ok(())
}

fn relation_schema(
    name: &str,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
) -> Option<Arc<datatypes::Schema>> {
    stream_defs
        .get(name)
        .map(|definition| definition.schema())
        .or_else(|| table_defs.get(name).map(|definition| definition.schema()))
}

fn expr_without_outer_nested(expr: &sqlparser::ast::Expr) -> &sqlparser::ast::Expr {
    use sqlparser::ast::Expr;

    let mut current = expr;
    while let Expr::Nested(inner) = current {
        current = inner.as_ref();
    }
    current
}

fn validate_expr_against_sources(
    expr: &sqlparser::ast::Expr,
    sources: &[SourceSchemaEntry],
) -> Result<(), String> {
    use sqlparser::ast::{Expr, JsonOperator};

    fn extract_const_int(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(sqlparser::ast::Value::Number(num, _)) => num.parse::<i64>().ok(),
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Minus,
                expr,
            } => extract_const_int(expr.as_ref()).map(|v| -v),
            Expr::Nested(inner) => extract_const_int(inner.as_ref()),
            _ => None,
        }
    }

    match expr {
        Expr::JsonAccess {
            left,
            operator: JsonOperator::Arrow,
            right,
        } => {
            let left_type = infer_expr_datatype(left.as_ref(), sources)?;
            let Some(ConcreteDatatype::Struct(struct_type)) = left_type else {
                return Ok(());
            };
            let field_name = match right.as_ref() {
                Expr::Identifier(ident) => ident.value.as_str(),
                _ => return Ok(()),
            };
            if !struct_type
                .fields()
                .iter()
                .any(|field| field.name() == field_name)
            {
                let available = struct_type
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "field `{}` not found in struct (available: {})",
                    field_name, available
                ));
            }

            validate_expr_against_sources(left.as_ref(), sources)?;
            Ok(())
        }
        Expr::MapAccess { column, keys } => {
            let column_type = infer_expr_datatype(column.as_ref(), sources)?;
            if let Some(dtype) = column_type {
                if !matches!(dtype, ConcreteDatatype::List(_)) {
                    return Err(format!("list index requires List type, got {:?}", dtype));
                }
            }
            validate_expr_against_sources(column.as_ref(), sources)?;
            for key in keys {
                if let Some(value) = extract_const_int(key) {
                    if value < 0 {
                        return Err(format!("list index must be non-negative, got {}", value));
                    }
                }
                validate_expr_against_sources(key, sources)?;
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr_against_sources(left.as_ref(), sources)?;
            validate_expr_against_sources(right.as_ref(), sources)?;
            Ok(())
        }
        Expr::UnaryOp { expr, .. } => validate_expr_against_sources(expr.as_ref(), sources),
        Expr::Nested(expr) => validate_expr_against_sources(expr.as_ref(), sources),
        Expr::Cast { expr, .. } => validate_expr_against_sources(expr.as_ref(), sources),
        Expr::Function(func) => {
            for arg in &func.args {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) => validate_expr_against_sources(expr, sources)?,
                    sqlparser::ast::FunctionArg::Named {
                        arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ..
                    } => validate_expr_against_sources(expr, sources)?,
                    _ => {}
                }
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(expr) = operand.as_ref() {
                validate_expr_against_sources(expr, sources)?;
            }
            for expr in conditions {
                validate_expr_against_sources(expr, sources)?;
            }
            for expr in results {
                validate_expr_against_sources(expr, sources)?;
            }
            if let Some(expr) = else_result.as_ref() {
                validate_expr_against_sources(expr, sources)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr_against_sources(expr.as_ref(), sources)?;
            validate_expr_against_sources(low.as_ref(), sources)?;
            validate_expr_against_sources(high.as_ref(), sources)?;
            Ok(())
        }
        Expr::InList { expr, list, .. } => {
            validate_expr_against_sources(expr.as_ref(), sources)?;
            for item in list {
                validate_expr_against_sources(item, sources)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn infer_expr_datatype(
    expr: &sqlparser::ast::Expr,
    sources: &[SourceSchemaEntry],
) -> Result<Option<ConcreteDatatype>, String> {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Identifier(ident) => Ok(resolve_column_datatype(None, &ident.value, sources)),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Ok(None);
            }
            Ok(resolve_column_datatype(
                Some(&idents[0].value),
                &idents[1].value,
                sources,
            ))
        }
        Expr::Nested(expr) => infer_expr_datatype(expr.as_ref(), sources),
        Expr::Cast { expr, .. } => infer_expr_datatype(expr.as_ref(), sources),
        Expr::MapAccess { column, .. } => {
            let Some(dtype) = infer_expr_datatype(column.as_ref(), sources)? else {
                return Ok(None);
            };
            let ConcreteDatatype::List(list_type) = dtype else {
                return Ok(None);
            };
            Ok(Some(list_type.item_type().clone()))
        }
        Expr::JsonAccess { left, right, .. } => {
            let Some(dtype) = infer_expr_datatype(left.as_ref(), sources)? else {
                return Ok(None);
            };
            let ConcreteDatatype::Struct(struct_type) = dtype else {
                return Ok(None);
            };
            let Expr::Identifier(ident) = right.as_ref() else {
                return Ok(None);
            };
            Ok(struct_type
                .fields()
                .iter()
                .find(|f| f.name() == ident.value)
                .map(|f| f.data_type().clone()))
        }
        _ => Ok(None),
    }
}

fn resolve_column_datatype(
    qualifier: Option<&str>,
    column_name: &str,
    sources: &[SourceSchemaEntry],
) -> Option<ConcreteDatatype> {
    if sources.is_empty() {
        return None;
    }

    let matches: Vec<_> = sources
        .iter()
        .filter(|src| match qualifier {
            Some(q) => src.source_name == q,
            None => true,
        })
        .filter_map(|src| src.column_lookup.get(column_name).cloned())
        .collect();

    match matches.len() {
        1 => Some(matches[0].clone()),
        _ => None,
    }
}

fn validate_group_by_requires_aggregates(select_stmt: &SelectStmt) -> Result<(), String> {
    if select_stmt.aggregate_mappings.is_empty() && !select_stmt.group_by_exprs.is_empty() {
        return Err(
            "GROUP BY without aggregate functions is not supported; use aggregates or remove GROUP BY"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_having_constraints(select_stmt: &SelectStmt) -> Result<(), String> {
    let Some(having) = &select_stmt.having else {
        return Ok(());
    };

    if select_stmt.window.is_none() {
        return Err("HAVING requires GROUP BY window".to_string());
    }

    if !expr_contains_aggregate_placeholder(having)
        && !expr_contains_last_agg_hit_count(having)
        && !expr_contains_last_hit_time_unix_ms(having)
    {
        return Err(format!(
            "HAVING expression '{}' must reference aggregate functions",
            having
        ));
    }

    let referenced_columns = collect_non_placeholder_column_refs(having);
    if !referenced_columns.is_empty() {
        return Err(format!(
            "HAVING expression '{}' must not reference non-aggregate columns: {}",
            having,
            referenced_columns.join(", ")
        ));
    }

    Ok(())
}

fn validate_window_metadata_function_placement(select_stmt: &SelectStmt) -> Result<(), String> {
    let uses_window_metadata = select_stmt
        .select_fields
        .iter()
        .any(|field| expr_contains_window_metadata_function(&field.expr))
        || select_stmt
            .where_condition
            .as_ref()
            .is_some_and(expr_contains_window_metadata_function)
        || select_stmt
            .having
            .as_ref()
            .is_some_and(expr_contains_window_metadata_function)
        || select_stmt
            .order_by
            .iter()
            .any(|item| expr_contains_window_metadata_function(&item.expr))
        || select_stmt
            .group_by_exprs
            .iter()
            .any(expr_contains_window_metadata_function)
        || select_stmt
            .aggregate_mappings
            .values()
            .any(expr_contains_window_metadata_function)
        || select_stmt
            .window
            .as_ref()
            .is_some_and(window_contains_window_metadata_function);

    if uses_window_metadata && select_stmt.window.is_none() {
        return Err(
            "window metadata functions `window_start` and `window_end` require a windowed query"
                .to_string(),
        );
    }

    for field in &select_stmt.select_fields {
        validate_window_metadata_function_calls(&field.expr)?;
    }

    if let Some(expr) = &select_stmt.where_condition {
        if expr_contains_window_metadata_function(expr) {
            return Err(
                "window metadata functions `window_start` and `window_end` are only allowed in SELECT fields"
                    .to_string(),
            );
        }
    }

    if let Some(expr) = &select_stmt.having {
        if expr_contains_window_metadata_function(expr) {
            return Err(
                "window metadata functions `window_start` and `window_end` are only allowed in SELECT fields"
                    .to_string(),
            );
        }
    }

    for item in &select_stmt.order_by {
        if expr_contains_window_metadata_function(&item.expr) {
            return Err(
                "window metadata functions `window_start` and `window_end` are only allowed in SELECT fields"
                    .to_string(),
            );
        }
    }

    for expr in &select_stmt.group_by_exprs {
        if expr_contains_window_metadata_function(expr) {
            return Err(
                "window metadata functions `window_start` and `window_end` are only allowed in SELECT fields"
                    .to_string(),
            );
        }
    }

    for expr in select_stmt.aggregate_mappings.values() {
        if expr_contains_window_metadata_function(expr) {
            return Err(
                "window metadata functions `window_start` and `window_end` are only allowed in SELECT fields"
                    .to_string(),
            );
        }
    }

    if let Some(window) = select_stmt.window.as_ref() {
        if window_contains_window_metadata_function(window) {
            return Err(
                "window metadata functions `window_start` and `window_end` are only allowed in SELECT fields"
                    .to_string(),
            );
        }
    }

    Ok(())
}

fn validate_last_agg_hit_count_placement(select_stmt: &SelectStmt) -> Result<(), String> {
    for field in &select_stmt.select_fields {
        if expr_contains_last_agg_hit_count(&field.expr) {
            return Err(
                "pipeline state function `last_agg_hit_count` is only allowed in HAVING"
                    .to_string(),
            );
        }
    }

    if let Some(expr) = &select_stmt.where_condition {
        if expr_contains_last_agg_hit_count(expr) {
            return Err(
                "pipeline state function `last_agg_hit_count` is only allowed in HAVING"
                    .to_string(),
            );
        }
    }

    for expr in &select_stmt.group_by_exprs {
        if expr_contains_last_agg_hit_count(expr) {
            return Err(
                "pipeline state function `last_agg_hit_count` is only allowed in HAVING"
                    .to_string(),
            );
        }
    }

    for item in &select_stmt.order_by {
        if expr_contains_last_agg_hit_count(&item.expr) {
            return Err(
                "pipeline state function `last_agg_hit_count` is only allowed in HAVING"
                    .to_string(),
            );
        }
    }

    for expr in select_stmt.aggregate_mappings.values() {
        if expr_contains_last_agg_hit_count(expr) {
            return Err(
                "pipeline state function `last_agg_hit_count` is not allowed inside aggregate function arguments"
                    .to_string(),
            );
        }
    }

    if let Some(window) = select_stmt.window.as_ref() {
        if window_contains_last_agg_hit_count(window) {
            return Err(
                "pipeline state function `last_agg_hit_count` is only allowed in HAVING"
                    .to_string(),
            );
        }
    }

    Ok(())
}

fn validate_last_hit_time_unix_ms_placement(select_stmt: &SelectStmt) -> Result<(), String> {
    if let Some(expr) = &select_stmt.having {
        if expr_contains_last_hit_time_unix_ms(expr) {
            return Err(
                "pipeline state function `last_hit_time_unix_ms` is not allowed in HAVING"
                    .to_string(),
            );
        }
    }

    for expr in &select_stmt.group_by_exprs {
        if expr_contains_last_hit_time_unix_ms(expr) {
            return Err(
                "pipeline state function `last_hit_time_unix_ms` is not allowed in GROUP BY"
                    .to_string(),
            );
        }
    }

    for item in &select_stmt.order_by {
        if expr_contains_last_hit_time_unix_ms(&item.expr) {
            return Err(
                "pipeline state function `last_hit_time_unix_ms` is not allowed in ORDER BY"
                    .to_string(),
            );
        }
    }

    for expr in select_stmt.aggregate_mappings.values() {
        if expr_contains_last_hit_time_unix_ms(expr) {
            return Err(
                "pipeline state function `last_hit_time_unix_ms` is not allowed inside aggregate function arguments"
                    .to_string(),
            );
        }
    }

    if let Some(window) = select_stmt.window.as_ref() {
        if window_contains_last_hit_time_unix_ms_in_disallowed_position(window) {
            return Err(
                "pipeline state function `last_hit_time_unix_ms` is only allowed in slidingwindow OVER WHEN within window clauses"
                    .to_string(),
            );
        }
    }

    Ok(())
}

fn validate_aggregation_projection(select_stmt: &SelectStmt) -> Result<(), String> {
    if select_stmt.aggregate_mappings.is_empty() {
        return Ok(());
    }

    let group_by_exprs: std::collections::HashSet<String> = select_stmt
        .group_by_exprs
        .iter()
        .map(|expr| expr.to_string())
        .collect();

    for field in &select_stmt.select_fields {
        if group_by_exprs.contains(&field.expr.to_string()) {
            continue;
        }

        if expr_contains_window_metadata_function(&field.expr) {
            continue;
        }

        if !expr_contains_aggregate_placeholder(&field.expr) {
            return Err(format!(
                "SELECT expression '{}' must be an aggregate or appear in GROUP BY",
                field.expr
            ));
        }

        let referenced_columns = collect_non_placeholder_column_refs(&field.expr);
        for column in referenced_columns {
            if !group_by_exprs.contains(&column) {
                return Err(format!(
                    "SELECT expression '{}' references column '{}' which must appear in GROUP BY",
                    field.expr, column
                ));
            }
        }
    }

    Ok(())
}

fn validate_order_by_constraints(select_stmt: &SelectStmt) -> Result<(), String> {
    if select_stmt.order_by.is_empty() {
        return Ok(());
    }

    // For non-aggregation queries, ORDER BY can reference any scalar expressions.
    // Unsupported expressions will be rejected later during physical compilation.
    if select_stmt.aggregate_mappings.is_empty() {
        return Ok(());
    }

    let group_by_exprs: std::collections::HashSet<String> = select_stmt
        .group_by_exprs
        .iter()
        .map(|expr| expr_without_outer_nested(expr).to_string())
        .collect();

    for item in &select_stmt.order_by {
        if group_by_exprs.contains(&expr_without_outer_nested(&item.expr).to_string()) {
            continue;
        }

        if !expr_contains_aggregate_placeholder(&item.expr) {
            return Err(format!(
                "ORDER BY expression '{}' must be an aggregate or appear in GROUP BY",
                item.expr
            ));
        }

        let referenced_columns = collect_non_placeholder_column_refs(&item.expr);
        for column in referenced_columns {
            if !group_by_exprs.contains(&column) {
                return Err(format!(
                    "ORDER BY expression '{}' references column '{}' which must appear in GROUP BY",
                    item.expr, column
                ));
            }
        }

        if expr_references_any_stateful_placeholder(&item.expr, &select_stmt.stateful_mappings) {
            return Err(
                "ORDER BY must not reference stateful functions when aggregation is present"
                    .to_string(),
            );
        }
    }

    Ok(())
}

fn validate_window_filter_constraints(select_stmt: &SelectStmt) -> Result<(), String> {
    let Some(window) = select_stmt.window.as_ref() else {
        return Ok(());
    };
    let Some(filter) = window.filter() else {
        return Ok(());
    };

    if let Some(reason) = find_disallowed_window_filter_expression(filter) {
        return Err(format!(
            "window FILTER expression '{}' is not supported: {}",
            filter, reason
        ));
    }

    Ok(())
}

fn validate_eos_window_source_constraints(
    select_stmt: &SelectStmt,
    table_sources: usize,
) -> Result<(), String> {
    let Some(window) = select_stmt.window.as_ref() else {
        return Ok(());
    };
    if !matches!(window, parser_window::Window::Eos { .. }) {
        return Ok(());
    }

    if table_sources == 0 {
        return Err("eoswindow() is only supported for table scan queries".to_string());
    }
    if select_stmt.source_infos.len() != 1 {
        return Err("eoswindow() requires exactly one table source".to_string());
    }

    Ok(())
}

fn expr_references_any_stateful_placeholder(
    expr: &sqlparser::ast::Expr,
    stateful_mappings: &[parser::StatefulMappingEntry],
) -> bool {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

    match expr {
        Expr::Identifier(ident) => stateful_mappings
            .iter()
            .any(|entry| entry.output_column == ident.value),
        Expr::CompoundIdentifier(_) => false,
        Expr::BinaryOp { left, right, .. } => {
            expr_references_any_stateful_placeholder(left, stateful_mappings)
                || expr_references_any_stateful_placeholder(right, stateful_mappings)
        }
        Expr::UnaryOp { expr, .. } => {
            expr_references_any_stateful_placeholder(expr, stateful_mappings)
        }
        Expr::Nested(expr) => expr_references_any_stateful_placeholder(expr, stateful_mappings),
        Expr::Cast { expr, .. } => {
            expr_references_any_stateful_placeholder(expr, stateful_mappings)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_any_stateful_placeholder(expr, stateful_mappings)
                || expr_references_any_stateful_placeholder(low, stateful_mappings)
                || expr_references_any_stateful_placeholder(high, stateful_mappings)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_any_stateful_placeholder(expr, stateful_mappings)
                || list
                    .iter()
                    .any(|item| expr_references_any_stateful_placeholder(item, stateful_mappings))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().is_some_and(|expr| {
                expr_references_any_stateful_placeholder(expr, stateful_mappings)
            }) || conditions
                .iter()
                .any(|expr| expr_references_any_stateful_placeholder(expr, stateful_mappings))
                || results
                    .iter()
                    .any(|expr| expr_references_any_stateful_placeholder(expr, stateful_mappings))
                || else_result.as_ref().is_some_and(|expr| {
                    expr_references_any_stateful_placeholder(expr, stateful_mappings)
                })
        }
        Expr::Function(func) => func.args.iter().any(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                expr_references_any_stateful_placeholder(expr, stateful_mappings)
            }
            FunctionArg::Named { arg, .. } => {
                if let FunctionArgExpr::Expr(expr) = arg {
                    return expr_references_any_stateful_placeholder(expr, stateful_mappings);
                }
                false
            }
            _ => false,
        }),
        Expr::JsonAccess { left, right, .. } => {
            expr_references_any_stateful_placeholder(left, stateful_mappings)
                || expr_references_any_stateful_placeholder(right, stateful_mappings)
        }
        Expr::MapAccess { column, keys } => {
            expr_references_any_stateful_placeholder(column, stateful_mappings)
                || keys
                    .iter()
                    .any(|expr| expr_references_any_stateful_placeholder(expr, stateful_mappings))
        }
        _ => false,
    }
}

fn collect_non_placeholder_column_refs(expr: &sqlparser::ast::Expr) -> Vec<String> {
    use sqlparser::ast::Expr;

    fn is_placeholder(ident: &sqlparser::ast::Ident) -> bool {
        crate::expr::internal_columns::is_parser_placeholder(ident.value.as_str())
    }

    fn collect(expr: &Expr, out: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::Identifier(ident) if !is_placeholder(ident) => {
                out.insert(ident.value.clone());
            }
            Expr::Identifier(_) => {}
            Expr::CompoundIdentifier(idents) => {
                out.insert(Expr::CompoundIdentifier(idents.clone()).to_string());
            }
            Expr::BinaryOp { left, right, .. } => {
                collect(left, out);
                collect(right, out);
            }
            Expr::UnaryOp { expr, .. } => collect(expr, out),
            Expr::Nested(expr) => collect(expr, out),
            Expr::Cast { expr, .. } => collect(expr, out),
            Expr::Between {
                expr, low, high, ..
            } => {
                collect(expr, out);
                collect(low, out);
                collect(high, out);
            }
            Expr::InList { expr, list, .. } => {
                collect(expr, out);
                for item in list {
                    collect(item, out);
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(expr) = operand.as_ref() {
                    collect(expr, out);
                }
                for expr in conditions {
                    collect(expr, out);
                }
                for expr in results {
                    collect(expr, out);
                }
                if let Some(expr) = else_result.as_ref() {
                    collect(expr, out);
                }
            }
            Expr::Function(func) => {
                for arg in &func.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) => collect(expr, out),
                        sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                            ..
                        } => collect(expr, out),
                        _ => {}
                    }
                }
            }
            Expr::JsonAccess { left, right, .. } => {
                collect(left, out);
                collect(right, out);
            }
            Expr::MapAccess { column, keys } => {
                collect(column, out);
                for expr in keys {
                    collect(expr, out);
                }
            }
            _ => {}
        }
    }

    let mut out = std::collections::HashSet::new();
    collect(expr, &mut out);
    let mut out: Vec<_> = out.into_iter().collect();
    out.sort();
    out
}

fn expr_contains_aggregate_placeholder(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;

    fn is_placeholder(ident: &sqlparser::ast::Ident) -> bool {
        crate::expr::internal_columns::is_parser_placeholder(ident.value.as_str())
    }

    match expr {
        Expr::Identifier(ident) => is_placeholder(ident),
        Expr::CompoundIdentifier(_) => false,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate_placeholder(left) || expr_contains_aggregate_placeholder(right)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_aggregate_placeholder(expr),
        Expr::Nested(expr) => expr_contains_aggregate_placeholder(expr),
        Expr::Cast { expr, .. } => expr_contains_aggregate_placeholder(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate_placeholder(expr)
                || expr_contains_aggregate_placeholder(low)
                || expr_contains_aggregate_placeholder(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate_placeholder(expr)
                || list.iter().any(expr_contains_aggregate_placeholder)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_contains_aggregate_placeholder(expr))
                || conditions.iter().any(expr_contains_aggregate_placeholder)
                || results.iter().any(expr_contains_aggregate_placeholder)
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_contains_aggregate_placeholder(expr))
        }
        Expr::Function(func) => func.args.iter().any(|arg| match arg {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => {
                expr_contains_aggregate_placeholder(expr)
            }
            sqlparser::ast::FunctionArg::Named { arg, .. } => {
                if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                    return expr_contains_aggregate_placeholder(expr);
                }
                false
            }
            _ => false,
        }),
        Expr::JsonAccess { left, right, .. } => {
            expr_contains_aggregate_placeholder(left) || expr_contains_aggregate_placeholder(right)
        }
        Expr::MapAccess { column, keys } => {
            expr_contains_aggregate_placeholder(column)
                || keys.iter().any(expr_contains_aggregate_placeholder)
        }
        _ => false,
    }
}

fn expr_contains_last_agg_hit_count(expr: &sqlparser::ast::Expr) -> bool {
    expr_contains_function_name(expr, "last_agg_hit_count")
}

fn expr_contains_last_hit_time_unix_ms(expr: &sqlparser::ast::Expr) -> bool {
    expr_contains_function_name(expr, "last_hit_time_unix_ms")
}

fn expr_contains_window_metadata_function(expr: &sqlparser::ast::Expr) -> bool {
    expr_contains_function_name(expr, "window_start")
        || expr_contains_function_name(expr, "window_end")
}

fn is_window_metadata_function_name(function_name: &str) -> bool {
    function_name.eq_ignore_ascii_case("window_start")
        || function_name.eq_ignore_ascii_case("window_end")
}

fn validate_window_metadata_function_calls(expr: &sqlparser::ast::Expr) -> Result<(), String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

    match expr {
        Expr::Function(func) => {
            let func_name = func.name.to_string();
            if is_window_metadata_function_name(&func_name) {
                if !func.args.is_empty() {
                    return Err(format!(
                        "window metadata function `{func_name}` takes zero arguments"
                    ));
                }
                if func.filter.is_some() {
                    return Err(format!(
                        "window metadata function `{func_name}` does not support FILTER"
                    ));
                }
                if func.over.is_some() {
                    return Err(format!(
                        "window metadata function `{func_name}` does not support OVER"
                    ));
                }
                if !func.order_by.is_empty() {
                    return Err(format!(
                        "window metadata function `{func_name}` does not support ORDER BY"
                    ));
                }
            }

            for arg in &func.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        validate_window_metadata_function_calls(expr)?
                    }
                    FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    } => validate_window_metadata_function_calls(expr)?,
                    _ => {}
                }
            }
            if let Some(filter) = func.filter.as_ref() {
                validate_window_metadata_function_calls(filter)?;
            }
            if let Some(window_type) = func.over.as_ref() {
                validate_window_type_metadata_function_calls(window_type)?;
            }
            for item in &func.order_by {
                validate_window_metadata_function_calls(&item.expr)?;
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_window_metadata_function_calls(left)?;
            validate_window_metadata_function_calls(right)
        }
        Expr::UnaryOp { expr, .. } => validate_window_metadata_function_calls(expr),
        Expr::Nested(expr) => validate_window_metadata_function_calls(expr),
        Expr::Cast { expr, .. } => validate_window_metadata_function_calls(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_window_metadata_function_calls(expr)?;
            validate_window_metadata_function_calls(low)?;
            validate_window_metadata_function_calls(high)
        }
        Expr::InList { expr, list, .. } => {
            validate_window_metadata_function_calls(expr)?;
            for item in list {
                validate_window_metadata_function_calls(item)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand.as_ref() {
                validate_window_metadata_function_calls(operand)?;
            }
            for condition in conditions {
                validate_window_metadata_function_calls(condition)?;
            }
            for result in results {
                validate_window_metadata_function_calls(result)?;
            }
            if let Some(else_result) = else_result.as_ref() {
                validate_window_metadata_function_calls(else_result)?;
            }
            Ok(())
        }
        Expr::JsonAccess { left, right, .. } => {
            validate_window_metadata_function_calls(left)?;
            validate_window_metadata_function_calls(right)
        }
        Expr::MapAccess { column, keys } => {
            validate_window_metadata_function_calls(column)?;
            for key in keys {
                validate_window_metadata_function_calls(key)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_window_type_metadata_function_calls(
    window_type: &sqlparser::ast::WindowType,
) -> Result<(), String> {
    match window_type {
        sqlparser::ast::WindowType::WindowSpec(spec) => {
            for expr in &spec.partition_by {
                validate_window_metadata_function_calls(expr)?;
            }
            for item in &spec.order_by {
                validate_window_metadata_function_calls(&item.expr)?;
            }
            Ok(())
        }
        sqlparser::ast::WindowType::NamedWindow(_) => Ok(()),
    }
}

fn expr_contains_function_name(expr: &sqlparser::ast::Expr, target_name: &str) -> bool {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

    match expr {
        Expr::Function(func) => {
            func.name.to_string().eq_ignore_ascii_case(target_name)
                || func.args.iter().any(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        expr_contains_function_name(expr, target_name)
                    }
                    FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    } => expr_contains_function_name(expr, target_name),
                    _ => false,
                })
                || func
                    .filter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_function_name(expr, target_name))
                || func.over.as_ref().is_some_and(|window_type| {
                    window_type_contains_function_name(window_type, target_name)
                })
                || func
                    .order_by
                    .iter()
                    .any(|item| expr_contains_function_name(&item.expr, target_name))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_function_name(left, target_name)
                || expr_contains_function_name(right, target_name)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_function_name(expr, target_name),
        Expr::Nested(expr) => expr_contains_function_name(expr, target_name),
        Expr::Cast { expr, .. } => expr_contains_function_name(expr, target_name),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_function_name(expr, target_name)
                || expr_contains_function_name(low, target_name)
                || expr_contains_function_name(high, target_name)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_function_name(expr, target_name)
                || list
                    .iter()
                    .any(|expr| expr_contains_function_name(expr, target_name))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_contains_function_name(expr, target_name))
                || conditions
                    .iter()
                    .any(|expr| expr_contains_function_name(expr, target_name))
                || results
                    .iter()
                    .any(|expr| expr_contains_function_name(expr, target_name))
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_contains_function_name(expr, target_name))
        }
        Expr::JsonAccess { left, right, .. } => {
            expr_contains_function_name(left, target_name)
                || expr_contains_function_name(right, target_name)
        }
        Expr::MapAccess { column, keys } => {
            expr_contains_function_name(column, target_name)
                || keys
                    .iter()
                    .any(|expr| expr_contains_function_name(expr, target_name))
        }
        _ => false,
    }
}

fn window_type_contains_function_name(
    window_type: &sqlparser::ast::WindowType,
    target_name: &str,
) -> bool {
    match window_type {
        sqlparser::ast::WindowType::WindowSpec(spec) => {
            spec.partition_by
                .iter()
                .any(|expr| expr_contains_function_name(expr, target_name))
                || spec
                    .order_by
                    .iter()
                    .any(|item| expr_contains_function_name(&item.expr, target_name))
        }
        sqlparser::ast::WindowType::NamedWindow(_) => false,
    }
}

fn window_contains_last_agg_hit_count(window: &parser_window::Window) -> bool {
    match window {
        parser_window::Window::Tumbling {
            partition_by,
            filter,
            ..
        }
        | parser_window::Window::Count {
            partition_by,
            filter,
            ..
        }
        | parser_window::Window::Sliding {
            partition_by,
            filter,
            ..
        } => {
            partition_by.iter().any(expr_contains_last_agg_hit_count)
                || filter
                    .as_deref()
                    .is_some_and(expr_contains_last_agg_hit_count)
        }
        parser_window::Window::Eos { filter } => filter
            .as_deref()
            .is_some_and(expr_contains_last_agg_hit_count),
        parser_window::Window::State {
            open,
            emit,
            partition_by,
            filter,
        } => {
            expr_contains_last_agg_hit_count(open)
                || expr_contains_last_agg_hit_count(emit)
                || partition_by.iter().any(expr_contains_last_agg_hit_count)
                || filter
                    .as_deref()
                    .is_some_and(expr_contains_last_agg_hit_count)
        }
    }
}

fn window_contains_last_hit_time_unix_ms_in_disallowed_position(
    window: &parser_window::Window,
) -> bool {
    match window {
        parser_window::Window::Tumbling {
            partition_by,
            filter,
            ..
        }
        | parser_window::Window::Count {
            partition_by,
            filter,
            ..
        } => {
            partition_by.iter().any(expr_contains_last_hit_time_unix_ms)
                || filter
                    .as_deref()
                    .is_some_and(expr_contains_last_hit_time_unix_ms)
        }
        parser_window::Window::Sliding {
            partition_by,
            filter,
            ..
        } => {
            partition_by.iter().any(expr_contains_last_hit_time_unix_ms)
                || filter
                    .as_deref()
                    .is_some_and(expr_contains_last_hit_time_unix_ms)
        }
        parser_window::Window::Eos { filter } => filter
            .as_deref()
            .is_some_and(expr_contains_last_hit_time_unix_ms),
        parser_window::Window::State {
            open,
            emit,
            partition_by,
            filter,
        } => {
            expr_contains_last_hit_time_unix_ms(open)
                || expr_contains_last_hit_time_unix_ms(emit)
                || partition_by.iter().any(expr_contains_last_hit_time_unix_ms)
                || filter
                    .as_deref()
                    .is_some_and(expr_contains_last_hit_time_unix_ms)
        }
    }
}

fn window_contains_window_metadata_function(window: &parser_window::Window) -> bool {
    match window {
        parser_window::Window::Tumbling {
            partition_by,
            filter,
            ..
        }
        | parser_window::Window::Count {
            partition_by,
            filter,
            ..
        }
        | parser_window::Window::Sliding {
            partition_by,
            filter,
            ..
        } => {
            partition_by
                .iter()
                .any(expr_contains_window_metadata_function)
                || filter
                    .as_deref()
                    .is_some_and(expr_contains_window_metadata_function)
        }
        parser_window::Window::Eos { filter } => filter
            .as_deref()
            .is_some_and(expr_contains_window_metadata_function),
        parser_window::Window::State {
            open,
            emit,
            partition_by,
            filter,
        } => {
            expr_contains_window_metadata_function(open)
                || expr_contains_window_metadata_function(emit)
                || partition_by
                    .iter()
                    .any(expr_contains_window_metadata_function)
                || filter
                    .as_deref()
                    .is_some_and(expr_contains_window_metadata_function)
        }
    }
}

fn find_disallowed_window_filter_expression(expr: &Expr) -> Option<String> {
    fn visit(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                if is_window_function_name(&func_name) {
                    return Some(format!("window function `{}`", func.name));
                }
                if func_name == "last_hit_count" {
                    return Some("pipeline state function `last_hit_count`".to_string());
                }
                if parser::aggregate_registry::default_aggregate_registry()
                    .is_aggregate_function(&func_name)
                {
                    return Some(format!("aggregate function `{}`", func.name));
                }
                if parser::stateful_registry::default_stateful_registry()
                    .is_stateful_function(&func_name)
                {
                    return Some(format!("stateful function `{}`", func.name));
                }
                if func.filter.is_some() {
                    return Some(format!("function FILTER clause on `{}`", func.name));
                }
                if func.over.is_some() {
                    return Some(format!("function OVER clause on `{}`", func.name));
                }
                if !func.order_by.is_empty() {
                    return Some(format!("function ORDER BY clause on `{}`", func.name));
                }

                for arg in &func.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) => {
                            if let Some(reason) = visit(expr) {
                                return Some(reason);
                            }
                        }
                        sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                            ..
                        } => {
                            if let Some(reason) = visit(expr) {
                                return Some(reason);
                            }
                        }
                        _ => {}
                    }
                }
                None
            }
            Expr::BinaryOp { left, right, .. } => visit(left).or_else(|| visit(right)),
            Expr::UnaryOp { expr, .. } => visit(expr),
            Expr::Nested(expr) => visit(expr),
            Expr::Cast { expr, .. } => visit(expr),
            Expr::Between {
                expr, low, high, ..
            } => visit(expr).or_else(|| visit(low)).or_else(|| visit(high)),
            Expr::InList { expr, list, .. } => visit(expr).or_else(|| list.iter().find_map(visit)),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => operand
                .as_deref()
                .and_then(visit)
                .or_else(|| conditions.iter().find_map(visit))
                .or_else(|| results.iter().find_map(visit))
                .or_else(|| else_result.as_deref().and_then(visit)),
            Expr::JsonAccess { left, right, .. } => visit(left).or_else(|| visit(right)),
            Expr::MapAccess { column, keys } => {
                visit(column).or_else(|| keys.iter().find_map(visit))
            }
            _ => None,
        }
    }

    visit(expr)
}

fn is_window_function_name(name: &str) -> bool {
    matches!(
        name,
        "tumblingwindow" | "countwindow" | "slidingwindow" | "statewindow" | "eoswindow"
    )
}

fn split_window_filter(window: parser_window::Window) -> (parser_window::Window, Option<Expr>) {
    let filter = window.filter().cloned();
    (window.with_filter(None), filter)
}

fn convert_window_spec(window: parser_window::Window) -> Result<LogicalWindowSpec, String> {
    match window {
        parser_window::Window::Tumbling {
            time_unit,
            length,
            partition_by,
            ..
        } => {
            let unit = match time_unit {
                parser_window::TimeUnit::Milliseconds => TimeUnit::Milliseconds,
                parser_window::TimeUnit::Seconds => TimeUnit::Seconds,
            };
            Ok(LogicalWindowSpec::Tumbling {
                time_unit: unit,
                length,
                partition_by,
            })
        }
        parser_window::Window::Count {
            count,
            partition_by,
            ..
        } => Ok(LogicalWindowSpec::Count {
            count,
            partition_by,
        }),
        parser_window::Window::Sliding {
            time_unit,
            lookback,
            lookahead,
            partition_by,
            trigger_condition,
            ..
        } => {
            let unit = match time_unit {
                parser_window::TimeUnit::Milliseconds => TimeUnit::Milliseconds,
                parser_window::TimeUnit::Seconds => TimeUnit::Seconds,
            };
            Ok(LogicalWindowSpec::Sliding {
                time_unit: unit,
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
            })
        }
        parser_window::Window::State {
            open,
            emit,
            partition_by,
            ..
        } => Ok(LogicalWindowSpec::State {
            open,
            emit,
            partition_by,
        }),
        parser_window::Window::Eos { .. } => Ok(LogicalWindowSpec::Eos),
    }
}

/// Helper function to print logical plan structure for debugging
pub fn print_logical_plan(plan: &Arc<LogicalPlan>, indent: usize) {
    plan.print_topology(indent);
}

fn max_plan_index(plan: &Arc<LogicalPlan>) -> i64 {
    let mut max_index = plan.get_plan_index();
    for child in plan.children() {
        let child_max = max_plan_index(child);
        if child_max > max_index {
            max_index = child_max;
        }
    }
    max_index
}
