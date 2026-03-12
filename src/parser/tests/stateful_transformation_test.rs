//! Tests for stateful function transformation (SQL -> rewritten Expr placeholders + mappings)
//! Validates transformation via a JSON view over the parsed SelectStmt.

use parser::{
    StaticAggregateRegistry, StaticStatefulRegistry, parse_sql, parse_sql_with_registries,
};
use serde_json::{Value, json};
use std::sync::Arc;

fn parse_to_json(sql: &str) -> Value {
    let select_stmt = parse_sql(sql).expect("parse sql");
    select_stmt_to_json(select_stmt)
}

fn parse_to_json_with_stateful(sql: &str, names: &[&str]) -> Value {
    let select_stmt = parse_sql_with_registries(
        sql,
        Arc::new(StaticAggregateRegistry::new([
            "sum", "count", "last_row", "ndv",
        ])),
        Arc::new(StaticStatefulRegistry::new(names.iter().copied())),
    )
    .expect("parse sql with custom stateful registry");
    select_stmt_to_json(select_stmt)
}

fn select_stmt_to_json(select_stmt: parser::SelectStmt) -> Value {
    let mut aggregate_mappings: Vec<_> = select_stmt
        .aggregate_mappings
        .iter()
        .map(|(col, expr)| json!({ "col": col, "expr": expr.to_string() }))
        .collect();
    aggregate_mappings.sort_by(|a, b| a["col"].as_str().cmp(&b["col"].as_str()));

    let mut stateful_mappings: Vec<_> = select_stmt
        .stateful_mappings
        .iter()
        .map(|(col, call)| {
            json!({
                "col": col,
                "expr": call.original_expr.to_string(),
                "func_name": call.func_name,
                "args": call.args.iter().map(|expr| expr.to_string()).collect::<Vec<_>>(),
                "when": call.when.as_ref().map(|expr| expr.to_string()),
                "partition_by": call.partition_by.iter().map(|expr| expr.to_string()).collect::<Vec<_>>(),
            })
        })
        .collect();
    stateful_mappings.sort_by(|a, b| a["col"].as_str().cmp(&b["col"].as_str()));

    json!({
        "select_exprs": select_stmt.select_fields.iter().map(|f| f.expr.to_string()).collect::<Vec<_>>(),
        "where": select_stmt.where_condition.as_ref().map(|e| e.to_string()),
        "having": select_stmt.having.as_ref().map(|e| e.to_string()),
        "aggregate_mappings": aggregate_mappings,
        "stateful_mappings": stateful_mappings,
    })
}

#[test]
fn case_1_select_lag() {
    let got = parse_to_json("SELECT lag(a) FROM stream");
    let expected = json!({
        "select_exprs": ["col_1"],
        "where": null,
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [{
            "col": "col_1",
            "expr": "lag(a)",
            "func_name": "lag",
            "args": ["a"],
            "when": null,
            "partition_by": [],
        }],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_2_select_lag_twice_dedup() {
    let got = parse_to_json("SELECT lag(a), lag(a) FROM stream");
    let expected = json!({
        "select_exprs": ["col_1", "col_1"],
        "where": null,
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [{
            "col": "col_1",
            "expr": "lag(a)",
            "func_name": "lag",
            "args": ["a"],
            "when": null,
            "partition_by": [],
        }],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_3_where_lag() {
    let got = parse_to_json("SELECT a FROM stream WHERE lag(a) > 0");
    let expected = json!({
        "select_exprs": ["a"],
        "where": "col_1 > 0",
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [{
            "col": "col_1",
            "expr": "lag(a)",
            "func_name": "lag",
            "args": ["a"],
            "when": null,
            "partition_by": [],
        }],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_4_select_and_where_share_lag() {
    let got = parse_to_json("SELECT lag(a) FROM stream WHERE lag(a) > 0");
    let expected = json!({
        "select_exprs": ["col_1"],
        "where": "col_1 > 0",
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [{
            "col": "col_1",
            "expr": "lag(a)",
            "func_name": "lag",
            "args": ["a"],
            "when": null,
            "partition_by": [],
        }],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_5_select_lag_a_lag_b_no_dedup() {
    let got = parse_to_json("SELECT lag(a), lag(b) FROM stream");
    let expected = json!({
        "select_exprs": ["col_1", "col_2"],
        "where": null,
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [
            { "col": "col_1", "expr": "lag(a)", "func_name": "lag", "args": ["a"], "when": null, "partition_by": [] },
            { "col": "col_2", "expr": "lag(b)", "func_name": "lag", "args": ["b"], "when": null, "partition_by": [] },
        ],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_6_nested_expr_and_where_share_lag() {
    let got = parse_to_json("SELECT lag(a) + 1 FROM stream WHERE lag(a) > 0");
    let expected = json!({
        "select_exprs": ["col_1 + 1"],
        "where": "col_1 > 0",
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [{
            "col": "col_1",
            "expr": "lag(a)",
            "func_name": "lag",
            "args": ["a"],
            "when": null,
            "partition_by": [],
        }],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_8_aggregate_and_stateful_share_allocator_and_dedup() {
    let got = parse_to_json(
        "SELECT sum(a) + lag(b) FROM stream WHERE lag(b) > 0 GROUP BY c HAVING sum(a) > 0",
    );
    let expected = json!({
        "select_exprs": ["col_2 + col_1"],
        "where": "col_1 > 0",
        "having": "col_2 > 0",
        "aggregate_mappings": [{ "col": "col_2", "expr": "sum(a)" }],
        "stateful_mappings": [{
            "col": "col_1",
            "expr": "lag(b)",
            "func_name": "lag",
            "args": ["b"],
            "when": null,
            "partition_by": [],
        }],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_9_select_lag_filter_over() {
    let got = parse_to_json(
        "SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) FROM stream",
    );
    let expected = json!({
        "select_exprs": ["col_1"],
        "where": null,
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [{
            "col": "col_1",
            "expr": "lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2)",
            "func_name": "lag",
            "args": ["a"],
            "when": "flag = 1",
            "partition_by": ["k1", "k2"],
        }],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_10_nested_stateful_in_filter_rewrites_inner_first() {
    let got = parse_to_json("SELECT lag(temp) FILTER (WHERE lag(id1) > 1) FROM stream");
    let expected = json!({
        "select_exprs": ["col_2"],
        "where": null,
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [
            {
                "col": "col_1",
                "expr": "lag(id1)",
                "func_name": "lag",
                "args": ["id1"],
                "when": null,
                "partition_by": [],
            },
            {
                "col": "col_2",
                "expr": "lag(temp) FILTER (WHERE lag(id1) > 1)",
                "func_name": "lag",
                "args": ["temp"],
                "when": "col_1 > 1",
                "partition_by": [],
            }
        ],
    });
    assert_eq!(got, expected);
}

#[test]
fn case_11_custom_stateful_in_filter_rewrites_inner_first() {
    let got = parse_to_json_with_stateful(
        "SELECT lag(ts, 1, ts, true) FILTER (WHERE had_changed(true, statusCode)) FROM demo",
        &["lag", "had_changed"],
    );
    let expected = json!({
        "select_exprs": ["col_2"],
        "where": null,
        "having": null,
        "aggregate_mappings": [],
        "stateful_mappings": [
            {
                "col": "col_1",
                "expr": "had_changed(true, statusCode)",
                "func_name": "had_changed",
                "args": ["true", "statusCode"],
                "when": null,
                "partition_by": [],
            },
            {
                "col": "col_2",
                "expr": "lag(ts, 1, ts, true) FILTER (WHERE had_changed(true, statusCode))",
                "func_name": "lag",
                "args": ["ts", "1", "ts", "true"],
                "when": "col_1",
                "partition_by": [],
            }
        ],
    });
    assert_eq!(got, expected);
}
