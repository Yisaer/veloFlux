use super::*;
use crate::aggregation::{AggregateFunction, CountFunction};
use crate::expr::scalar::ColumnRef;
use crate::expr::BinaryFunc;
use crate::model::{Message, Tuple};
use crate::planner::physical::StreamingWindowSpec;
use datatypes::{ConcreteDatatype, Int64Type};
use sqlparser::ast::Ident;
use std::time::{Instant, SystemTime};

fn column(index: usize) -> ScalarExpr {
    ScalarExpr::Column(ColumnRef::ByIndex {
        source_name: "s".into(),
        column_index: index,
    })
}

fn call(name: &str, args: Vec<ScalarExpr>) -> AggregateCall {
    AggregateCall {
        supports_inline_execution: true,
        output_column: name.into(),
        func_name: name.into(),
        args,
        distinct: false,
    }
}

fn worker(
    calls: Vec<AggregateCall>,
    grouped: bool,
    registry: Arc<AggregateFunctionRegistry>,
) -> AggregationWorker {
    let group_exprs = if grouped {
        vec![Expr::Identifier(Ident::new("k"))]
    } else {
        vec![]
    };
    let group_scalars = if grouped { vec![column(1)] } else { vec![] };
    let meta = build_group_by_meta(&group_exprs, &group_scalars);
    let physical = Arc::new(PhysicalStreamingAggregation::new(
        StreamingWindowSpec::Eos,
        HashMap::new(),
        group_exprs,
        calls,
        group_scalars,
        vec![],
        0,
    ));
    AggregationWorker::new(physical, registry, meta)
}

fn tuple(value: Value, key: i64) -> Tuple {
    Tuple::with_timestamp(
        Arc::from(vec![Arc::new(Message::new(
            "s",
            vec![Arc::from("v"), Arc::from("k")],
            vec![value, Value::Int64(key)],
        ))]),
        SystemTime::UNIX_EPOCH,
    )
}

// Deliberately exposes only the pre-existing interface, including when it
// delegates to a builtin. No production switch is needed to test boxed calls.
struct RegisteredFunction {
    name: String,
    implementation: Arc<dyn AggregateFunction>,
}

impl AggregateFunction for RegisteredFunction {
    fn name(&self) -> &str {
        &self.name
    }
    fn return_type(&self, types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        self.implementation.return_type(types)
    }
    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        self.implementation.create_accumulator()
    }
    fn supports_incremental(&self) -> bool {
        true
    }
}

fn boxed_registry() -> Arc<AggregateFunctionRegistry> {
    let registry = AggregateFunctionRegistry::with_builtins();
    for name in ["count", "sum", "avg", "min", "max"] {
        let implementation = registry.get(name).unwrap();
        registry.register_function(Arc::new(RegisteredFunction {
            name: name.into(),
            implementation,
        }));
    }
    registry
}

fn output(worker: &mut AggregationWorker) -> Vec<(i64, Vec<Value>)> {
    let calls = worker.physical.aggregate_calls.clone();
    let Some(batch) = worker.finalize_current_window().unwrap() else {
        return vec![];
    };
    let mut rows: Vec<_> = batch
        .rows()
        .iter()
        .map(|row| {
            let key = match row.value_by_index("s", 1) {
                Some(Value::Int64(k)) => *k,
                _ => 0,
            };
            let values = calls
                .iter()
                .map(|call| row.value_by_name("", &call.output_column).unwrap().clone())
                .collect();
            (key, values)
        })
        .collect();
    rows.sort_by_key(|row| row.0);
    rows
}

#[test]
fn bound_calls_match_boxed_semantics_table_driven() {
    for grouped in [false, true] {
        for name in ["count", "sum", "avg", "min", "max"] {
            for values in [
                vec![Value::Null, Value::Int64(2), Value::Int64(7), Value::Null],
                vec![Value::Null, Value::Null],
                vec![
                    Value::Int8(1),
                    Value::Uint16(2),
                    Value::Float32(3.5),
                    Value::Float64(4.5),
                ],
                vec![
                    Value::Float64(-0.0),
                    Value::Float64(0.0),
                    Value::Float64(f64::NAN),
                ],
                vec![Value::Int64(i64::MAX), Value::Int64(-1)],
                vec![
                    Value::String("3".to_string().into()),
                    Value::String("bad".to_string().into()),
                ],
            ] {
                let mut concrete = worker(
                    vec![call(name, vec![column(0)])],
                    grouped,
                    AggregateFunctionRegistry::with_builtins(),
                );
                let mut boxed =
                    worker(vec![call(name, vec![column(0)])], grouped, boxed_registry());
                for (index, value) in values.into_iter().enumerate() {
                    let row = tuple(value, (index % 2) as i64);
                    assert_eq!(
                        concrete.update_groups(&row),
                        boxed.update_groups(&row),
                        "{name}"
                    );
                }
                assert_eq!(output(&mut concrete), output(&mut boxed), "{name}");
            }
        }
    }
}

#[test]
fn mixed_calls_and_expression_arguments_keep_local_specialization() {
    let expression = ScalarExpr::CallBinary {
        func: BinaryFunc::Add,
        expr1: Box::new(column(0)),
        expr2: Box::new(ScalarExpr::literal(
            Value::Int64(1),
            ConcreteDatatype::Int64(Int64Type),
        )),
    };
    let calls = vec![
        call("sum", vec![column(0)]),
        call("avg", vec![expression]),
        call("last_row", vec![column(0)]),
    ];
    let mut concrete = worker(
        calls.clone(),
        false,
        AggregateFunctionRegistry::with_builtins(),
    );
    let mut boxed = worker(calls, false, boxed_registry());
    let bound = concrete.bound.calls.as_ref().unwrap();
    assert!(matches!(bound[0].factory, AccumulatorFactory::Prepared(_)));
    assert!(matches!(bound[1].factory, AccumulatorFactory::Prepared(_)));
    assert!(matches!(
        bound[2].factory,
        AccumulatorFactory::Registered(_)
    ));
    for value in [Value::Int64(2), Value::Null, Value::Int64(4)] {
        let row = tuple(value, 0);
        assert_eq!(concrete.update_groups(&row), boxed.update_groups(&row));
    }
    assert_eq!(output(&mut concrete), output(&mut boxed));
}

#[test]
fn builtin_name_override_uses_registered_implementation() {
    let registry = AggregateFunctionRegistry::with_builtins();
    registry.register_function(Arc::new(RegisteredFunction {
        name: "sum".into(),
        implementation: Arc::new(CountFunction::new()),
    }));
    let mut worker = worker(
        vec![call("sum", vec![column(0)]), call("avg", vec![column(0)])],
        false,
        registry,
    );
    worker.update_groups(&tuple(Value::Int64(10), 0)).unwrap();
    worker.update_groups(&tuple(Value::Int64(20), 0)).unwrap();
    assert_eq!(
        output(&mut worker),
        vec![(0, vec![Value::Int64(2), Value::Float64(15.0)])]
    );
}

#[test]
fn failed_row_does_not_partially_commit_or_leave_empty_groups() {
    let registry = AggregateFunctionRegistry::with_builtins();
    // Mix a registered accumulator before a fallible concrete AVG.
    registry.register_function(Arc::new(RegisteredFunction {
        name: "custom_count".into(),
        implementation: Arc::new(CountFunction::new()),
    }));
    let mut worker = worker(
        vec![
            call("count", vec![column(0)]),
            call("custom_count", vec![column(0)]),
            call("avg", vec![column(0)]),
        ],
        true,
        registry,
    );
    let bad = tuple(Value::String("bad".to_string().into()), 0);
    assert!(worker.update_groups(&bad).is_err());
    assert!(worker.groups.is_empty());
    let good = tuple(Value::Int64(4), 0);
    worker.update_groups(&good).unwrap();
    assert!(worker.update_groups(&bad).is_err());
    assert!(worker.scratch.arguments.is_empty());
    assert!(worker.scratch.updates.is_empty());
    let state = worker.groups.values().next().unwrap();
    assert!(Arc::ptr_eq(&state.last_tuple.messages, &good.messages));
    assert_eq!(
        output(&mut worker),
        vec![(
            0,
            vec![Value::Int64(1), Value::Int64(1), Value::Float64(4.0)]
        )]
    );
}

#[test]
fn clear_and_new_windows_share_bindings_not_state() {
    let mut first = worker(
        vec![call("sum", vec![column(0)])],
        false,
        AggregateFunctionRegistry::with_builtins(),
    );
    let mut second = first.empty();
    assert!(Arc::ptr_eq(&first.bound, &second.bound));
    let row = tuple(Value::Int64(10), 0);
    first.update_groups(&row).unwrap();
    assert_eq!(Arc::strong_count(&row.messages), 2);
    first.clear();
    assert_eq!(Arc::strong_count(&row.messages), 1);
    assert!(output(&mut first).is_empty());
    first.update_groups(&tuple(Value::Int64(3), 0)).unwrap();
    second.update_groups(&tuple(Value::Int64(7), 0)).unwrap();
    assert_eq!(output(&mut first), vec![(0, vec![Value::Int64(3)])]);
    assert_eq!(output(&mut second), vec![(0, vec![Value::Int64(7)])]);
}

#[test]
fn wildcard_arity_and_missing_columns_match_registered_calls() {
    for args in [
        vec![],
        vec![column(9)],
        vec![ScalarExpr::Wildcard { source_name: None }],
        vec![ScalarExpr::Wildcard {
            source_name: Some("s".into()),
        }],
        vec![ScalarExpr::Wildcard {
            source_name: Some("missing".into()),
        }],
    ] {
        for name in ["count", "sum", "avg", "min", "max"] {
            let mut concrete = worker(
                vec![call(name, args.clone())],
                false,
                AggregateFunctionRegistry::with_builtins(),
            );
            let mut boxed = worker(vec![call(name, args.clone())], false, boxed_registry());
            assert_eq!(
                concrete.update_groups(&tuple(Value::Null, 0)),
                boxed.update_groups(&tuple(Value::Null, 0)),
                "{name}"
            );
            assert_eq!(output(&mut concrete), output(&mut boxed), "{name}");
        }
    }
}

// Kept compatible with the original PR worker for paired local comparison.
#[test]
#[ignore = "manual release-mode performance comparison"]
fn aggregation_row_cost_comparison() {
    use std::hint::black_box;
    const ROWS: usize = 16_384;
    for groups in [0, 16, 4096] {
        for names in [vec!["sum"], vec!["count", "sum", "avg", "min", "max"]] {
            let rows: Vec<_> = (0..ROWS)
                .map(|index| {
                    tuple(
                        Value::Int64((index % 101) as i64),
                        if groups == 0 {
                            0
                        } else {
                            (index % groups) as i64
                        },
                    )
                })
                .collect();
            let calls = names
                .iter()
                .map(|name| call(name, vec![column(0)]))
                .collect::<Vec<_>>();
            let mut worker = worker(
                calls,
                groups != 0,
                AggregateFunctionRegistry::with_builtins(),
            );
            let mut samples = Vec::new();
            for _ in 0..7 {
                let start = Instant::now();
                for _ in 0..8 {
                    for row in &rows {
                        worker.update_groups(black_box(row)).unwrap();
                    }
                    let batch = worker.finalize_current_window().unwrap().unwrap();
                    assert_eq!(batch.num_rows(), groups.max(1));
                    black_box(batch);
                }
                samples.push(start.elapsed().as_nanos() as f64 / (ROWS * 8) as f64);
            }
            samples.sort_by(f64::total_cmp);
            println!(
                "groups={groups} aggregates={} median_ns_per_row={:.1}",
                names.len(),
                samples[3]
            );
        }
    }
}
