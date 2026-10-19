use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::expr::ScalarExpr;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::state_window_processor::{
    eval_state_condition, state_window_output, StateWindowOutput,
};
use crate::processor::window_metadata;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};
use crate::processor::{ProcessorError, ProcessorStats, StreamData};
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
#[cfg(test)]
use tokio::sync::broadcast;

struct PartitionAggState {
    active: bool,
    opened_at: Option<SystemTime>,
    last_seen_at: Option<SystemTime>,
    worker: AggregationWorker,
}

struct StateWindowConfig {
    open_expr: Expr,
    emit_expr: Expr,
    open_scalar: ScalarExpr,
    emit_scalar: ScalarExpr,
    partition_by_scalars: Vec<ScalarExpr>,
}

impl StreamingStateStage {
    fn extract_window_spec(
        physical: &PhysicalStreamingAggregation,
    ) -> Result<StateWindowConfig, ProcessorError> {
        match &physical.window {
            StreamingWindowSpec::State {
                open_expr,
                emit_expr,
                open_scalar,
                emit_scalar,
                partition_by_scalars,
                ..
            } => Ok(StateWindowConfig {
                open_expr: open_expr.clone(),
                emit_expr: emit_expr.clone(),
                open_scalar: open_scalar.clone(),
                emit_scalar: emit_scalar.clone(),
                partition_by_scalars: partition_by_scalars.clone(),
            }),
            other => Err(ProcessorError::InvalidConfiguration(format!(
                "streaming state aggregation requires state window spec, got {other:?}",
            ))),
        }
    }
}

/// Shared synchronous state-window aggregation. Outputs follow row completion order.
pub(crate) struct StreamingStateStage {
    physical: Arc<PhysicalStreamingAggregation>,
    registry: Arc<AggregateFunctionRegistry>,
    group_by: Vec<GroupByMeta>,
    config: Arc<StateWindowConfig>,
    ids: HashMap<PartitionKey, usize>,
    states: Vec<PartitionAggState>,
}

impl StreamingStateStage {
    pub(crate) fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        registry: Arc<AggregateFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
        let group_by = build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let config = Arc::new(StreamingStateStage::extract_window_spec(&physical)?);
        Ok(Self {
            physical,
            registry,
            group_by,
            config,
            ids: HashMap::new(),
            states: Vec::new(),
        })
    }
    pub(crate) fn clear(&mut self) {
        self.ids.clear();
        self.states.clear();
    }
    pub(crate) fn process(
        &mut self,
        data: StreamData,
        stats: &ProcessorStats,
    ) -> StateWindowOutput {
        if data.is_terminal() && !matches!(&data, StreamData::Control(s) if s.is_graceful_end()) {
            self.clear();
        }
        state_window_output(data, stats, "streaming state aggregation processor error")
    }
    fn take_batch(
        state: &mut PartitionAggState,
        closed_at: SystemTime,
        stats: &ProcessorStats,
    ) -> Option<StreamData> {
        let start = state.opened_at.unwrap_or(closed_at);
        state.active = false;
        state.opened_at = None;
        state.last_seen_at = None;
        match state.worker.finalize_current_window() {
            Ok(Some(batch)) => {
                match window_metadata::attach_from_system_time(batch, start, closed_at) {
                    Ok(batch) => Some(StreamData::collection(batch)),
                    Err(error) => {
                        stats.record_error_logged(
                            "streaming state aggregation processor error",
                            error.to_string(),
                        );
                        None
                    }
                }
            }
            Ok(None) => None,
            Err(error) => {
                stats.record_error_logged("streaming state aggregation processor error", error);
                None
            }
        }
    }
    pub(crate) fn next_output(
        &mut self,
        output: &mut StateWindowOutput,
        stats: &ProcessorStats,
    ) -> Result<Option<StreamData>, ProcessorError> {
        match output {
            StateWindowOutput::Single(data) => Ok(data.take()),
            StateWindowOutput::Drain {
                partition,
                terminal,
            } => {
                while let Some(state) = self.states.get_mut(*partition) {
                    *partition += 1;
                    if state.active {
                        let closed_at = state
                            .last_seen_at
                            .or(state.opened_at)
                            .unwrap_or(SystemTime::UNIX_EPOCH);
                        if let Some(batch) = Self::take_batch(state, closed_at, stats) {
                            return Ok(Some(batch));
                        }
                    }
                }
                Ok(terminal.take().map(StreamData::control))
            }
            StateWindowOutput::Rows(rows) => {
                for tuple in rows.by_ref() {
                    let key = match eval_partition_key(
                        &self.config.partition_by_scalars,
                        &tuple,
                        "statewindow",
                    ) {
                        Ok(key) => key,
                        Err(error) => {
                            stats.record_error_logged(
                                "streaming state aggregation processor error",
                                error,
                            );
                            continue;
                        }
                    };
                    let next = self.states.len();
                    let index = *self.ids.entry(key).or_insert_with(|| {
                        self.states.push(PartitionAggState {
                            active: false,
                            opened_at: None,
                            last_seen_at: None,
                            worker: AggregationWorker::new(
                                Arc::clone(&self.physical),
                                Arc::clone(&self.registry),
                                self.group_by.clone(),
                            ),
                        });
                        next
                    });
                    let open = match eval_state_condition(
                        &self.config.open_scalar,
                        &tuple,
                        "open",
                        Some(&self.config.open_expr),
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            stats.record_error_logged(
                                "streaming state aggregation processor error",
                                error,
                            );
                            continue;
                        }
                    };
                    let emit = match eval_state_condition(
                        &self.config.emit_scalar,
                        &tuple,
                        "emit",
                        Some(&self.config.emit_expr),
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            stats.record_error_logged(
                                "streaming state aggregation processor error",
                                error,
                            );
                            continue;
                        }
                    };
                    let state = &mut self.states[index];
                    if !state.active {
                        if open {
                            state.active = true;
                            state.opened_at = Some(tuple.timestamp);
                            state.last_seen_at = Some(tuple.timestamp);
                            if let Err(error) = state.worker.update_groups(&tuple) {
                                stats.record_error_logged(
                                    "streaming state aggregation processor error",
                                    error,
                                );
                            }
                        }
                        continue;
                    }
                    if let Err(error) = state.worker.update_groups(&tuple) {
                        stats.record_error_logged(
                            "streaming state aggregation processor error",
                            error,
                        );
                        continue;
                    }
                    state.last_seen_at = Some(tuple.timestamp);
                    if emit {
                        if let Some(batch) = Self::take_batch(state, tuple.timestamp, stats) {
                            return Ok(Some(batch));
                        }
                    }
                }
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::processor::base::*;
    use crate::processor::*;

    use crate::planner::physical::*;

    use super::*;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ScalarExpr;
    use crate::planner::physical::AggregateCall;
    use crate::planner::physical::PhysicalStreamingAggregation;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::runtime::TaskSpawner;
    use datatypes::{BooleanType, ConcreteDatatype, Value};
    use sqlparser::ast::{Expr, Ident};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn lit_bool(v: bool) -> ScalarExpr {
        ScalarExpr::Literal(Value::Bool(v), ConcreteDatatype::Bool(BooleanType))
    }

    fn col(name: &str) -> ScalarExpr {
        ScalarExpr::Column(ColumnRef::ByName {
            column_name: name.to_string(),
        })
    }

    fn tuple_with(cols: &[(&str, Value)]) -> crate::model::Tuple {
        let mut tuple = crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            std::time::UNIX_EPOCH,
        );
        for (k, v) in cols {
            tuple.add_affiliate_column(Arc::new((*k).to_string()), v.clone());
        }
        tuple
    }

    fn make_physical(
        open: ScalarExpr,
        emit: ScalarExpr,
        partition_by: Vec<ScalarExpr>,
    ) -> Arc<PhysicalStreamingAggregation> {
        let call = AggregateCall {
            supports_inline_execution: true,
            output_column: "col_1".to_string(),
            func_name: "sum".to_string(),
            args: vec![col("a")],
            distinct: false,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "col_1".to_string(),
            Expr::Function(sqlparser::ast::Function {
                name: sqlparser::ast::ObjectName(vec![Ident::new("sum")]),
                args: vec![sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(Ident::new("a"))),
                )],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            }),
        );

        let window = StreamingWindowSpec::State {
            open_expr: Expr::Identifier(Ident::new("open")),
            emit_expr: Expr::Identifier(Ident::new("emit")),
            partition_by_exprs: vec![],
            open_scalar: open,
            emit_scalar: emit,
            partition_by_scalars: partition_by,
        };

        Arc::new(PhysicalStreamingAggregation::new(
            window,
            mappings,
            Vec::new(),
            vec![call],
            Vec::new(),
            Vec::new(),
            0,
        ))
    }

    #[tokio::test]
    async fn streaming_state_agg_open_then_emit_outputs_sum() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical(lit_bool(true), lit_bool(true), Vec::new());

        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "s".into(),
            &[
                crate::planner::physical::PhysicalPlan::StreamingAggregation(
                    (Arc::clone(&physical)).as_ref().clone(),
                ),
            ],
            crate::processor::region_processor::RegionResources {
                aggregate_registry: Some(Arc::clone(&aggregate_registry)),
                ..Default::default()
            },
            crate::processor::base::default_channel_capacities(),
        )
        .expect("state processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // First tuple opens (emit ignored because it was inactive).
        let batch = crate::model::RecordBatch::new(vec![tuple_with(&[("a", Value::Int64(1))])])
            .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;
        assert!(output_rx.try_recv().is_err());

        // Second tuple updates and triggers emit -> sum includes both.
        let batch = crate::model::RecordBatch::new(vec![tuple_with(&[("a", Value::Int64(2))])])
            .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        match output_rx.recv().await.unwrap() {
            StreamData::Collection(collection) => {
                assert_eq!(collection.rows().len(), 1);
                let out = collection.rows().first().unwrap();
                assert_eq!(out.value_by_name("", "col_1"), Some(&Value::Int64(3)));
            }
            other => panic!("unexpected output: {}", other.description()),
        }
    }

    #[tokio::test]
    async fn streaming_state_agg_partitioned_by_key_isolated() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical(lit_bool(true), lit_bool(true), vec![col("k")]);

        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "s".into(),
            &[
                crate::planner::physical::PhysicalPlan::StreamingAggregation(
                    (Arc::clone(&physical)).as_ref().clone(),
                ),
            ],
            crate::processor::region_processor::RegionResources {
                aggregate_registry: Some(Arc::clone(&aggregate_registry)),
                ..Default::default()
            },
            crate::processor::base::default_channel_capacities(),
        )
        .expect("state processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // Interleaved keys; each needs its own open+emit cycle.
        let batch = crate::model::RecordBatch::new(vec![
            tuple_with(&[("k", Value::Int64(1)), ("a", Value::Int64(1))]),
            tuple_with(&[("k", Value::Int64(2)), ("a", Value::Int64(10))]),
            tuple_with(&[("k", Value::Int64(1)), ("a", Value::Int64(2))]),
            tuple_with(&[("k", Value::Int64(2)), ("a", Value::Int64(20))]),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let first = output_rx.recv().await.unwrap();
        let second = output_rx.recv().await.unwrap();

        let (b1, b2) = match (first, second) {
            (StreamData::Collection(c1), StreamData::Collection(c2)) => (c1, c2),
            (StreamData::Collection(c1), other) => {
                let next = output_rx.recv().await.unwrap();
                match next {
                    StreamData::Collection(c2) => (c1, c2),
                    other2 => panic!(
                        "expected second collection, got {} then {}",
                        other.description(),
                        other2.description()
                    ),
                }
            }
            (other1, other2) => panic!(
                "expected collections, got {} and {}",
                other1.description(),
                other2.description()
            ),
        };

        assert_eq!(b1.rows().len(), 1);
        assert_eq!(b2.rows().len(), 1);
        let v1 = b1
            .rows()
            .first()
            .unwrap()
            .value_by_name("", "col_1")
            .cloned()
            .unwrap_or(Value::Null);
        let v2 = b2
            .rows()
            .first()
            .unwrap()
            .value_by_name("", "col_1")
            .cloned()
            .unwrap_or(Value::Null);

        // key=1 sum is 1+2=3, key=2 sum is 10+20=30.
        let mut seen = vec![v1, v2];
        seen.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(seen, vec![Value::Int64(3), Value::Int64(30)]);
    }
}
