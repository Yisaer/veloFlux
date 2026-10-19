//! Synchronous state-window buffering between open/emit conditions.
//!
//! Semantics:
//! - When inactive and `open == true`, start buffering (do not emit even if `emit == true`).
//! - When active, buffer every incoming tuple. If `emit == true`, emit the buffered batch and close.
//! - When inactive and `emit == true`, ignore.

use crate::planner::physical::PhysicalStateWindow;
use crate::processor::window_metadata;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};
use crate::processor::{ControlSignal, ProcessorError, ProcessorStats, StreamData};
use datatypes::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;
#[cfg(test)]
use tokio::sync::broadcast;

struct PartitionState {
    active: bool,
    opened_at: Option<SystemTime>,
    rows: VecDeque<crate::model::Tuple>,
}

/// Input-owned iteration makes each completed state window a backpressure boundary.
pub(crate) enum StateWindowOutput {
    Single(Option<StreamData>),
    Rows(std::vec::IntoIter<crate::model::Tuple>),
    Drain {
        partition: usize,
        terminal: Option<ControlSignal>,
    },
}

pub(crate) fn state_window_output(
    data: StreamData,
    stats: &ProcessorStats,
    context: &'static str,
) -> StateWindowOutput {
    match data {
        StreamData::Collection(collection) => match collection.into_rows() {
            Ok(rows) => StateWindowOutput::Rows(rows.into_iter()),
            Err(error) => {
                stats.record_error_logged(context, format!("failed to extract rows: {error}"));
                StateWindowOutput::Single(None)
            }
        },
        StreamData::Control(signal) if signal.is_graceful_end() => StateWindowOutput::Drain {
            partition: 0,
            terminal: Some(signal),
        },
        other => StateWindowOutput::Single(Some(other)),
    }
}

pub(crate) fn eval_state_condition(
    scalar: &crate::expr::ScalarExpr,
    tuple: &crate::model::Tuple,
    name: &str,
    expression: Option<&sqlparser::ast::Expr>,
) -> Result<bool, String> {
    match scalar.eval_with_tuple(tuple) {
        Ok(Value::Bool(value)) => Ok(value),
        Ok(value) => Err(match expression {
            Some(expr) => format!("statewindow {name} must be bool, got {value:?} (expr={expr})"),
            None => format!("statewindow {name} must be bool, got {value:?}"),
        }),
        Err(error) => Err(match expression {
            Some(expr) => format!("failed to evaluate statewindow {name} (expr={expr}): {error}"),
            None => format!("failed to evaluate statewindow {name}: {error}"),
        }),
    }
}

pub(crate) struct StateWindowStage {
    physical: Arc<PhysicalStateWindow>,
    ids: HashMap<PartitionKey, usize>,
    states: Vec<PartitionState>,
}

impl StateWindowStage {
    pub(crate) fn new(physical: Arc<PhysicalStateWindow>) -> Self {
        Self {
            physical,
            ids: HashMap::new(),
            states: Vec::new(),
        }
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
        state_window_output(data, stats, "state window processor error")
    }
    fn take_batch(
        state: &mut PartitionState,
        closed_at: SystemTime,
        stats: &ProcessorStats,
    ) -> Option<StreamData> {
        let rows = state.rows.drain(..).collect();
        let start = state.opened_at.unwrap_or(closed_at);
        state.active = false;
        state.opened_at = None;
        match window_metadata::record_batch_from_system_time(rows, start, closed_at) {
            Ok(batch) => Some(StreamData::collection(Box::new(batch))),
            Err(error) => {
                stats.record_error_logged("state window processor error", error.to_string());
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
                    if state.active && !state.rows.is_empty() {
                        let closed_at = state
                            .rows
                            .back()
                            .map(|r| r.timestamp)
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
                        &self.physical.partition_by_scalars,
                        &tuple,
                        "statewindow",
                    ) {
                        Ok(key) => key,
                        Err(error) => {
                            stats.record_error_logged("state window processor error", error);
                            continue;
                        }
                    };
                    let next = self.states.len();
                    let index = *self.ids.entry(key).or_insert_with(|| {
                        self.states.push(PartitionState {
                            active: false,
                            opened_at: None,
                            rows: VecDeque::new(),
                        });
                        next
                    });
                    let open = match eval_state_condition(
                        &self.physical.open_scalar,
                        &tuple,
                        "open",
                        None,
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            stats.record_error_logged("state window processor error", error);
                            continue;
                        }
                    };
                    let emit = match eval_state_condition(
                        &self.physical.emit_scalar,
                        &tuple,
                        "emit",
                        None,
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            stats.record_error_logged("state window processor error", error);
                            continue;
                        }
                    };
                    let state = &mut self.states[index];
                    if !state.active {
                        if open {
                            state.active = true;
                            state.opened_at = Some(tuple.timestamp);
                            state.rows.push_back(tuple);
                        }
                        continue;
                    }
                    let timestamp = tuple.timestamp;
                    state.rows.push_back(tuple);
                    if emit {
                        if let Some(batch) = Self::take_batch(state, timestamp, stats) {
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
    use crate::processor::*;

    use crate::planner::physical::*;

    use super::*;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ScalarExpr;
    use crate::processor::base::{Processor, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::runtime::TaskSpawner;
    use datatypes::{BooleanType, ConcreteDatatype, Value};
    use sqlparser::ast::{Expr, Ident};
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        )
    }

    fn tuple_with_key_at(sec: u64, key: i64) -> crate::model::Tuple {
        let mut tuple = tuple_at(sec);
        tuple.add_affiliate_column(std::sync::Arc::new("k".to_string()), Value::Int64(key));
        tuple
    }

    fn lit_bool(v: bool) -> ScalarExpr {
        ScalarExpr::Literal(Value::Bool(v), ConcreteDatatype::Bool(BooleanType))
    }

    // coverage-covers: stream.window.state
    #[tokio::test]
    async fn statewindow_open_then_emit_buffers_and_flushes() {
        let spawner = test_spawner();
        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            Vec::new(),
            lit_bool(true),
            lit_bool(true),
            Vec::new(),
            Vec::new(),
            0,
        );
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::StateWindow(
                Box::new(physical),
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // First tuple opens the window (emit ignored because it was inactive).
        let batch = crate::model::RecordBatch::new(vec![tuple_at(1)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;
        assert!(output_rx.try_recv().is_err());

        // Second tuple is buffered and triggers emit -> output includes both tuples.
        let batch = crate::model::RecordBatch::new(vec![tuple_at(2)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        match output_rx.recv().await.unwrap() {
            StreamData::Collection(collection) => {
                assert_eq!(collection.rows().len(), 2);
            }
            other => panic!("unexpected output: {}", other.description()),
        }
    }

    #[tokio::test]
    async fn statewindow_emit_ignored_when_inactive() {
        let spawner = test_spawner();
        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            Vec::new(),
            lit_bool(false),
            lit_bool(true),
            Vec::new(),
            Vec::new(),
            0,
        );
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::StateWindow(
                Box::new(physical),
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![tuple_at(1)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;
        assert!(output_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn statewindow_partition_by_independent_buffers() {
        let spawner = test_spawner();
        let key_expr = ScalarExpr::Column(ColumnRef::ByName {
            column_name: "k".to_string(),
        });

        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            vec![Expr::Identifier(Ident::new("k"))],
            lit_bool(true),
            lit_bool(true),
            vec![key_expr],
            Vec::new(),
            0,
        );

        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::StateWindow(
                Box::new(physical),
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_with_key_at(1, 1),
            tuple_with_key_at(2, 2),
            tuple_with_key_at(3, 1),
            tuple_with_key_at(4, 2),
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

        assert_eq!(b1.rows().len(), 2);
        assert_eq!(b2.rows().len(), 2);
        for t in b1.rows() {
            assert_eq!(t.value_by_name("", "k"), Some(&Value::Int64(1)));
        }
        for t in b2.rows() {
            assert_eq!(t.value_by_name("", "k"), Some(&Value::Int64(2)));
        }
    }

    #[tokio::test]
    async fn statewindow_partition_by_graceful_end_flushes_all_partitions() {
        let spawner = test_spawner();
        let key_expr = ScalarExpr::Column(ColumnRef::ByName {
            column_name: "k".to_string(),
        });

        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            vec![Expr::Identifier(Ident::new("k"))],
            lit_bool(true),
            lit_bool(false),
            vec![key_expr],
            Vec::new(),
            0,
        );

        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::StateWindow(
                Box::new(physical),
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();

        let stats = Arc::clone(&processor.stage_stats()[0].stats);
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_with_key_at(1, 1), tuple_with_key_at(2, 2)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;

        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let mut collections = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv")
            {
                StreamData::Collection(c) => collections.push(c),
                StreamData::Control(ControlSignal::Barrier(
                    crate::processor::BarrierControlSignal::StreamGracefulEnd { .. },
                )) => break,
                other => panic!("unexpected output: {}", other.description()),
            }
        }

        assert_eq!(collections.len(), 2);
        assert!(collections.iter().all(|c| c.rows().len() == 1));
        let mut seen = collections
            .iter()
            .flat_map(|c| c.rows().iter())
            .filter_map(|t| t.value_by_name("", "k").cloned())
            .collect::<Vec<_>>();
        seen.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(seen, vec![Value::Int64(1), Value::Int64(2)]);
        assert_eq!(stats.snapshot().records_out, Some(2));
    }
}
