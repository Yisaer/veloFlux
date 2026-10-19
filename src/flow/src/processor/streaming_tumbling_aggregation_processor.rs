use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::tumbling_window_processor::TumblingWindowOutput;
use crate::processor::window_metadata;
use crate::processor::{ProcessorError, ProcessorStats, StreamData};

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
#[cfg(test)]
use tokio::sync::broadcast;

use crate::processor::window_partition::{eval_partition_key, PartitionKey};

impl StreamingTumblingStage {
    fn extract_window_length(
        physical: &PhysicalStreamingAggregation,
    ) -> Result<Duration, ProcessorError> {
        match &physical.window {
            StreamingWindowSpec::Tumbling {
                time_unit, length, ..
            } => Ok(time_unit.duration(*length)),
            other => Err(ProcessorError::InvalidConfiguration(format!(
                "streaming tumbling aggregation requires tumbling window spec, got {other:?}",
            ))),
        }
    }
}

/// Per-window aggregation state.
struct WindowAggState {
    start: SystemTime,
    end: SystemTime,
    worker: AggregationWorker,
}

impl WindowAggState {
    fn new(
        start: SystemTime,
        end: SystemTime,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
    ) -> Self {
        Self {
            start,
            end,
            worker: AggregationWorker::new(
                Arc::clone(&physical),
                Arc::clone(&aggregate_registry),
                group_by_meta,
            ),
        }
    }
}

/// Processing-time windows assuming monotonically increasing timestamps.
struct ProcessingWindowState {
    windows: VecDeque<WindowAggState>,
    window_length: Duration,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_meta: Vec<GroupByMeta>,
}

impl ProcessingWindowState {
    fn new(
        window_length: Duration,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
    ) -> Self {
        Self {
            windows: VecDeque::new(),
            window_length,
            physical,
            aggregate_registry,
            group_by_meta,
        }
    }

    fn add_row_at(&mut self, start: SystemTime, row: &crate::model::Tuple) -> Result<(), String> {
        let end = start
            .checked_add(self.window_length)
            .ok_or_else(|| "tumblingwindow end timestamp overflow".to_string())?;
        if let Some(back) = self.windows.back_mut() {
            if back.start == start {
                return back.worker.update_groups(row);
            }
        }
        let mut new_state = WindowAggState::new(
            start,
            end,
            Arc::clone(&self.physical),
            Arc::clone(&self.aggregate_registry),
            self.group_by_meta.clone(),
        );
        new_state.worker.update_groups(row)?;
        self.windows.push_back(new_state);
        Ok(())
    }

    fn next_batch(
        &mut self,
        watermark: Option<SystemTime>,
    ) -> Result<Option<Box<dyn crate::model::Collection>>, ProcessorError> {
        while let Some(mut state) = self.windows.pop_front() {
            if watermark.is_some_and(|limit| state.end > limit) {
                self.windows.push_front(state);
                return Ok(None);
            }
            if let Some(batch) = state
                .worker
                .finalize_current_window()
                .map_err(ProcessorError::ProcessingError)?
            {
                return window_metadata::attach_from_system_time(batch, state.start, state.end)
                    .map(Some);
            }
        }
        Ok(None)
    }
}

struct PartitionedProcessingWindowState {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<ProcessingWindowState>,
    window_length: Duration,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_meta: Vec<GroupByMeta>,
    partition_by_scalars: Vec<crate::expr::ScalarExpr>,
}

enum AddRowError {
    BeforeMutation(String),
    AfterMutation(String),
}

impl PartitionedProcessingWindowState {
    fn new(
        window_length: Duration,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
        partition_by_scalars: Vec<crate::expr::ScalarExpr>,
    ) -> Self {
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
            window_length,
            physical,
            aggregate_registry,
            group_by_meta,
            partition_by_scalars,
        }
    }

    fn add_row(&mut self, row: &crate::model::Tuple) -> Result<(), AddRowError> {
        let partition_key = eval_partition_key(&self.partition_by_scalars, row, "tumblingwindow")
            .map_err(AddRowError::BeforeMutation)?;
        let window_start =
            window_metadata::floor_to_window_start(row.timestamp, self.window_length, "timestamp")
                .map_err(|err| AddRowError::BeforeMutation(err.to_string()))?;
        let window_end = window_start
            .checked_add(self.window_length)
            .ok_or_else(|| {
                AddRowError::BeforeMutation("tumblingwindow end timestamp overflow".to_string())
            })?;
        window_metadata::validate_system_time(window_end)
            .map_err(|err| AddRowError::BeforeMutation(err.to_string()))?;
        self.get_or_insert(partition_key)
            .add_row_at(window_start, row)
            .map_err(AddRowError::AfterMutation)
    }

    fn next_batch(
        &mut self,
        partition: &mut usize,
        watermark: Option<SystemTime>,
    ) -> Result<Option<Box<dyn crate::model::Collection>>, ProcessorError> {
        while let Some(state) = self.states.get_mut(*partition) {
            if let Some(batch) = state.next_batch(watermark)? {
                return Ok(Some(batch));
            }
            *partition += 1;
        }
        Ok(None)
    }

    fn get_or_insert(&mut self, key: PartitionKey) -> &mut ProcessingWindowState {
        if let Some(id) = self.state_ids.get(&key).copied() {
            return &mut self.states[id];
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(ProcessingWindowState::new(
            self.window_length,
            Arc::clone(&self.physical),
            Arc::clone(&self.aggregate_registry),
            self.group_by_meta.clone(),
        ));
        &mut self.states[id]
    }
}

/// Synchronous incremental tumbling aggregation shared by both execution placements.
pub(crate) struct StreamingTumblingStage {
    state: PartitionedProcessingWindowState,
    stats: Arc<ProcessorStats>,
}

impl StreamingTumblingStage {
    pub(crate) fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        registry: Arc<AggregateFunctionRegistry>,
        stats: Arc<ProcessorStats>,
    ) -> Result<Self, ProcessorError> {
        let length = StreamingTumblingStage::extract_window_length(&physical)?;
        let partition_by = match &physical.window {
            StreamingWindowSpec::Tumbling {
                partition_by_scalars,
                ..
            } => partition_by_scalars.clone(),
            other => {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "streaming tumbling aggregation requires tumbling window spec, got {other:?}",
                )))
            }
        };
        let group_by = build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        Ok(Self {
            state: PartitionedProcessingWindowState::new(
                length,
                physical,
                registry,
                group_by,
                partition_by,
            ),
            stats,
        })
    }

    pub(crate) fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    pub(crate) fn clear(&mut self) {
        self.state.state_ids.clear();
        self.state.states.clear();
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
    ) -> Result<TumblingWindowOutput, ProcessorError> {
        match data {
            StreamData::Collection(collection) => {
                for row in collection.rows() {
                    if let Err(error) = self.state.add_row(row) {
                        let message = match error {
                            AddRowError::BeforeMutation(message)
                            | AddRowError::AfterMutation(message) => message,
                        };
                        self.stats.record_error_logged(
                            "streaming tumbling aggregation processor error",
                            message,
                        );
                    }
                }
                Ok(TumblingWindowOutput::Single(None))
            }
            StreamData::Watermark(watermark) => {
                match window_metadata::validate_system_time(watermark) {
                    Ok(()) => Ok(TumblingWindowOutput::Drain {
                        partition: 0,
                        watermark: Some(watermark),
                        terminal: None,
                    }),
                    Err(ProcessorError::ProcessingError(message)) => {
                        self.stats.record_error_logged(
                            "streaming tumbling aggregation processor error",
                            message,
                        );
                        Ok(TumblingWindowOutput::Single(None))
                    }
                    Err(error) => Err(error),
                }
            }
            StreamData::Control(signal) if signal.is_terminal() => {
                if signal.is_graceful_end() {
                    Ok(TumblingWindowOutput::Drain {
                        partition: 0,
                        watermark: None,
                        terminal: Some(signal),
                    })
                } else {
                    self.clear();
                    Ok(TumblingWindowOutput::Single(Some(StreamData::control(
                        signal,
                    ))))
                }
            }
            other => Ok(TumblingWindowOutput::Single(Some(other))),
        }
    }

    pub(crate) fn next_output(
        &mut self,
        output: &mut TumblingWindowOutput,
    ) -> Result<Option<StreamData>, ProcessorError> {
        match output {
            TumblingWindowOutput::Single(data) => Ok(data.take()),
            TumblingWindowOutput::Drain {
                partition,
                watermark,
                terminal,
            } => {
                if let Some(batch) = self.state.next_batch(partition, *watermark)? {
                    return Ok(Some(StreamData::collection(batch)));
                }
                Ok(terminal.take().map(StreamData::control))
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
    use crate::aggregation::AggregateFunctionRegistry;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ScalarExpr;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::AggregateCall;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::runtime::TaskSpawner;
    use datatypes::Value;
    use sqlparser::ast::{Expr, Ident};
    use std::collections::HashMap;
    use std::time::UNIX_EPOCH;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn col(name: &str) -> ScalarExpr {
        ScalarExpr::Column(ColumnRef::ByName {
            column_name: name.to_string(),
        })
    }

    fn tuple_at(sec: u64, cols: &[(&str, Value)]) -> crate::model::Tuple {
        let mut tuple = crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        );
        for (k, v) in cols {
            tuple.add_affiliate_column(Arc::new((*k).to_string()), v.clone());
        }
        tuple
    }

    fn tuple_at_millis(millis: u64, cols: &[(&str, Value)]) -> crate::model::Tuple {
        let mut tuple = crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(millis),
        );
        for (key, value) in cols {
            tuple.add_affiliate_column(Arc::new((*key).to_string()), value.clone());
        }
        tuple
    }

    fn make_physical() -> Arc<PhysicalStreamingAggregation> {
        make_physical_with_window(TimeUnit::Seconds, 10)
    }

    fn make_physical_with_window(
        time_unit: TimeUnit,
        length: u64,
    ) -> Arc<PhysicalStreamingAggregation> {
        let call = AggregateCall {
            supports_inline_execution: true,
            output_column: "sum_a".to_string(),
            func_name: "sum".to_string(),
            args: vec![col("a")],
            distinct: false,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "sum_a".to_string(),
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

        Arc::new(PhysicalStreamingAggregation::new(
            StreamingWindowSpec::Tumbling {
                time_unit,
                length,
                partition_by_exprs: Vec::new(),
                partition_by_scalars: Vec::new(),
            },
            mappings,
            vec![Expr::Nested(Box::new(Expr::Identifier(Ident::new("b"))))],
            vec![call],
            vec![col("b")],
            Vec::new(),
            0,
        ))
    }

    fn extract_grouped_rows(collection: &dyn crate::model::Collection) -> Vec<(i64, i64)> {
        let mut rows = collection
            .rows()
            .iter()
            .map(|tuple| {
                let group = tuple
                    .value_by_name("", "(b)")
                    .cloned()
                    .expect("group-by value");
                let sum = tuple
                    .value_by_name("", "sum_a")
                    .cloned()
                    .expect("aggregate value");
                let Value::Int64(group) = group else {
                    panic!("expected Int64 group value, got {group:?}");
                };
                let Value::Int64(sum) = sum else {
                    panic!("expected Int64 aggregate value, got {sum:?}");
                };
                (group, sum)
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|(group, _)| *group);
        rows
    }

    #[tokio::test]
    async fn tumbling_aggregation_flushes_grouped_rows_on_watermark() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical();

        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "tumbling".into(),
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
        .expect("tumbling processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let first_window = crate::model::RecordBatch::new(vec![
            tuple_at(1, &[("a", Value::Int64(2)), ("b", Value::Int64(1))]),
            tuple_at(2, &[("a", Value::Int64(4)), ("b", Value::Int64(2))]),
            tuple_at(3, &[("a", Value::Int64(3)), ("b", Value::Int64(1))]),
        ])
        .expect("first batch");
        assert!(input
            .send(StreamData::collection(Box::new(first_window)))
            .is_ok());
        assert!(input
            .send(StreamData::Watermark(UNIX_EPOCH + Duration::from_secs(10)))
            .is_ok());

        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(first) = first else {
            panic!("expected first window collection");
        };
        assert_eq!(extract_grouped_rows(first.as_ref()), vec![(1, 5), (2, 4)]);

        let second_window = crate::model::RecordBatch::new(vec![tuple_at(
            11,
            &[("a", Value::Int64(7)), ("b", Value::Int64(2))],
        )])
        .expect("second batch");
        assert!(input
            .send(StreamData::collection(Box::new(second_window)))
            .is_ok());
        assert!(input
            .send(StreamData::Watermark(UNIX_EPOCH + Duration::from_secs(20)))
            .is_ok());

        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(second) = second else {
            panic!("expected second window collection");
        };
        assert_eq!(extract_grouped_rows(second.as_ref()), vec![(2, 7)]);
    }

    #[tokio::test]
    async fn tumbling_aggregation_graceful_end_flushes_open_window_before_terminal_control() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical();

        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "tumbling".into(),
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
        .expect("tumbling processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_at(1, &[("a", Value::Int64(2)), ("b", Value::Int64(1))]),
            tuple_at(2, &[("a", Value::Int64(3)), ("b", Value::Int64(1))]),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");

        let StreamData::Collection(collection) = first else {
            panic!("expected tumbling aggregation output before terminal control");
        };
        assert_eq!(extract_grouped_rows(collection.as_ref()), vec![(1, 5)]);
        assert!(matches!(
            second,
            StreamData::Control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn millisecond_tumbling_aggregation_uses_exact_boundaries_and_metadata() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical_with_window(TimeUnit::Milliseconds, 100);
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "tumbling".into(),
            &[
                crate::planner::physical::PhysicalPlan::StreamingAggregation(
                    (physical).as_ref().clone(),
                ),
            ],
            crate::processor::region_processor::RegionResources {
                aggregate_registry: Some(aggregate_registry),
                ..Default::default()
            },
            crate::processor::base::default_channel_capacities(),
        )
        .expect("tumbling processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_at_millis(1_050, &[("a", Value::Int64(2)), ("b", Value::Int64(1))]),
            tuple_at_millis(1_099, &[("a", Value::Int64(3)), ("b", Value::Int64(1))]),
            tuple_at_millis(1_100, &[("a", Value::Int64(7)), ("b", Value::Int64(1))]),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_099),
            ))
            .is_ok());
        assert!(
            timeout(Duration::from_millis(100), output_rx.recv())
                .await
                .is_err(),
            "window must not flush before its end boundary"
        );

        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_100),
            ))
            .is_ok());
        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(first) = first else {
            panic!("expected first millisecond tumbling aggregation");
        };
        assert_eq!(extract_grouped_rows(first.as_ref()), vec![(1, 5)]);
        let metadata = first.metadata().window.as_deref().expect("window metadata");
        assert_eq!(metadata.start.epoch_micros(), 1_000_000);
        assert_eq!(metadata.end.epoch_micros(), 1_100_000);

        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_200),
            ))
            .is_ok());
        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(second) = second else {
            panic!("expected second millisecond tumbling aggregation");
        };
        assert_eq!(extract_grouped_rows(second.as_ref()), vec![(1, 7)]);
    }
}
