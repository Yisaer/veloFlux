//! Sliding-window stage and region regression tests.
//!
//! Processing-time mode assumes tuple timestamps are non-decreasing.
//! Window flushing for lookahead windows is driven by incoming watermarks.

#[cfg(test)]
use crate::expr::ScalarExpr;
#[cfg(test)]
use std::time::SystemTime;
#[cfg(test)]
use tokio::sync::broadcast;

#[cfg(test)]
mod tests {
    use crate::processor::base::*;
    use crate::processor::*;

    use crate::planner::physical::*;

    use tokio::time::*;

    use super::*;
    use crate::expr::func::BinaryFunc;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ProcStateField;
    use crate::planner::logical::TimeUnit;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::processor::processor_state::ProcessorState;
    use crate::runtime::TaskSpawner;
    use datatypes::{ConcreteDatatype, Int64Type, Value};
    use sqlparser::ast::{Expr, Ident};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
    use tokio::time::timeout;

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

    fn tuple_at_millis(millis: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(millis),
        )
    }

    fn tuple_with_a_at(timestamp: SystemTime, value: i64) -> crate::model::Tuple {
        let mut tuple =
            crate::model::Tuple::with_timestamp(crate::model::Tuple::empty_messages(), timestamp);
        tuple.add_affiliate_column(Arc::new("a".to_string()), Value::Int64(value));
        tuple
    }

    fn a_greater_than_five() -> ScalarExpr {
        ScalarExpr::Column(ColumnRef::ByName {
            column_name: "a".to_string(),
        })
        .call_binary(
            ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type)),
            BinaryFunc::Gt,
        )
    }

    // coverage-covers: stream.window.sliding
    #[tokio::test]
    async fn sliding_window_without_lookahead_emits_on_data() {
        let spawner = test_spawner();
        let physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, None, Vec::new(), 0);
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::SlidingWindow(
                physical,
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(100), tuple_at(105)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut seen = Vec::new();
        for _ in 0..2 {
            match output_rx.recv().await.unwrap() {
                StreamData::Collection(collection) => {
                    seen.push(collection.rows().len());
                }
                _ => panic!("unexpected output"),
            }
        }

        // For t=100, window [90,100] contains 1 row; for t=105, window [95,105] contains 2 rows.
        assert_eq!(seen, vec![1, 2]);
    }

    #[tokio::test]
    async fn sliding_window_updates_last_hit_time_after_trigger_hit() {
        let spawner = test_spawner();
        let state = Arc::new(ProcessorState::new());
        let mut physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, None, Vec::new(), 0);
        physical.trigger_condition_scalar = Some(ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            }),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(100_500),
                ConcreteDatatype::Int64(Int64Type),
            )),
        });
        physical.trigger_processor_state = Some(Arc::clone(&state));
        physical.trigger_state_usage = PipelineStateUsage {
            last_hit_time_unix_ms: true,
            ..PipelineStateUsage::default()
        };
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::SlidingWindow(
                physical,
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(100), tuple_at(101), tuple_at(102)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut seen = Vec::new();
        for _ in 0..2 {
            match timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv")
            {
                StreamData::Collection(collection) => seen.push(collection.rows().len()),
                other => panic!("unexpected output: {}", other.description()),
            }
        }

        assert_eq!(seen, vec![1, 2]);
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 101_000);
        assert!(output_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn sliding_window_skips_trigger_hit_state_error_before_lookahead_mutation() {
        let spawner = test_spawner();
        let state = Arc::new(ProcessorState::new());
        let mut physical =
            PhysicalSlidingWindow::new(TimeUnit::Seconds, 2, Some(10), Vec::new(), 0);
        physical.trigger_condition_scalar = Some(ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            }),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(100_000),
                ConcreteDatatype::Int64(Int64Type),
            )),
        });
        physical.trigger_processor_state = Some(Arc::clone(&state));
        physical.trigger_state_usage = PipelineStateUsage {
            last_hit_time_unix_ms: true,
            ..PipelineStateUsage::default()
        };
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::SlidingWindow(
                physical,
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

        let bad_timestamp = UNIX_EPOCH
            .checked_sub(Duration::from_secs(5))
            .expect("pre-epoch timestamp");
        let good_timestamp = UNIX_EPOCH + Duration::from_secs(20);
        let batch = crate::model::RecordBatch::new(vec![
            tuple_with_a_at(bad_timestamp, 1),
            tuple_with_a_at(good_timestamp, 2),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::watermark(UNIX_EPOCH + Duration::from_secs(30)))
            .is_ok());

        let item = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(collection) = item else {
            panic!("expected sliding window collection");
        };
        assert_eq!(collection.rows().len(), 1);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.error_count, 1);
        assert!(snapshot
            .last_error
            .as_deref()
            .is_some_and(|err| err.contains("invalid timestamp")));
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 20_000);
        assert!(output_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn sliding_window_with_lookahead_waits_for_watermark() {
        let spawner = test_spawner();
        let physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, Some(15), Vec::new(), 0);
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::SlidingWindow(
                physical,
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(100), tuple_at(110), tuple_at(115)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        // No output until watermark reaches end of first window: 100 + 15 = 115.
        assert!(input
            .send(StreamData::watermark(UNIX_EPOCH + Duration::from_secs(114)))
            .is_ok());

        assert!(output_rx.try_recv().is_err());

        assert!(input
            .send(StreamData::watermark(UNIX_EPOCH + Duration::from_secs(115)))
            .is_ok());

        let out = output_rx.recv().await.unwrap();
        match out {
            StreamData::Collection(collection) => {
                // First trigger at 100: window [90,115] includes all 3 rows.
                assert_eq!(collection.rows().len(), 3);
            }
            _ => panic!("unexpected output"),
        }
    }

    #[tokio::test]
    async fn sliding_window_with_lookahead_graceful_end_flushes_pending_windows_before_terminal_control(
    ) {
        let spawner = test_spawner();
        let physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, Some(15), Vec::new(), 0);
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::SlidingWindow(
                physical,
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(100), tuple_at(110), tuple_at(115)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let mut window_sizes = Vec::new();
        loop {
            match timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv")
            {
                StreamData::Collection(collection) => window_sizes.push(collection.rows().len()),
                StreamData::Control(ControlSignal::Barrier(
                    crate::processor::BarrierControlSignal::StreamGracefulEnd { .. },
                )) => break,
                other => panic!("unexpected output: {}", other.description()),
            }
        }

        assert_eq!(window_sizes, vec![3, 3, 2]);
    }

    #[tokio::test]
    async fn sliding_window_validates_end_only_for_trigger_tuple() {
        let max_timestamp_secs = u64::try_from(i64::MAX).expect("i64 max fits u64") / 1_000_000;
        let lookahead_secs = 10;
        let first_timestamp =
            UNIX_EPOCH + Duration::from_secs(max_timestamp_secs.saturating_sub(lookahead_secs));
        let second_timestamp =
            UNIX_EPOCH + Duration::from_secs(max_timestamp_secs.saturating_sub(1));
        let watermark = UNIX_EPOCH + Duration::from_secs(max_timestamp_secs);

        for (second_value, expected_rows, expected_errors) in [(1, 2, 0), (10, 1, 1)] {
            let spawner = test_spawner();
            let physical = PhysicalSlidingWindow::new_with_trigger(
                TimeUnit::Seconds,
                2,
                Some(lookahead_secs),
                Vec::new(),
                Vec::new(),
                Some(Expr::Identifier(Ident::new("a_gt_5"))),
                Some(a_greater_than_five()),
                Vec::new(),
                0,
            );
            let mut processor = crate::processor::region_processor::RegionProcessor::new(
                "sw".into(),
                &[crate::planner::physical::PhysicalPlan::SlidingWindow(
                    physical,
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

            let batch = crate::model::RecordBatch::new(vec![
                tuple_with_a_at(first_timestamp, 10),
                tuple_with_a_at(second_timestamp, second_value),
            ])
            .expect("batch");
            assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
            assert!(input.send(StreamData::watermark(watermark)).is_ok());

            let item = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            let StreamData::Collection(collection) = item else {
                panic!("expected sliding window collection");
            };
            assert_eq!(collection.rows().len(), expected_rows);

            let snapshot = stats.snapshot();
            assert_eq!(snapshot.error_count, expected_errors);
            assert_eq!(snapshot.last_error.is_some(), expected_errors > 0);
        }
    }

    #[tokio::test]
    async fn millisecond_sliding_window_uses_exact_boundaries_and_metadata() {
        let spawner = test_spawner();
        let physical = PhysicalSlidingWindow::new(TimeUnit::Milliseconds, 100, None, Vec::new(), 0);
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "sw".into(),
            &[crate::planner::physical::PhysicalPlan::SlidingWindow(
                physical,
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // lookback=100ms, no lookahead => each tuple emits a [t-100ms, t] window immediately.
        let batch = crate::model::RecordBatch::new(vec![
            tuple_at_millis(150),
            tuple_at_millis(199),
            tuple_at_millis(250),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut counts = Vec::new();
        for _ in 0..3 {
            match output_rx.recv().await.unwrap() {
                StreamData::Collection(collection) => {
                    let metadata = collection
                        .metadata()
                        .window
                        .as_deref()
                        .expect("window metadata");
                    starts.push(metadata.start.epoch_micros());
                    ends.push(metadata.end.epoch_micros());
                    counts.push(collection.rows().len());
                }
                _ => panic!("unexpected output"),
            }
        }

        assert_eq!(starts, vec![50_000, 99_000, 150_000]);
        assert_eq!(ends, vec![150_000, 199_000, 250_000]);
        assert_eq!(counts, vec![1, 2, 3]);
    }
}
