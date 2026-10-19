//! Batch stage and region regression tests.

#[cfg(test)]
use datatypes::Value;
#[cfg(test)]
use tokio::sync::broadcast;

#[cfg(test)]
mod tests {
    use crate::processor::base::*;
    use crate::processor::batch_policy::BatchPolicy;
    use crate::processor::*;

    use tokio::time::*;

    use super::*;
    use crate::model::batch_from_columns_simple;
    use crate::processor::base::{DEFAULT_CONTROL_CHANNEL_CAPACITY, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::runtime::TaskSpawner;
    use tokio::time::Duration;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    #[tokio::test]
    async fn test_batch_processor_count_only() {
        let spawner = test_spawner();
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "batch_count".into(),
            &[crate::planner::physical::PhysicalPlan::Batch(
                crate::planner::physical::PhysicalBatch::new(
                    vec![],
                    0,
                    "test".into(),
                    crate::planner::sink::CommonSinkProps {
                        batch_count: Some(2),
                        batch_duration: None,
                    },
                ),
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (tx, rx) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(rx);
        let (control_tx, control_rx) = broadcast::channel(DEFAULT_CONTROL_CHANNEL_CAPACITY);
        processor.add_control_input(control_rx);
        processor.output.subscribe(); // ensure there is at least one subscriber
        let mut output = processor
            .subscribe_output()
            .expect("output available to subscribe");
        processor.start(&spawner);

        let data = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "val".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )])
        .expect("batch");
        let _ = tx.send(StreamData::collection(Box::new(data)));

        let first = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("first batch timeout")
            .expect("first batch missing");
        assert_eq!(
            first.as_collection().unwrap().num_rows(),
            2,
            "count-only should flush first two rows"
        );
        drop(tx);
        let second = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("second batch timeout")
            .expect("second batch missing");
        assert_eq!(
            second.as_collection().unwrap().num_rows(),
            1,
            "remaining row should flush on close"
        );
        let _ = control_tx.send(ControlSignal::Instant(
            crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 0 },
        ));
        let _ = control_tx.send(ControlSignal::Instant(
            crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 0 },
        ));
    }

    #[tokio::test]
    async fn test_batch_processor_duration_only() {
        let spawner = test_spawner();
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "batch_duration".into(),
            &[crate::planner::physical::PhysicalPlan::Batch(
                crate::planner::physical::PhysicalBatch::new(
                    vec![],
                    0,
                    "test".into(),
                    crate::planner::sink::CommonSinkProps {
                        batch_count: None,
                        batch_duration: Some(Duration::from_millis(50)),
                    },
                ),
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();
        let (tx, rx) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(rx);
        let (control_tx, control_rx) = broadcast::channel(DEFAULT_CONTROL_CHANNEL_CAPACITY);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().expect("output");
        processor.start(&spawner);

        let data = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "val".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("batch");
        let _ = tx.send(StreamData::collection(Box::new(data)));

        let batch = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("duration batch timeout")
            .expect("duration batch missing");
        assert_eq!(
            batch.as_collection().unwrap().num_rows(),
            1,
            "duration-only should flush after timeout"
        );
        let _ = control_tx.send(ControlSignal::Instant(
            crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 0 },
        ));
    }

    #[test]
    fn batch_duration_must_fit_supported_millisecond_range() {
        let too_small = BatchPolicy::validate_batch_config(None, Some(Duration::from_nanos(999)))
            .expect_err("sub-millisecond duration should be rejected");
        assert!(matches!(too_small, ProcessorError::InvalidConfiguration(_)));

        let fractional_ms =
            BatchPolicy::validate_batch_config(None, Some(Duration::from_micros(1500)))
                .expect_err("sub-millisecond precision should be rejected");
        assert!(matches!(
            fractional_ms,
            ProcessorError::InvalidConfiguration(_)
        ));

        let too_large =
            BatchPolicy::validate_batch_config(None, Some(Duration::from_secs(u64::MAX)))
                .expect_err("duration beyond u64 milliseconds should be rejected");
        assert!(matches!(too_large, ProcessorError::InvalidConfiguration(_)));
    }

    #[test]
    fn next_boundary_aligns_to_fixed_grid_and_is_right_open() {
        let d = Duration::from_secs(10);
        let origin = Instant::now();

        // Tuples arriving at different offsets within the same cell [0, 10s) all
        // resolve to the SAME boundary: the grid never re-anchors to a batch's
        // first tuple.
        for offset_ms in [1u64, 3_000, 9_999] {
            assert_eq!(
                BatchPolicy::next_boundary(origin, d, origin + Duration::from_millis(offset_ms)),
                origin + d,
            );
        }
        assert_eq!(
            BatchPolicy::next_boundary(origin, d, origin + Duration::from_nanos(9_999_999_999)),
            origin + d,
        );

        // Right-open [t, t+D): a `now` exactly on a grid line opens the new cell at
        // that line, so its boundary is one full period later.
        assert_eq!(BatchPolicy::next_boundary(origin, d, origin), origin + d);
        assert_eq!(
            BatchPolicy::next_boundary(origin, d, origin + d),
            origin + Duration::from_secs(20),
        );

        // Catch-up across missed boundaries jumps to the next future boundary.
        assert_eq!(
            BatchPolicy::next_boundary(origin, d, origin + Duration::from_secs(35)),
            origin + Duration::from_secs(40),
        );
    }
}
