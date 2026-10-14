//! Native sink lifecycle and delivery boundaries, shared with encoded sinks.
use super::*;
use crate::connector::sink::SinkConnector;
use crate::model::{Message, RecordBatch, Tuple};
use crate::processor::InstantControlSignal;
use async_trait::async_trait;
use datatypes::Value;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Calls {
    ready: AtomicUsize,
    collections: AtomicUsize,
    close: AtomicUsize,
}

struct NativeConnector {
    calls: Arc<Calls>,
    fail_first: bool,
}

#[async_trait]
impl SinkConnector for NativeConnector {
    fn id(&self) -> &str {
        "native-test"
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        self.calls.ready.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn send_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        assert_eq!(collection.num_rows(), 1);
        let n = self.calls.collections.fetch_add(1, Ordering::SeqCst);
        if self.fail_first && n == 0 {
            return Err(SinkConnectorError::Transient(
                "first collection fails".into(),
            ));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.calls.close.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn processor(forward: bool, fail_first: bool) -> (SinkProcessor, Arc<ProcessorStats>, Arc<Calls>) {
    let mut processor = SinkProcessor::new("native-sink");
    processor.set_input_domain(DataDomain::Collection);
    processor.set_retry_config(SinkRetryConfig {
        max_attempts: Some(3),
        ..Default::default()
    });
    if forward {
        processor.enable_result_forwarding();
    } else {
        processor.disable_result_forwarding();
    }
    let stats = Arc::new(ProcessorStats::default());
    processor.set_stats(Arc::clone(&stats));
    let calls = Arc::new(Calls::default());
    processor.add_connector(Box::new(NativeConnector {
        calls: Arc::clone(&calls),
        fail_first,
    }));
    (processor, stats, calls)
}

fn collection() -> StreamData {
    let message = Arc::new(Message::new(
        "source",
        vec![Arc::from("a")],
        vec![Value::Int64(1)],
    ));
    StreamData::Collection(Box::new(
        RecordBatch::new(vec![Tuple::new(vec![message])]).unwrap(),
    ))
}

async fn join(start: ProcessorStart) -> Result<(), ProcessorError> {
    tokio::time::timeout(Duration::from_secs(2), start.handle)
        .await
        .expect("sink terminates")
        .expect("join")
}

// coverage-covers: sink.connector.nop_collection, pipeline.runtime.stats
#[tokio::test]
async fn native_delivery_preserves_forwarding_metrics_and_failure_boundary() {
    for forward in [false, true] {
        for fail_first in [false, true] {
            let (mut processor, stats, calls) = processor(forward, fail_first);
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            processor.add_input(rx);
            let mut output = processor.subscribe_output().unwrap();
            let start =
                processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
            tx.send(collection()).await.unwrap();
            tx.send(collection()).await.unwrap();
            tx.send(StreamData::stream_end()).await.unwrap();
            join(start)
                .await
                .expect("native delivery continues after an error");
            let delivered = if fail_first { 1 } else { 2 };
            if forward {
                for _ in 0..delivered {
                    assert!(matches!(
                        output.recv().await.unwrap(),
                        StreamData::Collection(_)
                    ));
                }
            }
            assert!(output.recv().await.unwrap().is_terminal());
            assert_eq!(calls.ready.load(Ordering::SeqCst), 1);
            // Native collections retain single-attempt semantics even with retry configured.
            assert_eq!(calls.collections.load(Ordering::SeqCst), 2);
            assert_eq!(calls.close.load(Ordering::SeqCst), 1);
            let snapshot = stats.snapshot();
            assert_eq!(snapshot.records_in, Some(2));
            assert_eq!(snapshot.custom["collections_in"], 2);
            assert_eq!(snapshot.custom["messages_out"], delivered);
            assert_eq!(snapshot.custom["messages_dropped"], u64::from(fail_first));
            assert_eq!(snapshot.error_count, u64::from(fail_first));
            assert!(!snapshot.custom.contains_key("bytes_in"));
            assert!(!snapshot.custom.contains_key("bytes_delivered"));
        }
    }
}

// coverage-covers: sink.connector.nop_collection
#[tokio::test]
async fn native_terminal_preserves_channels_and_closes_once() {
    for terminal in ["data", "control", "closed"] {
        let (mut processor, _, calls) = processor(false, false);
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let (control_tx, control_rx) = tokio::sync::mpsc::channel(8);
        processor.add_input(rx);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().unwrap();
        let mut control_output = processor.subscribe_control_output().unwrap();
        let start = processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
        match terminal {
            "data" => {
                tx.send(StreamData::stream_end()).await.unwrap();
                assert!(output.recv().await.unwrap().is_terminal());
            }
            "control" => {
                control_tx
                    .send(ControlSignal::Instant(
                        InstantControlSignal::StreamQuickEnd { signal_id: 1 },
                    ))
                    .await
                    .unwrap();
                assert!(control_output.recv().await.unwrap().is_terminal());
            }
            _ => drop(tx),
        }
        join(start).await.unwrap();
        assert_eq!(calls.close.load(Ordering::SeqCst), 1);
    }
}

// coverage-covers: sink.connector.nop_collection
#[tokio::test]
async fn native_capture_records_the_original_empty_delivery_checkpoint() {
    let (mut processor, _, calls) = processor(false, false);
    let collector = Arc::new(CheckpointSnapshotCollector::new());
    processor.set_checkpoint_snapshot_collector(Some(Arc::clone(&collector)));
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(8);
    processor.add_input(rx);
    processor.add_control_input(control_rx);
    let _output = processor.subscribe_output().unwrap();
    let mut control_output = processor.subscribe_control_output().unwrap();
    let start = processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
    control_tx
        .send(ControlSignal::Barrier(BarrierControlSignal::SinkCapture {
            barrier_id: 1,
        }))
        .await
        .unwrap();
    assert!(control_output.recv().await.unwrap().is_sink_capture());
    tx.send(StreamData::checkpoint(7, CheckpointMode::Final))
        .await
        .unwrap();
    join(start).await.unwrap();
    let snapshots = collector.collect(7).unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(
        snapshots[0].state,
        SinkProcessor::checkpoint_state(&VecDeque::new())
    );
    assert_eq!(calls.close.load(Ordering::SeqCst), 1);
}

// coverage-covers: sink.connector.nop_collection
#[tokio::test]
async fn native_capture_keeps_rejecting_collection_delivery() {
    let (mut processor, _, calls) = processor(false, false);
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(8);
    processor.add_input(rx);
    processor.add_control_input(control_rx);
    let mut control_output = processor.subscribe_control_output().unwrap();
    let start = processor.start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()));
    control_tx
        .send(ControlSignal::Barrier(BarrierControlSignal::SinkCapture {
            barrier_id: 1,
        }))
        .await
        .unwrap();
    assert!(control_output.recv().await.unwrap().is_sink_capture());
    tx.send(collection()).await.unwrap();
    let err = join(start)
        .await
        .expect_err("native capture remains unsupported");
    assert!(err
        .to_string()
        .contains("collection-native sink delivery cannot be captured"));
    assert_eq!(calls.collections.load(Ordering::SeqCst), 0);
}
