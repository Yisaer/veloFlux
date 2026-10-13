//! Unit tests for SinkProcessor retry behaviour.

#[cfg(test)]
mod tests {
    use super::super::{
        CompletedDelivery, ConnectorBinding, DeliveryAttemptOutcome, PendingDelivery,
    };
    use crate::connector::sink::{DeliveryResult, SinkConnector, SinkConnectorError};
    use crate::planner::physical::DataDomain;
    use crate::planner::sink::{SinkRetryConfig, SinkRetryUpTo};
    use crate::processor::data_metrics::SinkMetrics;
    use crate::processor::{
        EncodedDeliveryFlags, Processor, ProcessorStats, SinkProcessor, StreamData,
    };
    use crate::runtime::TaskSpawner;
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    /// Returns `Permanent` (prototype: no clone needed).
    fn permanent(msg: &str) -> SinkConnectorError {
        SinkConnectorError::Permanent(msg.into())
    }

    fn transient(msg: &str) -> SinkConnectorError {
        SinkConnectorError::Transient(msg.into())
    }

    fn uncertain(msg: &str) -> SinkConnectorError {
        SinkConnectorError::Uncertain(msg.into())
    }

    /// A connector that fails `fail_count` times before forwarding payloads.
    struct RetryMockConnector {
        fail_count: Arc<AtomicUsize>,
        attempt_count: Arc<AtomicUsize>,
        fail_with_kind: &'static str,
        sender: mpsc::Sender<Vec<u8>>,
        buffer: Option<Vec<u8>>,
        observed_properties: Arc<Mutex<Vec<String>>>,
        update_registry_after_failure: Option<crate::PropertyRegistry>,
    }

    struct RetryMockHandle {
        receiver: mpsc::Receiver<Vec<u8>>,
        observed_properties: Arc<Mutex<Vec<String>>>,
    }

    struct PendingStartConnector {
        started: mpsc::Sender<()>,
        abort_count: Arc<AtomicUsize>,
    }

    impl RetryMockConnector {
        fn new(fail_count: usize, fail_with_kind: &'static str) -> (Self, RetryMockHandle) {
            let (sender, receiver) = mpsc::channel(16);
            let observed_properties = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    fail_count: Arc::new(AtomicUsize::new(fail_count)),
                    attempt_count: Arc::new(AtomicUsize::new(0)),
                    fail_with_kind,
                    sender,
                    buffer: None,
                    observed_properties: Arc::clone(&observed_properties),
                    update_registry_after_failure: None,
                },
                RetryMockHandle {
                    receiver,
                    observed_properties,
                },
            )
        }

        fn update_registry_after_failure(mut self, registry: crate::PropertyRegistry) -> Self {
            self.update_registry_after_failure = Some(registry);
            self
        }

        fn build_error(&self) -> SinkConnectorError {
            match self.fail_with_kind {
                "transient" => transient("injected failure"),
                "uncertain" => uncertain("injected failure"),
                "permanent" => permanent("bad request"),
                _ => permanent("unknown"),
            }
        }
    }

    impl RetryMockHandle {
        async fn recv(&mut self) -> Option<Vec<u8>> {
            self.receiver.recv().await
        }
    }

    #[async_trait]
    impl SinkConnector for PendingStartConnector {
        fn id(&self) -> &str {
            "pending_start"
        }

        async fn start_delivery(
            &mut self,
            _properties: &crate::PropertyContext,
        ) -> Result<(), SinkConnectorError> {
            self.started
                .send(())
                .await
                .map_err(|err| permanent(&err.to_string()))?;
            std::future::pending().await
        }

        async fn abort_delivery(&mut self) {
            self.abort_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn pending_start_is_aborted_after_capture_cancels_attempt() {
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let abort_count = Arc::new(AtomicUsize::new(0));
        let connector = PendingStartConnector {
            started: started_tx,
            abort_count: abort_count.clone(),
        };
        let mut binding = ConnectorBinding {
            connector: Box::new(connector),
            active_delivery: false,
        };

        let properties = crate::PropertyContext::default();
        let mut attempt = Box::pin(binding.attempt_delivery(b"payload", &properties));
        tokio::select! {
            result = &mut attempt => panic!("delivery start unexpectedly completed: {result:?}"),
            started = started_rx.recv() => assert_eq!(started, Some(())),
        }
        drop(attempt);

        binding.abort_active_delivery().await;

        assert_eq!(abort_count.load(Ordering::Relaxed), 1);
    }

    #[async_trait]
    impl SinkConnector for RetryMockConnector {
        fn id(&self) -> &str {
            "retry_mock"
        }

        async fn ready(&mut self) -> Result<(), SinkConnectorError> {
            Ok(())
        }

        async fn start_delivery(
            &mut self,
            properties: &crate::PropertyContext,
        ) -> Result<(), SinkConnectorError> {
            let value = properties
                .get("vin")
                .map(|value| value.expose().to_string())
                .unwrap_or_default();
            self.observed_properties
                .lock()
                .expect("lock observed properties")
                .push(value);
            self.buffer = Some(Vec::new());
            Ok(())
        }

        async fn write_chunk(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
            let Some(buf) = self.buffer.as_mut() else {
                return Err(permanent("retry_mock: chunk without active delivery"));
            };
            buf.extend_from_slice(payload);
            Ok(())
        }

        async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
            let payload = self
                .buffer
                .take()
                .ok_or_else(|| permanent("retry_mock: finish without active delivery"))?;
            let bytes_written = payload.len() as u64;
            self.attempt_count.fetch_add(1, Ordering::Relaxed);

            if self.fail_count.load(Ordering::Relaxed) > 0 {
                self.fail_count.fetch_sub(1, Ordering::Relaxed);
                if let Some(registry) = &self.update_registry_after_failure {
                    registry
                        .apply_override("vin", crate::secret::SecretString::new("VIN-UPDATED"))
                        .expect("update vin after failed attempt");
                }
                return Err(self.build_error());
            }

            let _ = self.sender.send(payload).await;
            Ok(DeliveryResult { bytes_written })
        }

        async fn abort_delivery(&mut self) {
            self.buffer = None;
        }

        async fn close(&mut self) -> Result<(), SinkConnectorError> {
            Ok(())
        }
    }

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::from_handle(tokio::runtime::Handle::current())
    }

    fn make_encoded_delivery(payload: &[u8]) -> Vec<StreamData> {
        let split = 1.min(payload.len());
        let rest = &payload[split..];
        let mut chunks = vec![StreamData::EncodedDelivery {
            flags: EncodedDeliveryFlags::START,
            bytes: bytes::Bytes::copy_from_slice(&payload[..split]),
        }];
        chunks.push(StreamData::EncodedDelivery {
            flags: EncodedDeliveryFlags::END,
            bytes: bytes::Bytes::copy_from_slice(rest),
        });
        chunks
    }

    async fn drive_and_assert(
        handle: tokio::task::JoinHandle<Result<(), crate::processor::ProcessorError>>,
        mut handle_rx: RetryMockHandle,
        expect_success: bool,
    ) {
        if expect_success {
            let got = timeout(Duration::from_secs(10), handle_rx.recv())
                .await
                .expect("timeout")
                .expect("delivery received");
            let json: serde_json::Value = serde_json::from_slice(&got).expect("json parse");
            assert_eq!(json, serde_json::json!([{"a": 42}]));
        }
        // Don't unwrap the join handle: the processor may have exited or be idle.
        // Just ensure we don't hang.
        let _ = timeout(Duration::from_secs(5), handle).await;
    }

    fn assert_message_metrics(
        stats: &ProcessorStats,
        messages_out: u64,
        messages_dropped: u64,
        bytes_delivered: u64,
    ) {
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.records_in, None);
        assert_eq!(snapshot.records_out, None);
        assert_eq!(snapshot.custom["messages_in"], 1);
        assert_eq!(snapshot.custom["bytes_in"], 10);
        assert_eq!(snapshot.custom["messages_out"], messages_out);
        assert_eq!(snapshot.custom["messages_dropped"], messages_dropped);
        assert_eq!(snapshot.custom["bytes_delivered"], bytes_delivered);
    }

    // ── test cases ──

    #[tokio::test]
    async fn transient_failure_with_retry_succeeds() {
        let (connector, handle) = RetryMockConnector::new(2, "transient");

        let mut processor = SinkProcessor::new("retry_sink");
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        processor.set_retry_config(SinkRetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            ..Default::default()
        });
        processor.add_connector(Box::new(connector));

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        processor.add_input(rx);
        let mut start = processor.start(&test_spawner());
        let ready = start.take_ready().expect("ready");
        ready.await.expect("ready ok").expect("processor ready");

        let payload = br#"[{"a":42}]"#;
        for data in make_encoded_delivery(payload) {
            let _ = tx.send(data);
        }
        let _ = tx.send(StreamData::stream_end());

        drive_and_assert(start.handle, handle, true).await;
        assert_message_metrics(stats.as_ref(), 1, 0, payload.len() as u64);
    }

    #[tokio::test]
    async fn permanent_failure_respects_retry_policy() {
        for (retry_up_to, expected_attempts) in [
            (SinkRetryUpTo::Uncertain, 1),
            (SinkRetryUpTo::Transient, 1),
            (SinkRetryUpTo::All, 3),
        ] {
            let (connector, handle) = RetryMockConnector::new(5, "permanent");
            let attempts = Arc::clone(&connector.attempt_count);

            let mut processor = SinkProcessor::new("retry_sink");
            let stats = Arc::new(ProcessorStats::default());
            processor.set_stats(Arc::clone(&stats));
            processor.set_retry_config(SinkRetryConfig {
                retry_up_to,
                max_attempts: Some(3),
                initial_backoff_ms: 10,
                max_backoff_ms: 100,
                ..Default::default()
            });
            processor.add_connector(Box::new(connector));

            let (tx, rx) = tokio::sync::broadcast::channel(8);
            processor.add_input(rx);
            let mut start = processor.start(&test_spawner());
            let ready = start.take_ready().expect("ready");
            ready.await.expect("ready ok").expect("processor ready");

            let payload = br#"[{"a":42}]"#;
            for data in make_encoded_delivery(payload) {
                let _ = tx.send(data);
            }
            let _ = tx.send(StreamData::stream_end());

            drive_and_assert(start.handle, handle, false).await;
            assert_message_metrics(stats.as_ref(), 0, 1, 0);
            assert_eq!(
                attempts.load(Ordering::Relaxed),
                expected_attempts,
                "retry_up_to={retry_up_to:?}"
            );
        }
    }

    #[tokio::test]
    async fn default_uncertain_policy_retries_uncertain_failure() {
        let (connector, handle) = RetryMockConnector::new(2, "uncertain");
        let attempts = Arc::clone(&connector.attempt_count);
        let mut processor = SinkProcessor::new("retry_sink");
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        processor.set_retry_config(SinkRetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            ..Default::default()
        });
        processor.add_connector(Box::new(connector));

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        processor.add_input(rx);
        let mut start = processor.start(&test_spawner());
        start
            .take_ready()
            .expect("ready")
            .await
            .expect("ready ok")
            .expect("processor ready");
        let payload = br#"[{"a":42}]"#;
        for data in make_encoded_delivery(payload) {
            let _ = tx.send(data);
        }
        let _ = tx.send(StreamData::stream_end());

        drive_and_assert(start.handle, handle, true).await;
        assert_message_metrics(stats.as_ref(), 1, 0, payload.len() as u64);
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn transient_only_policy_drops_uncertain_after_first_attempt() {
        let (connector, handle) = RetryMockConnector::new(5, "uncertain");
        let attempts = Arc::clone(&connector.attempt_count);
        let mut processor = SinkProcessor::new("retry_sink");
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        processor.set_retry_config(SinkRetryConfig {
            retry_up_to: SinkRetryUpTo::Transient,
            max_attempts: Some(3),
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            ..Default::default()
        });
        processor.add_connector(Box::new(connector));

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        processor.add_input(rx);
        let mut start = processor.start(&test_spawner());
        start
            .take_ready()
            .expect("ready")
            .await
            .expect("ready ok")
            .expect("processor ready");
        for data in make_encoded_delivery(br#"[{"a":42}]"#) {
            let _ = tx.send(data);
        }
        let _ = tx.send(StreamData::stream_end());

        drive_and_assert(start.handle, handle, false).await;
        assert_message_metrics(stats.as_ref(), 0, 1, 0);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn restored_delivery_stops_replay_on_ineligible_error() {
        let config = SinkRetryConfig {
            retry_up_to: SinkRetryUpTo::Transient,
            max_attempts: Some(3),
            ..Default::default()
        };
        let (connector, _) = RetryMockConnector::new(0, "uncertain");
        let mut binding = ConnectorBinding {
            connector: Box::new(connector),
            active_delivery: false,
        };
        let stats = ProcessorStats::default();
        let metrics = SinkMetrics::new(&stats, DataDomain::Message);
        let payload = bytes::Bytes::from_static(b"payload");
        let mut pending = PendingDelivery::new(
            CompletedDelivery {
                payload,
                forward_events: Vec::new(),
            },
            crate::PropertyContext::default(),
            &config,
        );
        pending.from_checkpoint = true;
        pending.attempt = 1;

        let outcome = SinkProcessor::handle_delivery_attempt_result(
            "retry_sink",
            &stats,
            &mut binding,
            &config,
            &mut pending,
            &metrics,
            Err(uncertain("remote outcome unknown")),
        );

        assert!(matches!(outcome, DeliveryAttemptOutcome::ReplayFailed(_)));
        assert!(pending.retry_at.is_none());
        assert_eq!(stats.snapshot().custom["messages_dropped"], 0);
    }

    #[tokio::test]
    async fn exhausted_transient_retry_drops_delivery_and_continues() {
        let (connector, handle) = RetryMockConnector::new(3, "transient");

        let mut processor = SinkProcessor::new("retry_sink");
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        processor.set_retry_config(SinkRetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            ..Default::default()
        });
        processor.add_connector(Box::new(connector));

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        processor.add_input(rx);
        let mut start = processor.start(&test_spawner());
        let ready = start.take_ready().expect("ready");
        ready.await.expect("ready ok").expect("processor ready");

        let payload = br#"[{"a":42}]"#;
        for data in make_encoded_delivery(payload) {
            let _ = tx.send(data);
        }
        let _ = tx.send(StreamData::stream_end());

        // 3 failures, 3 attempts -> exhausted -> drop -> pipeline continues,
        // no data received because all attempts failed.
        drive_and_assert(start.handle, handle, false).await;
        assert_message_metrics(stats.as_ref(), 0, 1, 0);
    }

    #[tokio::test]
    async fn retry_reuses_the_original_property_snapshot() {
        let registry = crate::PropertyRegistry::new(
            BTreeMap::from([(
                "vin".to_string(),
                crate::secret::SecretString::new("VIN-INITIAL"),
            )]),
            true,
        );
        let (connector, handle) = RetryMockConnector::new(1, "transient");
        let connector = connector.update_registry_after_failure(registry.clone());
        let observed = Arc::clone(&handle.observed_properties);

        let mut processor = SinkProcessor::new("retry_sink");
        processor.set_property_registry(registry.clone());
        processor.set_retry_config(SinkRetryConfig {
            max_attempts: Some(2),
            initial_backoff_ms: 10,
            max_backoff_ms: 10,
            ..Default::default()
        });
        processor.add_connector(Box::new(connector));

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        processor.add_input(rx);
        let mut start = processor.start(&test_spawner());
        start
            .take_ready()
            .expect("ready")
            .await
            .expect("ready ok")
            .expect("processor ready");
        for data in make_encoded_delivery(br#"[{"a":42}]"#) {
            let _ = tx.send(data);
        }
        let _ = tx.send(StreamData::stream_end());

        drive_and_assert(start.handle, handle, true).await;
        assert_eq!(
            observed
                .lock()
                .expect("lock observed properties")
                .as_slice(),
            ["VIN-INITIAL", "VIN-INITIAL"]
        );
        assert_eq!(
            registry.snapshot().get("vin").expect("vin exists").expose(),
            "VIN-UPDATED"
        );
    }

    // coverage-covers: sink.fused_stateless_sink
    #[test]
    fn delivery_limit_is_checked_incrementally_before_end() {
        use super::super::{AccumulatorAction, DeliveryAccumulator};
        use bytes::Bytes;
        let mut accumulator = DeliveryAccumulator::new();
        assert!(matches!(
            accumulator
                .push(
                    EncodedDeliveryFlags::START,
                    Bytes::from_static(b"ab"),
                    false,
                    Some(4)
                )
                .expect("start"),
            AccumulatorAction::Pending
        ));
        assert!(matches!(
            accumulator
                .push(
                    EncodedDeliveryFlags::empty(),
                    Bytes::from_static(b"cd"),
                    false,
                    Some(4)
                )
                .expect("exact limit"),
            AccumulatorAction::Pending
        ));
        let error = accumulator
            .push(
                EncodedDeliveryFlags::empty(),
                Bytes::from_static(b"e"),
                false,
                Some(4),
            )
            .err()
            .expect("over limit before END");
        assert!(error.to_string().contains("exceeds connector limit 4"));
        assert!(!accumulator.is_active());
        assert!(accumulator.buffer.is_empty());
        assert!(matches!(
            accumulator
                .push(
                    EncodedDeliveryFlags::START | EncodedDeliveryFlags::END,
                    Bytes::from_static(b"ok"),
                    false,
                    Some(4)
                )
                .expect("fresh delivery"),
            AccumulatorAction::Completed(_)
        ));
    }
}
