//! Encryption stage and region regression tests.

#[cfg(test)]
use tokio::sync::broadcast;

#[cfg(test)]
mod tests {
    use crate::processor::byte_stage::EncryptDriver;
    use crate::processor::delivery_transform::{encrypt_delivery_step, EncryptDelivery};
    use bytes::Bytes;
    use std::sync::Arc;
    fn handle_delivery(
        driver: &mut EncryptDriver,
        delivery: &mut EncryptDelivery,
        flags: EncodedDeliveryFlags,
        bytes: Bytes,
        output: &mut Vec<StreamData>,
        _: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        let step = encrypt_delivery_step(
            driver,
            delivery,
            flags,
            bytes,
            Some(&TransformMetrics::new(stats, SINK_ENCRYPT_METRICS)),
        );
        if let Some(frame) = step.output {
            output.push(frame);
        }
        step.error.map_or(Ok(()), Err)
    }

    use crate::processor::base::*;
    use crate::processor::data_metrics::*;
    use crate::processor::*;

    use super::*;
    use crate::codec::{
        AesGcmStreamWriter, InlineEncryptionKey, SecretEncoding, SinkEncryptionConfig,
    };
    use crate::runtime::TaskSpawner;
    use tokio::runtime::Handle;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::from_handle(Handle::current())
    }

    fn writer() -> AesGcmStreamWriter {
        let config = SinkEncryptionConfig::aes_gcm(
            "sink-aes-v1",
            InlineEncryptionKey::new(hex::encode([4u8; 32]), SecretEncoding::Hex),
        )
        .expect("encryption config");
        AesGcmStreamWriter::from_config(&config).expect("writer")
    }

    async fn recv_output(output: &mut LinkReceiver<StreamData>) -> StreamData {
        timeout(Duration::from_secs(2), output.recv())
            .await
            .expect("output timeout")
            .expect("output")
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn single_chunk_delivery_forwards_start_end() {
        let mut output = Vec::new();
        let stats = Arc::new(ProcessorStats::default());
        let mut driver = EncryptDriver::new(Box::new(writer()));
        let mut delivery = EncryptDelivery::default();

        handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::START | EncodedDeliveryFlags::END,
            Bytes::from_static(b"hello"),
            &mut output,
            16,
            &stats,
        )
        .expect("handle delivery");

        let StreamData::EncodedDelivery { flags, bytes } = output.remove(0) else {
            panic!("expected encoded delivery")
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));
        assert!(flags.contains(EncodedDeliveryFlags::END));
        assert!(!flags.contains(EncodedDeliveryFlags::ABORT));
        assert!(!bytes.is_empty());
        assert!(!delivery.active);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.records_in, None);
        assert_eq!(snapshot.records_out, None);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn multi_chunk_empty_middle_and_empty_end_lifecycle() {
        let mut output = Vec::new();
        let stats = Arc::new(ProcessorStats::default());
        let mut driver = EncryptDriver::new(Box::new(writer()));
        let mut delivery = EncryptDelivery::default();

        handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::START,
            Bytes::new(),
            &mut output,
            16,
            &stats,
        )
        .expect("start");
        let StreamData::EncodedDelivery { flags, bytes } = output.remove(0) else {
            panic!("expected start")
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));
        assert!(!flags.contains(EncodedDeliveryFlags::END));
        assert!(
            !bytes.is_empty(),
            "START should carry encrypted stream header"
        );

        handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::empty(),
            Bytes::new(),
            &mut output,
            16,
            &stats,
        )
        .expect("empty middle");
        assert!(output.is_empty());

        handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::END,
            Bytes::new(),
            &mut output,
            16,
            &stats,
        )
        .expect("end");
        let StreamData::EncodedDelivery { flags, bytes } = output.remove(0) else {
            panic!("expected end")
        };
        assert!(flags.contains(EncodedDeliveryFlags::END));
        assert!(!flags.contains(EncodedDeliveryFlags::START));
        assert!(!bytes.is_empty(), "END should carry final encrypted frame");
        assert!(!delivery.active);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn abort_forwarding_depends_on_downstream_start() {
        let mut output = Vec::new();
        let stats = Arc::new(ProcessorStats::default());
        let mut driver = EncryptDriver::new(Box::new(writer()));
        let mut delivery = EncryptDelivery::default();

        handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::ABORT,
            Bytes::new(),
            &mut output,
            16,
            &stats,
        )
        .expect("abort before start");
        assert!(output.is_empty());

        handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::START,
            Bytes::from_static(b"hello"),
            &mut output,
            16,
            &stats,
        )
        .expect("start");
        let _ = output.remove(0);

        handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::ABORT,
            Bytes::new(),
            &mut output,
            16,
            &stats,
        )
        .expect("abort after start");
        let StreamData::EncodedDelivery { flags, bytes } = output.remove(0) else {
            panic!("expected abort")
        };
        assert!(flags.contains(EncodedDeliveryFlags::ABORT));
        assert!(bytes.is_empty());
        assert!(!delivery.active);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn protocol_errors_reset_and_forward_abort_when_needed() {
        let mut output = Vec::new();
        let stats = Arc::new(ProcessorStats::default());
        let mut driver = EncryptDriver::new(Box::new(writer()));
        let mut delivery = EncryptDelivery::default();

        let err = handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::empty(),
            Bytes::from_static(b"chunk"),
            &mut output,
            16,
            &stats,
        )
        .expect_err("chunk without start");
        assert!(err.to_string().contains("without active delivery"));

        handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::START,
            Bytes::from_static(b"hello"),
            &mut output,
            16,
            &stats,
        )
        .expect("start");
        let _ = output.remove(0);

        let err = handle_delivery(
            &mut driver,
            &mut delivery,
            EncodedDeliveryFlags::START,
            Bytes::from_static(b"again"),
            &mut output,
            16,
            &stats,
        )
        .expect_err("second start");
        assert!(err.to_string().contains("already active"));
        let StreamData::EncodedDelivery { flags, .. } = output.remove(0) else {
            panic!("expected abort")
        };
        assert!(flags.contains(EncodedDeliveryFlags::ABORT));
        assert!(!delivery.active);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn terminal_mid_delivery_forwards_abort_without_end() {
        let mut processor = crate::processor::test_support::encrypted_region(
            "terminal_mid_delivery",
            default_channel_capacities(),
        );
        let (input, input_rx) = broadcast::channel(16);
        let (control, control_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().unwrap();
        let mut control_output = processor.subscribe_control_output().unwrap();
        let handle = processor.start(&test_spawner());

        assert!(input
            .send(StreamData::encoded_delivery_start(b"hello".to_vec()))
            .is_ok());
        let StreamData::EncodedDelivery { flags, .. } = recv_output(&mut output).await else {
            panic!("expected start output")
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));

        assert!(control
            .send(ControlSignal::Instant(
                crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 1 },
            ))
            .is_ok());
        drop(input);

        let StreamData::EncodedDelivery { flags, bytes } = recv_output(&mut output).await else {
            panic!("expected abort output")
        };
        assert!(flags.contains(EncodedDeliveryFlags::ABORT));
        assert!(bytes.is_empty());

        let control_signal = timeout(Duration::from_secs(2), control_output.recv())
            .await
            .expect("control timeout")
            .expect("control signal");
        assert!(control_signal.is_terminal());
        handle.await.expect("join").expect("processor result");
    }
}
