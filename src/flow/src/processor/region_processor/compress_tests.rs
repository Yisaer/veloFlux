//! Compression region regression tests.

#[cfg(test)]
use tokio::sync::broadcast;

#[cfg(test)]
mod tests {
    use crate::processor::base::*;
    use crate::processor::*;
    use std::sync::Arc;

    use super::*;
    use crate::codec::compress::CompressionCodec;
    use crate::processor::EncodedDeliveryFlags;
    use crate::runtime::TaskSpawner;
    use tokio::runtime::Handle;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::from_handle(Handle::current())
    }

    fn gzip_processor(id: &str) -> crate::processor::region_processor::RegionProcessor {
        crate::processor::test_support::compress_region(
            id,
            CompressionCodec::gzip(),
            default_channel_capacities(),
        )
    }

    fn zstd_processor(id: &str) -> crate::processor::region_processor::RegionProcessor {
        crate::processor::test_support::compress_region(
            id,
            CompressionCodec::zstd(),
            default_channel_capacities(),
        )
    }

    async fn recv_output(
        output: &mut LinkReceiver<StreamData>,
    ) -> Result<StreamData, ProcessorError> {
        timeout(Duration::from_secs(2), output.recv())
            .await
            .map_err(|_| ProcessorError::ProcessingError("timed out waiting for output".into()))?
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))
    }

    fn decompress_gzip(data: &[u8]) -> Vec<u8> {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(data);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("gzip decompress");
        out
    }

    fn decompress_zstd(data: &[u8]) -> Vec<u8> {
        zstd::stream::decode_all(data).expect("zstd decompress")
    }

    async fn run_single_chunk_delivery(
        processor: crate::processor::region_processor::RegionProcessor,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut processor = processor;
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        assert!(input
            .send(StreamData::encoded_delivery_single(payload.to_vec()))
            .is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());

        let mut chunks: Vec<bytes::Bytes> = Vec::new();
        loop {
            let data = recv_output(&mut output).await.unwrap();
            if data.is_terminal() {
                break;
            }
            if let StreamData::EncodedDelivery { bytes, flags } = data {
                assert!(!flags.contains(EncodedDeliveryFlags::ABORT));
                chunks.push(bytes);
            }
        }
        handle.await.unwrap().unwrap();
        chunks.concat()
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn gzip_single_chunk_delivery_compresses_correctly() {
        let payload = b"hello gzip world";

        let processor = gzip_processor("gzip");
        let stats = Arc::clone(&processor.stage_stats()[0].stats);
        let compressed = run_single_chunk_delivery(processor, payload).await;
        assert_eq!(decompress_gzip(&compressed), payload);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.records_in, None);
        assert_eq!(snapshot.records_out, None);
    }

    // coverage-covers: sink.compress.zstd_delivery
    #[tokio::test]
    async fn zstd_single_chunk_delivery_compresses_correctly() {
        let payload = b"hello zstd world";
        let compressed = run_single_chunk_delivery(zstd_processor("zstd"), payload).await;
        assert_eq!(decompress_zstd(&compressed), payload);
    }

    async fn run_multi_chunk_delivery(codec: CompressionCodec, chunks: Vec<Vec<u8>>) -> Vec<u8> {
        let mut processor = crate::processor::test_support::compress_region(
            "multi",
            codec,
            default_channel_capacities(),
        );
        let (input, input_rx) = broadcast::channel(32);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        let n = chunks.len();
        for (i, chunk) in chunks.iter().enumerate() {
            let flags = if i == 0 && n == 1 {
                EncodedDeliveryFlags::START | EncodedDeliveryFlags::END
            } else if i == 0 {
                EncodedDeliveryFlags::START
            } else if i == n - 1 {
                EncodedDeliveryFlags::END
            } else {
                EncodedDeliveryFlags::empty()
            };
            assert!(input
                .send(StreamData::encoded_delivery(flags, chunk.clone()))
                .is_ok());
        }
        assert!(input.send(StreamData::stream_end()).is_ok());

        let mut out_chunks = Vec::new();
        loop {
            let data = recv_output(&mut output).await.unwrap();
            if data.is_terminal() {
                break;
            }
            if let StreamData::EncodedDelivery { bytes, flags } = data {
                assert!(!flags.contains(EncodedDeliveryFlags::ABORT));
                out_chunks.push(bytes);
            }
        }
        handle.await.unwrap().unwrap();
        out_chunks.concat()
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn gzip_multi_chunk_delivery_roundtrips() {
        let chunks = vec![
            b"chunk one ".to_vec(),
            b"chunk two ".to_vec(),
            b"chunk three".to_vec(),
        ];
        let expected: Vec<u8> = chunks.concat();
        let compressed = run_multi_chunk_delivery(CompressionCodec::gzip(), chunks).await;
        assert_eq!(decompress_gzip(&compressed), expected);
    }

    // coverage-covers: sink.compress.zstd_delivery
    #[tokio::test]
    async fn zstd_multi_chunk_delivery_roundtrips() {
        let chunks = vec![
            b"chunk one ".to_vec(),
            b"chunk two ".to_vec(),
            b"chunk three".to_vec(),
        ];
        let expected: Vec<u8> = chunks.concat();
        let compressed = run_multi_chunk_delivery(CompressionCodec::zstd(), chunks).await;
        assert_eq!(decompress_zstd(&compressed), expected);
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn abort_before_start_does_not_forward_abort() {
        let mut processor = gzip_processor("abort_before");
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        assert!(input.send(StreamData::encoded_delivery_abort()).is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());

        let terminal = recv_output(&mut output).await.unwrap();
        assert!(terminal.is_terminal());
        handle.await.unwrap().unwrap();
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn abort_after_start_forwards_abort_to_downstream() {
        let mut processor = gzip_processor("abort_after");
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        // Send START to trigger begin_delivery and emitted_chunk=true
        assert!(input
            .send(StreamData::encoded_delivery_start(b"partial".to_vec()))
            .is_ok());
        // Wait for the START to arrive downstream
        let start_out = recv_output(&mut output).await.unwrap();
        let StreamData::EncodedDelivery { flags, .. } = start_out else {
            panic!("expected encoded delivery");
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));

        // Send ABORT
        assert!(input.send(StreamData::encoded_delivery_abort()).is_ok());
        let abort_out = recv_output(&mut output).await.unwrap();
        let StreamData::EncodedDelivery { flags, .. } = abort_out else {
            panic!("expected encoded delivery for ABORT");
        };
        assert!(flags.contains(EncodedDeliveryFlags::ABORT));

        assert!(input.send(StreamData::stream_end()).is_ok());
        let terminal = recv_output(&mut output).await.unwrap();
        assert!(terminal.is_terminal());
        handle.await.unwrap().unwrap();
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn protocol_error_start_without_prior_end_returns_error() {
        let mut processor = gzip_processor("protocol_err");
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        // First START
        assert!(input
            .send(StreamData::encoded_delivery_start(b"first".to_vec()))
            .is_ok());
        let first = recv_output(&mut output).await.unwrap();
        assert!(
            matches!(&first, StreamData::EncodedDelivery { flags, .. } if flags.contains(EncodedDeliveryFlags::START))
        );

        // Second START without END → protocol error
        assert!(input
            .send(StreamData::encoded_delivery_start(b"second".to_vec()))
            .is_ok());

        // The processor should error out; drain output including possible ABORT
        let mut saw_error = false;
        for _ in 0..5 {
            match recv_output(&mut output).await {
                Ok(StreamData::EncodedDelivery { flags, .. })
                    if flags.contains(EncodedDeliveryFlags::ABORT) =>
                {
                    saw_error = true;
                    break;
                }
                Ok(StreamData::EncodedDelivery { .. }) => continue,
                Ok(data) if data.is_terminal() => {
                    saw_error = true;
                    break;
                }
                _ => break,
            }
        }
        // Either the ABORT was forwarded or the task terminated with error
        drop(input);
        let result = handle.await.unwrap();
        assert!(result.is_err() || saw_error, "expected error or abort");
    }

    // Regression test: encoder always sends END to the data channel before forwarding
    // the control terminal. With biased select the control branch could win when both
    // are ready simultaneously, causing abort_in_flight to discard a complete delivery.
    // The fix parks the terminal until the data loop drains the END.
    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn end_and_control_terminal_simultaneous_does_not_abort_delivery() {
        let mut processor = gzip_processor("drain_race");
        let (input, input_rx) = broadcast::channel(16);
        let (control, control_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().unwrap();
        let mut control_output = processor.subscribe_control_output().unwrap();
        let handle = processor.start(&test_spawner());

        // Send START and wait for it so delivery is active before we race.
        assert!(input
            .send(StreamData::encoded_delivery_start(b"hello ".to_vec()))
            .is_ok());
        let start_out = recv_output(&mut output).await.unwrap();
        assert!(
            matches!(&start_out, StreamData::EncodedDelivery { flags, .. }
                if flags.contains(EncodedDeliveryFlags::START))
        );

        // Now queue END on the data channel and terminal on the control channel
        // back-to-back without yielding — replicating what the encoder does.
        assert!(input
            .send(StreamData::encoded_delivery_end(b"world".to_vec()))
            .is_ok());
        assert!(control
            .send(ControlSignal::Instant(
                crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 1 }
            ))
            .is_ok());

        // The processor sends END data to the output channel before it forwards the
        // parked terminal to the control channel, so we can receive them sequentially.
        let end_out = recv_output(&mut output).await.unwrap();
        let StreamData::EncodedDelivery {
            flags: end_flags,
            bytes: end_bytes,
        } = end_out
        else {
            panic!("expected END delivery chunk");
        };
        assert!(
            !end_flags.contains(EncodedDeliveryFlags::ABORT),
            "last delivery must not be aborted"
        );
        assert!(end_flags.contains(EncodedDeliveryFlags::END));

        let ctrl = tokio::time::timeout(Duration::from_secs(2), control_output.recv())
            .await
            .expect("timed out waiting for terminal")
            .expect("control channel closed");
        assert!(ctrl.is_terminal());
        handle.await.unwrap().unwrap();

        let start_bytes = match start_out {
            StreamData::EncodedDelivery { bytes, .. } => bytes,
            _ => unreachable!(),
        };
        let compressed: Vec<u8> = [start_bytes.as_ref(), end_bytes.as_ref()].concat();
        assert_eq!(decompress_gzip(&compressed), b"hello world");
    }
}
