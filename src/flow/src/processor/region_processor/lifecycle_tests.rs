//! Lifecycle regressions migrated from the retired template executor.
use super::*;
use crate::codec::{EncodeError, SinkEncoder};
use crate::model::{batch_from_columns_simple, Collection};
use crate::planner::physical::{PhysicalBatch, PhysicalSinkEncoder};
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use crate::processor::EncodedDeliveryFlags;
use bytes::Bytes;
use datatypes::Value;
use tokio::time::{advance, Duration, Instant};

fn input() -> StreamData {
    StreamData::collection(Box::new(
        batch_from_columns_simple(vec![("test".into(), "value".into(), vec![Value::Int64(1)])])
            .unwrap(),
    ))
}

struct FixedEncoder;
impl SinkEncoder for FixedEncoder {
    fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
        Ok(None)
    }
    fn append(&mut self, _: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
        Ok(Some(Bytes::from_static(b"AB")))
    }
    fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
        Ok(None)
    }
}

fn encoder(encoder: Box<dyn SinkEncoder>, count: Option<usize>) -> Stage {
    let plan = PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
        vec![],
        0,
        "test".into(),
        SinkEncoderConfig::json(),
        CommonSinkProps {
            batch_count: count,
            batch_duration: Some(Duration::from_secs(1)),
        },
    ));
    let mut resources = RegionResources::default();
    resources.encoders.insert(
        0,
        EncoderStage::new(
            encoder,
            BatchPolicy::new(count, Some(Duration::from_secs(1))),
        ),
    );
    let mut stage = Stage::from_plan(&plan, &mut resources).unwrap();
    stage.start_clock();
    stage
}

fn collect(stage: &mut Stage, mut pending: StageOutput, output: &mut Vec<StreamData>) {
    while let Some(data) = stage.next_output(&mut pending).unwrap() {
        output.push(data);
    }
}

fn completed_payloads(output: &[StreamData]) -> Vec<Vec<u8>> {
    let mut completed = Vec::new();
    let mut pending = Vec::new();
    for data in output {
        if let StreamData::EncodedDelivery { flags, bytes } = data {
            if flags.contains(EncodedDeliveryFlags::ABORT) {
                pending.clear();
                continue;
            }
            if flags.contains(EncodedDeliveryFlags::START) {
                pending.clear();
            }
            pending.extend_from_slice(bytes);
            if flags.contains(EncodedDeliveryFlags::END) {
                completed.push(std::mem::take(&mut pending));
            }
        }
    }
    completed
}

// coverage-covers: sink.fused_stateless_sink
#[tokio::test(start_paused = true)]
async fn shared_encoder_input_checks_deadline_after_row_processing() {
    for count in [None, Some(3)] {
        for row_work_ms in [0, 5, 15] {
            let origin = Instant::now();
            let mut stage = encoder(Box::new(FixedEncoder), count);
            let mut output = Vec::new();
            let pending = stage.process(input()).unwrap();
            collect(&mut stage, pending, &mut output);
            advance(Duration::from_millis(995)).await;
            advance(Duration::from_millis(row_work_ms)).await;
            let pending = stage.process(input()).unwrap();
            collect(&mut stage, pending, &mut output);
            if row_work_ms >= 5 {
                assert_eq!(completed_payloads(&output), vec![b"AB".to_vec()]);
                assert_eq!(
                    stage.clock_deadline().unwrap().unwrap().at,
                    origin + Duration::from_secs(2)
                );
            }
            let pending = stage.finish_input().unwrap();
            collect(&mut stage, pending, &mut output);
            assert_eq!(
                completed_payloads(&output),
                if row_work_ms < 5 {
                    vec![b"ABAB".to_vec()]
                } else {
                    vec![b"AB".to_vec(), b"AB".to_vec()]
                }
            );
        }
    }
}

// coverage-covers: sink.fused_stateless_sink
#[tokio::test(start_paused = true)]
async fn shared_native_input_checks_deadline_after_row_processing() {
    for count in [None, Some(3)] {
        for row_work_ms in [0, 5, 15] {
            let origin = Instant::now();
            let plan = PhysicalPlan::Batch(PhysicalBatch::new(
                vec![],
                0,
                "test".into(),
                CommonSinkProps {
                    batch_count: count,
                    batch_duration: Some(Duration::from_secs(1)),
                },
            ));
            let mut stage = Stage::from_plan(&plan, &mut RegionResources::default()).unwrap();
            stage.start_clock();
            let mut output = Vec::new();
            let pending = stage.process(input()).unwrap();
            collect(&mut stage, pending, &mut output);
            advance(Duration::from_millis(995)).await;
            advance(Duration::from_millis(row_work_ms)).await;
            let pending = stage.process(input()).unwrap();
            collect(&mut stage, pending, &mut output);
            if row_work_ms >= 5 {
                assert_eq!(output.len(), 1);
                assert_eq!(
                    stage.clock_deadline().unwrap().unwrap().at,
                    origin + Duration::from_secs(2)
                );
            }
            let pending = stage.finish_input().unwrap();
            collect(&mut stage, pending, &mut output);
            let counts: Vec<_> = output
                .iter()
                .map(|data| data.as_collection().unwrap().num_rows())
                .collect();
            assert_eq!(counts, if row_work_ms < 5 { vec![2] } else { vec![1, 1] });
        }
    }
}

// coverage-covers: sink.fused_stateless_sink
#[tokio::test(start_paused = true)]
async fn shared_due_flush_failure_does_not_drop_the_new_input() {
    struct FailFirstFinish(bool);
    impl SinkEncoder for FailFirstFinish {
        fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
            Ok(None)
        }
        fn append(&mut self, _: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
            Ok(Some(Bytes::from_static(b"AB")))
        }
        fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
            if std::mem::take(&mut self.0) {
                Err(EncodeError::Other("old batch finish failed".into()))
            } else {
                Ok(None)
            }
        }
    }
    let origin = Instant::now();
    let mut stage = encoder(Box::new(FailFirstFinish(true)), None);
    let mut output = Vec::new();
    let pending = stage.process(input()).unwrap();
    collect(&mut stage, pending, &mut output);
    advance(Duration::from_secs(1)).await;
    let pending = stage.process(input()).unwrap();
    collect(&mut stage, pending, &mut output);
    assert_eq!(stage.stats.snapshot().error_count, 1);
    assert!(matches!(&stage.engine, CollectionStage::Encoder(engine) if engine.has_data()));
    assert_eq!(
        stage.clock_deadline().unwrap().unwrap().at,
        origin + Duration::from_secs(2)
    );
    let pending = stage.finish_input().unwrap();
    collect(&mut stage, pending, &mut output);
    assert_eq!(completed_payloads(&output), vec![b"AB".to_vec()]);
}
