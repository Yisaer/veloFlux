//! Synchronous barrier alignment across indexed upstreams; region owns polling.
//!
//! This processor is a dedicated operator inserted by physical plan optimization for fan-in nodes
//! (`children.len() > 1`). It forwards all data downstream, while aligning barrier-style control
//! signals per-channel before forwarding them.

use crate::processor::barrier::{align_control_signal, BarrierAligner};
use crate::processor::{
    BarrierControlSignal, BarrierControlSignalKind, ControlSignal, ProcessorError, StreamData,
};

struct PendingCheckpoint {
    checkpoint_id: u64,
    kind: BarrierControlSignalKind,
    arrived: Vec<bool>,
    signal: BarrierControlSignal,
}

enum CheckpointDataAction {
    Forward { data: StreamData },
    Pause,
    Complete { signal: BarrierControlSignal },
}

fn handle_checkpoint_data(
    pending: &mut Option<PendingCheckpoint>,
    expected_upstreams: usize,
    upstream: usize,
    data: StreamData,
    data_barrier_pending: bool,
) -> Result<CheckpointDataAction, ProcessorError> {
    if upstream >= expected_upstreams {
        return Err(ProcessorError::InvalidConfiguration(format!(
            "checkpoint barrier received from invalid upstream index {upstream}"
        )));
    }

    let checkpoint = match &data {
        StreamData::Control(ControlSignal::Barrier(
            barrier @ BarrierControlSignal::Checkpoint { .. },
        )) => Some(barrier),
        _ => None,
    };

    if let Some(state) = pending.as_mut() {
        if state.arrived[upstream] {
            return Err(ProcessorError::ProcessingError(format!(
                "received data from paused upstream {upstream} while waiting for checkpoint_id={}",
                state.checkpoint_id
            )));
        }

        if let Some(barrier) = checkpoint {
            if barrier.barrier_id() != state.checkpoint_id || barrier.kind() != state.kind {
                return Err(ProcessorError::ProcessingError(format!(
                    "checkpoint barrier overlap: pending checkpoint_id={}, got checkpoint_id={}",
                    state.checkpoint_id,
                    barrier.barrier_id()
                )));
            }

            state.arrived[upstream] = true;
            if state.arrived.iter().all(|arrived| *arrived) {
                let state = pending.take().expect("pending checkpoint state must exist");
                return Ok(CheckpointDataAction::Complete {
                    signal: state.signal,
                });
            }
            return Ok(CheckpointDataAction::Pause);
        }

        if matches!(data, StreamData::Control(ControlSignal::Barrier(_))) {
            return Err(ProcessorError::ProcessingError(format!(
                "barrier overlap while waiting for checkpoint_id={}",
                state.checkpoint_id
            )));
        }

        return Ok(CheckpointDataAction::Forward { data });
    }

    let Some(barrier) = checkpoint else {
        return Ok(CheckpointDataAction::Forward { data });
    };

    if data_barrier_pending {
        return Err(ProcessorError::ProcessingError(format!(
            "checkpoint barrier overlaps with pending barrier on data channel: checkpoint_id={}",
            barrier.barrier_id()
        )));
    }

    let mut arrived = vec![false; expected_upstreams];
    arrived[upstream] = true;
    let signal = barrier.clone();
    if arrived.iter().all(|arrived| *arrived) {
        return Ok(CheckpointDataAction::Complete { signal });
    }

    *pending = Some(PendingCheckpoint {
        checkpoint_id: barrier.barrier_id(),
        kind: barrier.kind(),
        arrived,
        signal,
    });
    Ok(CheckpointDataAction::Pause)
}

/// Synchronous alignment state; transport pause/resume belongs to the driver.
pub(crate) struct BarrierStage {
    expected_upstreams: usize,
    data: BarrierAligner,
    control: BarrierAligner,
    pending_checkpoint: Option<PendingCheckpoint>,
}

pub(crate) enum BarrierAction {
    Pause,
    Forward(Option<StreamData>),
    Resume(StreamData),
}

impl BarrierStage {
    pub(crate) fn new(expected_upstreams: usize) -> Self {
        Self {
            expected_upstreams,
            data: BarrierAligner::new("data", expected_upstreams),
            control: BarrierAligner::new("control", expected_upstreams),
            pending_checkpoint: None,
        }
    }
    pub(crate) fn expected_upstreams(&self) -> usize {
        self.expected_upstreams
    }
    pub(crate) fn control(
        &mut self,
        signal: ControlSignal,
    ) -> Result<Option<ControlSignal>, ProcessorError> {
        align_control_signal(&mut self.control, signal)
    }
    pub(crate) fn process(
        &mut self,
        upstream: usize,
        data: StreamData,
    ) -> Result<BarrierAction, ProcessorError> {
        match handle_checkpoint_data(
            &mut self.pending_checkpoint,
            self.expected_upstreams,
            upstream,
            data,
            self.data.is_pending(),
        )? {
            CheckpointDataAction::Pause => Ok(BarrierAction::Pause),
            CheckpointDataAction::Complete { signal } => Ok(BarrierAction::Resume(
                StreamData::control(ControlSignal::Barrier(signal)),
            )),
            CheckpointDataAction::Forward { data, .. } => {
                let data = match data {
                    StreamData::Control(signal) => {
                        align_control_signal(&mut self.data, signal)?.map(StreamData::control)
                    }
                    data => Some(data),
                };
                Ok(BarrierAction::Forward(data))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::processor::base::*;
    use crate::processor::*;
    use std::sync::Arc;

    use crate::planner::physical::*;

    use crate::checkpoint::CheckpointMode;
    use crate::processor::base::{DEFAULT_CONTROL_CHANNEL_CAPACITY, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::processor::{BarrierControlSignal, InstantControlSignal};
    use crate::runtime::TaskSpawner;
    use std::time::SystemTime;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    struct Upstreams {
        data: Vec<LinkOutput<StreamData>>,
        control: Vec<LinkOutput<ControlSignal>>,
    }

    fn setup_processor(
        upstreams: usize,
    ) -> (
        crate::processor::region_processor::RegionProcessor,
        Upstreams,
        LinkReceiver<StreamData>,
        LinkReceiver<ControlSignal>,
    ) {
        let mut processor = crate::processor::region_processor::RegionProcessor::new(
            "test_barrier".into(),
            &[crate::planner::physical::PhysicalPlan::Barrier(
                crate::planner::physical::PhysicalBarrier::new(
                    (0..upstreams)
                        .map(|index| {
                            Arc::new(PhysicalPlan::Compute(PhysicalCompute::new(
                                vec![],
                                vec![],
                                index as i64 + 1,
                            )))
                        })
                        .collect(),
                    0,
                ),
            )],
            crate::processor::region_processor::RegionResources::default(),
            crate::processor::base::default_channel_capacities(),
        )
        .unwrap();

        let mut data = Vec::with_capacity(upstreams);
        let mut control = Vec::with_capacity(upstreams);
        for _ in 0..upstreams {
            let data_tx = LinkOutput::broadcast(DEFAULT_DATA_CHANNEL_CAPACITY);
            let control_tx = LinkOutput::broadcast(DEFAULT_CONTROL_CHANNEL_CAPACITY);
            let data_rx = data_tx.subscribe().expect("data receiver");
            let control_rx = control_tx.subscribe().expect("control receiver");
            processor.add_input(data_rx);
            processor.add_control_input(control_rx);
            data.push(data_tx);
            control.push(control_tx);
        }

        let out = processor.subscribe_output().expect("output receiver");
        let control_out = processor
            .subscribe_control_output()
            .expect("control output receiver");

        (processor, Upstreams { data, control }, out, control_out)
    }

    // coverage-covers: processor.barrier.alignment
    #[tokio::test]
    async fn data_channel_barrier_waits_until_all_upstreams_arrive() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, _control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        let barrier = ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id: 1 });
        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier (data upstream 0)"));

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "barrier should not be forwarded before all upstreams arrive"
        );

        upstreams
            .data
            .get(1)
            .expect("upstream 1")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier (data upstream 1)"));

        let item = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for forwarded barrier")
            .expect("output channel closed");
        match item {
            StreamData::Control(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id,
            })) => assert_eq!(barrier_id, 1),
            other => panic!("unexpected output item: {}", other.description()),
        }

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "barrier should be forwarded only once"
        );

        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(ControlSignal::Instant(
                InstantControlSignal::StreamQuickEnd { signal_id: 0 },
            ))
            .expect("send terminal control signal");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }

    #[tokio::test]
    async fn checkpoint_pauses_post_barrier_data_until_all_upstreams_arrive() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, _control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::checkpoint(10, CheckpointMode::Continue))
            .expect("send checkpoint (data upstream 0)");
        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::watermark(SystemTime::UNIX_EPOCH))
            .expect("send post-barrier data (data upstream 0)");

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "post-barrier data must stay paused until all upstreams arrive"
        );

        upstreams
            .data
            .get(1)
            .expect("upstream 1")
            .send(StreamData::checkpoint(10, CheckpointMode::Continue))
            .expect("send checkpoint (data upstream 1)");

        let barrier = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for aligned checkpoint")
            .expect("output channel closed");
        assert_eq!(
            barrier
                .as_control()
                .and_then(ControlSignal::checkpoint_mode),
            Some(CheckpointMode::Continue)
        );

        let post_barrier_data = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for post-barrier data")
            .expect("output channel closed");
        assert!(matches!(
            post_barrier_data,
            StreamData::Watermark(SystemTime::UNIX_EPOCH)
        ));

        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(ControlSignal::Instant(
                InstantControlSignal::StreamQuickEnd { signal_id: 0 },
            ))
            .expect("send terminal control signal");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }

    #[tokio::test]
    async fn control_channel_barrier_waits_until_all_upstreams_arrive() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, mut control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        let barrier = ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id: 1 });
        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(barrier.clone())
            .expect("send barrier (control upstream 0)");

        assert!(
            timeout(Duration::from_millis(200), control_out.recv())
                .await
                .is_err(),
            "barrier should not be forwarded on control output before all upstreams arrive"
        );

        upstreams
            .control
            .get(1)
            .expect("upstream 1 control")
            .send(barrier.clone())
            .expect("send barrier (control upstream 1)");

        let received = timeout(Duration::from_millis(200), control_out.recv())
            .await
            .expect("timeout waiting for forwarded control barrier")
            .expect("control output channel closed");
        match received {
            ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id }) => {
                assert_eq!(barrier_id, 1);
            }
            other => panic!("unexpected control output signal: {other:?}"),
        }

        assert!(
            timeout(Duration::from_millis(200), control_out.recv())
                .await
                .is_err(),
            "barrier should be forwarded only once on control output"
        );

        upstreams
            .data
            .get(0)
            .expect("upstream 0 data")
            .send(StreamData::control(ControlSignal::Instant(
                InstantControlSignal::StreamQuickEnd { signal_id: 0 },
            )))
            .unwrap_or_else(|_| panic!("send terminal via data channel"));

        let _ = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for output drain");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }

    #[tokio::test]
    async fn terminal_barrier_waits_until_all_data_upstreams_arrive_before_forwarding() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, _control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        let terminal =
            ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd { barrier_id: 9 });
        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::control(terminal.clone()))
            .unwrap_or_else(|_| panic!("send terminal barrier (data upstream 0)"));

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "terminal barrier should stay pending until all upstreams arrive"
        );

        upstreams
            .data
            .get(1)
            .expect("upstream 1")
            .send(StreamData::control(terminal.clone()))
            .unwrap_or_else(|_| panic!("send terminal barrier (data upstream 1)"));

        let item = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for forwarded terminal barrier")
            .expect("output channel closed");
        match item {
            StreamData::Control(ControlSignal::Barrier(
                BarrierControlSignal::StreamGracefulEnd { barrier_id },
            )) => assert_eq!(barrier_id, 9),
            other => panic!("unexpected output item: {}", other.description()),
        }

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }

    #[tokio::test]
    async fn overlapping_barriers_on_same_channel_return_error() {
        let spawner = test_spawner();
        let (mut processor, upstreams, _out, _control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id: 1,
            }))
            .expect("send first pending barrier");
        upstreams
            .control
            .get(1)
            .expect("upstream 1 control")
            .send(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id: 2,
            }))
            .expect("send overlapping barrier");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to fail")
            .expect("join error");
        match result {
            Err(ProcessorError::ProcessingError(message)) => {
                assert!(
                    message.contains("barrier overlap on control channel"),
                    "unexpected overlap error: {message}"
                );
            }
            other => panic!("expected overlap error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn barrier_alignment_state_is_isolated_per_channel() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, mut control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        let barrier = ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id: 11 });
        upstreams
            .data
            .get(0)
            .expect("upstream 0 data")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier on data upstream 0"));
        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(barrier.clone())
            .expect("send barrier on control upstream 0");

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "data barrier should not complete from control-channel arrivals"
        );
        assert!(
            timeout(Duration::from_millis(200), control_out.recv())
                .await
                .is_err(),
            "control barrier should not complete from data-channel arrivals"
        );

        upstreams
            .data
            .get(1)
            .expect("upstream 1 data")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier on data upstream 1"));
        upstreams
            .control
            .get(1)
            .expect("upstream 1 control")
            .send(barrier.clone())
            .expect("send barrier on control upstream 1");

        let data_item = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for data barrier")
            .expect("output channel closed");
        match data_item {
            StreamData::Control(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id,
            })) => assert_eq!(barrier_id, 11),
            other => panic!("unexpected data output item: {}", other.description()),
        }

        let control_item = timeout(Duration::from_millis(200), control_out.recv())
            .await
            .expect("timeout waiting for control barrier")
            .expect("control output channel closed");
        match control_item {
            ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id }) => {
                assert_eq!(barrier_id, 11);
            }
            other => panic!("unexpected control output signal: {other:?}"),
        }

        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(ControlSignal::Instant(
                InstantControlSignal::StreamQuickEnd { signal_id: 0 },
            ))
            .expect("send terminal control signal");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }
}
