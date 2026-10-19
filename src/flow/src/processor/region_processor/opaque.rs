//! Transport for an isolated stage whose callbacks may block.
//!
//! Only synchronous work crosses the blocking boundary. Jobs never own channel
//! senders, so aborting the transport drops late results without publishing them.
use super::*;
use crate::processor::base::ControlInputStream;
use std::sync::OnceLock;
use tokio::sync::Semaphore;

fn worker_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| {
        // One process-wide bound also covers detached callbacks from old rules.
        let count = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(4);
        Arc::new(Semaphore::new(count))
    })
}

async fn blocking<T: Send + 'static>(
    spawner: &TaskSpawner,
    slots: &Arc<Semaphore>,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, ProcessorError> {
    // Acquire before spawning; an arbitrary number of rules cannot fill Tokio's
    // blocking queue with callbacks. Cancellation also cancels this acquisition.
    let permit = Arc::clone(slots)
        .acquire_owned()
        .await
        .map_err(|_| ProcessorError::ProcessingError("opaque worker pool closed".into()))?;
    spawner
        .spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(|error| {
            ProcessorError::ProcessingError(format!("opaque stage worker failed: {error}"))
        })
}

pub(super) struct OpaqueDriver {
    pub(super) stage: Stage,
    pub(super) input: RegionInput,
    pub(super) control: ControlInputStream,
    pub(super) output: LinkOutput<StreamData>,
    pub(super) control_output: LinkOutput<ControlSignal>,
    pub(super) capacities: ProcessorChannelCapacities,
    pub(super) spawner: TaskSpawner,
    pub(super) id: String,
}

impl OpaqueDriver {
    pub(super) async fn run(self) -> Result<(), ProcessorError> {
        let Self {
            mut stage,
            mut input,
            mut control,
            output,
            control_output,
            capacities,
            spawner,
            id,
        } = self;
        let mut control_active = !control.is_empty();
        let mut clock = stage.start_clock();
        loop {
            if let Some(clock) = &mut clock {
                clock.update(stage.clock_deadline()?);
            }
            enum Event {
                Data(StreamData),
                Timer(ClockEvent),
                Finish,
                Closed,
            }
            let (event, terminal) = tokio::select! {
                biased;
                signal = control.next(), if control_active => {
                    match signal {
                        Some(Ok(signal)) => {
                            let terminal = signal.is_terminal();
                            // Encoder control termination flushes before control;
                            // samplers and native batching flush after forwarding.
                            if terminal && matches!(stage.engine, CollectionStage::Encoder(_)) {
                                stage = run_event(stage, EventWork::Finish, &spawner, &output, capacities.data).await?;
                            }
                            send_control_with_backpressure(&control_output, capacities.control, signal).await?;
                            if terminal { (Event::Finish, true) } else { continue; }
                        }
                        Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                            log_broadcast_lagged(&id, skipped, "opaque region control input");
                            if matches!(stage.engine, CollectionStage::Encoder(_)) { control_active = false; }
                            continue;
                        }
                        None => {
                            if stage.strict_input_closure { return Err(stage.channel_closed("control")); }
                            control_active = false;
                            continue;
                        }
                    }
                }
                event = poll_fn(|cx| match &mut clock {
                    Some(clock) => clock.poll_next(cx),
                    None => std::task::Poll::Pending,
                }), if clock.is_some() => (Event::Timer(event), false),
                item = input.next() => {
                    match item {
                        Some(Ok(IndexedInput::Item { item, .. })) => {
                            let terminal = item.is_terminal();
                            (Event::Data(item), terminal)
                        }
                        Some(Ok(IndexedInput::Closed)) => {
                            if stage.strict_input_closure { return Err(stage.channel_closed("data")); }
                            continue;
                        }
                        Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                            if stage.fatal_data_lag {
                                stage.input_lagged(skipped);
                                return Err(ProcessorError::ChannelClosed);
                            }
                            log_broadcast_lagged(&id, skipped, "opaque region data input");
                            continue;
                        }
                        None => (Event::Closed, true),
                    }
                }
            };
            let work = match event {
                Event::Data(data) => EventWork::Data(data),
                Event::Timer(event) => EventWork::Timer(event),
                Event::Finish => EventWork::Finish,
                Event::Closed => EventWork::Closed,
            };
            stage = run_event(stage, work, &spawner, &output, capacities.data).await?;
            if terminal {
                return Ok(());
            }
        }
    }
}

enum EventWork {
    Data(StreamData),
    Timer(ClockEvent),
    Finish,
    Closed,
}

async fn run_event(
    stage: Stage,
    work: EventWork,
    spawner: &TaskSpawner,
    output: &LinkOutput<StreamData>,
    capacity: usize,
) -> Result<Stage, ProcessorError> {
    let (mut stage, mut pending) = blocking(spawner, worker_slots(), move || {
        let mut stage = stage;
        let result = match work {
            EventWork::Data(data) => stage.process(data).map(Some),
            EventWork::Timer(event) => stage.on_timer(event).map(Some),
            EventWork::Finish => Ok(stage.finish_input()),
            EventWork::Closed => stage.input_closed(),
        };
        result.map(|pending| (stage, pending))
    })
    .await??;
    while let Some(mut emission) = pending.take() {
        let (returned_stage, returned_emission, next) =
            blocking(spawner, worker_slots(), move || {
                let mut stage = stage;
                let result = stage.next_output(&mut emission);
                result.map(|next| (stage, emission, next))
            })
            .await??;
        stage = returned_stage;
        let Some(data) = next else {
            break;
        };
        let flags = match &data {
            StreamData::EncodedDelivery { flags, .. } => Some(*flags),
            _ => None,
        };
        pending = Some(returned_emission);
        if let Err(error) = send_with_backpressure(output, capacity, data, None).await {
            let (returned_stage, returned_pending, recovered) =
                blocking(spawner, worker_slots(), move || {
                    let mut stage = stage;
                    let mut pending = pending;
                    let recovered = stage.output_failed(flags, pending.as_mut());
                    (stage, pending, recovered)
                })
                .await?;
            stage = returned_stage;
            pending = returned_pending;
            if recovered {
                continue;
            }
            return Err(error);
        }
    }
    Ok(stage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::custom_func::CustomFunc;
    use crate::expr::func::EvalError;
    use crate::expr::ScalarExpr;
    use crate::planner::physical::{PhysicalCompute, PhysicalComputeField};
    use crate::processor::base::LinkKind;
    use datatypes::Value;
    use sqlparser::ast::{Expr, Value as SqlValue};
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};

    #[derive(Debug, Default)]
    struct Gate(Mutex<bool>, Condvar);
    impl Gate {
        fn wait(&self) {
            let (guard, _) = self
                .1
                .wait_timeout_while(self.0.lock().unwrap(), Duration::from_secs(3), |released| {
                    !*released
                })
                .unwrap();
            assert!(*guard, "worker release timed out");
        }
        fn release(&self) {
            *self.0.lock().unwrap() = true;
            self.1.notify_all();
        }
    }
    struct ReleaseOnDrop(Arc<Gate>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    #[derive(Debug)]
    struct BlockingFunc {
        gate: Arc<Gate>,
        started: Mutex<Option<oneshot::Sender<std::thread::ThreadId>>>,
        finished: Mutex<Option<oneshot::Sender<()>>>,
    }
    impl CustomFunc for BlockingFunc {
        fn name(&self) -> &str {
            "opaque_test"
        }
        fn validate_row(&self, _: &[Value]) -> Result<(), EvalError> {
            Ok(())
        }
        fn eval_row(&self, _: &[Value]) -> Result<Value, EvalError> {
            self.started
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(std::thread::current().id())
                .unwrap();
            self.gate.wait();
            let _ = self.finished.lock().unwrap().take().unwrap().send(());
            Ok(Value::Int64(42))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn opaque_region_keeps_async_worker_free_and_discards_cancelled_results() {
        for cancel in [false, true] {
            let gate = Arc::new(Gate::default());
            let _release = ReleaseOnDrop(Arc::clone(&gate));
            let (started_tx, started_rx) = oneshot::channel();
            let (finished_tx, finished_rx) = oneshot::channel();
            let function = Arc::new(BlockingFunc {
                gate: Arc::clone(&gate),
                started: Mutex::new(Some(started_tx)),
                finished: Mutex::new(Some(finished_tx)),
            });
            let plan = PhysicalPlan::Compute(PhysicalCompute::new(
                vec![PhysicalComputeField::new(
                    "answer".into(),
                    Expr::Value(SqlValue::Number("42".into(), false)),
                    ScalarExpr::call_func(function, vec![]),
                )],
                vec![],
                1,
            ));
            let mut region = RegionProcessor::new(
                "opaque".into(),
                &[plan],
                RegionResources {
                    isolate_opaque: true,
                    ..Default::default()
                },
                ProcessorChannelCapacities::new(1, 1).with_link_kind(LinkKind::Mpsc),
            )
            .unwrap();
            let (tx, rx) = mpsc::channel(1);
            region.add_input(rx);
            let mut output = region.subscribe_output().unwrap();
            let handle = region
                .start(&TaskSpawner::from_handle(tokio::runtime::Handle::current()))
                .handle;
            let rows = crate::model::batch_from_columns_simple(vec![(
                "input".into(),
                "a".into(),
                vec![Value::Int64(7)],
            )])
            .unwrap();
            tx.send(StreamData::collection(Box::new(rows)))
                .await
                .unwrap();
            let callback_thread = tokio::time::timeout(Duration::from_secs(1), started_rx)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(callback_thread, std::thread::current().id());
            // This timer must progress while the callback is still blocked.
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert!(!*gate.0.lock().unwrap());
            if cancel {
                handle.abort();
                assert!(handle.await.unwrap_err().is_cancelled());
                gate.release();
                finished_rx.await.unwrap();
                drop(region);
                assert!(output.recv().await.is_err());
            } else {
                gate.release();
                let StreamData::Collection(result) = output.recv().await.unwrap() else {
                    panic!("expected collection")
                };
                assert_eq!(result.num_rows(), 1);
                assert_eq!(
                    result.rows()[0].affiliate().unwrap().value("answer"),
                    Some(&Value::Int64(42))
                );
                tx.send(StreamData::control(ControlSignal::Instant(
                    crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 1 },
                )))
                .await
                .unwrap();
                output.recv().await.unwrap();
                handle.await.unwrap().unwrap();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn opaque_worker_bound_survives_transport_cancellation() {
        let spawner = TaskSpawner::from_handle(tokio::runtime::Handle::current());
        let slots = Arc::new(Semaphore::new(1));
        let gate = Arc::new(Gate::default());
        let _release = ReleaseOnDrop(Arc::clone(&gate));
        let (started_tx, started_rx) = oneshot::channel();
        let first = {
            let spawner = spawner.clone();
            let slots = Arc::clone(&slots);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                blocking(&spawner, &slots, move || {
                    started_tx.send(()).unwrap();
                    gate.wait();
                })
                .await
            })
        };
        started_rx.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(slots.available_permits(), 0);
        let (second_tx, mut second_rx) = oneshot::channel();
        let second = {
            let slots = Arc::clone(&slots);
            tokio::spawn(async move {
                blocking(&spawner, &slots, move || second_tx.send(()).unwrap()).await
            })
        };
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(matches!(
            second_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        gate.release();
        second_rx.await.unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(slots.available_permits(), 1);
    }
}
