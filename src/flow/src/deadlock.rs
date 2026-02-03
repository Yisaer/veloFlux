#[cfg(feature = "deadlock_detection")]
pub(crate) fn start_deadlock_detector_once() {
    use std::sync::OnceLock;
    use std::time::Duration;

    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        std::thread::Builder::new()
            .name("parking_lot-deadlock-detector".to_string())
            .spawn(|| loop {
                std::thread::sleep(Duration::from_secs(1));

                let deadlocks = parking_lot::deadlock::check_deadlock();
                if deadlocks.is_empty() {
                    continue;
                }

                tracing::error!(
                    deadlock_count = deadlocks.len(),
                    "parking_lot detected deadlock"
                );

                for (idx, deadlock) in deadlocks.into_iter().enumerate() {
                    tracing::error!(deadlock_idx = idx, "deadlock cycle");
                    for thread in deadlock {
                        tracing::error!(
                            thread_id = ?thread.thread_id(),
                            backtrace = ?thread.backtrace(),
                            "deadlocked thread"
                        );
                    }
                }

                std::process::abort();
            })
            .expect("spawn parking_lot deadlock detector");
    });
}

#[cfg(not(feature = "deadlock_detection"))]
pub(crate) fn start_deadlock_detector_once() {}
