use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type StartupResult = Result<(), String>;

static EMBEDDED_RUNTIME_ACTIVE: AtomicBool = AtomicBool::new(false);

pub struct EmbeddedServerHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join_handle: Option<std::thread::JoinHandle<Result<(), String>>>,
}

impl EmbeddedServerHandle {
    pub fn start(config_path: impl AsRef<Path>) -> Result<Self, BoxError> {
        let config_path = config_path.as_ref();
        let config_path = config_path
            .to_str()
            .ok_or("config path must be valid UTF-8 for embedded startup")?
            .to_string();

        if EMBEDDED_RUNTIME_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("an embedded veloFlux runtime is already active in this process".into());
        }

        let (startup_tx, startup_rx) = std::sync::mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let thread_config_path = config_path.clone();
        let join_handle = match std::thread::Builder::new()
            .name("veloflux-embedded".to_string())
            .spawn(move || run_embedded_runtime(thread_config_path, shutdown_rx, startup_tx))
        {
            Ok(join_handle) => join_handle,
            Err(err) => {
                EMBEDDED_RUNTIME_ACTIVE.store(false, Ordering::Release);
                return Err(err.into());
            }
        };

        match startup_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                shutdown_tx: Some(shutdown_tx),
                join_handle: Some(join_handle),
            }),
            Ok(Err(err)) => {
                let _ = join_handle.join();
                EMBEDDED_RUNTIME_ACTIVE.store(false, Ordering::Release);
                Err(err.into())
            }
            Err(err) => {
                let join_result = join_handle.join();
                EMBEDDED_RUNTIME_ACTIVE.store(false, Ordering::Release);
                match join_result {
                    Ok(Ok(())) => Err(format!(
                        "embedded startup channel closed before readiness confirmation: {err}"
                    )
                    .into()),
                    Ok(Err(join_err)) => Err(join_err.into()),
                    Err(_) => Err(format!(
                        "embedded startup thread terminated unexpectedly before readiness confirmation: {err}"
                    )
                    .into()),
                }
            }
        }
    }

    pub fn stop(&mut self) -> Result<(), BoxError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        let join_result = match self.join_handle.take() {
            Some(join_handle) => join_handle.join(),
            None => {
                EMBEDDED_RUNTIME_ACTIVE.store(false, Ordering::Release);
                return Ok(());
            }
        };
        EMBEDDED_RUNTIME_ACTIVE.store(false, Ordering::Release);

        match join_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err.into()),
            Err(_) => Err("embedded runtime thread panicked".into()),
        }
    }
}

impl Drop for EmbeddedServerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub fn start_embedded(config_path: impl AsRef<Path>) -> Result<EmbeddedServerHandle, BoxError> {
    EmbeddedServerHandle::start(config_path)
}

fn send_startup_result(startup_tx: &mut Option<SyncSender<StartupResult>>, result: StartupResult) {
    if let Some(tx) = startup_tx.take() {
        let _ = tx.send(result);
    }
}

fn validate_embedded_flow_instances(
    flow_instances: &[manager::FlowInstanceSpec],
) -> Result<(), String> {
    manager::find_default_flow_instance_spec(flow_instances)?;
    Ok(())
}

fn run_embedded_runtime(
    config_path: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    startup_tx: SyncSender<StartupResult>,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("veloflux-embedded-worker")
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;

    runtime.block_on(async move {
        let mut startup_tx = Some(startup_tx);

        let bootstrap = match crate::bootstrap::init_options_from_config_path_with_logging_context(
            &config_path,
            &crate::logging::LoggingContext::embedded(),
        ) {
            Ok(bootstrap) => bootstrap,
            Err(err) => {
                send_startup_result(&mut startup_tx, Err(err.to_string()));
                return Err(err.to_string());
            }
        };
        let _logging_guard = bootstrap.logging_guard;

        if let Err(err) = validate_embedded_flow_instances(&bootstrap.options.flow_instances) {
            send_startup_result(&mut startup_tx, Err(err.clone()));
            return Err(err);
        }

        let instance = match crate::server::prepare_registry(&bootstrap.options.flow_instances) {
            Ok(instance) => instance,
            Err(err) => {
                send_startup_result(&mut startup_tx, Err(err.to_string()));
                return Err(err.to_string());
            }
        };
        crate::distro::register_selected_distro(&instance);

        let ctx = match crate::server::init(bootstrap.options, instance).await {
            Ok(ctx) => ctx,
            Err(err) => {
                send_startup_result(&mut startup_tx, Err(err.to_string()));
                return Err(err.to_string());
            }
        };

        match crate::server::start_with_shutdown_and_signal(
            ctx,
            async move {
                let _ = shutdown_rx.await;
            },
            startup_tx.take(),
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(err) => {
                send_startup_result(&mut startup_tx, Err(err.to_string()));
                Err(err.to_string())
            }
        }
    })
}
