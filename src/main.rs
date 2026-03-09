#[cfg(all(feature = "allocator-jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use veloflux::server;
use veloflux::startup::StartupPhase;

#[derive(Debug, Clone)]
struct WorkerCliArgs {
    instance_id: String,
    config_path: String,
}

impl WorkerCliArgs {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut instance_id = None;
        let mut config_path = None;
        let mut it = args.into_iter().peekable();
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--flow-instance-id" => {
                    instance_id = it.next();
                }
                "--config" => {
                    config_path = it.next();
                }
                _ => {}
            }
        }

        let instance_id = instance_id.ok_or("--flow-instance-id is required in --worker mode")?;
        let config_path = config_path.ok_or("--config is required in --worker mode")?;
        Ok(Self {
            instance_id,
            config_path,
        })
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--worker") {
        let worker_args = WorkerCliArgs::parse(args)?;
        return run_worker(worker_args).await;
    }

    let result = veloflux::bootstrap::default_init()?;
    let _logging_guard = result.logging_guard;
    let ctx = server::init(result.options, result.instance).await?;
    server::start(ctx).await
}

async fn run_worker(
    worker_args: WorkerCliArgs,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let instance_id = worker_args.instance_id;
    let config_path = worker_args.config_path;

    flow::init_process_once();
    flow::metrics::set_flow_instance_id(&instance_id);
    #[cfg(feature = "metrics")]
    telemetry::set_flow_instance_id(&instance_id);
    let mut cfg = veloflux::config::AppConfig::load_required(&config_path)?;

    let spec = cfg
        .server
        .flow_instances
        .iter()
        .find(|spec| spec.id.trim() == instance_id)
        .ok_or_else(|| {
            format!("worker instance {instance_id} is not declared in config server.flow_instances")
        })?
        .clone();

    match cfg.logging.output {
        veloflux::config::LoggingOutput::File => {
            cfg.logging.file.dir = std::path::Path::new(&cfg.logging.file.dir)
                .join(instance_id.as_str())
                .to_string_lossy()
                .to_string();
        }
        veloflux::config::LoggingOutput::Stdout => {}
    }

    let logging_guard = veloflux::logging::init_logging(&cfg.logging)?;
    let _logging_guard = logging_guard;

    tracing::info!(
        mode = "worker",
        flow_instance_id = %instance_id,
        config_path = %config_path,
        result = "configured",
        backend = ?spec.backend,
        worker_threads = ?spec.runtime.worker_threads,
        thread_name_prefix = ?spec.runtime.thread_name_prefix,
        "worker startup configuration"
    );

    let runtime_prepare_phase = StartupPhase::new(
        "worker",
        instance_id.as_str(),
        "runtime_prepare",
        Some(&config_path),
    );

    if !matches!(
        spec.backend,
        manager::FlowInstanceBackendKind::WorkerProcess
    ) {
        let err = format!("instance {instance_id} is not configured as worker_process");
        runtime_prepare_phase.log_failure(&err);
        return Err(err.into());
    }
    if spec.thread_cgroup_path().is_some() {
        let err = format!("worker_process instance {instance_id} cannot set cgroup.thread_path");
        runtime_prepare_phase.log_failure(&err);
        return Err(err.into());
    }

    let worker_addr_raw = spec
        .worker_addr()
        .ok_or_else(|| format!("worker_process instance {instance_id} requires worker_addr"));
    let worker_addr_raw = match worker_addr_raw {
        Ok(addr) => addr,
        Err(err) => {
            runtime_prepare_phase.log_failure(&err);
            return Err(err.into());
        }
    };
    let worker_addr: std::net::SocketAddr = match worker_addr_raw.parse() {
        Ok(addr) => addr,
        Err(err) => {
            let message = format!("invalid worker_addr for {instance_id}: {err}");
            runtime_prepare_phase.log_failure(&message);
            return Err(message.into());
        }
    };
    if !worker_addr.ip().is_loopback() {
        let err = format!(
            "worker_addr for {instance_id} must be loopback, got {}",
            worker_addr_raw
        );
        runtime_prepare_phase.log_failure(&err);
        return Err(err.into());
    }

    let metrics_addr_raw = spec
        .metrics_addr()
        .ok_or_else(|| format!("worker_process instance {instance_id} requires metrics_addr"));
    let metrics_addr_raw = match metrics_addr_raw {
        Ok(addr) => addr,
        Err(err) => {
            runtime_prepare_phase.log_failure(&err);
            return Err(err.into());
        }
    };
    let metrics_addr: std::net::SocketAddr = match metrics_addr_raw.parse() {
        Ok(addr) => addr,
        Err(err) => {
            let message = format!("invalid metrics_addr for {instance_id}: {err}");
            runtime_prepare_phase.log_failure(&message);
            return Err(message.into());
        }
    };
    if !metrics_addr.ip().is_loopback() {
        let err = format!(
            "metrics_addr for {instance_id} must be loopback, got {}",
            metrics_addr_raw
        );
        runtime_prepare_phase.log_failure(&err);
        return Err(err.into());
    }

    let profile_addr_raw = spec
        .profile_addr()
        .ok_or_else(|| format!("worker_process instance {instance_id} requires profile_addr"));
    let profile_addr_raw = match profile_addr_raw {
        Ok(addr) => addr,
        Err(err) => {
            runtime_prepare_phase.log_failure(&err);
            return Err(err.into());
        }
    };
    let profile_addr: std::net::SocketAddr = match profile_addr_raw.parse() {
        Ok(addr) => addr,
        Err(err) => {
            let message = format!("invalid profile_addr for {instance_id}: {err}");
            runtime_prepare_phase.log_failure(&message);
            return Err(message.into());
        }
    };
    if !profile_addr.ip().is_loopback() {
        let err = format!(
            "profile_addr for {instance_id} must be loopback, got {}",
            profile_addr_raw
        );
        runtime_prepare_phase.log_failure(&err);
        return Err(err.into());
    }

    let mut opts = cfg.to_server_options();
    opts.metrics_addr = Some(metrics_addr.to_string());
    opts.profile_addr = Some(profile_addr.to_string());
    let services_phase = StartupPhase::new(
        "worker",
        instance_id.as_str(),
        "services_init",
        Some(&config_path),
    );
    if let Err(err) = veloflux::server::init_metrics_exporter(&opts).await {
        services_phase.log_failure(err.as_ref());
        return Err(err);
    }
    if opts.profiling_enabled.unwrap_or(false) {
        veloflux::server::start_profile_server(&opts);
    }
    tracing::info!(
        mode = services_phase.mode(),
        flow_instance_id = %services_phase.flow_instance_id(),
        phase = services_phase.phase(),
        config_path = services_phase.config_path(),
        result = "succeeded",
        elapsed_ms = services_phase.elapsed_ms(),
        metrics_addr = %metrics_addr,
        profile_addr = %profile_addr,
        profiling_enabled = opts.profiling_enabled.unwrap_or(false),
        "startup phase"
    );

    let default_spec = match manager::find_default_flow_instance_spec(&cfg.server.flow_instances) {
        Ok(spec) => spec,
        Err(err) => {
            let err = std::io::Error::new(std::io::ErrorKind::InvalidInput, err);
            runtime_prepare_phase.log_failure(&err);
            return Err(err.into());
        }
    };
    let default_instance = match manager::build_in_process_flow_instance(default_spec, None) {
        Ok(instance) => instance,
        Err(err) => {
            let err = std::io::Error::new(std::io::ErrorKind::InvalidInput, err);
            runtime_prepare_phase.log_failure(&err);
            return Err(err.into());
        }
    };
    let shared = default_instance.shared_registries();
    let instance = flow::FlowInstance::new(flow::instance::FlowInstanceOptions::dedicated_runtime(
        instance_id.clone(),
        Some(shared),
        flow::instance::FlowInstanceDedicatedRuntimeOptions {
            worker_threads: spec.runtime.worker_threads,
            thread_name_prefix: spec.runtime.thread_name_prefix.clone(),
            thread_cgroup_path: spec.thread_cgroup_path().map(|path| path.to_string()),
        },
    ));
    tracing::info!(
        mode = runtime_prepare_phase.mode(),
        flow_instance_id = %runtime_prepare_phase.flow_instance_id(),
        phase = runtime_prepare_phase.phase(),
        config_path = runtime_prepare_phase.config_path(),
        result = "succeeded",
        elapsed_ms = runtime_prepare_phase.elapsed_ms(),
        worker_addr = %worker_addr,
        metrics_addr = %metrics_addr,
        profile_addr = %profile_addr,
        worker_threads = ?spec.runtime.worker_threads,
        thread_name_prefix = ?spec.runtime.thread_name_prefix,
        "startup phase"
    );

    let bind_phase = StartupPhase::new(
        "worker",
        instance_id.as_str(),
        "server_bind",
        Some(&config_path),
    );
    let listener = match tokio::net::TcpListener::bind(worker_addr).await {
        Ok(listener) => listener,
        Err(err) => {
            bind_phase.log_failure(&err);
            return Err(err.into());
        }
    };
    tracing::info!(
        mode = bind_phase.mode(),
        flow_instance_id = %bind_phase.flow_instance_id(),
        phase = bind_phase.phase(),
        config_path = bind_phase.config_path(),
        result = "succeeded",
        elapsed_ms = bind_phase.elapsed_ms(),
        worker_addr = %worker_addr,
        metrics_addr = %metrics_addr,
        profile_addr = %profile_addr,
        "flow worker listening"
    );

    manager::start_flow_worker_with_listener(listener, instance).await?;
    Ok(())
}
