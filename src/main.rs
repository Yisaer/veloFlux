#[cfg(all(feature = "allocator-jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use veloflux::server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--worker") {
        return run_worker(args).await;
    }

    let result = veloflux::bootstrap::default_init()?;
    // Keep logging guard alive for the duration of the application
    let _logging_guard = result.logging_guard;

    let ctx = server::init(result.options, result.instance).await?;
    server::start(ctx).await
}

async fn run_worker(args: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    flow::init_process_once();
    let mut cfg = veloflux::config::AppConfig::load_required(&config_path)?;

    let spec = cfg
        .server
        .extra_flow_instances
        .iter()
        .find(|spec| spec.id.trim() == instance_id)
        .ok_or_else(|| {
            format!(
                "worker instance {instance_id} is not declared in config server.extra_flow_instances"
            )
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

    let worker_addr: std::net::SocketAddr = spec
        .worker_addr
        .parse()
        .map_err(|e| format!("invalid worker_addr for {instance_id}: {e}"))?;
    if !worker_addr.ip().is_loopback() {
        return Err(format!(
            "worker_addr for {instance_id} must be loopback, got {}",
            spec.worker_addr
        )
        .into());
    }

    let metrics_addr: std::net::SocketAddr = spec
        .metrics_addr
        .parse()
        .map_err(|e| format!("invalid metrics_addr for {instance_id}: {e}"))?;
    if !metrics_addr.ip().is_loopback() {
        return Err(format!(
            "metrics_addr for {instance_id} must be loopback, got {}",
            spec.metrics_addr
        )
        .into());
    }

    let profile_addr: std::net::SocketAddr = spec
        .profile_addr
        .parse()
        .map_err(|e| format!("invalid profile_addr for {instance_id}: {e}"))?;
    if !profile_addr.ip().is_loopback() {
        return Err(format!(
            "profile_addr for {instance_id} must be loopback, got {}",
            spec.profile_addr
        )
        .into());
    }

    let mut opts = cfg.to_server_options();
    opts.metrics_addr = Some(metrics_addr.to_string());
    opts.profile_addr = Some(profile_addr.to_string());
    veloflux::server::init_metrics_exporter(&opts).await?;
    if opts.profiling_enabled.unwrap_or(false) {
        veloflux::server::start_profile_server(&opts);
    }

    let default_instance = server::prepare_registry();
    let shared = default_instance.shared_registries();
    let instance = flow::FlowInstance::new_with_id(&instance_id, Some(shared));

    let listener = tokio::net::TcpListener::bind(worker_addr).await?;
    tracing::info!(
        flow_instance_id = %instance_id,
        worker_addr = %worker_addr,
        metrics_addr = %metrics_addr,
        profile_addr = %profile_addr,
        "flow worker listening"
    );

    manager::start_flow_worker_with_listener(listener, instance).await?;
    Ok(())
}
