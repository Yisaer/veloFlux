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
    cfg.logging.output = veloflux::config::LoggingOutput::Stderr;
    let logging_guard = veloflux::logging::init_logging(&cfg.logging)?;
    let _logging_guard = logging_guard;

    let default_instance = server::prepare_registry();
    let shared = default_instance.shared_registries();
    let instance = flow::FlowInstance::new_with_id(&instance_id, Some(shared));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("{addr}");
    use std::io::Write;
    std::io::stdout().flush().ok();

    manager::start_flow_worker_with_listener(listener, instance).await?;
    Ok(())
}
