use flow::FlowInstance;
#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use parking_lot::Mutex;
use std::io::{BufRead, BufReader};
#[cfg(feature = "profiling")]
use std::io::{Read, Write};
#[cfg(any(feature = "metrics", feature = "profiling"))]
use std::net::SocketAddr;
#[cfg(feature = "profiling")]
use std::net::{TcpListener, TcpStream};
#[cfg(any(feature = "metrics", feature = "profiling"))]
use std::process;
#[cfg(feature = "profiling")]
use std::thread;
#[cfg(feature = "metrics")]
use std::time::Duration as StdDuration;
use storage::StorageManager;

#[cfg(feature = "profiling")]
use pprof::{protos::Message, ProfilerGuardBuilder};
#[cfg(feature = "metrics")]
use sysinfo::{Pid, System};
#[cfg(feature = "metrics")]
use telemetry::{
    spawn_tokio_metrics_collector, CPU_USAGE_GAUGE, HEAP_IN_ALLOCATOR_GAUGE, HEAP_IN_USE_GAUGE,
    MEMORY_USAGE_GAUGE,
};
#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use tikv_jemalloc_ctl::raw;
#[cfg(all(
    feature = "metrics",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use tikv_jemalloc_ctl::{epoch, stats};
#[cfg(feature = "metrics")]
use tokio::time::{sleep, Duration};

#[cfg(feature = "profiling")]
const DEFAULT_PROFILE_ADDR: &str = "0.0.0.0:6060";
#[cfg(feature = "profiling")]
const DEFAULT_CPU_PROFILE_FREQUENCY_HZ: i32 = 100;
#[cfg(feature = "profiling")]
const DEFAULT_CPU_PROFILE_BLOCKLIST: [&str; 4] = ["libc", "libgcc", "pthread", "vdso"];
#[cfg(feature = "metrics")]
const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9898";
pub const DEFAULT_DATA_DIR: &str = "./tmp";
pub const DEFAULT_MANAGER_ADDR: &str = "0.0.0.0:8080";
#[cfg(feature = "metrics")]
const DEFAULT_METRICS_POLL_INTERVAL_SECS: u64 = 5;

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
static PPROF_ENDPOINT_MUTEX: Mutex<()> = Mutex::new(());

/// Options for initializing the server runtime.
#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    /// Enable profiling endpoints (feature-gated); if None, uses default (false).
    pub profiling_enabled: Option<bool>,
    /// Custom data directory for storage; if None, uses DEFAULT_DATA_DIR.
    pub data_dir: Option<String>,
    /// Manager listen address; if None, uses default.
    pub manager_addr: Option<String>,
    /// Profiling server bind address (feature-gated); if None, uses default.
    pub profile_addr: Option<String>,
    /// CPU profiling sample frequency in Hz (feature-gated); if None, uses default.
    pub cpu_profile_freq_hz: Option<i32>,
    /// Metrics exporter bind address (feature-gated); if None, uses default.
    pub metrics_addr: Option<String>,
    /// Metrics polling interval in seconds (feature-gated); if None, uses default.
    pub metrics_poll_interval_secs: Option<u64>,
    /// Extra flow instances (excluding `default`) declared by config.
    pub extra_flow_instances: Vec<manager::FlowInstanceSpec>,
    /// Loaded config path (passed to worker subprocesses).
    pub config_path: Option<String>,
}

/// Runtime context returned by [`init`] and consumed by [`start`].
pub struct ServerContext {
    instance: FlowInstance,
    storage: StorageManager,
    manager_addr: String,
    profiling_enabled: bool,
    extra_flow_instances: Vec<manager::FlowInstanceSpec>,
    config_path: Option<String>,
}

impl ServerContext {
    /// Access the FlowInstance for custom registrations before starting.
    pub fn instance(&self) -> &FlowInstance {
        &self.instance
    }

    /// Mutable access to FlowInstance if needed for setup.
    pub fn instance_mut(&mut self) -> &mut FlowInstance {
        &mut self.instance
    }

    /// Manager address that will be used when starting the server.
    pub fn manager_addr(&self) -> &str {
        &self.manager_addr
    }

    fn into_parts(self) -> (FlowInstance, StorageManager, String, bool) {
        (
            self.instance,
            self.storage,
            self.manager_addr,
            self.profiling_enabled,
        )
    }

    fn extra_flow_instances(&self) -> &[manager::FlowInstanceSpec] {
        &self.extra_flow_instances
    }

    fn config_path(&self) -> Option<&str> {
        self.config_path.as_deref()
    }
}

/// Initialize the Flow server: metrics/profiling, storage, FlowInstance, and catalog load.
pub async fn init(
    opts: ServerOptions,
    instance: FlowInstance,
) -> Result<ServerContext, Box<dyn std::error::Error + Send + Sync>> {
    flow::init_process_once();
    flow::metrics::set_flow_instance_id("default");
    #[cfg(feature = "metrics")]
    telemetry::set_flow_instance_id("default");
    log_allocator();
    let profiling_enabled = opts.profiling_enabled.unwrap_or(false);
    if profiling_enabled {
        ensure_jemalloc_profiling();
    }

    init_metrics_exporter(&opts).await?;
    if profiling_enabled {
        start_profile_server(&opts);
    }

    let manager_addr = opts
        .manager_addr
        .unwrap_or_else(|| DEFAULT_MANAGER_ADDR.to_string());
    let data_dir = opts
        .data_dir
        .unwrap_or_else(|| DEFAULT_DATA_DIR.to_string());

    let storage = StorageManager::new(&data_dir)?;
    tracing::info!(
        storage_dir = %storage.base_dir().display(),
        "storage initialized"
    );

    Ok(ServerContext {
        instance,
        storage,
        manager_addr,
        profiling_enabled,
        extra_flow_instances: opts.extra_flow_instances,
        config_path: opts.config_path,
    })
}

/// Start the manager server and await termination (Ctrl+C or server error).
pub async fn start(ctx: ServerContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let extra_instances = ctx.extra_flow_instances().to_vec();
    let config_path = ctx.config_path().map(|s| s.to_string());
    let (instance, storage, manager_addr, _profiling_enabled) = ctx.into_parts();
    let (worker_endpoints, mut worker_children) =
        spawn_flow_workers(&extra_instances, config_path.as_deref()).await?;
    tracing::info!(manager_addr = %manager_addr, "starting manager");
    let manager_future = manager::start_server(
        manager_addr.clone(),
        instance,
        storage,
        extra_instances,
        worker_endpoints,
    );

    let result = tokio::select! {
        result = manager_future => {
            if let Err(err) = result {
                tracing::error!(error = %err, "manager server exited with error");
                return Err(err);
            }
            Ok(())
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl+c received, shutting down");
            Ok(())
        }
    };

    for child in &mut worker_children {
        let _ = child.kill();
    }
    for child in &mut worker_children {
        let _ = child.wait();
    }

    result
}

type FlowWorkerEndpoints = Vec<(String, String)>;
type FlowWorkerChildren = Vec<std::process::Child>;

fn forward_worker_output(
    instance_id: String,
    reader: impl std::io::Read + Send + 'static,
    to_stderr: bool,
) {
    std::thread::spawn(move || {
        let prefix = format!("[worker:{instance_id}] ");
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let mut s = line.as_str();
                    if let Some(stripped) = s.strip_suffix('\n') {
                        s = stripped;
                    }
                    if let Some(stripped) = s.strip_suffix('\r') {
                        s = stripped;
                    }
                    if to_stderr {
                        eprintln!("{prefix}{s}");
                    } else {
                        println!("{prefix}{s}");
                    }
                }
                Err(_) => break,
            }
        }
    });
}

async fn spawn_flow_workers(
    extra_instances: &[manager::FlowInstanceSpec],
    config_path: Option<&str>,
) -> Result<(FlowWorkerEndpoints, FlowWorkerChildren), Box<dyn std::error::Error + Send + Sync>> {
    if extra_instances.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let config_path =
        config_path.ok_or("extra_flow_instances configured but --config not provided")?;

    let exe = std::env::current_exe()?;
    #[derive(Debug, Clone)]
    struct ParsedSpec {
        id: String,
        worker_addr: std::net::SocketAddr,
        metrics_addr: std::net::SocketAddr,
        profile_addr: std::net::SocketAddr,
    }

    let mut seen_ids = std::collections::HashSet::new();
    let mut seen_worker_addrs = std::collections::HashSet::new();
    let mut parsed = Vec::with_capacity(extra_instances.len());
    for spec in extra_instances {
        let id = spec.id.trim();
        if id.is_empty() {
            return Err("extra_flow_instances contains an empty id".into());
        }
        if id == "default" {
            return Err("extra_flow_instances must not include default".into());
        }
        if !seen_ids.insert(id.to_string()) {
            return Err(format!("duplicate flow instance id in config: {id}").into());
        }

        let worker_addr: std::net::SocketAddr = spec
            .worker_addr
            .parse()
            .map_err(|e| format!("invalid worker_addr for {id}: {e}"))?;
        if !worker_addr.ip().is_loopback() {
            return Err(format!(
                "worker_addr for {id} must be loopback, got {}",
                spec.worker_addr
            )
            .into());
        }
        if !seen_worker_addrs.insert(worker_addr) {
            return Err(format!("duplicate worker_addr in config: {worker_addr}").into());
        }

        let metrics_addr: std::net::SocketAddr = spec
            .metrics_addr
            .parse()
            .map_err(|e| format!("invalid metrics_addr for {id}: {e}"))?;
        if !metrics_addr.ip().is_loopback() {
            return Err(format!(
                "metrics_addr for {id} must be loopback, got {}",
                spec.metrics_addr
            )
            .into());
        }

        let profile_addr: std::net::SocketAddr = spec
            .profile_addr
            .parse()
            .map_err(|e| format!("invalid profile_addr for {id}: {e}"))?;
        if !profile_addr.ip().is_loopback() {
            return Err(format!(
                "profile_addr for {id} must be loopback, got {}",
                spec.profile_addr
            )
            .into());
        }

        parsed.push(ParsedSpec {
            id: id.to_string(),
            worker_addr,
            metrics_addr,
            profile_addr,
        });
    }

    let mut endpoints: FlowWorkerEndpoints = Vec::with_capacity(parsed.len());
    let mut children: FlowWorkerChildren = Vec::with_capacity(parsed.len());
    for spec in parsed {
        tracing::info!(
            flow_instance_id = %spec.id,
            worker_addr = %spec.worker_addr,
            metrics_addr = %spec.metrics_addr,
            profile_addr = %spec.profile_addr,
            "spawning flow worker"
        );

        let mut cmd = std::process::Command::new(&exe);
        cmd.arg("--worker")
            .arg("--flow-instance-id")
            .arg(&spec.id)
            .arg("--config")
            .arg(config_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn()?;
        let pid = child.id();
        endpoints.push((spec.id.to_string(), format!("http://{}", spec.worker_addr)));

        if let Some(stdout) = child.stdout.take() {
            forward_worker_output(spec.id.clone(), stdout, false);
        }
        if let Some(stderr) = child.stderr.take() {
            forward_worker_output(spec.id.clone(), stderr, true);
        }

        let mut ready = false;
        for _ in 1..=60 {
            match tokio::net::TcpStream::connect(spec.worker_addr).await {
                Ok(_) => {
                    ready = true;
                    break;
                }
                Err(_) => {
                    if let Ok(Some(status)) = child.try_wait() {
                        let _ = child.kill();
                        let _ = child.wait();
                        for mut prior in children {
                            let _ = prior.kill();
                            let _ = prior.wait();
                        }
                        return Err(format!("worker {} exited early: {status}", spec.id).into());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
        if !ready {
            let _ = child.kill();
            let _ = child.wait();
            for mut prior in children {
                let _ = prior.kill();
                let _ = prior.wait();
            }
            return Err(format!(
                "worker {} did not become ready on {}",
                spec.id, spec.worker_addr
            )
            .into());
        }

        tracing::info!(
            flow_instance_id = %spec.id,
            pid = ?pid,
            worker_addr = %spec.worker_addr,
            "flow worker ready"
        );
        children.push(child);
    }

    Ok((endpoints, children))
}

#[cfg(all(feature = "allocator-jemalloc", not(target_env = "msvc")))]
fn log_allocator() {
    tracing::info!("global allocator: jemalloc");
}

#[cfg(all(feature = "allocator-jemalloc", target_env = "msvc"))]
fn log_allocator() {
    tracing::info!("allocator-jemalloc enabled but using system allocator on MSVC");
}

#[cfg(not(feature = "allocator-jemalloc"))]
fn log_allocator() {
    tracing::info!("global allocator: system default");
}

#[cfg(feature = "profiling")]
pub fn start_profile_server(opts: &ServerOptions) {
    let addr_str = opts
        .profile_addr
        .clone()
        .unwrap_or_else(|| DEFAULT_PROFILE_ADDR.to_string());
    let default_freq_hz = opts
        .cpu_profile_freq_hz
        .filter(|freq| *freq > 0)
        .unwrap_or(DEFAULT_CPU_PROFILE_FREQUENCY_HZ);
    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(err) => {
            tracing::error!(error = %err, profile_addr = %addr_str, "invalid profile addr");
            return;
        }
    };

    tracing::info!(profile_addr = %addr, "enabling profiling endpoints");
    thread::spawn(move || {
        if let Err(err) = run_profile_server(addr, default_freq_hz) {
            tracing::error!(error = %err, "profile server error");
        }
    });
}

#[cfg(not(feature = "profiling"))]
pub fn start_profile_server(_opts: &ServerOptions) {}

#[cfg(feature = "profiling")]
fn run_profile_server(addr: SocketAddr, default_freq_hz: i32) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    tracing::info!(
        profile_addr = %addr,
        "CPU/heap endpoints at http://{addr}/debug/pprof/{{profile,heap}}"
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    disable_heap_profiling_for_current_thread();
                    if let Err(err) = handle_profile_connection(stream, default_freq_hz) {
                        tracing::error!(error = %err, "profile connection failed");
                    }
                });
            }
            Err(err) => tracing::error!(error = %err, "profile accept error"),
        }
    }
    Ok(())
}

#[cfg(feature = "profiling")]
fn handle_profile_connection(
    mut stream: TcpStream,
    default_freq_hz: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0u8; 2048];
    let len = stream.read(&mut buf)?;
    if len == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buf[..len]);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");
    if method != "GET" {
        write_response(&mut stream, 405, "text/plain", b"method not allowed")?;
        return Ok(());
    }
    let (path, query) = split_target(target);
    match path {
        "/debug/pprof/profile" => {
            let duration = parse_seconds(query).unwrap_or(30);
            let frequency_hz = parse_i32_param(query, "freq")
                .filter(|freq| *freq > 0)
                .unwrap_or(default_freq_hz);
            match generate_profile(duration, frequency_hz) {
                Ok(body) => write_response(&mut stream, 200, "application/octet-stream", &body)?,
                Err(err) => write_response(&mut stream, 500, "text/plain", err.as_bytes())?,
            }
        }
        "/debug/pprof/heap" => match capture_heap_profile() {
            Ok(body) => write_response(&mut stream, 200, "application/octet-stream", &body)?,
            Err(err) => write_response(&mut stream, 500, "text/plain", err.as_bytes())?,
        },
        _ => {
            write_response(&mut stream, 404, "text/plain", b"not found")?;
        }
    }
    Ok(())
}

#[cfg(feature = "profiling")]
fn generate_profile(duration: u64, frequency_hz: i32) -> Result<Vec<u8>, String> {
    // When jemalloc heap profiling is active, generating CPU pprof can allocate heavily and
    // pollute `/debug/pprof/heap` inuse samples. Disable heap profiling for the entire CPU profile
    // request; heap dumps will block until profiling finishes.
    suspend_jemalloc_heap_profiling(|| {
        let mut builder = ProfilerGuardBuilder::default().frequency(frequency_hz);
        #[cfg(any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64"
        ))]
        {
            builder = builder.blocklist(&DEFAULT_CPU_PROFILE_BLOCKLIST);
        }
        let guard = builder.build().map_err(|err| err.to_string())?;
        thread::sleep(std::time::Duration::from_secs(duration));

        let report = guard.report().build().map_err(|err| err.to_string())?;
        let profile = report.pprof().map_err(|err| err.to_string())?;
        let mut body = Vec::new();
        profile
            .write_to_vec(&mut body)
            .map_err(|err| err.to_string())?;
        drop(guard);
        Ok(body)
    })
}

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
fn capture_heap_profile() -> Result<Vec<u8>, String> {
    let _lock = PPROF_ENDPOINT_MUTEX.lock();

    // Ensure profiling is active; if jemalloc lacks profiling support, return a clear error.
    if let Err(err) = unsafe { raw::write(b"prof.active\0", true) } {
        return Err(format!(
            "jemalloc heap profiling not enabled (need _RJEM_MALLOC_CONF='prof:true,prof_active:true' with tikv-jemallocator profiling). error: {}",
            err
        ));
    }

    let filename = format!("/tmp/veloflux.{}.heap", process::id());
    let c_path = CString::new(filename.clone()).map_err(|err| err.to_string())?;
    unsafe {
        raw::write(b"prof.dump\0", c_path.as_ptr()).map_err(|err| err.to_string())?;
    }
    let body = fs::read(&filename).map_err(|err| err.to_string())?;
    let _ = fs::remove_file(&filename);
    Ok(body)
}

#[cfg(all(
    feature = "profiling",
    any(not(feature = "allocator-jemalloc"), target_env = "msvc")
))]
fn capture_heap_profile() -> Result<Vec<u8>, String> {
    Err(
        "jemalloc heap profiling requires feature `allocator-jemalloc` and a non-MSVC build"
            .to_string(),
    )
}

#[cfg(feature = "profiling")]
fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n",
        status,
        status_text,
        body.len(),
        content_type
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    Ok(())
}

#[cfg(feature = "profiling")]
fn split_target(target: &str) -> (&str, Option<&str>) {
    if let Some((path, query)) = target.split_once('?') {
        (path, Some(query))
    } else {
        (target, None)
    }
}

/// Prepare Flow registries/instance before initialization, so callers can
/// register custom encoders/decoders/connectors prior to loading storage.
pub fn prepare_registry() -> FlowInstance {
    manager::new_default_flow_instance()
}

#[cfg(feature = "profiling")]
fn parse_seconds(query: Option<&str>) -> Option<u64> {
    query
        .and_then(|q| {
            q.split('&').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                if key == "seconds" {
                    Some(value)
                } else {
                    None
                }
            })
        })
        .and_then(|value| value.parse::<u64>().ok())
}

#[cfg(feature = "profiling")]
fn parse_i32_param(query: Option<&str>, key: &str) -> Option<i32> {
    query
        .and_then(|q| {
            q.split('&').find_map(|pair| {
                let (k, value) = pair.split_once('=')?;
                if k == key {
                    Some(value)
                } else {
                    None
                }
            })
        })
        .and_then(|value| value.parse::<i32>().ok())
}

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
fn ensure_jemalloc_profiling() {
    // Best-effort: try to activate runtime profiling. If jemalloc was built
    // without profiling, mallctl will return an error and heap endpoint will
    // later surface a clearer message.
    let _ = unsafe { raw::write(b"prof.active\0", true) };
}

#[cfg(any(
    not(feature = "profiling"),
    not(feature = "allocator-jemalloc"),
    target_env = "msvc"
))]
fn ensure_jemalloc_profiling() {}

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
fn disable_heap_profiling_for_current_thread() {
    let _ = unsafe { raw::write(b"prof.thread_active\0", false) };
}

#[cfg(all(
    feature = "profiling",
    any(not(feature = "allocator-jemalloc"), target_env = "msvc")
))]
fn disable_heap_profiling_for_current_thread() {}

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
fn suspend_jemalloc_heap_profiling<T>(f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    let _lock = PPROF_ENDPOINT_MUTEX.lock();

    let prev_active: Option<bool> = match unsafe { raw::read(b"prof.active\0") } {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::warn!(error = %err, "failed to read jemalloc prof.active");
            None
        }
    };

    if let Err(err) = unsafe { raw::write(b"prof.active\0", false) } {
        tracing::warn!(error = %err, "failed to disable jemalloc prof.active");
        return f();
    }

    let result = f();

    let restore_active = prev_active.unwrap_or(true);
    if let Err(err) = unsafe { raw::write(b"prof.active\0", restore_active) } {
        tracing::warn!(error = %err, "failed to restore jemalloc prof.active");
    }

    result
}

#[cfg(all(
    feature = "profiling",
    any(not(feature = "allocator-jemalloc"), target_env = "msvc")
))]
fn suspend_jemalloc_heap_profiling<T>(f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    f()
}

#[cfg(feature = "metrics")]
pub async fn init_metrics_exporter(
    opts: &ServerOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = opts
        .metrics_addr
        .clone()
        .unwrap_or_else(|| DEFAULT_METRICS_ADDR.to_string())
        .parse()?;
    tracing::info!(metrics_addr = %addr, "enabling metrics exporter");
    let exporter = prometheus_exporter::start(addr)?;
    // Leak exporter handle so the HTTP endpoint stays alive for the duration of the process.
    Box::leak(Box::new(exporter));

    let poll_interval = opts
        .metrics_poll_interval_secs
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_METRICS_POLL_INTERVAL_SECS);

    spawn_tokio_metrics_collector(StdDuration::from_secs(poll_interval));

    tokio::spawn(async move {
        let mut system = System::new();
        let pid = Pid::from_u32(process::id());
        loop {
            system.refresh_process(pid);
            if let Some(proc_info) = system.process(pid) {
                let cpu_usage_percent = proc_info.cpu_usage() as f64;
                CPU_USAGE_GAUGE.set(cpu_usage_percent as i64);
                MEMORY_USAGE_GAUGE.set(proc_info.memory() as i64);
            } else {
                CPU_USAGE_GAUGE.set(0);
                MEMORY_USAGE_GAUGE.set(0);
            }
            update_heap_metrics();
            sleep(Duration::from_secs(poll_interval)).await;
        }
    });
    Ok(())
}

#[cfg(not(feature = "metrics"))]
pub async fn init_metrics_exporter(
    _opts: &ServerOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Ok(())
}

#[cfg(all(
    feature = "metrics",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
fn update_heap_metrics() {
    if epoch::advance().is_err() {
        return;
    }
    let allocated = stats::allocated::read().unwrap_or(0);
    let resident = stats::resident::read().unwrap_or(0);
    HEAP_IN_USE_GAUGE.set(clamp_usize_to_i64(allocated));
    HEAP_IN_ALLOCATOR_GAUGE.set(clamp_usize_to_i64(resident));
}

#[cfg(all(
    feature = "metrics",
    any(not(feature = "allocator-jemalloc"), target_env = "msvc")
))]
fn update_heap_metrics() {
    HEAP_IN_USE_GAUGE.set(0);
    HEAP_IN_ALLOCATOR_GAUGE.set(0);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)]
fn update_heap_metrics() {}

#[cfg(all(feature = "metrics", feature = "allocator-jemalloc"))]
fn clamp_usize_to_i64(value: usize) -> i64 {
    if value > i64::MAX as usize {
        i64::MAX
    } else {
        value as i64
    }
}
#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use std::ffi::CString;
#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use std::fs;
