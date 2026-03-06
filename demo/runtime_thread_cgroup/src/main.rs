use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tokio::runtime::{Builder, Runtime};

type Error = Box<dyn std::error::Error + Send + Sync>;

const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup/veloflux-runtime-demo";
const DEFAULT_DURATION_SECS: u64 = 20;
const DEFAULT_SAMPLE_MS: u64 = 1000;
const DEFAULT_WORKER_THREADS: usize = 1;
const DEFAULT_TASKS_PER_RUNTIME: usize = 8;
const DEFAULT_CHUNK_ITERS: u64 = 5_000_000;

fn main() -> Result<(), Error> {
    let config = Config::parse(std::env::args().skip(1))?;
    validate_process_in_root_cgroup(&config.cgroup_root)?;
    println!(
        "demo config: cgroup_root={} duration={}s sample={}ms worker_threads={} tasks_per_runtime={} chunk_iters={} machine_cpus={}",
        config.cgroup_root.display(),
        config.duration_secs,
        config.sample_ms,
        config.worker_threads,
        config.tasks_per_runtime,
        config.chunk_iters,
        config.machine_cpus,
    );

    let runtime_a = DemoRuntime::new(
        RuntimeSpec::new(
            "rt_a",
            config.cgroup_root.join("rt_a"),
            config.worker_threads,
            config.tasks_per_runtime,
            config.chunk_iters,
        ),
        config.ticks_per_second,
    )?;
    let runtime_b = DemoRuntime::new(
        RuntimeSpec::new(
            "rt_b",
            config.cgroup_root.join("rt_b"),
            config.worker_threads,
            config.tasks_per_runtime,
            config.chunk_iters,
        ),
        config.ticks_per_second,
    )?;

    runtime_a.start();
    runtime_b.start();

    let sampler = Sampler::new(config.machine_cpus, config.ticks_per_second);
    let start = Instant::now();
    let mut previous_a = sampler.capture(&runtime_a)?;
    let mut previous_b = sampler.capture(&runtime_b)?;

    while start.elapsed() < Duration::from_secs(config.duration_secs) {
        thread::sleep(Duration::from_millis(config.sample_ms));
        let current_a = sampler.capture(&runtime_a)?;
        let current_b = sampler.capture(&runtime_b)?;
        let wall = current_a.captured_at.duration_since(previous_a.captured_at);

        print_runtime_delta(
            &runtime_a.spec.id,
            &previous_a,
            &current_a,
            wall,
            config.machine_cpus,
        );
        print_runtime_delta(
            &runtime_b.spec.id,
            &previous_b,
            &current_b,
            wall,
            config.machine_cpus,
        );
        println!();

        previous_a = current_a;
        previous_b = current_b;
    }

    runtime_a.stop();
    runtime_b.stop();

    let final_a = sampler.capture(&runtime_a)?;
    let final_b = sampler.capture(&runtime_b)?;
    println!("final summary:");
    print_runtime_total(
        &runtime_a.spec.id,
        &final_a,
        start.elapsed(),
        config.machine_cpus,
    );
    print_runtime_total(
        &runtime_b.spec.id,
        &final_b,
        start.elapsed(),
        config.machine_cpus,
    );
    Ok(())
}

fn print_runtime_delta(
    id: &str,
    previous: &RuntimeSnapshot,
    current: &RuntimeSnapshot,
    wall: Duration,
    machine_cpus: usize,
) {
    let cpu_delta = current.thread_cpu_seconds - previous.thread_cpu_seconds;
    let core_usage = if wall.is_zero() {
        0.0
    } else {
        cpu_delta / wall.as_secs_f64()
    };
    let one_core_percent = core_usage * 100.0;
    let machine_percent = if machine_cpus == 0 {
        0.0
    } else {
        one_core_percent / machine_cpus as f64
    };
    let ops_delta = current.ops.saturating_sub(previous.ops);
    let ops_per_sec = if wall.is_zero() {
        0.0
    } else {
        ops_delta as f64 / wall.as_secs_f64()
    };
    let busy_delta = current.busy_seconds - previous.busy_seconds;
    let busy_core_usage = if wall.is_zero() {
        0.0
    } else {
        busy_delta / wall.as_secs_f64()
    };
    let cgroup_usage_delta = current
        .cgroup_cpu
        .usage_usec
        .saturating_sub(previous.cgroup_cpu.usage_usec);
    let cgroup_core_usage = if wall.is_zero() {
        0.0
    } else {
        cgroup_usage_delta as f64 / wall.as_micros() as f64
    };
    let throttled_delta = current
        .cgroup_cpu
        .throttled_usec
        .saturating_sub(previous.cgroup_cpu.throttled_usec);
    let nr_throttled_delta = current
        .cgroup_cpu
        .nr_throttled
        .saturating_sub(previous.cgroup_cpu.nr_throttled);

    println!(
        "{id}: ops/s={ops_per_sec:.0} thread_cpu={one_core_percent:.1}%_of_one_core machine={machine_percent:.2}% busy={:.1}%_of_one_core cgroup={:.1}%_of_one_core threads={} nr_throttled=+{} throttled_ms=+{:.1}",
        busy_core_usage * 100.0,
        cgroup_core_usage * 100.0,
        current.thread_ids.len(),
        nr_throttled_delta,
        throttled_delta as f64 / 1000.0,
    );
}

fn print_runtime_total(id: &str, snapshot: &RuntimeSnapshot, wall: Duration, machine_cpus: usize) {
    let core_usage = if wall.is_zero() {
        0.0
    } else {
        snapshot.thread_cpu_seconds / wall.as_secs_f64()
    };
    let one_core_percent = core_usage * 100.0;
    let machine_percent = if machine_cpus == 0 {
        0.0
    } else {
        one_core_percent / machine_cpus as f64
    };

    println!(
        "{id}: total_ops={} thread_cpu={one_core_percent:.1}%_of_one_core machine={machine_percent:.2}% cgroup_usage_ms={:.1} throttled_count={} throttled_ms={:.1}",
        snapshot.ops,
        snapshot.cgroup_cpu.usage_usec as f64 / 1000.0,
        snapshot.cgroup_cpu.nr_throttled,
        snapshot.cgroup_cpu.throttled_usec as f64 / 1000.0,
    );
}

#[derive(Clone, Debug)]
struct Config {
    cgroup_root: PathBuf,
    duration_secs: u64,
    sample_ms: u64,
    worker_threads: usize,
    tasks_per_runtime: usize,
    chunk_iters: u64,
    ticks_per_second: u64,
    machine_cpus: usize,
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, Error> {
        let mut values = BTreeMap::new();
        let mut iter = args.peekable();
        while let Some(arg) = iter.next() {
            if !arg.starts_with("--") {
                return Err(format!("unsupported positional argument: {arg}").into());
            }
            let Some(key) = arg.strip_prefix("--") else {
                continue;
            };
            let value = iter
                .next()
                .ok_or_else(|| format!("missing value for --{key}"))?;
            values.insert(key.to_string(), value);
        }

        let ticks_per_second = ticks_per_second()?;
        let machine_cpus = thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);

        Ok(Self {
            cgroup_root: PathBuf::from(
                values
                    .remove("cgroup-root")
                    .unwrap_or_else(|| DEFAULT_CGROUP_ROOT.to_string()),
            ),
            duration_secs: parse_u64(&mut values, "duration-secs", DEFAULT_DURATION_SECS)?,
            sample_ms: parse_u64(&mut values, "sample-ms", DEFAULT_SAMPLE_MS)?,
            worker_threads: parse_usize(&mut values, "worker-threads", DEFAULT_WORKER_THREADS)?,
            tasks_per_runtime: parse_usize(
                &mut values,
                "tasks-per-runtime",
                DEFAULT_TASKS_PER_RUNTIME,
            )?,
            chunk_iters: parse_u64(&mut values, "chunk-iters", DEFAULT_CHUNK_ITERS)?,
            ticks_per_second,
            machine_cpus,
        })
    }
}

fn parse_u64(values: &mut BTreeMap<String, String>, key: &str, default: u64) -> Result<u64, Error> {
    match values.remove(key) {
        Some(value) => value
            .parse::<u64>()
            .map_err(|err| format!("parse --{key}={value}: {err}").into()),
        None => Ok(default),
    }
}

fn parse_usize(
    values: &mut BTreeMap<String, String>,
    key: &str,
    default: usize,
) -> Result<usize, Error> {
    match values.remove(key) {
        Some(value) => value
            .parse::<usize>()
            .map_err(|err| format!("parse --{key}={value}: {err}").into()),
        None => Ok(default),
    }
}

#[derive(Clone, Debug)]
struct RuntimeSpec {
    id: String,
    cgroup_path: PathBuf,
    worker_threads: usize,
    tasks_per_runtime: usize,
    chunk_iters: u64,
}

impl RuntimeSpec {
    fn new(
        id: impl Into<String>,
        cgroup_path: PathBuf,
        worker_threads: usize,
        tasks_per_runtime: usize,
        chunk_iters: u64,
    ) -> Self {
        Self {
            id: id.into(),
            cgroup_path,
            worker_threads,
            tasks_per_runtime,
            chunk_iters,
        }
    }
}

struct DemoRuntime {
    spec: RuntimeSpec,
    runtime: Runtime,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    thread_registry: Arc<ThreadRegistry>,
}

impl DemoRuntime {
    fn new(spec: RuntimeSpec, ticks_per_second: u64) -> Result<Self, Error> {
        let thread_registry = Arc::new(ThreadRegistry::default());
        let stop = Arc::new(AtomicBool::new(false));
        let ops = Arc::new(AtomicU64::new(0));
        let cgroup_path = spec.cgroup_path.clone();
        let runtime_id = spec.id.clone();
        let registry_on_start = Arc::clone(&thread_registry);
        let name_counter = Arc::new(AtomicU64::new(0));
        let name_counter_for_thread = Arc::clone(&name_counter);
        let runtime_id_for_name = runtime_id.clone();
        let runtime_id_for_start = runtime_id.clone();
        let cgroup_path_for_start = cgroup_path.clone();

        let runtime = Builder::new_multi_thread()
            .worker_threads(spec.worker_threads)
            .enable_all()
            .thread_name_fn(move || {
                let next = name_counter_for_thread.fetch_add(1, Ordering::Relaxed);
                format!("demo-{runtime_id_for_name}-w{next}")
            })
            .on_thread_start(move || {
                match register_current_thread(&cgroup_path_for_start, &registry_on_start) {
                    Ok(tid) => {
                        println!(
                            "thread start: runtime={} tid={} cgroup={}",
                            runtime_id_for_start,
                            tid,
                            cgroup_path_for_start.display(),
                        );
                    }
                    Err(err) => {
                        eprintln!(
                            "thread start bind failed: runtime={} cgroup={} error={err}",
                            runtime_id_for_start,
                            cgroup_path_for_start.display(),
                        );
                        std::process::exit(1);
                    }
                }
            })
            .build()?;

        if ticks_per_second == 0 {
            return Err("ticks_per_second must be positive".into());
        }

        Ok(Self {
            spec,
            runtime,
            stop,
            ops,
            thread_registry,
        })
    }

    fn start(&self) {
        for task_index in 0..self.spec.tasks_per_runtime {
            let stop = Arc::clone(&self.stop);
            let ops = Arc::clone(&self.ops);
            let chunk_iters = self.spec.chunk_iters;
            let runtime_id = self.spec.id.clone();
            self.runtime.spawn(async move {
                let mut state = seed_from(runtime_id.as_bytes(), task_index as u64 + 1);
                while !stop.load(Ordering::Relaxed) {
                    state = burn_cpu(state, chunk_iters);
                    ops.fetch_add(chunk_iters, Ordering::Relaxed);
                    black_box(state);
                    tokio::task::yield_now().await;
                }
            });
        }
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.runtime.block_on(async {
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
    }
}

#[derive(Default)]
struct ThreadRegistry {
    tids: Mutex<BTreeSet<u32>>,
}

impl ThreadRegistry {
    fn insert(&self, tid: u32) {
        self.tids.lock().expect("lock thread registry").insert(tid);
    }

    fn snapshot(&self) -> Vec<u32> {
        self.tids
            .lock()
            .expect("lock thread registry")
            .iter()
            .copied()
            .collect()
    }
}

fn seed_from(prefix: &[u8], salt: u64) -> u64 {
    let mut state = salt.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for byte in prefix {
        state ^= *byte as u64;
        state = mix(state);
    }
    state
}

fn burn_cpu(mut state: u64, chunk_iters: u64) -> u64 {
    for step in 0..chunk_iters {
        state = mix(state ^ step);
    }
    state
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^= value >> 31;
    value.rotate_left(17)
}

fn register_current_thread(cgroup_path: &Path, registry: &ThreadRegistry) -> Result<u32, Error> {
    let tid = current_tid();
    join_tid_to_cgroup(tid, cgroup_path)?;
    registry.insert(tid);
    Ok(tid)
}

#[cfg(target_os = "linux")]
fn current_tid() -> u32 {
    unsafe { libc::syscall(libc::SYS_gettid) as u32 }
}

#[cfg(not(target_os = "linux"))]
fn current_tid() -> u32 {
    std::process::id()
}

fn join_tid_to_cgroup(tid: u32, cgroup_path: &Path) -> Result<(), Error> {
    let path = cgroup_path.join("cgroup.threads");
    fs::write(&path, format!("{tid}\n"))
        .map_err(|err| format!("write {}: {err}", path.display()).into())
}

#[derive(Clone, Debug, Default)]
struct CgroupCpuStat {
    usage_usec: u64,
    nr_periods: u64,
    nr_throttled: u64,
    throttled_usec: u64,
}

#[derive(Clone, Debug)]
struct RuntimeSnapshot {
    captured_at: Instant,
    ops: u64,
    thread_ids: Vec<u32>,
    thread_cpu_seconds: f64,
    busy_seconds: f64,
    cgroup_cpu: CgroupCpuStat,
}

struct Sampler {
    machine_cpus: usize,
    ticks_per_second: u64,
}

impl Sampler {
    fn new(machine_cpus: usize, ticks_per_second: u64) -> Self {
        Self {
            machine_cpus,
            ticks_per_second,
        }
    }

    fn capture(&self, runtime: &DemoRuntime) -> Result<RuntimeSnapshot, Error> {
        let thread_ids = runtime.thread_registry.snapshot();
        let thread_cpu_seconds = thread_ids.iter().try_fold(0.0f64, |sum, tid| {
            let stat = read_thread_cpu_ticks(*tid)?;
            Ok::<_, Error>(sum + stat as f64 / self.ticks_per_second as f64)
        })?;
        let busy_seconds = runtime_busy_seconds(runtime.runtime.metrics());
        let cgroup_cpu = read_cgroup_cpu_stat(&runtime.spec.cgroup_path)?;

        let _ = self.machine_cpus;

        Ok(RuntimeSnapshot {
            captured_at: Instant::now(),
            ops: runtime.ops.load(Ordering::Relaxed),
            thread_ids,
            thread_cpu_seconds,
            busy_seconds,
            cgroup_cpu,
        })
    }
}

fn runtime_busy_seconds(metrics: tokio::runtime::RuntimeMetrics) -> f64 {
    let workers = metrics.num_workers();
    let mut total = 0.0f64;
    for worker in 0..workers {
        total += metrics.worker_total_busy_duration(worker).as_secs_f64();
    }
    total
}

fn read_thread_cpu_ticks(tid: u32) -> Result<u64, Error> {
    let path = format!("/proc/self/task/{tid}/stat");
    let raw = fs::read_to_string(&path).map_err(|err| format!("read {path}: {err}"))?;
    let end = raw
        .rfind(')')
        .ok_or_else(|| format!("malformed task stat: missing ')' in {path}"))?;
    let rest = raw
        .get(end + 2..)
        .ok_or_else(|| format!("malformed task stat payload: {path}"))?;
    let fields = rest.split_whitespace().collect::<Vec<_>>();
    if fields.len() <= 12 {
        return Err(format!(
            "malformed task stat field count in {path}: {}",
            fields.len()
        )
        .into());
    }
    let utime = fields[11]
        .parse::<u64>()
        .map_err(|err| format!("parse utime from {path}: {err}"))?;
    let stime = fields[12]
        .parse::<u64>()
        .map_err(|err| format!("parse stime from {path}: {err}"))?;
    Ok(utime + stime)
}

fn read_cgroup_cpu_stat(path: &Path) -> Result<CgroupCpuStat, Error> {
    let stat_path = path.join("cpu.stat");
    let raw = fs::read_to_string(&stat_path)
        .map_err(|err| format!("read {}: {err}", stat_path.display()))?;
    let mut stat = CgroupCpuStat::default();
    for line in raw.lines() {
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            continue;
        };
        let Some(value) = parts.next() else {
            continue;
        };
        match key {
            "usage_usec" => stat.usage_usec = parse_stat_u64(key, value, &stat_path)?,
            "nr_periods" => stat.nr_periods = parse_stat_u64(key, value, &stat_path)?,
            "nr_throttled" => stat.nr_throttled = parse_stat_u64(key, value, &stat_path)?,
            "throttled_usec" => stat.throttled_usec = parse_stat_u64(key, value, &stat_path)?,
            _ => {}
        }
    }
    Ok(stat)
}

fn parse_stat_u64(key: &str, value: &str, path: &Path) -> Result<u64, Error> {
    value
        .parse::<u64>()
        .map_err(|err| format!("parse {key}={} from {}: {err}", value, path.display()).into())
}

fn ticks_per_second() -> Result<u64, Error> {
    let value = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if value <= 0 {
        return Err("sysconf(_SC_CLK_TCK) returned a non-positive value".into());
    }
    Ok(value as u64)
}

fn validate_process_in_root_cgroup(cgroup_root: &Path) -> Result<(), Error> {
    let expected = expected_cgroup_relative_path(cgroup_root)?;
    let actual = current_process_cgroup_path()?;
    if actual != expected {
        return Err(format!(
            "current process is not in the threaded domain root: expected {expected}, got {actual}. launch the demo via run_demo.sh or move the process into {expected} before starting runtimes"
        )
        .into());
    }
    Ok(())
}

fn expected_cgroup_relative_path(cgroup_root: &Path) -> Result<String, Error> {
    let root = cgroup_root.strip_prefix("/sys/fs/cgroup").map_err(|_| {
        format!(
            "cgroup root must stay under /sys/fs/cgroup, got {}",
            cgroup_root.display()
        )
    })?;
    let text = if root.as_os_str().is_empty() {
        "/".to_string()
    } else {
        format!("/{}", root.display())
    };
    Ok(text.replace("//", "/"))
}

fn current_process_cgroup_path() -> Result<String, Error> {
    let raw = fs::read_to_string("/proc/self/cgroup")
        .map_err(|err| format!("read /proc/self/cgroup: {err}"))?;
    for line in raw.lines() {
        if let Some(path) = line.strip_prefix("0::") {
            return Ok(path.trim().to_string());
        }
    }
    Err("missing unified cgroup v2 entry in /proc/self/cgroup".into())
}
