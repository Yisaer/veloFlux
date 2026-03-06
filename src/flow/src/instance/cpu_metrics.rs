use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Default)]
pub(super) struct RuntimeThreadRegistry {
    tids: Arc<Mutex<BTreeSet<u32>>>,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl RuntimeThreadRegistry {
    pub(super) fn insert(&self, tid: u32) {
        self.tids.lock().insert(tid);
    }

    fn snapshot(&self) -> Vec<u32> {
        self.tids.lock().iter().copied().collect()
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone)]
pub(super) struct FlowInstanceCpuMetricsState {
    ticks_per_second: u64,
    thread_registry: RuntimeThreadRegistry,
    previous: Arc<Mutex<Option<CpuSamplePoint>>>,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Copy)]
struct CpuSamplePoint {
    captured_at: Instant,
    total_cpu_ticks: u64,
}

static FLOW_INSTANCE_CPU_METRICS_REGISTRY: Lazy<
    Mutex<HashMap<String, FlowInstanceCpuMetricsState>>,
> = Lazy::new(|| Mutex::new(HashMap::new()));

pub(super) fn build_flow_instance_cpu_metrics_state(
    thread_registry: RuntimeThreadRegistry,
) -> Option<FlowInstanceCpuMetricsState> {
    #[cfg(target_os = "linux")]
    {
        let ticks_per_second = match ticks_per_second() {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = %err, "disable flow instance cpu metrics");
                return None;
            }
        };
        Some(FlowInstanceCpuMetricsState {
            ticks_per_second,
            thread_registry,
            previous: Arc::new(Mutex::new(None)),
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = thread_registry;
        None
    }
}

pub(super) fn sample_flow_instance_cpu_usage_percent(
    state: &FlowInstanceCpuMetricsState,
) -> Result<Option<f64>, super::FlowInstanceCpuMetricsError> {
    #[cfg(target_os = "linux")]
    {
        let mut total_cpu_ticks = 0u64;
        for tid in state.thread_registry.snapshot() {
            match read_thread_cpu_ticks(tid) {
                Ok(value) => {
                    total_cpu_ticks = total_cpu_ticks.saturating_add(value);
                }
                Err(super::FlowInstanceCpuMetricsError::ThreadNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }

        let captured_at = Instant::now();
        let mut guard = state.previous.lock();
        let previous = guard.replace(CpuSamplePoint {
            captured_at,
            total_cpu_ticks,
        });
        let Some(previous) = previous else {
            return Ok(None);
        };

        let wall = captured_at.saturating_duration_since(previous.captured_at);
        if wall.is_zero() {
            return Ok(None);
        }

        let delta_ticks = total_cpu_ticks.saturating_sub(previous.total_cpu_ticks);
        let cpu_seconds = delta_ticks as f64 / state.ticks_per_second as f64;
        let usage_percent = (cpu_seconds / wall.as_secs_f64()) * 100.0;
        Ok(Some(usage_percent.max(0.0)))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = state;
        Err(super::FlowInstanceCpuMetricsError::Unsupported)
    }
}

pub(super) fn register_flow_instance_cpu_metrics_state(
    instance_id: &str,
    state: Option<FlowInstanceCpuMetricsState>,
) {
    let mut guard = FLOW_INSTANCE_CPU_METRICS_REGISTRY.lock();
    if let Some(state) = state {
        guard.insert(instance_id.to_string(), state);
    } else {
        guard.remove(instance_id);
    }
}

pub(crate) fn collect_registered_flow_instance_cpu_metrics() {
    let samplers = {
        let guard = FLOW_INSTANCE_CPU_METRICS_REGISTRY.lock();
        guard
            .iter()
            .map(|(instance_id, state)| (instance_id.clone(), state.clone()))
            .collect::<Vec<_>>()
    };

    for (instance_id, state) in samplers {
        match sample_flow_instance_cpu_usage_percent(&state) {
            Ok(Some(value)) => {
                crate::metrics::set_flow_instance_cpu_usage_percent(&instance_id, value);
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    flow_instance_id = %instance_id,
                    error = %err,
                    "sample flow instance cpu usage failed"
                );
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn ticks_per_second() -> Result<u64, super::FlowInstanceCpuMetricsError> {
    let value = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if value <= 0 {
        return Err(super::FlowInstanceCpuMetricsError::Other(
            "sysconf(_SC_CLK_TCK) returned non-positive value".to_string(),
        ));
    }
    Ok(value as u64)
}

#[cfg(target_os = "linux")]
fn read_thread_cpu_ticks(tid: u32) -> Result<u64, super::FlowInstanceCpuMetricsError> {
    let path = format!("/proc/self/task/{tid}/stat");
    let raw = std::fs::read_to_string(&path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            super::FlowInstanceCpuMetricsError::ThreadNotFound(tid)
        } else {
            super::FlowInstanceCpuMetricsError::Other(format!("read {path}: {err}"))
        }
    })?;

    let end = raw.rfind(')').ok_or_else(|| {
        super::FlowInstanceCpuMetricsError::Other(format!("malformed task stat: {path}"))
    })?;
    let rest = raw.get(end + 2..).ok_or_else(|| {
        super::FlowInstanceCpuMetricsError::Other(format!("malformed task stat: {path}"))
    })?;
    let fields = rest.split_whitespace().collect::<Vec<_>>();
    if fields.len() <= 12 {
        return Err(super::FlowInstanceCpuMetricsError::Other(format!(
            "malformed task stat field count in {path}: {}",
            fields.len()
        )));
    }

    let utime = fields[11].parse::<u64>().map_err(|err| {
        super::FlowInstanceCpuMetricsError::Other(format!("parse utime from {path}: {err}"))
    })?;
    let stime = fields[12].parse::<u64>().map_err(|err| {
        super::FlowInstanceCpuMetricsError::Other(format!("parse stime from {path}: {err}"))
    })?;
    Ok(utime + stime)
}
