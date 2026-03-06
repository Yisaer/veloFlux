use std::future::Future;
use std::sync::Arc;

use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

enum ManagedRuntimeInner {
    Owned(Option<Runtime>),
    Shared(Handle),
}

impl ManagedRuntimeInner {
    fn handle(&self) -> &Handle {
        match self {
            ManagedRuntimeInner::Owned(runtime) => {
                runtime.as_ref().expect("managed runtime missing").handle()
            }
            ManagedRuntimeInner::Shared(handle) => handle,
        }
    }
}

impl Drop for ManagedRuntimeInner {
    fn drop(&mut self) {
        if let ManagedRuntimeInner::Owned(runtime) = self {
            if let Some(runtime) = runtime.take() {
                runtime.shutdown_background();
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct TaskSpawner {
    runtime: Arc<ManagedRuntimeInner>,
}

impl TaskSpawner {
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self {
            runtime: Arc::new(ManagedRuntimeInner::Owned(Some(runtime))),
        }
    }

    pub(crate) fn from_handle(handle: Handle) -> Self {
        Self {
            runtime: Arc::new(ManagedRuntimeInner::Shared(handle)),
        }
    }

    pub(crate) fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.handle().spawn(fut)
    }

    pub(crate) fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.runtime.handle().spawn_blocking(f)
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn current_thread_tid() -> Result<u32, String> {
    let link = std::fs::read_link("/proc/thread-self")
        .map_err(|err| format!("read /proc/thread-self: {err}"))?;
    let tid = link
        .components()
        .next_back()
        .and_then(|component| component.as_os_str().to_str())
        .ok_or_else(|| format!("parse /proc/thread-self target: {}", link.display()))?;
    tid.parse::<u32>()
        .map_err(|err| format!("parse thread tid `{tid}`: {err}"))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn current_thread_tid() -> Result<u32, String> {
    Err("current thread tid lookup is unsupported on this platform".to_string())
}

#[cfg(target_os = "linux")]
pub(crate) fn bind_current_thread_to_cgroup(thread_cgroup_path: &str) -> Result<u32, String> {
    if !thread_cgroup_path.starts_with('/') {
        return Err(format!(
            "thread cgroup path must be absolute, got `{thread_cgroup_path}`"
        ));
    }

    let tid = current_thread_tid()?;
    let cgroup_threads = format!("/sys/fs/cgroup{thread_cgroup_path}/cgroup.threads");
    std::fs::write(&cgroup_threads, tid.to_string())
        .map_err(|err| format!("write {cgroup_threads}: {err}"))?;
    Ok(tid)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn bind_current_thread_to_cgroup(thread_cgroup_path: &str) -> Result<u32, String> {
    Err(format!(
        "thread cgroup binding is unsupported on this platform: {thread_cgroup_path}"
    ))
}
