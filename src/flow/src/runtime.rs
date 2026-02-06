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
