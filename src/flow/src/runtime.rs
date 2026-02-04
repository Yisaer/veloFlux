use std::future::Future;
use std::sync::Arc;

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

struct ManagedRuntime {
    runtime: Option<Runtime>,
}

impl ManagedRuntime {
    fn new(runtime: Runtime) -> Self {
        Self {
            runtime: Some(runtime),
        }
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        self.runtime
            .as_ref()
            .expect("managed runtime missing")
            .handle()
    }
}

impl Drop for ManagedRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

#[derive(Clone)]
pub(crate) struct TaskSpawner {
    runtime: Arc<ManagedRuntime>,
}

impl TaskSpawner {
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self {
            runtime: Arc::new(ManagedRuntime::new(runtime)),
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
