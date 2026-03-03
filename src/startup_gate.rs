use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const READY_SUFFIX: &str = ".ready";
const FAIL_SUFFIX: &str = ".fail";
const WAIT_POLL_INTERVAL_MS: u64 = 20;
pub const DEFAULT_STARTUP_GATE_TIMEOUT_MS: u64 = 15_000;

fn marker_name(instance_id: &str, suffix: &str) -> String {
    let sanitized = instance_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{sanitized}{suffix}")
}

fn ready_file(run_dir: &Path, instance_id: &str) -> PathBuf {
    run_dir.join(marker_name(instance_id, READY_SUFFIX))
}

fn fail_file(run_dir: &Path, instance_id: &str) -> PathBuf {
    run_dir.join(marker_name(instance_id, FAIL_SUFFIX))
}

fn write_atomic(
    path: &Path,
    content: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let parent = path
        .parent()
        .ok_or("startup gate marker path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("gate"),
        std::process::id()
    ));
    std::fs::write(&tmp, content)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn remove_if_exists(path: &Path) {
    if let Err(err) = std::fs::remove_file(path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %path.display(), error = %err, "failed to remove startup gate marker");
        }
    }
}

fn create_run_id() -> String {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("boot-{}-{ts_ms}", std::process::id())
}

fn cleanup_stale_entries(base_dir: &Path) {
    let entries = match std::fs::read_dir(base_dir) {
        Ok(entries) => entries,
        Err(err) => {
            tracing::warn!(
                startup_gate_base = %base_dir.display(),
                error = %err,
                "failed to scan startup gate base directory"
            );
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let removed = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        if let Err(err) = removed {
            tracing::warn!(
                startup_gate_path = %path.display(),
                error = %err,
                "failed to remove stale startup gate entry"
            );
        } else {
            tracing::info!(
                startup_gate_path = %path.display(),
                "removed stale startup gate entry"
            );
        }
    }
}

#[derive(Debug)]
pub struct StartupGateSession {
    run_dir: PathBuf,
    timeout_ms: u64,
}

impl StartupGateSession {
    pub fn create(
        base_dir: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let base_dir = base_dir.trim();
        if base_dir.is_empty() {
            return Err("startup_gate_path must not be empty".into());
        }
        let base_dir = PathBuf::from(base_dir);
        std::fs::create_dir_all(&base_dir)?;
        cleanup_stale_entries(&base_dir);

        let run_dir = base_dir.join(create_run_id());
        std::fs::create_dir_all(&run_dir)?;
        let timeout_ms = timeout_ms.unwrap_or(DEFAULT_STARTUP_GATE_TIMEOUT_MS);
        if timeout_ms == 0 {
            return Err("startup_gate_timeout_ms must be greater than 0".into());
        }

        Ok(Self {
            run_dir,
            timeout_ms,
        })
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    pub fn mark_ready(
        &self,
        instance_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ready = ready_file(&self.run_dir, instance_id);
        let fail = fail_file(&self.run_dir, instance_id);
        remove_if_exists(&fail);
        write_atomic(&ready, "ready\n")
    }

    pub fn mark_failed(
        &self,
        instance_id: &str,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ready = ready_file(&self.run_dir, instance_id);
        let fail = fail_file(&self.run_dir, instance_id);
        remove_if_exists(&ready);
        write_atomic(&fail, &format!("{reason}\n"))
    }

    pub fn cleanup(&self) {
        if let Err(err) = std::fs::remove_dir_all(&self.run_dir) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    startup_gate_run_dir = %self.run_dir.display(),
                    error = %err,
                    "failed to cleanup startup gate run directory"
                );
            }
        } else {
            tracing::info!(
                startup_gate_run_dir = %self.run_dir.display(),
                "cleaned startup gate run directory"
            );
        }
    }
}

pub fn wait_until_ready(
    run_dir: &str,
    instance_id: &str,
    timeout_ms: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if timeout_ms == 0 {
        return Err("startup gate timeout must be > 0".into());
    }
    let run_dir = PathBuf::from(run_dir.trim());
    let ready = ready_file(&run_dir, instance_id);
    let fail = fail_file(&run_dir, instance_id);
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);

    loop {
        if ready.exists() {
            return Ok(());
        }
        if fail.exists() {
            let reason = std::fs::read_to_string(&fail)
                .unwrap_or_else(|_| "unknown startup gate failure".to_string());
            return Err(format!(
                "startup gate failed for instance {instance_id}: {}",
                reason.trim()
            )
            .into());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "startup gate timed out for instance {instance_id} after {} ms (run_dir={})",
                timeout_ms,
                run_dir.display()
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(WAIT_POLL_INTERVAL_MS));
    }
}
