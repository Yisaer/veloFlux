#[cfg(target_os = "linux")]
use std::io::Write;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
use std::path::{Component, Path};

#[cfg(target_os = "linux")]
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

fn validate_cgroup_path(path: &str) -> Result<&str, Box<dyn std::error::Error + Send + Sync>> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("cgroup path must not be empty".into());
    }
    if !trimmed.starts_with('/') {
        return Err(format!("cgroup path must start with '/', got: {trimmed}").into());
    }
    if trimmed.contains('\0') {
        return Err("cgroup path must not contain NUL".into());
    }
    let p = Path::new(trimmed);
    for comp in p.components() {
        if matches!(comp, Component::ParentDir) {
            return Err(format!("cgroup path must not contain '..': {trimmed}").into());
        }
    }
    Ok(trimmed)
}

#[cfg(target_os = "linux")]
fn cgroup_procs_path(
    cgroup_path: &str,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let cgroup_path = validate_cgroup_path(cgroup_path)?;
    let rel = cgroup_path.strip_prefix('/').unwrap_or(cgroup_path).trim();
    let dir = Path::new(CGROUP_ROOT).join(rel);
    Ok(dir.join("cgroup.procs"))
}

#[cfg(target_os = "linux")]
pub fn join_pid(
    pid: u32,
    cgroup_path: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let procs_path = cgroup_procs_path(cgroup_path)?;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(&procs_path)
        .map_err(|err| format!("open {}: {err}", procs_path.display()))?;
    writeln!(f, "{pid}").map_err(|err| format!("write {}: {err}", procs_path.display()))?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn join_pid(
    _pid: u32,
    cgroup_path: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = validate_cgroup_path(cgroup_path)?;
    Err("cgroup binding is only supported on Linux".into())
}

pub fn join_current_process(
    cgroup_path: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    join_pid(std::process::id(), cgroup_path)
}
