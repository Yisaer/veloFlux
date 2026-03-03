#[cfg(target_os = "linux")]
use std::io::{ErrorKind, Write};
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
fn cgroup_dir(cgroup_path: &str) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let cgroup_path = validate_cgroup_path(cgroup_path)?;
    let rel = cgroup_path.strip_prefix('/').unwrap_or(cgroup_path).trim();
    Ok(Path::new(CGROUP_ROOT).join(rel))
}

#[cfg(target_os = "linux")]
fn cgroup_procs_path(
    cgroup_path: &str,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let dir = cgroup_dir(cgroup_path)?;
    Ok(dir.join("cgroup.procs"))
}

#[cfg(target_os = "linux")]
fn cgroup_threads_path(
    cgroup_path: &str,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let dir = cgroup_dir(cgroup_path)?;
    Ok(dir.join("cgroup.threads"))
}

#[cfg(target_os = "linux")]
pub fn debug_snapshot(cgroup_path: &str) -> String {
    let dir = match cgroup_dir(cgroup_path) {
        Ok(dir) => dir,
        Err(err) => return format!("invalid cgroup_path={cgroup_path}: {err}"),
    };

    let mut out = vec![format!("path={cgroup_path}")];
    for name in [
        "cgroup.type",
        "cgroup.subtree_control",
        "cpu.max",
        "cpuset.cpus",
        "cpuset.mems",
        "cpuset.cpus.effective",
        "cpuset.mems.effective",
    ] {
        let p = dir.join(name);
        let value = match std::fs::read_to_string(&p) {
            Ok(v) => {
                let trimmed = v.trim();
                if trimmed.is_empty() {
                    "<empty>".to_string()
                } else {
                    trimmed.to_string()
                }
            }
            Err(err) => format!("<{}>", err),
        };
        out.push(format!("{name}={value}"));
    }
    out.join(", ")
}

#[cfg(not(target_os = "linux"))]
pub fn debug_snapshot(cgroup_path: &str) -> String {
    format!("path={cgroup_path}, unsupported_platform")
}

#[cfg(target_os = "linux")]
fn join_pid_via_threads(
    pid: u32,
    cgroup_path: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let threads_path = cgroup_threads_path(cgroup_path)?;
    let task_dir = PathBuf::from(format!("/proc/{pid}/task"));
    let tids = std::fs::read_dir(&task_dir)
        .map_err(|err| format!("read {}: {err}", task_dir.display()))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter_map(|name| name.parse::<u32>().ok())
        .collect::<Vec<_>>();

    if tids.is_empty() {
        return Err(format!("no threads found under {}", task_dir.display()).into());
    }

    for tid in tids {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&threads_path)
            .map_err(|err| format!("open {}: {err}", threads_path.display()))?;
        writeln!(f, "{tid}").map_err(|err| format!("write {}: {err}", threads_path.display()))?;
    }
    Ok(())
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
    if let Err(err) = writeln!(f, "{pid}") {
        if err.kind() == ErrorKind::InvalidInput {
            if let Err(fallback_err) = join_pid_via_threads(pid, cgroup_path) {
                return Err(format!(
                    "write {}: {err}; fallback to cgroup.threads failed: {fallback_err}",
                    procs_path.display()
                )
                .into());
            }
            return Ok(());
        }
        return Err(format!("write {}: {err}", procs_path.display()).into());
    }
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
