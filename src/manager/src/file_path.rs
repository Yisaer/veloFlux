use std::path::{Component, Path, PathBuf};

/// Resolve a resource's server-local path without changing its persisted value.
/// This defines a base directory, not a filesystem sandbox.
pub(crate) fn resolve(data_dir: &Path, value: &str) -> Result<PathBuf, String> {
    let path = Path::new(value);
    if value.is_empty() {
        return Err("file path must not be empty".to_string());
    }
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    if path
        .components()
        .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
    {
        return Err(format!(
            "relative file path `{value}` must not contain parent directories or a platform prefix; use an absolute path for external files"
        ));
    }
    let root = std::path::absolute(data_dir)
        .map_err(|err| format!("resolve data directory `{}`: {err}", data_dir.display()))?;
    Ok(root.join(path))
}

pub(crate) fn resolve_string(data_dir: &Path, value: &str) -> Result<String, String> {
    resolve(data_dir, value)?
        .into_os_string()
        .into_string()
        .map_err(|_| "resolved file path is not UTF-8".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_paths_use_explicit_data_root() {
        let root = tempfile::tempdir().expect("data root");
        assert_ne!(root.path(), std::env::current_dir().unwrap());
        for value in ["uploads/schema.zip", "uploads/nested/schema.zip", "output"] {
            assert_eq!(
                resolve(root.path(), value).unwrap(),
                root.path().join(value)
            );
        }
        let external = tempfile::tempdir().expect("external root");
        let path = external.path().join("schema.zip");
        assert_eq!(resolve(root.path(), path.to_str().unwrap()).unwrap(), path);
        for value in ["", "../schema.zip", "uploads/../schema.zip"] {
            assert!(resolve(root.path(), value).is_err(), "{value}");
        }
        let relative_root = Path::new("nonexistent-data-root");
        assert_eq!(
            resolve(relative_root, "output").unwrap(),
            std::env::current_dir()
                .unwrap()
                .join(relative_root)
                .join("output")
        );
    }
}
