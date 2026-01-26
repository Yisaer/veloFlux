use crate::config::FileLoggingConfig;
use crate::logging::cleanup::prune_rotated_logs;
use crate::logging::filename::{format_rotated_filename, parse_rotated_filename};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

pub struct RollingFileWriter {
    cfg: FileLoggingConfig,
    path: PathBuf,
    file: Option<File>,
    bytes_written: u64,
}

impl RollingFileWriter {
    pub fn open(cfg: FileLoggingConfig) -> io::Result<Self> {
        let dir = PathBuf::from(&cfg.dir);
        fs::create_dir_all(&dir)?;
        let path = dir.join(&cfg.file_name);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes_written = file.metadata().map(|m| m.len()).unwrap_or(0);
        let writer = Self {
            cfg,
            path,
            file: Some(file),
            bytes_written,
        };
        prune_rotated_logs(&dir, &writer.cfg.file_name, &writer.cfg.rotation)?;
        Ok(writer)
    }

    fn max_size_bytes(&self) -> u64 {
        self.cfg.rotation.max_size_mb * 1024 * 1024
    }

    fn maybe_rotate(&mut self, incoming_bytes: usize) -> io::Result<()> {
        let max_size = self.max_size_bytes();
        if max_size == 0 {
            return Ok(());
        }
        if self.bytes_written.saturating_add(incoming_bytes as u64) < max_size {
            return Ok(());
        }
        self.rotate()
    }

    fn rotate(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.file.take() {
            let _ = file.flush();
            drop(file);
        }

        let dir = Path::new(&self.cfg.dir);
        let stem = self
            .path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("app");

        let now = OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let seq = next_seq_for_timestamp(dir, stem, now);
        let rotated_name = format_rotated_filename(stem, now, seq);
        let rotated_path = dir.join(rotated_name);

        fs::rename(&self.path, &rotated_path)?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.file = Some(file);
        self.bytes_written = 0;

        prune_rotated_logs(dir, &self.cfg.file_name, &self.cfg.rotation)?;
        Ok(())
    }
}

impl Write for RollingFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.maybe_rotate(buf.len())?;
        let written = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("log file not open"))?
            .write(buf)?;
        self.bytes_written = self.bytes_written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("log file not open"))?
            .flush()
    }
}

fn next_seq_for_timestamp(dir: &Path, stem: &str, ts: OffsetDateTime) -> u16 {
    let mut max_seen: u16 = 0;
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return 1,
    };
    for entry in read_dir.flatten() {
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let parsed = match parse_rotated_filename(&file_name, stem) {
            Some(p) => p,
            None => continue,
        };
        if parsed.timestamp_utc == ts && parsed.seq > max_seen {
            max_seen = parsed.seq;
        }
    }
    max_seen.saturating_add(1).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FileLoggingConfig, LogRotationConfig};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    fn unique_suffix() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    fn test_config(dir: &str) -> FileLoggingConfig {
        FileLoggingConfig {
            dir: dir.to_string(),
            file_name: "test.log".to_string(),
            rotation: LogRotationConfig {
                keep_days: 7,
                max_num: 5,
                max_size_mb: 1, // 1MB for easier testing
            },
        }
    }

    #[test]
    fn test_open_creates_file_and_dir() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let log_dir = temp_dir.path().join(format!("logs_{}", unique_suffix()));
        let cfg = test_config(log_dir.to_str().unwrap());

        let writer = RollingFileWriter::open(cfg.clone()).expect("should open");
        assert!(log_dir.exists(), "directory should be created");
        assert!(
            log_dir.join(&cfg.file_name).exists(),
            "log file should be created"
        );
        drop(writer);
    }

    #[test]
    fn test_write_and_flush() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let log_dir = temp_dir.path().join(format!("logs_{}", unique_suffix()));
        let cfg = test_config(log_dir.to_str().unwrap());

        let mut writer = RollingFileWriter::open(cfg.clone()).expect("should open");
        let data = b"hello world\n";
        let written = writer.write(data).expect("should write");
        assert_eq!(written, data.len());
        assert_eq!(writer.bytes_written, data.len() as u64);

        writer.flush().expect("should flush");

        // Verify content was written
        let content = fs::read_to_string(log_dir.join(&cfg.file_name)).expect("read file");
        assert_eq!(content, "hello world\n");
    }

    #[test]
    fn test_max_size_bytes() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let log_dir = temp_dir.path().join(format!("logs_{}", unique_suffix()));
        let mut cfg = test_config(log_dir.to_str().unwrap());
        cfg.rotation.max_size_mb = 256;

        let writer = RollingFileWriter::open(cfg).expect("should open");
        assert_eq!(writer.max_size_bytes(), 256 * 1024 * 1024);
    }

    #[test]
    fn test_rotation_disabled_when_max_size_zero() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let log_dir = temp_dir.path().join(format!("logs_{}", unique_suffix()));
        let mut cfg = test_config(log_dir.to_str().unwrap());
        cfg.rotation.max_size_mb = 0; // disable rotation

        let mut writer = RollingFileWriter::open(cfg.clone()).expect("should open");

        // Write a lot of data - should not rotate
        for _ in 0..100 {
            writer.write(b"test data line\n").expect("should write");
        }
        writer.flush().expect("should flush");

        // Should still be writing to same file (no rotated files)
        let entries: Vec<_> = fs::read_dir(&log_dir)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1, "should only have 1 file (no rotation)");
    }

    #[test]
    fn test_maybe_rotate_no_rotation_when_under_limit() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let log_dir = temp_dir.path().join(format!("logs_{}", unique_suffix()));
        let mut cfg = test_config(log_dir.to_str().unwrap());
        cfg.rotation.max_size_mb = 1; // 1MB limit

        let mut writer = RollingFileWriter::open(cfg.clone()).expect("should open");

        // Write small data - should not trigger rotation
        writer.write(b"small data\n").expect("should write");

        let entries: Vec<_> = fs::read_dir(&log_dir)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1, "should only have 1 file");
    }

    #[test]
    fn test_next_seq_for_timestamp_empty_dir() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let ts = OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let seq = next_seq_for_timestamp(temp_dir.path(), "app", ts);
        assert_eq!(seq, 1);
    }

    #[test]
    fn test_next_seq_for_timestamp_nonexistent_dir() {
        let nonexistent = Path::new("/nonexistent/path/that/does/not/exist");
        let ts = OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let seq = next_seq_for_timestamp(nonexistent, "app", ts);
        assert_eq!(seq, 1);
    }

    #[test]
    fn test_next_seq_for_timestamp_with_existing_files() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let ts = OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        // Create some rotated files
        let name1 = format_rotated_filename("app", ts, 1);
        let name2 = format_rotated_filename("app", ts, 2);
        fs::write(temp_dir.path().join(&name1), b"").expect("create file 1");
        fs::write(temp_dir.path().join(&name2), b"").expect("create file 2");

        let seq = next_seq_for_timestamp(temp_dir.path(), "app", ts);
        assert_eq!(seq, 3, "should return next seq after max seen");
    }

    #[test]
    fn test_next_seq_ignores_different_stem() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let ts = OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        // Create rotated files with different stem
        let name1 = format_rotated_filename("other", ts, 5);
        fs::write(temp_dir.path().join(&name1), b"").expect("create file");

        let seq = next_seq_for_timestamp(temp_dir.path(), "app", ts);
        assert_eq!(seq, 1, "should ignore files with different stem");
    }
}
