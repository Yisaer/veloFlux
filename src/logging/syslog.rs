use crate::config::SyslogLoggingConfig;
use std::io;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::{Level, Metadata};
use tracing_subscriber::fmt::writer::MakeWriter;

#[cfg(unix)]
use std::os::unix::net::UnixDatagram;

const SYSLOG_QUEUE_CAPACITY: usize = 8192;
const SYSLOG_USER_FACILITY_CODE: u8 = 1;

pub struct SyslogMakeWriter {
    sender: SyncSender<SyslogRecord>,
}

pub struct SyslogWorkerGuard {
    shutdown: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
}

struct SyslogRecord {
    severity: SyslogSeverity,
    body: Vec<u8>,
}

pub struct SyslogEventWriter {
    sender: SyncSender<SyslogRecord>,
    severity: SyslogSeverity,
    buf: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyslogSeverity {
    Error,
    Warning,
    Informational,
    Debug,
}

pub fn open_syslog(
    cfg: &SyslogLoggingConfig,
    effective_app_name: &str,
) -> io::Result<(SyslogMakeWriter, SyslogWorkerGuard)> {
    let _ = open_syslog_socket(&cfg.path)?;
    let (sender, receiver) = sync_channel(SYSLOG_QUEUE_CAPACITY);
    let worker_cfg = cfg.clone();
    let app_name = Arc::<str>::from(effective_app_name.to_string());
    let worker_app_name = Arc::clone(&app_name);
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = Arc::clone(&shutdown);
    let join_handle = thread::Builder::new()
        .name("veloflux-syslog".to_string())
        .spawn(move || run_syslog_worker(worker_cfg, worker_app_name, receiver, worker_shutdown))?;
    let make_writer = SyslogMakeWriter {
        sender: sender.clone(),
    };
    let guard = SyslogWorkerGuard {
        shutdown,
        join_handle: Some(join_handle),
    };
    Ok((make_writer, guard))
}

impl SyslogMakeWriter {
    fn writer_for_level(&self, level: Level) -> SyslogEventWriter {
        SyslogEventWriter {
            sender: self.sender.clone(),
            severity: SyslogSeverity::from_level(level),
            buf: Vec::with_capacity(256),
        }
    }
}

impl<'a> MakeWriter<'a> for SyslogMakeWriter {
    type Writer = SyslogEventWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.writer_for_level(Level::INFO)
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        self.writer_for_level(*meta.level())
    }
}

impl Write for SyslogEventWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for SyslogEventWriter {
    fn drop(&mut self) {
        trim_line_endings(&mut self.buf);
        if self.buf.is_empty() {
            return;
        }
        let record = SyslogRecord {
            severity: self.severity,
            body: std::mem::take(&mut self.buf),
        };
        match self.sender.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

impl Drop for SyslogWorkerGuard {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

fn trim_line_endings(buf: &mut Vec<u8>) {
    while matches!(buf.last(), Some(b'\n' | b'\r')) {
        let _ = buf.pop();
    }
}

fn run_syslog_worker(
    cfg: SyslogLoggingConfig,
    app_name: Arc<str>,
    receiver: Receiver<SyslogRecord>,
    shutdown: Arc<AtomicBool>,
) {
    #[cfg(unix)]
    {
        run_syslog_worker_unix(cfg, app_name, receiver, shutdown);
    }
    #[cfg(not(unix))]
    {
        let _ = (cfg, app_name, receiver, shutdown);
    }
}

#[cfg(unix)]
fn run_syslog_worker_unix(
    cfg: SyslogLoggingConfig,
    app_name: Arc<str>,
    receiver: Receiver<SyslogRecord>,
    shutdown: Arc<AtomicBool>,
) {
    let mut socket = open_syslog_socket(&cfg.path).ok();
    let pid = std::process::id();

    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let record = match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(record) => record,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let payload = format_syslog_message(&app_name, pid, record.severity, &record.body);
        if let Some(sock) = socket.as_ref() {
            if sock.send(&payload).is_ok() {
                continue;
            }
        }

        socket = open_syslog_socket(&cfg.path).ok();
        if let Some(sock) = socket.as_ref() {
            let _ = sock.send(&payload);
        }
    }
}

#[cfg(unix)]
fn open_syslog_socket(path: &str) -> io::Result<UnixDatagram> {
    let socket = UnixDatagram::unbound()?;
    socket.connect(path)?;
    Ok(socket)
}

#[cfg(not(unix))]
fn open_syslog_socket(_path: &str) -> io::Result<()> {
    Err(io::Error::other(
        "syslog output requires Unix-domain socket support on this platform",
    ))
}

fn format_syslog_message(
    app_name: &str,
    pid: u32,
    severity: SyslogSeverity,
    body: &[u8],
) -> Vec<u8> {
    let pri = SYSLOG_USER_FACILITY_CODE * 8 + severity.code();
    let mut message = Vec::with_capacity(body.len() + app_name.len() + 32);
    let header = format!("<{pri}>{app_name}[{pid}]: ");
    message.extend_from_slice(header.as_bytes());
    message.extend_from_slice(body);
    message
}

impl SyslogSeverity {
    fn from_level(level: Level) -> Self {
        match level {
            Level::ERROR => Self::Error,
            Level::WARN => Self::Warning,
            Level::INFO => Self::Informational,
            Level::DEBUG | Level::TRACE => Self::Debug,
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Error => 3,
            Self::Warning => 4,
            Self::Informational => 6,
            Self::Debug => 7,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_socket_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("veloflux_test.{name}.{nanos}.sock"))
    }

    #[test]
    fn formats_syslog_priority() {
        let message = format_syslog_message(
            "veloflux-manager",
            42,
            SyslogSeverity::Warning,
            b"connector failed",
        );
        assert_eq!(
            String::from_utf8(message).expect("valid utf8"),
            "<12>veloflux-manager[42]: connector failed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn worker_sends_record_to_local_socket() {
        let path = unique_socket_path("syslog");
        let receiver = UnixDatagram::bind(&path).expect("bind unix datagram");
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("set read timeout");

        let cfg = SyslogLoggingConfig {
            enable: true,
            level: None,
            tag: "veloflux".to_string(),
            path: path.to_string_lossy().to_string(),
        };
        let (make_writer, guard) =
            open_syslog(&cfg, "veloflux-worker-default").expect("open syslog");
        {
            let mut writer = make_writer.writer_for_level(Level::WARN);
            writer
                .write_all(b"pipeline stalled\n")
                .expect("write event");
        }

        let mut buf = [0_u8; 512];
        let received = receiver.recv(&mut buf).expect("receive datagram");
        let payload = std::str::from_utf8(&buf[..received]).expect("utf8 payload");
        assert!(
            payload.contains("veloflux-worker-default"),
            "payload should contain effective app name: {payload}"
        );
        assert!(
            payload.ends_with("pipeline stalled"),
            "payload should contain trimmed event body: {payload}"
        );

        drop(guard);
        let _ = fs::remove_file(path);
    }
}
