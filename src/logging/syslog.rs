use crate::config::SyslogLoggingConfig;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
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

#[derive(Debug, Clone)]
enum SyslogDestination {
    #[cfg(unix)]
    Local { paths: Vec<PathBuf> },
}

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
    let destination = resolve_syslog_destination(cfg)?;
    open_syslog_with_destination(destination, effective_app_name)
}

fn open_syslog_with_destination(
    destination: SyslogDestination,
    effective_app_name: &str,
) -> io::Result<(SyslogMakeWriter, SyslogWorkerGuard)> {
    #[cfg(unix)]
    let _ = open_destination(&destination)?;

    #[cfg(not(unix))]
    let _ = &destination;

    let (sender, receiver) = sync_channel(SYSLOG_QUEUE_CAPACITY);
    let app_name = Arc::<str>::from(effective_app_name.to_string());
    let worker_destination = destination.clone();
    let worker_app_name = Arc::clone(&app_name);
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = Arc::clone(&shutdown);
    let join_handle = thread::Builder::new()
        .name("veloflux-syslog".to_string())
        .spawn(move || {
            run_syslog_worker(
                worker_destination,
                worker_app_name,
                receiver,
                worker_shutdown,
            )
        })?;
    let make_writer = SyslogMakeWriter {
        sender: sender.clone(),
    };
    let guard = SyslogWorkerGuard {
        shutdown,
        join_handle: Some(join_handle),
    };
    Ok((make_writer, guard))
}

fn resolve_syslog_destination(cfg: &SyslogLoggingConfig) -> io::Result<SyslogDestination> {
    let network = cfg.network.trim();
    let address = cfg.address.trim();

    if network.is_empty() && address.is_empty() {
        #[cfg(unix)]
        {
            return Ok(SyslogDestination::Local {
                paths: default_local_syslog_paths(),
            });
        }
        #[cfg(not(unix))]
        {
            return Err(io::Error::other(
                "local syslog output requires Unix-domain socket support on this platform",
            ));
        }
    }

    if network.is_empty() || address.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "logging.syslog.network and logging.syslog.address must either both be empty or both be set",
        ));
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "remote syslog transport is not supported yet; leave logging.syslog.network and logging.syslog.address empty to use local syslog",
    ))
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
    destination: SyslogDestination,
    app_name: Arc<str>,
    receiver: Receiver<SyslogRecord>,
    shutdown: Arc<AtomicBool>,
) {
    #[cfg(unix)]
    {
        run_syslog_worker_unix(destination, app_name, receiver, shutdown);
    }
    #[cfg(not(unix))]
    {
        let _ = (destination, app_name, receiver, shutdown);
    }
}

#[cfg(unix)]
fn run_syslog_worker_unix(
    destination: SyslogDestination,
    app_name: Arc<str>,
    receiver: Receiver<SyslogRecord>,
    shutdown: Arc<AtomicBool>,
) {
    let mut socket = open_destination(&destination).ok();
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

        socket = open_destination(&destination).ok();
        if let Some(sock) = socket.as_ref() {
            let _ = sock.send(&payload);
        }
    }
}

#[cfg(unix)]
fn open_destination(destination: &SyslogDestination) -> io::Result<UnixDatagram> {
    match destination {
        SyslogDestination::Local { paths } => open_local_syslog_socket(paths),
    }
}

#[cfg(unix)]
fn open_local_syslog_socket(paths: &[PathBuf]) -> io::Result<UnixDatagram> {
    let mut last_error = None;
    for path in paths {
        match open_syslog_socket(path) {
            Ok(socket) => return Ok(socket),
            Err(err) => last_error = Some((path.clone(), err)),
        }
    }

    if let Some((path, err)) = last_error {
        return Err(io::Error::new(
            err.kind(),
            format!(
                "failed to connect to local syslog socket via {}: {}",
                path.display(),
                err
            ),
        ));
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "no local syslog socket candidates are configured",
    ))
}

#[cfg(unix)]
fn open_syslog_socket(path: &Path) -> io::Result<UnixDatagram> {
    let socket = UnixDatagram::unbound()?;
    socket.connect(path)?;
    Ok(socket)
}

#[cfg(unix)]
fn default_local_syslog_paths() -> Vec<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        vec![PathBuf::from("/var/run/syslog")]
    }

    #[cfg(not(target_os = "macos"))]
    {
        vec![
            PathBuf::from("/dev/log"),
            PathBuf::from("/var/run/log"),
            PathBuf::from("/var/run/syslog"),
        ]
    }
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

    #[test]
    fn requires_network_and_address_to_be_set_together() {
        let cfg = SyslogLoggingConfig {
            enable: true,
            level: None,
            tag: "veloflux".to_string(),
            network: "udp".to_string(),
            address: String::new(),
        };

        let err = resolve_syslog_destination(&cfg).expect_err("missing address should fail");
        assert!(err
            .to_string()
            .contains("must either both be empty or both be set"));
    }

    #[test]
    fn rejects_remote_syslog_transport_for_now() {
        let cfg = SyslogLoggingConfig {
            enable: true,
            level: None,
            tag: "veloflux".to_string(),
            network: "udp".to_string(),
            address: "127.0.0.1:514".to_string(),
        };

        let err = resolve_syslog_destination(&cfg).expect_err("remote transport should fail");
        assert!(err
            .to_string()
            .contains("remote syslog transport is not supported yet"));
    }

    #[cfg(unix)]
    #[test]
    fn worker_sends_record_to_local_socket() {
        let path = unique_socket_path("syslog");
        let receiver = UnixDatagram::bind(&path).expect("bind unix datagram");
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("set read timeout");

        let destination = SyslogDestination::Local {
            paths: vec![path.clone()],
        };
        let (make_writer, guard) =
            open_syslog_with_destination(destination, "veloflux-worker-default")
                .expect("open syslog");
        {
            let mut writer = make_writer.writer_for_level(Level::WARN);
            writer
                .write_all(b"worker failed to open connector")
                .expect("write warning");
        }

        let mut buf = [0u8; 512];
        let len = receiver.recv(&mut buf).expect("receive syslog datagram");
        let message = std::str::from_utf8(&buf[..len]).expect("utf8 syslog payload");
        assert!(message.starts_with("<12>veloflux-worker-default["));
        assert!(message.ends_with(": worker failed to open connector"));

        drop(guard);
        let _ = fs::remove_file(&path);
    }
}
