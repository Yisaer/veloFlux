mod cleanup;
mod filename;
mod rolling_file;
mod syslog;

use crate::config::LoggingConfig;
use std::io;
use std::sync::{Mutex, OnceLock};
use tracing_subscriber::filter::LevelFilter;

pub use cleanup::prune_rotated_logs;
pub use filename::{format_rotated_filename, parse_rotated_filename, RotatedLogName};
pub use rolling_file::RollingFileWriter;

pub enum LogOutput {
    Stdout,
    File(RollingFileWriter),
}

pub fn open_output(cfg: &LoggingConfig) -> io::Result<LogOutput> {
    match cfg.output {
        crate::config::LoggingOutput::Stdout => Ok(LogOutput::Stdout),
        crate::config::LoggingOutput::File => {
            let writer = RollingFileWriter::open(cfg.file.clone())?;
            Ok(LogOutput::File(writer))
        }
        crate::config::LoggingOutput::Syslog => {
            Err(io::Error::other("syslog output is initialized separately"))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitializedLogging {
    config: LoggingConfig,
    context: LoggingContext,
}

static LOGGING_STATE: OnceLock<InitializedLogging> = OnceLock::new();
static LOGGING_WORKER_GUARDS: OnceLock<Mutex<Vec<Box<dyn Send>>>> = OnceLock::new();

#[derive(Debug, Default)]
pub struct LoggingGuard;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoggingContext {
    Manager,
    Worker { instance_id: String },
    Embedded,
}

impl LoggingContext {
    pub fn manager() -> Self {
        Self::Manager
    }

    pub fn worker(instance_id: impl Into<String>) -> Self {
        Self::Worker {
            instance_id: instance_id.into(),
        }
    }

    pub fn embedded() -> Self {
        Self::Embedded
    }

    fn effective_tag(&self, base_tag: &str) -> String {
        match self {
            Self::Manager => format!("{base_tag}-manager"),
            Self::Worker { instance_id } => format!("{base_tag}-worker-{instance_id}"),
            Self::Embedded => format!("{base_tag}-embedded"),
        }
    }
}

// SAFETY: lock on a OnceLock-backed static Mutex — never poisoned
#[allow(clippy::expect_used)]
fn retain_worker_guard<T>(guard: T)
where
    T: Send + 'static,
{
    let guards = LOGGING_WORKER_GUARDS.get_or_init(|| Mutex::new(Vec::new()));
    guards
        .lock()
        .expect("logging worker guard mutex poisoned")
        .push(Box::new(guard));
}

pub fn init_logging(
    cfg: &LoggingConfig,
) -> Result<LoggingGuard, Box<dyn std::error::Error + Send + Sync>> {
    init_logging_with_context(cfg, &LoggingContext::manager())
}

pub fn init_logging_with_context(
    cfg: &LoggingConfig,
    context: &LoggingContext,
) -> Result<LoggingGuard, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(existing) = LOGGING_STATE.get() {
        if existing.config != *cfg || existing.context != *context {
            return Err(format!(
                "logging already initialized with a different config or context: existing={existing:?}, requested_config={cfg:?}, requested_context={context:?}"
            )
            .into());
        }
        return Ok(LoggingGuard);
    }

    let level = level_filter(cfg.level);

    match cfg.output {
        crate::config::LoggingOutput::Stdout => {
            if cfg.disable_timestamp {
                let subscriber = tracing_subscriber::fmt()
                    .with_max_level(level)
                    .without_time()
                    .with_target(true)
                    .with_file(cfg.include_source)
                    .with_line_number(cfg.include_source)
                    .with_ansi(true)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)?;
            } else {
                let subscriber = tracing_subscriber::fmt()
                    .with_max_level(level)
                    .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                    .with_target(true)
                    .with_file(cfg.include_source)
                    .with_line_number(cfg.include_source)
                    .with_ansi(true)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)?;
            }
        }
        crate::config::LoggingOutput::File => {
            let writer = match open_output(cfg)? {
                LogOutput::File(writer) => writer,
                LogOutput::Stdout => {
                    return Err("expected file logging output".into());
                }
            };
            let (non_blocking, guard) = tracing_appender::non_blocking(writer);
            if cfg.disable_timestamp {
                let subscriber = tracing_subscriber::fmt()
                    .with_max_level(level)
                    .without_time()
                    .with_target(true)
                    .with_file(cfg.include_source)
                    .with_line_number(cfg.include_source)
                    .with_ansi(false)
                    .with_writer(non_blocking)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)?;
            } else {
                let subscriber = tracing_subscriber::fmt()
                    .with_max_level(level)
                    .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                    .with_target(true)
                    .with_file(cfg.include_source)
                    .with_line_number(cfg.include_source)
                    .with_ansi(false)
                    .with_writer(non_blocking)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)?;
            }
            retain_worker_guard(guard);
        }
        crate::config::LoggingOutput::Syslog => {
            if !cfg.syslog.enable {
                return Err("logging.output=syslog requires logging.syslog.enable=true".into());
            }
            let syslog_level = level_filter(cfg.syslog.level.unwrap_or(cfg.level));
            let effective_tag = context.effective_tag(&cfg.syslog.tag);
            let (make_writer, guard) = syslog::open_syslog(&cfg.syslog, &effective_tag)?;
            if cfg.disable_timestamp {
                let subscriber = tracing_subscriber::fmt()
                    .with_max_level(syslog_level)
                    .without_time()
                    .with_target(true)
                    .with_file(cfg.include_source)
                    .with_line_number(cfg.include_source)
                    .with_ansi(false)
                    .with_writer(make_writer)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)?;
            } else {
                let subscriber = tracing_subscriber::fmt()
                    .with_max_level(syslog_level)
                    .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                    .with_target(true)
                    .with_file(cfg.include_source)
                    .with_line_number(cfg.include_source)
                    .with_ansi(false)
                    .with_writer(make_writer)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)?;
            }
            retain_worker_guard(guard);
        }
    }

    let _ = LOGGING_STATE.set(InitializedLogging {
        config: cfg.clone(),
        context: context.clone(),
    });
    Ok(LoggingGuard)
}

fn level_filter(level: crate::config::LogLevel) -> LevelFilter {
    match level {
        crate::config::LogLevel::Trace => LevelFilter::TRACE,
        crate::config::LogLevel::Debug => LevelFilter::DEBUG,
        crate::config::LogLevel::Info => LevelFilter::INFO,
        crate::config::LogLevel::Warn => LevelFilter::WARN,
        crate::config::LogLevel::Error => LevelFilter::ERROR,
    }
}
