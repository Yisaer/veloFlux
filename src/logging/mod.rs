mod cleanup;
mod filename;
mod rolling_file;

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
    }
}

static LOGGING_CONFIG: OnceLock<LoggingConfig> = OnceLock::new();
static LOGGING_WORKER_GUARDS: OnceLock<Mutex<Vec<tracing_appender::non_blocking::WorkerGuard>>> =
    OnceLock::new();

#[derive(Debug, Default)]
pub struct LoggingGuard;

fn retain_worker_guard(guard: tracing_appender::non_blocking::WorkerGuard) {
    let guards = LOGGING_WORKER_GUARDS.get_or_init(|| Mutex::new(Vec::new()));
    guards
        .lock()
        .expect("logging worker guard mutex poisoned")
        .push(guard);
}

pub fn init_logging(
    cfg: &LoggingConfig,
) -> Result<LoggingGuard, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(existing) = LOGGING_CONFIG.get() {
        if existing != cfg {
            return Err(format!(
                "logging already initialized with a different config: existing={existing:?}, requested={cfg:?}"
            )
            .into());
        }
        return Ok(LoggingGuard);
    }

    let level = match cfg.level {
        crate::config::LogLevel::Trace => LevelFilter::TRACE,
        crate::config::LogLevel::Debug => LevelFilter::DEBUG,
        crate::config::LogLevel::Info => LevelFilter::INFO,
        crate::config::LogLevel::Warn => LevelFilter::WARN,
        crate::config::LogLevel::Error => LevelFilter::ERROR,
    };

    match open_output(cfg)? {
        LogOutput::Stdout => {
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
        LogOutput::File(writer) => {
            let (non_blocking, guard) = tracing_appender::non_blocking(writer);
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
            retain_worker_guard(guard);
        }
    };

    let _ = LOGGING_CONFIG.set(cfg.clone());
    Ok(LoggingGuard)
}
