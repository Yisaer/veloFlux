mod cleanup;
mod filename;
mod rolling_file;

use crate::config::LoggingConfig;
use std::io;
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

#[derive(Debug)]
pub struct LoggingGuard {
    _worker: Option<tracing_appender::non_blocking::WorkerGuard>,
}

pub fn init_logging(
    cfg: &LoggingConfig,
) -> Result<LoggingGuard, Box<dyn std::error::Error + Send + Sync>> {
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
            Ok(LoggingGuard { _worker: None })
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
            Ok(LoggingGuard {
                _worker: Some(guard),
            })
        }
    }
}
