//! Synchronous EOS-window buffering until data-lane graceful termination.

use crate::model::{Collection, Tuple};
use crate::processor::window_metadata;
use crate::processor::{
    GaugeHandle, MetricKind, MetricSpec, ProcessorError, ProcessorStats, StreamData,
};
use std::sync::Arc;
use std::time::SystemTime;

pub(crate) struct EosWindowStage {
    rows: Vec<Tuple>,
    opened_at: Option<SystemTime>,
    last_seen_at: Option<SystemTime>,
    stats: Arc<ProcessorStats>,
    rows_buffered: GaugeHandle,
}

impl EosWindowStage {
    pub(crate) fn new(stats: Arc<ProcessorStats>) -> Self {
        let rows_buffered = stats.register_gauge(MetricSpec {
            id: "window.rows_buffered",
            flat_name: "rows_buffered",
            kind: MetricKind::Gauge,
        });
        rows_buffered.set(0);
        Self {
            rows: Vec::new(),
            opened_at: None,
            last_seen_at: None,
            stats,
            rows_buffered,
        }
    }

    pub(crate) fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.rows_buffered = stats.register_gauge(MetricSpec {
            id: "window.rows_buffered",
            flat_name: "rows_buffered",
            kind: MetricKind::Gauge,
        });
        self.rows_buffered.set(self.rows.len() as u64);
        self.stats = stats;
    }

    pub(crate) fn process(
        &mut self,
        data: StreamData,
    ) -> Result<[Option<StreamData>; 2], ProcessorError> {
        match data {
            StreamData::Collection(collection) => {
                self.add_collection(collection)?;
                Ok([None, None])
            }
            StreamData::Control(signal) if signal.is_terminal() => {
                let output = if signal.is_graceful_end() {
                    self.flush()?.map(StreamData::collection)
                } else {
                    self.clear();
                    None
                };
                Ok([output, Some(StreamData::control(signal))])
            }
            other => Ok([Some(other), None]),
        }
    }

    fn add_collection(&mut self, collection: Box<dyn Collection>) -> Result<(), ProcessorError> {
        let rows = match collection.into_rows() {
            Ok(rows) => rows,
            Err(err) => {
                self.stats.record_error_logged(
                    "eos window processor error",
                    format!("failed to extract rows: {err}"),
                );
                return Ok(());
            }
        };
        for row in rows {
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(err) => {
                    self.stats
                        .record_error_logged("eos window processor error", err.to_string());
                    continue;
                }
            }
            if self.opened_at.is_none() {
                self.opened_at = Some(row.timestamp);
            }
            self.last_seen_at = Some(row.timestamp);
            self.rows.push(row);
        }
        self.rows_buffered.set(self.rows.len() as u64);
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<Box<dyn Collection>>, ProcessorError> {
        if self.rows.is_empty() {
            self.rows_buffered.set(0);
            return Ok(None);
        }
        let rows = std::mem::take(&mut self.rows);
        self.rows_buffered.set(0);
        let closed_at = self
            .last_seen_at
            .take()
            .or(self.opened_at)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let collection = match window_metadata::record_batch_from_system_time(
            rows,
            self.opened_at.take().unwrap_or(closed_at),
            closed_at,
        ) {
            Ok(collection) => collection,
            Err(err) => {
                self.stats
                    .record_error_logged("eos window processor error", err.to_string());
                return Ok(None);
            }
        };
        Ok(Some(Box::new(collection)))
    }

    pub(crate) fn clear(&mut self) {
        self.rows.clear();
        self.opened_at = None;
        self.last_seen_at = None;
        self.rows_buffered.set(0);
    }
}
