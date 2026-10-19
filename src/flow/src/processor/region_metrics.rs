//! Preserve conversion and message-passthrough metric semantics at execution boundaries.
use super::data_metrics::{
    DataMeasurement, DataMetricDomains, DataMetrics, EncoderMetrics, PassthroughMeasurement,
    PassthroughMetrics, TransformMetricSpecs, TransformMetrics, REGION_METRICS,
};
use super::{EncodedDeliveryFlags, ProcessorError, ProcessorStats, StreamData};
use crate::planner::physical::DataDomain;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) enum RegionMetrics {
    Encoding {
        input: DataMetrics,
        output: EncoderMetrics,
        boundary: bool,
        aborted: AtomicBool,
    },
    Encoded {
        metrics: TransformMetrics,
        automatic_input: bool,
        aborted: AtomicBool,
    },
    Data(DataMetrics),
    Passthrough(PassthroughMetrics),
}
#[derive(Clone, Copy)]
pub(crate) enum RegionMeasurement {
    Encoded(Option<(EncodedDeliveryFlags, usize)>),
    Data(DataMeasurement),
    Passthrough(PassthroughMeasurement),
}
impl RegionMetrics {
    pub(crate) fn encoding(stats: &ProcessorStats, input: DataDomain, boundary: bool) -> Self {
        Self::Encoding {
            input: DataMetrics::new(
                stats,
                REGION_METRICS,
                DataMetricDomains::NONE.with_input(input),
            ),
            output: if boundary {
                EncoderMetrics::region(stats)
            } else {
                EncoderMetrics::new(stats)
            },
            boundary,
            aborted: AtomicBool::new(false),
        }
    }
    pub(crate) fn begin_delivery(&self) {
        match self {
            Self::Encoding { aborted, .. } | Self::Encoded { aborted, .. } => {
                aborted.store(false, Ordering::Relaxed)
            }
            _ => {}
        }
    }
    pub(crate) fn encoded(
        stats: &ProcessorStats,
        specs: TransformMetricSpecs,
        automatic_input: bool,
    ) -> Self {
        Self::Encoded {
            metrics: TransformMetrics::new(stats, specs),
            automatic_input,
            aborted: AtomicBool::new(false),
        }
    }
    /// Only boundary metrics deduplicate aborts across constituent transforms.
    pub(crate) fn record_delivery_abort(&self) {
        match self {
            Self::Encoded {
                metrics,
                automatic_input: true,
                aborted,
            } if !aborted.swap(true, Ordering::Relaxed) => metrics.record_aborted(),
            Self::Encoding {
                output,
                boundary: true,
                aborted,
                ..
            } if !aborted.swap(true, Ordering::Relaxed) => output.record_message_aborted(),
            _ => {}
        }
    }

    pub(crate) fn new(
        stats: &ProcessorStats,
        domains: DataMetricDomains,
        passthrough: Option<DataDomain>,
    ) -> Self {
        match passthrough {
            Some(domain) => {
                Self::Passthrough(PassthroughMetrics::new(stats, REGION_METRICS, domain))
            }
            None => Self::Data(DataMetrics::new(stats, REGION_METRICS, domains)),
        }
    }
    pub(crate) fn measure(&self, data: &StreamData) -> Result<RegionMeasurement, ProcessorError> {
        match self {
            Self::Encoding { .. } | Self::Encoded { .. } => {
                Ok(RegionMeasurement::Encoded(match data {
                    StreamData::EncodedDelivery { flags, bytes } => Some((*flags, bytes.len())),
                    _ => None,
                }))
            }
            Self::Data(_) => Ok(RegionMeasurement::Data(DataMetrics::measure(data))),
            Self::Passthrough(metrics) => metrics.measure(data).map(RegionMeasurement::Passthrough),
        }
    }
    pub(crate) fn record_input(
        &self,
        stats: &ProcessorStats,
        data: &StreamData,
    ) -> Result<(), ProcessorError> {
        match self {
            Self::Encoding {
                input, boundary, ..
            } => {
                if *boundary || matches!(data, StreamData::Collection(_)) {
                    input.record_input(stats, data)?;
                }
            }
            Self::Encoded {
                metrics,
                automatic_input,
                aborted,
            } => {
                // Stage engines count input at protocol validation points. Region
                // boundaries count external frames independently of stage errors.
                if *automatic_input {
                    if let StreamData::EncodedDelivery { flags, bytes } = data {
                        if flags.contains(EncodedDeliveryFlags::START) {
                            aborted.store(false, Ordering::Relaxed);
                        }
                        metrics.record_input_bytes(bytes.len());
                        if flags.contains(EncodedDeliveryFlags::ABORT) {
                            self.record_delivery_abort();
                        } else if flags.contains(EncodedDeliveryFlags::END) {
                            metrics.record_message_in();
                        }
                    }
                }
            }
            Self::Data(metrics) => {
                metrics.record_input(stats, data)?;
            }
            Self::Passthrough(metrics) => {
                metrics.record_input(stats, data)?;
            }
        }
        Ok(())
    }
    pub(crate) fn record_output(
        &self,
        stats: &ProcessorStats,
        measurement: RegionMeasurement,
    ) -> Result<(), ProcessorError> {
        match (self, measurement) {
            (Self::Encoding { output, .. }, RegionMeasurement::Encoded(value)) => {
                if let Some((flags, bytes)) = value {
                    if flags.contains(EncodedDeliveryFlags::ABORT) {
                        self.record_delivery_abort();
                    } else {
                        output.record_output_bytes(bytes);
                        if flags.contains(EncodedDeliveryFlags::END) {
                            output.record_message_out();
                        }
                    }
                }
                Ok(())
            }
            (Self::Encoded { metrics, .. }, RegionMeasurement::Encoded(value)) => {
                if let Some((flags, bytes)) = value {
                    if flags.contains(EncodedDeliveryFlags::ABORT) {
                        self.record_delivery_abort();
                    } else {
                        metrics.record_output_bytes(bytes);
                        if flags.contains(EncodedDeliveryFlags::END) {
                            metrics.record_message_out();
                        }
                    }
                }
                Ok(())
            }
            (Self::Data(metrics), RegionMeasurement::Data(value)) => {
                metrics.record_output(stats, value)
            }
            (Self::Passthrough(metrics), RegionMeasurement::Passthrough(value)) => {
                metrics.record_output(stats, value);
                Ok(())
            }
            _ => Err(ProcessorError::ProcessingError(
                "execution metric measurement contract mismatch".into(),
            )),
        }
    }
}
