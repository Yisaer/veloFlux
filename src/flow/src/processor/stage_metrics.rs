//! Metrics owned by a semantic stage, independent of its execution region.
use super::data_metrics::{
    DataMetricDomains, DataMetrics, EncoderMetrics, PassthroughMetrics, TransformMetricSpecs,
    TransformMetrics, STAGE_METRICS,
};
use super::{EncodedDeliveryFlags, ProcessorError, ProcessorStats, StreamData};
use crate::planner::physical::DataDomain;

pub(crate) enum StageMetrics {
    Encoding {
        input: DataMetrics,
        output: EncoderMetrics,
    },
    Encoded(TransformMetrics),
    Data(DataMetrics),
    Passthrough(PassthroughMetrics),
}
impl StageMetrics {
    pub(crate) fn encoding(stats: &ProcessorStats) -> Self {
        Self::Encoding {
            input: DataMetrics::new(
                stats,
                STAGE_METRICS,
                DataMetricDomains::NONE.with_input(DataDomain::Collection),
            ),
            output: EncoderMetrics::new(stats),
        }
    }
    pub(crate) fn encoded(stats: &ProcessorStats, specs: TransformMetricSpecs) -> Self {
        Self::Encoded(TransformMetrics::new(stats, specs))
    }
    pub(crate) fn new(
        stats: &ProcessorStats,
        domains: DataMetricDomains,
        passthrough: Option<DataDomain>,
    ) -> Self {
        match passthrough {
            Some(domain) => {
                Self::Passthrough(PassthroughMetrics::new(stats, STAGE_METRICS, domain))
            }
            None => Self::Data(DataMetrics::new(stats, STAGE_METRICS, domains)),
        }
    }
    pub(crate) fn encoder(&self) -> Option<&EncoderMetrics> {
        match self {
            Self::Encoding { output, .. } => Some(output),
            _ => None,
        }
    }
    pub(crate) fn transform(&self) -> Option<&TransformMetrics> {
        match self {
            Self::Encoded(metrics) => Some(metrics),
            _ => None,
        }
    }
    pub(crate) fn record_input(
        &self,
        stats: &ProcessorStats,
        data: &StreamData,
    ) -> Result<(), ProcessorError> {
        match self {
            Self::Encoding { input, .. } | Self::Data(input) => {
                input.record_input(stats, data)?;
            }
            Self::Passthrough(input) => {
                input.record_input(stats, data)?;
            }
            // Byte-transform engines account input at delivery validation points.
            Self::Encoded(_) => {}
        }
        Ok(())
    }
    pub(crate) fn record_output(
        &self,
        stats: &ProcessorStats,
        data: &StreamData,
    ) -> Result<(), ProcessorError> {
        match self {
            Self::Encoding { output, .. } => {
                if let StreamData::EncodedDelivery { flags, bytes } = data {
                    // Abort accounting belongs to the encoder step producing it.
                    if !flags.contains(EncodedDeliveryFlags::ABORT) {
                        output.record_output_bytes(bytes.len());
                        if flags.contains(EncodedDeliveryFlags::END) {
                            output.record_message_out();
                        }
                    }
                }
            }
            Self::Encoded(output) => {
                if let StreamData::EncodedDelivery { flags, bytes } = data {
                    // Transform delivery handlers account aborts exactly once.
                    if !flags.contains(EncodedDeliveryFlags::ABORT) {
                        output.record_output_bytes(bytes.len());
                        if flags.contains(EncodedDeliveryFlags::END) {
                            output.record_message_out();
                        }
                    }
                }
            }
            Self::Data(output) => output.record_output(stats, DataMetrics::measure(data))?,
            Self::Passthrough(output) => {
                output.record_output(stats, output.measure(data)?);
            }
        }
        Ok(())
    }
}
