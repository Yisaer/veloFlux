//! Physical-region fixtures for the migrated execution tests.
use super::base::ProcessorChannelCapacities;
use super::batch_policy::BatchPolicy;
use super::encoder_stage::EncoderStage;
use super::region_processor::{RegionProcessor, RegionResources};
use crate::codec::SinkEncoder;
use crate::planner::physical::{PhysicalPlan, PhysicalSinkEncoder};
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use std::time::Duration;

pub(crate) fn encoder_region(
    id: impl Into<String>,
    encoder: Box<dyn SinkEncoder>,
    count: Option<usize>,
    duration: Option<Duration>,
    capacities: ProcessorChannelCapacities,
) -> RegionProcessor {
    let plan = PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
        vec![],
        0,
        "test".into(),
        SinkEncoderConfig::json(),
        CommonSinkProps {
            batch_count: count,
            batch_duration: duration,
        },
    ));
    let mut resources = RegionResources::default();
    resources.encoders.insert(
        0,
        EncoderStage::new(encoder, BatchPolicy::new(count, duration)),
    );
    RegionProcessor::new(id.into(), &[plan], resources, capacities).unwrap()
}

pub(crate) fn compress_region(
    id: impl Into<String>,
    codec: crate::codec::CompressionCodec,
    capacities: ProcessorChannelCapacities,
) -> RegionProcessor {
    let child = std::sync::Arc::new(PhysicalPlan::EosWindow(
        crate::planner::physical::PhysicalEosWindow::new(vec![], 1),
    ));
    let plan = PhysicalPlan::SinkCompress(crate::planner::physical::PhysicalSinkCompress::new(
        child, 0, codec,
    ));
    RegionProcessor::new(id.into(), &[plan], RegionResources::default(), capacities).unwrap()
}

pub(crate) fn encrypted_region(
    id: impl Into<String>,
    capacities: ProcessorChannelCapacities,
) -> RegionProcessor {
    let child = std::sync::Arc::new(PhysicalPlan::EosWindow(
        crate::planner::physical::PhysicalEosWindow::new(vec![], 1),
    ));
    let plan = PhysicalPlan::SinkEncrypt(crate::planner::physical::PhysicalSinkEncrypt::new(
        child,
        0,
        crate::codec::EncryptionAlgorithm::AesGcm,
        "sink-aes-v1",
        256,
        std::sync::Arc::new(crate::codec::SecretBytes::new(vec![4; 32])),
    ));
    RegionProcessor::new(id.into(), &[plan], RegionResources::default(), capacities).unwrap()
}

pub(crate) fn region_chain(
    plans: &[PhysicalPlan],
    mut resources: RegionResources,
    capacities: ProcessorChannelCapacities,
    fused: bool,
) -> Vec<super::processor_builder::PlanProcessor> {
    let groups: Vec<&[PhysicalPlan]> = if fused {
        vec![plans]
    } else {
        plans.chunks(1).collect()
    };
    groups
        .into_iter()
        .enumerate()
        .map(|(index, group)| {
            let mut owned = RegionResources {
                aggregate_registry: resources.aggregate_registry.clone(),
                stateful_registry: resources.stateful_registry.clone(),
                isolate_opaque: resources.isolate_opaque,
                stage_default_domains: resources.stage_default_domains,
                input_domain: resources.input_domain,
                ..Default::default()
            };
            for plan in group {
                let id = plan.get_plan_index();
                if let Some(value) = resources.encoders.remove(&id) {
                    owned.encoders.insert(id, value);
                }
                if let Some(value) = resources.samplers.remove(&id) {
                    owned.samplers.insert(id, value);
                }
                if let Some(value) = resources.decoders.remove(&id) {
                    owned.decoders.insert(id, value);
                }
                if let Some(value) = resources.stage_stats.remove(&id) {
                    owned.stage_stats.insert(id, value);
                }
                if let Some(value) = resources.stage_domains.remove(&id) {
                    owned.stage_domains.insert(id, value);
                }
            }
            super::processor_builder::PlanProcessor::Region(Box::new(
                RegionProcessor::new(format!("test_region_{index}"), group, owned, capacities)
                    .unwrap(),
            ))
        })
        .collect()
}
