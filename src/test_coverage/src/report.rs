use std::collections::BTreeMap;

use crate::registry::{FeatureRegistry, InteractionRegistry};
use crate::scanner::ScanResult;

#[derive(Debug)]
pub struct DomainCoverage {
    pub domain: String,
    pub total: usize,
    pub covered: usize,
    pub coverage: f64,
}

#[derive(Debug)]
pub struct CoverageReport {
    pub active_features: usize,
    pub covered_features: usize,
    pub overall_coverage: f64,
    pub uncovered_features: Vec<String>,
    pub by_domain: Vec<DomainCoverage>,
}

#[derive(Debug)]
pub struct InteractionCoverageReport {
    pub active_interactions: usize,
    pub covered_interactions: usize,
    pub overall_coverage: f64,
    pub uncovered_interactions: Vec<String>,
    pub by_domain: Vec<DomainCoverage>,
}

impl CoverageReport {
    pub fn build(registry: &FeatureRegistry, scan: &ScanResult) -> Self {
        let active_features = registry.active_feature_ids();
        let covered_features_set = scan
            .records
            .iter()
            .flat_map(|record| record.features.iter())
            .filter(|feature_id| active_features.contains(*feature_id))
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();

        let uncovered_features = active_features
            .difference(&covered_features_set)
            .cloned()
            .collect::<Vec<_>>();

        let mut totals_by_domain = BTreeMap::<String, usize>::new();
        let mut covered_by_domain = BTreeMap::<String, usize>::new();

        for feature_id in &active_features {
            let domain = feature_domain(feature_id).to_string();
            *totals_by_domain.entry(domain.clone()).or_default() += 1;
            if covered_features_set.contains(feature_id) {
                *covered_by_domain.entry(domain).or_default() += 1;
            }
        }

        let by_domain = totals_by_domain
            .into_iter()
            .map(|(domain, total)| {
                let covered = covered_by_domain.get(&domain).copied().unwrap_or_default();
                DomainCoverage {
                    domain,
                    total,
                    covered,
                    coverage: ratio(covered, total),
                }
            })
            .collect();

        Self {
            active_features: active_features.len(),
            covered_features: covered_features_set.len(),
            overall_coverage: ratio(covered_features_set.len(), active_features.len()),
            uncovered_features,
            by_domain,
        }
    }
}

impl InteractionCoverageReport {
    pub fn build(interactions: &InteractionRegistry, scan: &ScanResult) -> Self {
        let active_interactions = interactions
            .interactions()
            .filter(|interaction| interaction.status == "active")
            .collect::<Vec<_>>();

        let coverage_sets = scan
            .records
            .iter()
            .map(|record| {
                record
                    .features
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>()
            })
            .collect::<Vec<_>>();

        let covered_interactions_set = active_interactions
            .iter()
            .filter(|interaction| {
                coverage_sets.iter().any(|features| {
                    interaction
                        .features
                        .iter()
                        .all(|feature_id| features.contains(feature_id))
                })
            })
            .map(|interaction| interaction.id.clone())
            .collect::<std::collections::BTreeSet<_>>();

        let uncovered_interactions = active_interactions
            .iter()
            .filter(|interaction| !covered_interactions_set.contains(&interaction.id))
            .map(|interaction| interaction.id.clone())
            .collect::<Vec<_>>();

        let mut totals_by_domain = BTreeMap::<String, usize>::new();
        let mut covered_by_domain = BTreeMap::<String, usize>::new();

        for interaction in &active_interactions {
            *totals_by_domain
                .entry(interaction.domain.clone())
                .or_default() += 1;
            if covered_interactions_set.contains(&interaction.id) {
                *covered_by_domain
                    .entry(interaction.domain.clone())
                    .or_default() += 1;
            }
        }

        let by_domain = totals_by_domain
            .into_iter()
            .map(|(domain, total)| {
                let covered = covered_by_domain.get(&domain).copied().unwrap_or_default();
                DomainCoverage {
                    domain,
                    total,
                    covered,
                    coverage: ratio(covered, total),
                }
            })
            .collect();

        Self {
            active_interactions: active_interactions.len(),
            covered_interactions: covered_interactions_set.len(),
            overall_coverage: ratio(covered_interactions_set.len(), active_interactions.len()),
            uncovered_interactions,
            by_domain,
        }
    }
}

fn feature_domain(feature_id: &str) -> &str {
    feature_id.split('.').next().unwrap_or(feature_id)
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::registry::{
        FeatureDefinition, FeatureRegistry, InteractionDefinition, InteractionRegistry,
    };
    use crate::scanner::{CoverageKind, CoverageRecord, ScanResult, SourceLocation};

    use super::*;

    #[test]
    fn computes_domain_coverage() {
        let mut features = BTreeMap::new();
        features.insert(
            "parser.select.projection".to_string(),
            FeatureDefinition {
                id: "parser.select.projection".to_string(),
                title: "Projection".to_string(),
                summary: "Summary".to_string(),
                doc_refs: vec![],
                status: "active".to_string(),
                source_file: PathBuf::from("parser.yaml"),
            },
        );
        features.insert(
            "planner.logical.rule".to_string(),
            FeatureDefinition {
                id: "planner.logical.rule".to_string(),
                title: "Rule".to_string(),
                summary: "Summary".to_string(),
                doc_refs: vec![],
                status: "active".to_string(),
                source_file: PathBuf::from("planner.yaml"),
            },
        );

        let registry = FeatureRegistry {
            features,
            registry_files: Vec::new(),
        };
        let scan = ScanResult {
            records: vec![CoverageRecord {
                id: "tests/file.rs::test_case".to_string(),
                kind: CoverageKind::Function,
                features: vec!["planner.logical.rule".to_string()],
                location: SourceLocation {
                    path: PathBuf::from("tests/file.rs"),
                    line: 1,
                },
            }],
            errors: Vec::new(),
        };

        let report = CoverageReport::build(&registry, &scan);
        assert_eq!(report.active_features, 2);
        assert_eq!(report.covered_features, 1);
        assert_eq!(
            report.uncovered_features,
            vec!["parser.select.projection".to_string()]
        );
    }

    #[test]
    fn computes_interaction_coverage_with_superset_matching() {
        let mut interactions = BTreeMap::new();
        interactions.insert(
            "runtime.eventtime_tumbling_watermark".to_string(),
            InteractionDefinition {
                id: "runtime.eventtime_tumbling_watermark".to_string(),
                domain: "runtime".to_string(),
                title: "Runtime interaction".to_string(),
                summary: "Summary".to_string(),
                features: vec![
                    "pipeline.runtime.eventtime".to_string(),
                    "stream.watermark.propagation".to_string(),
                ],
                status: "active".to_string(),
                source_file: PathBuf::from("runtime.yaml"),
            },
        );
        interactions.insert(
            "planner.uncovered".to_string(),
            InteractionDefinition {
                id: "planner.uncovered".to_string(),
                domain: "planner".to_string(),
                title: "Planner interaction".to_string(),
                summary: "Summary".to_string(),
                features: vec!["planner.a".to_string(), "planner.b".to_string()],
                status: "active".to_string(),
                source_file: PathBuf::from("planner.yaml"),
            },
        );
        let registry = InteractionRegistry {
            interactions,
            registry_files: Vec::new(),
        };
        let scan = ScanResult {
            records: vec![CoverageRecord {
                id: "tests/file.rs::test_case".to_string(),
                kind: CoverageKind::Function,
                features: vec![
                    "pipeline.runtime.eventtime".to_string(),
                    "stream.watermark.propagation".to_string(),
                    "stream.window.tumbling".to_string(),
                ],
                location: SourceLocation {
                    path: PathBuf::from("tests/file.rs"),
                    line: 1,
                },
            }],
            errors: Vec::new(),
        };

        let report = InteractionCoverageReport::build(&registry, &scan);
        assert_eq!(report.active_interactions, 2);
        assert_eq!(report.covered_interactions, 1);
        assert_eq!(
            report.uncovered_interactions,
            vec!["planner.uncovered".to_string()]
        );
    }
}
