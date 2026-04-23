use std::collections::BTreeSet;

use crate::registry::{FeatureRegistry, InteractionDefinition, InteractionRegistry};
use crate::scanner::{CoverageRecord, ScanResult, SourceLocation};

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub location: SourceLocation,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct ValidationResult {
    pub errors: Vec<ValidationError>,
}

pub fn validate(
    registry: &FeatureRegistry,
    interactions: &InteractionRegistry,
    scan: &ScanResult,
) -> ValidationResult {
    let mut errors = Vec::new();

    for error in &scan.errors {
        errors.push(ValidationError {
            location: error.location.clone(),
            message: error.message.clone(),
        });
    }

    validate_interaction_registry(registry, interactions, &mut errors);

    for record in &scan.records {
        validate_record(registry, record, &mut errors);
    }

    ValidationResult { errors }
}

fn validate_record(
    registry: &FeatureRegistry,
    record: &CoverageRecord,
    errors: &mut Vec<ValidationError>,
) {
    let mut seen = BTreeSet::new();

    for feature_id in &record.features {
        if !seen.insert(feature_id.clone()) {
            errors.push(ValidationError {
                location: record.location.clone(),
                message: format!(
                    "duplicate feature id `{}` in one coverage annotation",
                    feature_id
                ),
            });
        }

        if !registry.contains(feature_id) {
            errors.push(ValidationError {
                location: record.location.clone(),
                message: format!("unknown feature id `{}`", feature_id),
            });
        }
    }
}

fn validate_interaction_registry(
    registry: &FeatureRegistry,
    interactions: &InteractionRegistry,
    errors: &mut Vec<ValidationError>,
) {
    for interaction in interactions.interactions() {
        let location = SourceLocation {
            path: interaction.source_file.clone(),
            line: 1,
        };

        if interaction.status != "active" && interaction.status != "retired" {
            errors.push(ValidationError {
                location: location.clone(),
                message: format!(
                    "interaction `{}` has unsupported status `{}`",
                    interaction.id, interaction.status
                ),
            });
        }

        if !interaction
            .id
            .starts_with(&format!("{}.", interaction.domain))
        {
            errors.push(ValidationError {
                location: location.clone(),
                message: format!(
                    "interaction id `{}` does not match domain `{}`",
                    interaction.id, interaction.domain
                ),
            });
        }

        validate_interaction_features(registry, interaction, &location, errors);
    }
}

fn validate_interaction_features(
    registry: &FeatureRegistry,
    interaction: &InteractionDefinition,
    location: &SourceLocation,
    errors: &mut Vec<ValidationError>,
) {
    if interaction.status == "active" && interaction.features.len() < 2 {
        errors.push(ValidationError {
            location: location.clone(),
            message: format!(
                "active interaction `{}` must reference at least two features",
                interaction.id
            ),
        });
    }

    let mut seen = BTreeSet::new();
    for feature_id in &interaction.features {
        if !seen.insert(feature_id.clone()) {
            errors.push(ValidationError {
                location: location.clone(),
                message: format!(
                    "duplicate feature id `{}` in interaction `{}`",
                    feature_id, interaction.id
                ),
            });
        }

        if !registry.contains(feature_id) {
            errors.push(ValidationError {
                location: location.clone(),
                message: format!(
                    "interaction `{}` references unknown feature id `{}`",
                    interaction.id, feature_id
                ),
            });
            continue;
        }

        if interaction.status == "active" && !registry.is_active(feature_id) {
            errors.push(ValidationError {
                location: location.clone(),
                message: format!(
                    "active interaction `{}` references inactive feature id `{}`",
                    interaction.id, feature_id
                ),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::registry::{FeatureDefinition, InteractionDefinition};
    use crate::scanner::{CoverageKind, ScanError};

    use super::*;

    #[test]
    fn reports_unknown_feature_ids() {
        let mut features = BTreeMap::new();
        features.insert(
            "planner.logical.rule".to_string(),
            FeatureDefinition {
                id: "planner.logical.rule".to_string(),
                title: "Rule".to_string(),
                summary: "Summary".to_string(),
                doc_refs: vec![],
                status: "active".to_string(),
                source_file: PathBuf::from("features.yaml"),
            },
        );
        let registry = FeatureRegistry {
            features,
            registry_files: Vec::new(),
        };
        let interactions = InteractionRegistry {
            interactions: BTreeMap::new(),
            registry_files: Vec::new(),
        };
        let scan = ScanResult {
            records: vec![CoverageRecord {
                id: "tests/file.rs::test_case".to_string(),
                kind: CoverageKind::Function,
                features: vec!["planner.logical.missing".to_string()],
                location: SourceLocation {
                    path: PathBuf::from("tests/file.rs"),
                    line: 1,
                },
            }],
            errors: Vec::<ScanError>::new(),
        };

        let result = validate(&registry, &interactions, &scan);
        assert_eq!(result.errors.len(), 1);
        assert!(result.errors[0].message.contains("unknown feature id"));
    }

    #[test]
    fn reports_invalid_interaction_registry_entries() {
        let mut features = BTreeMap::new();
        features.insert(
            "pipeline.runtime.eventtime".to_string(),
            FeatureDefinition {
                id: "pipeline.runtime.eventtime".to_string(),
                title: "Eventtime".to_string(),
                summary: "Summary".to_string(),
                doc_refs: vec![],
                status: "active".to_string(),
                source_file: PathBuf::from("pipeline.yaml"),
            },
        );
        features.insert(
            "stream.watermark.propagation".to_string(),
            FeatureDefinition {
                id: "stream.watermark.propagation".to_string(),
                title: "Watermark".to_string(),
                summary: "Summary".to_string(),
                doc_refs: vec![],
                status: "retired".to_string(),
                source_file: PathBuf::from("stream.yaml"),
            },
        );
        let registry = FeatureRegistry {
            features,
            registry_files: Vec::new(),
        };

        let mut interaction_entries = BTreeMap::new();
        interaction_entries.insert(
            "runtime.invalid".to_string(),
            InteractionDefinition {
                id: "runtime.invalid".to_string(),
                domain: "planner".to_string(),
                title: "Invalid".to_string(),
                summary: "Summary".to_string(),
                features: vec![
                    "pipeline.runtime.eventtime".to_string(),
                    "pipeline.runtime.eventtime".to_string(),
                    "stream.watermark.propagation".to_string(),
                    "stream.window.missing".to_string(),
                ],
                status: "active".to_string(),
                source_file: PathBuf::from("runtime.yaml"),
            },
        );
        let interactions = InteractionRegistry {
            interactions: interaction_entries,
            registry_files: Vec::new(),
        };
        let scan = ScanResult::default();

        let result = validate(&registry, &interactions, &scan);
        assert_eq!(result.errors.len(), 4);
        assert!(result
            .errors
            .iter()
            .any(|error| error.message.contains("does not match domain")));
        assert!(result
            .errors
            .iter()
            .any(|error| error.message.contains("duplicate feature id")));
        assert!(result
            .errors
            .iter()
            .any(|error| error.message.contains("inactive feature id")));
        assert!(result
            .errors
            .iter()
            .any(|error| error.message.contains("unknown feature id")));
    }
}
