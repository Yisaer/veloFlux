use std::collections::BTreeSet;

use crate::registry::FeatureRegistry;
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

pub fn validate(registry: &FeatureRegistry, scan: &ScanResult) -> ValidationResult {
    let mut errors = Vec::new();

    for error in &scan.errors {
        errors.push(ValidationError {
            location: error.location.clone(),
            message: error.message.clone(),
        });
    }

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::registry::FeatureDefinition;
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

        let result = validate(&registry, &scan);
        assert_eq!(result.errors.len(), 1);
        assert!(result.errors[0].message.contains("unknown feature id"));
    }
}
