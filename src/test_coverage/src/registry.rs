use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct FeatureDefinition {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub doc_refs: Vec<String>,
    pub status: String,
    pub source_file: PathBuf,
}

#[derive(Debug)]
pub struct FeatureRegistry {
    pub(crate) features: BTreeMap<String, FeatureDefinition>,
    pub registry_files: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct FeatureFile {
    features: Vec<FeatureYaml>,
}

#[derive(Debug, Deserialize)]
struct FeatureYaml {
    id: String,
    title: String,
    summary: String,
    doc_refs: Vec<String>,
    status: String,
}

impl FeatureRegistry {
    pub fn load(repo_root: &Path) -> Result<Self, String> {
        let registry_root = repo_root.join("tests/docs/coverage/features");
        if !registry_root.exists() {
            return Err(format!(
                "feature registry directory does not exist: {}",
                registry_root.display()
            ));
        }

        let mut features = BTreeMap::new();
        let mut registry_files = Vec::new();

        for entry in WalkDir::new(&registry_root)
            .min_depth(1)
            .max_depth(1)
            .sort_by_file_name()
        {
            let entry = entry.map_err(|err| format!("failed to walk registry: {err}"))?;
            if !entry.file_type().is_file() {
                continue;
            }
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("yaml") {
                continue;
            }

            let yaml_path = entry.path().to_path_buf();
            registry_files.push(
                yaml_path
                    .strip_prefix(repo_root)
                    .unwrap_or(&yaml_path)
                    .to_path_buf(),
            );

            let content = fs::read_to_string(&yaml_path)
                .map_err(|err| format!("failed to read {}: {err}", yaml_path.display()))?;
            let parsed: FeatureFile = serde_yaml::from_str(&content)
                .map_err(|err| format!("failed to parse {}: {err}", yaml_path.display()))?;

            for feature in parsed.features {
                if features.contains_key(&feature.id) {
                    return Err(format!(
                        "duplicate feature id `{}` in {}",
                        feature.id,
                        yaml_path.display()
                    ));
                }

                features.insert(
                    feature.id.clone(),
                    FeatureDefinition {
                        id: feature.id,
                        title: feature.title,
                        summary: feature.summary,
                        doc_refs: feature.doc_refs,
                        status: feature.status,
                        source_file: yaml_path
                            .strip_prefix(repo_root)
                            .unwrap_or(entry.path())
                            .to_path_buf(),
                    },
                );
            }
        }

        Ok(Self {
            features,
            registry_files,
        })
    }

    pub fn contains(&self, feature_id: &str) -> bool {
        self.features.contains_key(feature_id)
    }

    pub fn active_feature_ids(&self) -> BTreeSet<String> {
        self.features
            .values()
            .filter(|feature| feature.status == "active")
            .map(|feature| feature.id.clone())
            .collect()
    }

    pub fn features(&self) -> impl Iterator<Item = &FeatureDefinition> {
        self.features.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_feature_ids_filter_by_status() {
        let mut registry = FeatureRegistry {
            features: BTreeMap::new(),
            registry_files: Vec::new(),
        };
        registry.features.insert(
            "planner.logical.rule".to_string(),
            FeatureDefinition {
                id: "planner.logical.rule".to_string(),
                title: "Rule".to_string(),
                summary: "Summary".to_string(),
                doc_refs: vec![],
                status: "active".to_string(),
                source_file: PathBuf::from("a.yaml"),
            },
        );
        registry.features.insert(
            "planner.logical.retired".to_string(),
            FeatureDefinition {
                id: "planner.logical.retired".to_string(),
                title: "Retired".to_string(),
                summary: "Summary".to_string(),
                doc_refs: vec![],
                status: "retired".to_string(),
                source_file: PathBuf::from("b.yaml"),
            },
        );

        let active = registry.active_feature_ids();
        assert!(active.contains("planner.logical.rule"));
        assert!(!active.contains("planner.logical.retired"));
    }
}
