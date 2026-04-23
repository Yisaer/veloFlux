use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use test_coverage::registry::{FeatureRegistry, InteractionRegistry};
use test_coverage::report::{CoverageReport, InteractionCoverageReport};
use test_coverage::scanner::scan_repository;
use test_coverage::validator::validate;

fn main() -> ExitCode {
    match run() {
        Ok(has_errors) => {
            if has_errors {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<bool, String> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        return Err(usage());
    };

    if command != "check" {
        return Err(usage());
    }

    let repo_root = env::current_dir().map_err(|err| format!("failed to resolve cwd: {err}"))?;
    let registry = FeatureRegistry::load(&repo_root)?;
    let interactions = InteractionRegistry::load(&repo_root)?;
    let scan = scan_repository(&repo_root)?;
    let validation = validate(&registry, &interactions, &scan);
    let report = CoverageReport::build(&registry, &scan);
    let interaction_report = InteractionCoverageReport::build(&interactions, &scan);

    print_summary(
        &repo_root,
        &registry,
        &interactions,
        &scan,
        &validation,
        &report,
        &interaction_report,
    );

    Ok(!validation.errors.is_empty())
}

fn usage() -> String {
    "usage: cargo run -p test_coverage -- check".to_string()
}

fn print_summary(
    repo_root: &PathBuf,
    registry: &FeatureRegistry,
    interactions: &InteractionRegistry,
    scan: &test_coverage::scanner::ScanResult,
    validation: &test_coverage::validator::ValidationResult,
    report: &CoverageReport,
    interaction_report: &InteractionCoverageReport,
) {
    println!("Feature coverage check");
    println!("repo: {}", repo_root.display());
    println!("feature_registry_files: {}", registry.registry_files.len());
    println!(
        "interaction_registry_files: {}",
        interactions.registry_files.len()
    );
    println!("feature_records: {}", registry.active_feature_ids().len());
    println!(
        "interaction_records: {}",
        interactions.active_interaction_ids().len()
    );
    println!("coverage_records: {}", scan.records.len());
    println!();

    if validation.errors.is_empty() {
        println!("Validation: OK");
    } else {
        println!("Validation errors: {}", validation.errors.len());
        for error in &validation.errors {
            println!(
                "  - {}:{}: {}",
                error.location.path.display(),
                error.location.line,
                error.message
            );
        }
    }

    println!();
    println!("Coverage summary");
    println!("  active_features: {}", report.active_features);
    println!("  covered_features: {}", report.covered_features);
    println!(
        "  overall_coverage: {:.2}%",
        report.overall_coverage * 100.0
    );
    println!("  uncovered_features: {}", report.uncovered_features.len());
    println!();
    println!("Coverage by domain");
    for domain in &report.by_domain {
        println!(
            "  - {}: {}/{} ({:.2}%)",
            domain.domain,
            domain.covered,
            domain.total,
            domain.coverage * 100.0
        );
    }

    if !report.uncovered_features.is_empty() {
        println!();
        println!("Uncovered features");
        for feature_id in &report.uncovered_features {
            println!("  - {feature_id}");
        }
    }

    println!();
    println!("Interaction coverage summary");
    println!(
        "  active_interactions: {}",
        interaction_report.active_interactions
    );
    println!(
        "  covered_interactions: {}",
        interaction_report.covered_interactions
    );
    println!(
        "  overall_coverage: {:.2}%",
        interaction_report.overall_coverage * 100.0
    );
    println!(
        "  uncovered_interactions: {}",
        interaction_report.uncovered_interactions.len()
    );
    println!();
    println!("Interaction coverage by domain");
    for domain in &interaction_report.by_domain {
        println!(
            "  - {}: {}/{} ({:.2}%)",
            domain.domain,
            domain.covered,
            domain.total,
            domain.coverage * 100.0
        );
    }

    if !interaction_report.uncovered_interactions.is_empty() {
        println!();
        println!("Uncovered interactions");
        for interaction_id in &interaction_report.uncovered_interactions {
            println!("  - {interaction_id}");
        }
    }
}
