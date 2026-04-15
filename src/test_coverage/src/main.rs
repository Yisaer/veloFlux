use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use test_coverage::registry::FeatureRegistry;
use test_coverage::report::CoverageReport;
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
    let scan = scan_repository(&repo_root)?;
    let validation = validate(&registry, &scan);
    let report = CoverageReport::build(&registry, &scan);

    print_summary(&repo_root, &registry, &scan, &validation, &report);

    Ok(!validation.errors.is_empty())
}

fn usage() -> String {
    "usage: cargo run -p test_coverage -- check".to_string()
}

fn print_summary(
    repo_root: &PathBuf,
    registry: &FeatureRegistry,
    scan: &test_coverage::scanner::ScanResult,
    validation: &test_coverage::validator::ValidationResult,
    report: &CoverageReport,
) {
    println!("Feature coverage check");
    println!("repo: {}", repo_root.display());
    println!("feature_registry_files: {}", registry.registry_files.len());
    println!("feature_records: {}", registry.active_feature_ids().len());
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
}
