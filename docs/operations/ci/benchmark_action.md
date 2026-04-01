# Benchmark GitHub Actions (pipeline E2E)

This document describes the **Criterion-based pipeline E2E benchmark** and the **GitHub Actions workflows** added in this PR.

## Goals

- Provide a lightweight, repeatable **CPU benchmark** for the `flow` pipeline hot path.
- Make it easy to run benchmarks in CI and view results in the **GitHub Actions Job Summary**.
- Enable manual comparison between `main` and selected branches/PRs (comparison logic is intentionally out of scope here).

## Non-goals

- No strict performance gating in CI (GitHub-hosted runner noise is expected).
- No persistent benchmark history storage or baseline comparison.
- No production-scale datasets yet (this PR uses a small dataset to validate the end-to-end workflow).

## What is benchmarked

We benchmark an **end-to-end (E2E) pipeline** path:

1. Publish **JSON bytes** into a Memory topic (`MemoryTopicKind::Bytes`).
2. The pipeline performs SQL processing.
3. The pipeline outputs **JSON bytes** into another Memory topic (`MemoryTopicKind::Bytes`).

This includes JSON decode/encode overhead in addition to pipeline processing.

### Benchmark cases

The benchmark suite currently includes three SQL cases:

- `pipeline_e2e/projection`
  - `SELECT col1, col2 FROM source_stream`
- `pipeline_e2e/filter`
  - `SELECT user_id, score FROM source_stream WHERE score > 0`
- `pipeline_e2e/expr_filter`
  - `SELECT user_id, score*1.1 as score2 FROM source_stream WHERE score > 0`

### Dataset

- Small dataset only (intended to validate the workflow end-to-end).
- Each iteration publishes a fixed number of small JSON messages.

## Implementation overview

### Criterion bench

- Bench target: `pipeline_e2e` (see `Cargo.toml` `[[bench]]` section)
- Bench code: `benches/pipeline_e2e.rs`

The benchmark measures the time from publishing a batch of input messages until all expected output messages have been received (with a timeout).

### Metrics

We collect the following metrics from Criterion estimates:

- Mean (ns)
- Median (ns)
- Std Dev (ns)

A renderer script parses `target/criterion/**/new/estimates.json` and prints a markdown table.

### Summary renderer

- Script: `scripts/bench/render_criterion.py`
- Input: `target/criterion/**/new/estimates.json`
- Output: a markdown table written to the GitHub Actions Job Summary.

## GitHub Actions workflows

### 1) Automatic: run on push to `main`

- Workflow: `.github/workflows/bench_main.yml`
- Trigger: `push` to `main`

### 2) Manual: run on selected ref

- Workflow: `.github/workflows/bench_manual.yml`
- Trigger: `workflow_dispatch`
- Input: `ref` (branch/tag/SHA)

### Reusable workflow

Both workflows call the reusable workflow:

- `.github/workflows/_bench_run.yml`

The reusable workflow:

1. Checks out the requested `ref`
2. Runs `cargo bench --bench pipeline_e2e`
3. Runs the renderer script
4. Appends the markdown output to `$GITHUB_STEP_SUMMARY`

## How to run locally

- Run formatting and clippy:

```bash
make fmt
make clippy
```

- Run the benchmark:

```bash
cargo bench --bench pipeline_e2e
```

## Extending benchmark coverage

- Add new cases in `benches/pipeline_e2e.rs`.
- Consider adding larger datasets and additional SQL patterns (agg/window/join) once the workflow is stable.

