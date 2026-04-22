# Feature Coverage V1

## Goal

This document defines the first version of feature-oriented test coverage for
veloFlux.

The goal is to measure whether a documented single feature is explicitly covered
by at least one test. This is not code coverage and it is not interaction
coverage yet.

## Scope

Feature coverage v1 answers one question only:

- Is a feature covered by at least one test unit or testcase?

This version intentionally excludes:

- multi-feature interaction coverage
- weight-based scoring
- automatic semantic inference from SQL or plan shape
- case tags such as `boundary`, `negative`, or `null`
- pure manager/control-plane REST DTO field-shape contracts; API documents may
  still define those contracts, but they should be validated by manager API
  tests unless the endpoint is the documented surface for a runtime feature

## Source of Truth

Feature semantics live in `docs/`.

Machine-readable feature IDs live in:

- `tests/docs/coverage/features/parser.yaml`
- `tests/docs/coverage/features/planner.yaml`
- `tests/docs/coverage/features/processor.yaml`
- `tests/docs/coverage/features/pipeline.yaml`
- `tests/docs/coverage/features/source.yaml`
- `tests/docs/coverage/features/sink.yaml`
- `tests/docs/coverage/features/stream.yaml`

Coverage evaluation must read the YAML registry instead of parsing Markdown
headings from `docs/`.

## Feature ID Model

Feature IDs use dot-separated lowercase segments:

- required format: `<domain>.<capability>.<subject>[.<variant>]`
- example: `planner.logical.top_level_column_pruning`
- example: `source.memory.collection_input`

The first segment must be one of:

- `parser`
- `planner`
- `processor`
- `pipeline`
- `source`
- `sink`
- `stream`

Feature IDs describe stable observable behavior. They must not encode:

- Rust type names
- function names
- rule numbers
- multi-feature combinations
- testcase-specific input variants

## Coverage Semantics

The only coverage field in v1 is `covers`.

`covers` means:

- the test explicitly asserts the documented behavior of the listed feature

`covers` does not mean:

- the SQL input merely contains related syntax
- the runtime path happens to execute related code
- the plan shape incidentally passes through a similar operator

If a test cannot justify a feature by its assertions, that feature must not be
listed in `covers`.

## Annotation Format

V1 uses two annotation forms, depending on test shape.

### Test Function Annotation

Non-table-driven test functions use a single-line coverage comment immediately
above the test attribute.

Example:

```rust
// coverage-covers: processor.barrier.alignment
#[test]
fn barrier_alignment_state_is_isolated_per_channel() {
    // ...
}
```

For multiple features, use a single line with comma-plus-space separation:

```rust
// coverage-covers: pipeline.runtime.eventtime, stream.watermark.propagation
#[tokio::test]
async fn eventtime_tumbling_window_drops_tuple_older_than_current_watermark() {
    // ...
}
```

Rules:

- the key must be exactly `coverage-covers:`
- the comment must be a single line
- feature IDs must be string literals written directly in the comment
- multiple feature IDs must be separated by `, `

### Table-Driven Testcase Annotation

Table-driven tests declare coverage inside the testcase struct.

Example:

```rust
struct TestCase {
    name: &'static str,
    sql: &'static str,
    covers: &'static [&'static str],
}
```

Example testcase:

```rust
TestCase {
    name: "wildcard_degradation",
    sql: "select * from stream_prune",
    covers: &["planner.logical.top_level_column_pruning"],
}
```

Rules:

- `covers` is required for every table-driven testcase
- `covers` must use string literals only
- `covers` must not be omitted and inferred from context

## Test Shape Rules

V1 keeps function-level coverage and testcase-level coverage separate.

- plain test functions use the `coverage-covers` comment
- table-driven testcases use the `covers` field inside each case
- there is no inheritance or merge rule between function-level and testcase-level coverage in v1

This keeps scanning and validation simple.

## Evaluator Requirements

Coverage evaluation should be CPU-only and should rely on static scanning.

The evaluator should:

1. load all feature IDs from `tests/docs/coverage/features/*.yaml`
2. scan test functions for `coverage-covers` comments
3. scan table-driven testcase literals for `covers: &[...]`
4. validate every referenced feature ID
5. compute the set of covered features
6. report uncovered features

The evaluator must not depend on model inference or token-based semantic
classification during normal coverage calculation.

## Validation Rules

The evaluator should report at least the following errors:

- unknown feature ID referenced by a test
- duplicate feature ID within one `coverage-covers` line
- duplicate feature ID within one testcase `covers` field
- malformed `coverage-covers` comment format
- missing `covers` field in a tracked table-driven testcase
- duplicate testcase `name` values within one table-driven test group

## Reporting Model

The minimal v1 reporting model is:

- total feature count
- covered feature count
- uncovered feature list
- coverage ratio by all features
- coverage ratio by top-level domain

The basic formula is:

`single_feature_coverage = covered_features / active_features`

## Non-Goals

The following items are explicitly left for later versions:

- primary versus secondary coverage
- testcase tags
- pairwise or higher-order interaction coverage
- risk-weighted coverage scoring
- automatic mapping from SQL text or EXPLAIN JSON to feature IDs
- REST response field-shape coverage for manager metadata APIs

## Maintenance Rules

- Update the YAML registry when a documented feature becomes part of supported
  behavior.
- Add or update `covers` annotations whenever a new test explicitly validates a
  feature.
- Prefer adding new feature IDs over renaming existing IDs.
- Keep annotations strict and conservative. Over-reporting coverage is worse
  than under-reporting it.
