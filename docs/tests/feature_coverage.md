# Feature Coverage

## Goal

This document defines feature-oriented test coverage for veloFlux.

The primary goal is to measure whether a documented single feature is explicitly
covered by at least one test. Explicit interaction coverage additionally
measures whether selected feature combinations are covered by the same test unit
or testcase.

This is not code coverage.

## Scope

Single-feature coverage answers one question:

- Is a feature covered by at least one test unit or testcase?

Interaction coverage answers one additional question:

- Is an explicitly registered feature combination covered by at least one test
  unit or testcase?

This version intentionally excludes:

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

Machine-readable interaction IDs live in:

- `tests/docs/coverage/interactions/planner.yaml`
- `tests/docs/coverage/interactions/runtime.yaml`
- `tests/docs/coverage/interactions/sink.yaml`

Interaction registries are split by interaction domain. Interaction domains may
describe cross-domain behavior and are not limited to the single-feature domain
list.

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

The only test-side coverage field is `covers`.

`covers` means:

- the test explicitly asserts the documented behavior of the listed feature

`covers` does not mean:

- the SQL input merely contains related syntax
- the runtime path happens to execute related code
- the plan shape incidentally passes through a similar operator

If a test cannot justify a feature by its assertions, that feature must not be
listed in `covers`.

## Annotation Format

Coverage uses two annotation forms, depending on test shape.

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

Coverage keeps function-level coverage and testcase-level coverage separate.

- plain test functions use the `coverage-covers` comment
- table-driven testcases use the `covers` field inside each case
- there is no inheritance or merge rule between function-level and testcase-level coverage

This keeps scanning and validation simple.

## Interaction Coverage

Interaction coverage uses a separate registry and the existing `covers`
annotations.

An interaction entry uses this schema:

- `id`: Stable interaction identifier.
- `title`: Human-readable title.
- `summary`: Short statement of the combined behavior being covered.
- `features`: The single-feature IDs that must be covered together.
- `status`: Current lifecycle state.
  - `active`: The interaction is a same-record coverage requirement and
    participates in coverage reporting.
  - `cross_module`: The interaction is intentionally kept in the registry
    because its required evidence is expected to stay split across planner and
    pipeline/runtime or similar layered suites. It remains visible for review,
    but it does not participate in uncovered-interaction reporting.
  - `retired`: The interaction is no longer part of the active coverage target
    set.

An active interaction is covered when at least one coverage record contains all
of the interaction's features in the same `covers` list. Matching is a superset
match: a record that covers `[A, B, C]` also covers an interaction that requires
`[A, B]`.

Interaction coverage does not introduce another test annotation. It is derived
from function-level `coverage-covers` comments and testcase-level `covers`
fields.

Cross-module interactions remain machine-readable design targets, but they
document intentional split evidence instead of requiring one same-record
coverage annotation. Use them when a cross-layer behavior should stay visible
in the registry even though planner-side and runtime-side assertions are
deliberately kept in different test modules.

## Evaluator Requirements

Coverage evaluation should be CPU-only and should rely on static scanning.

The evaluator should:

1. load all feature IDs from `tests/docs/coverage/features/*.yaml`
2. load all interaction IDs from `tests/docs/coverage/interactions/*.yaml`
3. scan test functions for `coverage-covers` comments
4. scan table-driven testcase literals for `covers: &[...]`
5. validate every referenced feature ID and interaction feature reference
6. compute the set of covered features and interactions
7. report uncovered features and interactions

The evaluator must not depend on model inference or token-based semantic
classification during normal coverage calculation.

## Validation Rules

The evaluator should report at least the following errors:

- unknown feature ID referenced by a test
- duplicate feature ID within one `coverage-covers` line
- duplicate feature ID within one testcase `covers` field
- malformed `coverage-covers` comment format
- missing `covers` field in a cross-module table-driven testcase
- duplicate testcase `name` values within one table-driven test group
- invalid interaction status
- interaction ID whose first segment does not match its registry domain
- interaction with fewer than two features
- duplicate feature ID within one interaction
- unknown feature ID referenced by an interaction
- active or cross_module interaction referencing an inactive feature

## Reporting Model

The minimal reporting model is:

- total feature count
- covered feature count
- uncovered feature list
- coverage ratio by all features
- coverage ratio by top-level domain
- interaction coverage count and ratio
- interaction coverage ratio by interaction domain

The basic formula is:

`single_feature_coverage = covered_features / active_features`

## Non-Goals

The following items are explicitly left for later versions:

- primary versus secondary coverage
- testcase tags
- automatic pairwise or higher-order interaction generation
- risk-weighted coverage scoring

## Registry Maintenance Notes

- Register an interaction as `active` only when the combination should be
  enforced as a same-record coverage target.
- Register an interaction as `cross_module` when the combination should remain
  visible in the registry, but the current test strategy intentionally keeps
  its evidence split across specialized suites such as planner explain tests
  and pipeline runtime tests.
- Promote a `cross_module` interaction to `active` only after a single test
  unit or testcase carries all required features in one `covers` record.
- automatic mapping from SQL text or EXPLAIN JSON to feature IDs
- REST response field-shape coverage for manager metadata APIs

## Maintenance Rules

- Update the YAML registry when a documented feature becomes part of supported
  behavior.
- Update the interaction registry when a documented combination should be
  recorded as a required same-record coverage target.
- Add or update `covers` annotations whenever a new test explicitly validates a
  feature.
- Prefer adding new feature IDs over renaming existing IDs.
- Keep annotations strict and conservative. Over-reporting coverage is worse
  than under-reporting it.
