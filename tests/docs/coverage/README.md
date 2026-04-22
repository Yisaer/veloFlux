# Feature Coverage Registry

This directory contains the machine-readable feature registry used by the test
coverage tooling.

## Purpose

- `docs/` remains the source of feature semantics and design rationale.
- `tests/docs/coverage/` defines the stable feature IDs used by tests and
  coverage evaluation.
- Runtime coverage calculation should read these YAML files instead of parsing
  Markdown headings directly.
- This registry is not API schema coverage. Do not add feature IDs only to track
  manager REST field names or DTO shape stability.

## Layout

- `features/parser.yaml`
- `features/planner.yaml`
- `features/processor.yaml`
- `features/pipeline.yaml`
- `features/source.yaml`
- `features/sink.yaml`
- `features/stream.yaml`

Each file owns a single top-level feature domain.

## Naming Rules

Feature IDs use dot-separated lowercase segments:

- Required format: `<domain>.<capability>.<subject>[.<variant>]`
- The first segment must be one of:
  - `parser`
  - `planner`
  - `processor`
  - `pipeline`
  - `source`
  - `sink`
  - `stream`
- IDs describe stable observable behavior, not implementation details.
- IDs should use noun-style phrases instead of function names or rule numbers.
- Single-feature IDs must not encode multi-feature combinations.

Examples:

- `parser.select.where_clause`
- `planner.logical.predicate_pushdown`
- `processor.barrier.alignment`
- `source.memory.collection_input`

## Field Semantics

Each feature entry uses the following minimal schema:

- `id`: Stable feature identifier.
- `title`: Human-readable title.
- `summary`: Short statement of the behavior being covered.
- `doc_refs`: Design or behavior documents that justify the feature.
- `status`: Current lifecycle state. Use `active` unless the feature is retired.

## Maintenance Rules

- Add or update a feature here when a new documented capability becomes part of
  the supported test coverage space.
- Keep pure manager/control-plane REST DTO contracts in API docs and manager API
  tests, unless the endpoint itself is the documented surface for a runtime
  feature.
- Keep `doc_refs` aligned with the current design documents.
- Prefer adding new IDs over renaming existing ones. If a rename is required,
  handle it as an explicit migration in follow-up tooling instead of silently
  changing the old ID.
- Keep feature granularity at the "independently assertable behavior" level.
  Do not encode case variants such as `null`, `alias`, or `empty` directly in
  the feature ID during the single-feature coverage phase.
