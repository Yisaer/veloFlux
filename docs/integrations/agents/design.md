# veloFlux Agent Design Guide

This document describes how to design an LLM agent that converts natural-language requirements into **veloFlux-valid SQL** by grounding on repository-derived metadata (syntax/functions/operators/streams) and by validating output with the project itself (parser/planner/explain).

Intended audience: developers (including AI coding assistants) implementing an agent. The runtime agent (a separate CLI process) should not rely on reading repository Markdown files. All runtime facts (streams/schema/capabilities) must come from veloFlux/Manager introspection APIs, plus validation/explain tools.

## Problem Statement

Users often:

- Don’t know which SQL syntax is supported by veloFlux’s `parser`.
- Don’t know which functions exist, what they do, and their type/argument rules.
- Don’t know which operators/processors exist and how the planner lowers SQL into a plan.

A generic LLM (without access to the codebase) will inevitably hallucinate unsupported syntax, nonexistent functions, or incorrect semantics.

## Goals

- Convert natural language (NL) → veloFlux SQL that is *actually supported*.
- Provide a verification loop: `validate` + `explain` to iteratively correct SQL.
- Explain results to users: “why this SQL”, “which functions/operators are used”, “what the plan does”.

## Non-Goals

- Being a general SQL tutor for arbitrary SQL dialects.
- Guessing schema/stream existence without checking a catalog.
- Relying on “reading match arms” at runtime as the primary grounding method.

## Key Principle: Productize “Source Understanding”

Do not expect the agent to infer capabilities by reading scattered implementation code.
Instead, **promote implicit behavior into explicit metadata** that can be exported as JSON and rendered as docs.

This metadata is the agent’s source of truth.

## Architecture Overview

Think of veloFlux as a compilation pipeline:

`NL intent → SQL (surface language) → AST (parser) → Logical Plan (planner) → Operators/Processors (execution)`

The agent should not choose “syntax-first” or “operator-first” exclusively.
The most reliable approach is:

- **Plan-first**: infer the intended relational pipeline (filter/project/aggregate/sort/limit…).
- **Syntax-constrained rendering**: emit SQL that stays within supported syntax/features.
- **Explain-driven grounding**: use `EXPLAIN` output as the judge for whether the plan matches intent.

## The 4 Metadata Catalogs (MVP)

### 1) Streams & Schema Catalog

The agent must be able to answer:

- Which streams exist?
- What columns exist in a stream?
- What are the column types and nullability?
- Are there time semantics (event time / watermark / primary key) that affect planning?

Recommended fields:

- `name`: stable identifier used in SQL.
- `description`: 1–3 sentence meaning.
- `columns`: list of `{ name, type, nullable, description }`.
- Optional: `event_time_column`, `primary_key`, `watermark`, `tags`.
- Optional: `aliases` for user-facing names.

Source of truth should be the **runtime catalog** (often built from `StreamDefinition`), and the agent should treat streams as **data** retrieved via introspection tools (e.g. `list_streams` / `describe_stream`), not as something inferred from implementation details.

### 2) Function Catalog

The agent must only use functions present in this catalog.

Recommended fields per function:

- `name` and `aliases`
- `args`: `{ name, type, optional, variadic }`
- `return_type`
- `description`
- `constraints`: e.g. null handling, timezone rules, determinism, units, value ranges
- `examples`: short SQL snippets

Keep function definitions close to the implementation, but registered centrally so they can be exported.

### 3) Syntax Feature Catalog (Parser Capabilities)

This tells the agent what syntactic constructs are supported and what is not.

Recommended organization:

- A hierarchical construct tree (statements, clauses, windowing, expressions, ...).
- For each construct: purpose/semantics, allowed placement, constraints, and examples.

Clients should treat constructs not present in the runtime catalog as unsupported by default.

### 4) Operator / Plan Node Catalog (Planner Output Semantics)

The agent must be able to explain what the plan does and verify the planner output.

Recommended fields per operator/plan-node kind:

- `name` / `kind` (stable identifier as appears in `EXPLAIN` JSON)
- `semantics`: short + extended description
- `inputs_outputs`: schema changes, row-count changes, ordering/partitioning expectations
- `traits`: stateful/order-sensitive/time-aware, etc.
- `constraints`: channel constraints, required keys, required sort, etc.
- `typical_sql_patterns`: “emitted by” hints, e.g. `WHERE` → `Filter`, `GROUP BY` → `Aggregate`
- `examples`: canonical SQL snippets (prefer those already covered by planner tests)

## Bridging SQL ↔ Plan: Use Explain as the Contract

To connect syntax and operators reliably:

- Ensure `EXPLAIN` provides a stable `kind`/`type` per node plus children and key expressions.
- Use planner tests (table-driven SQL → EXPLAIN JSON) as canonical mappings.
- Prefer embedding canonical SQL templates in operator metadata, rather than expecting the agent to reverse-engineer lowering rules.

## Tooling Contracts (What the Agent Calls)

Even if the agent can read the repository during development, the shipped runtime agent should primarily operate through a few stable runtime interfaces (recommended: REST APIs).

Minimum recommended tools:

- Stream introspection (e.g. `GET /streams`, `GET /streams/describe/:name`)
- Function introspection (e.g. `GET /functions`, `GET /functions/describe/:name`)
- Syntax capabilities introspection (e.g. `GET /capabilities/syntax`)
- Capabilities introspection (functions/operators) as machine-readable JSON
- SQL validation API (document path not finalized yet)
- SQL explain API (planner output as JSON + pretty string)

Optional later:

- `sample(stream, n)` (only if data access is allowed and safe)
- `run(sql)` or `preview(sql)` for end-to-end validation (higher complexity)

## Structured Errors (Critical for Self-Correction)

The difference between a “demo” agent and a “reliable” agent is whether it can self-correct.

Prefer returning validation errors with fields like:

- `code`: stable error identifier
- `message`: short human-readable summary
- `span`: `{ start_line, start_col, end_line, end_col }` (or byte offsets)
- `hints`: suggested fixes
- `candidates`: for unknown identifiers/functions/columns

## Agent Workflow (Recommended)

1) **Clarify inputs**: resolve stream names and identify required columns.
2) **Schema check**: `describe_stream()` to verify column existence and types.
3) **Intent sketch**: build a plan-level sketch (project/filter/aggregate/…).
4) **SQL rendering**: use only syntax/features/functions from catalogs.
5) **Validation loop**:
   - `validate_sql()`: fix syntax/semantic errors.
   - `explain_sql()`: verify plan includes expected node kinds and order.
6) **User-facing output**:
   - SQL
   - brief explanation of key clauses/functions
   - explain summary (pretty string or extracted highlights)

Stop and ask a clarifying question when:

- Stream name is ambiguous or not found.
- Required column is missing.
- Type rules make the requested operation invalid.
- The requested feature is unsupported (provide workaround if possible).

## How to Author Metadata in the Codebase

### Single Source of Truth

Metadata should be authored in code in a way that:

- Can be exported as JSON for agent use.
- Can be rendered into Markdown for human docs.
- Stays close enough to implementation that it won’t drift.

### Recommended Conventions

- Prefer explicit registries over scattered ad-hoc strings.
- Use stable identifiers for operator kinds and function names.
- Provide at least one example per function/operator (small SQL snippets).
- Document limitations and constraints explicitly (especially “unsupported” cases).

### Suggested Layout (Adjust to Repository Structure)

- `src/.../catalog/streams.rs`: exports stream schemas from `StreamDefinition` / registry
- `src/.../catalog/functions.rs`: function registry + metadata
- `src/.../catalog/operators.rs`: operator/plan-node metadata
- `src/.../catalog/syntax.rs`: parser feature/limitations catalog
- `src/.../cli/describe.rs`: `describe --json` output wiring

## Documentation Outputs

Generate (or maintain) docs derived from the same metadata:

- `docs/integrations/agents/design.md` (this guide)
- `docs/integrations/agents/runtime_playbook.md` (agent-facing runtime playbook)
- `docs/api/streams/schema.md` (stream/schema introspection contract)
- SQL validation design doc (add under `docs/api/` when the contract is documented)
- `docs/syntax/` (SQL syntax, functions, limitations) for user-facing reference
- operator reference docs (keep them under `docs/syntax/` or `docs/planner/` once finalized)

Docs are for humans and for development-time alignment; the runtime agent must use APIs/tools for all facts. Keep docs in English to match repository conventions.

## Iterative Delivery Plan (Suggested)

1) Add catalogs for **streams**, **functions**, **syntax features**, **operators** (minimal fields).
2) Add `describe/validate/explain` tooling contracts (CLI or library API).
3) Implement a minimal agent with the validation loop.
4) Improve structured errors and add more examples/templates.
5) Optionally add end-to-end data validation tools.
