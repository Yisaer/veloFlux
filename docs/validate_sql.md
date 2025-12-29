# SQL Validation: Background and Requirements

This document defines the background and requirements for a `validate_sql` capability in SynapseFlow.

`validate_sql` enables an agent (or user tooling) to submit SQL and receive **structured, actionable validation errors** without creating a pipeline or executing data.

## Background

Users write SQL based on natural language intent, but they often do not know:

- Which SQL syntax/features the parser supports.
- Which streams exist and what their schemas are.
- Which functions exist (scalar/aggregate/stateful) and what constraints they have.

Without validation feedback, SQL generation tends to hallucinate unsupported syntax, unknown columns, or invalid function usage.

## Goals

- Validate SQL against **current runtime facts** (streams/schemas/functions) and supported syntax.
- Return errors in a **machine-actionable** format so an agent can self-correct reliably.
- Keep validation **deterministic** and **side-effect free** (no pipeline creation, no execution).

## Non-Goals

- Executing queries or returning result data.
- Creating, starting, or persisting pipelines.
- Connector connectivity checks (MQTT, sinks, etc.).

## Runtime Grounding Requirements

Validation must use runtime sources of truth:

- Streams and schemas: manager catalog (`/streams`, `/streams/describe/:name`).
- Functions: function catalog (`/functions`, `/functions/describe/:name`).

## API Requirements

Expose a validation interface (recommended over REST) that accepts:

- `sql: string`

And returns:

- `ok: boolean`
- `errors: ValidationError[]` when `ok == false`

## Validation Error Requirements

Each error should be structured and stable enough for agent logic:

- `code: string` stable identifier for programmatic handling.
- `message: string` short human-readable summary.
- Optional `span` (line/column) to locate the issue.
- Optional `hints: string[]` describing possible fixes.
- Optional `candidates: string[]` for unknown names (streams/columns/functions).

The validator should explicitly cover, at minimum:

- Syntax/feature unsupported (e.g. `HAVING`, `ORDER BY` when not supported).
- Unknown stream / unknown column / ambiguous column.
- Unknown function, wrong arity, type mismatch.
- Function kind misuse:
  - aggregate functions in invalid contexts
  - stateful functions in unsupported contexts or violating requirements (e.g. deterministic order)
