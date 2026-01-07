# LangGraph Design Draft: `nl2pipeline` (v1 → server-ready)

This document proposes how to re-platform the current `nl2pipeline` state machine onto LangGraph so
the *same core workflow* can serve both:

- CLI (interactive REPL)
- Server (HTTP/SSE/WebSocket), with **interrupt/resume/observability**

Non-goals stay the same as v1:

- Do not auto-apply pipelines; emit pipeline JSON for manual apply.
- SQL validation is still implemented via temporary pipeline creation (`POST /pipelines`) until a
  dedicated `validate_sql` endpoint exists.

---

## 1) Goals and constraints

### Goals

- One workflow implementation shared by CLI and server.
- First-class **interrupt points** (need user input / confirm) with **checkpointable state**.
- **Resumable** execution using a `session_id`.
- **Observable** node-by-node traces (LLM calls + Manager calls + retries).

### Grounding constraints (must follow)

- Do not invent stream names, column names, types, or function names.
- Streams and schema are grounded by Manager introspection APIs:
  - `GET /streams`, `GET /streams/describe/:name`
- Functions are grounded by:
  - `GET /functions`
- Syntax/expr subset is grounded by:
  - `GET /capabilities/syntax`

---

## 2) Persistent Graph State (minimal schema)

Represent state as a single JSON-serializable object (dict or Pydantic model). Suggested fields:

- `session_id: str` (server-provided or server-generated; CLI can generate a UUID)
- `active_stream: str | null`
- `stream_schema: object | null` (from `GET /streams/describe/:name`)
- `catalog_digest: object` (compact digest of functions + syntax capabilities)
  - Optionally store `digest_hash` and keep the digest in a cache keyed by hash.
- `notes: list[str]` (short deterministic summaries, e.g. “available streams: …”)
- `history: list[message]` (OpenAI chat style, or “summary + last N messages”)
- `user_input: object`
  - Example: `{ "text": "...", "feedback": "...", "confirm": true }`
- `draft: object | null`
  - `preview_text: str | null`
  - `sql: str | null`
  - `assumptions: list[str]`
  - `questions: list[str]`
- `validation: object | null`
  - `tmp_pipeline_id: str | null`
  - `error: str | null`
  - `attempt: int`
  - `max_attempts: int`
- `explain: object | null`
  - `pretty: str | null`
  - `pipeline_request_json: object | null` (final payload, `id=REPLACE_ME`)
- `interrupt: object | null`
  - `type: "need_stream" | "need_clarification" | "need_confirm"`
  - `question: str`
  - `choices: object | null` (e.g. stream list)
- `metrics: object` (optional)
  - `llm_tokens`, `llm_latency_ms`, `manager_latency_ms`, `retries`, etc.

---

## 3) Nodes (responsibilities and side effects)

Each node should have a single, clear responsibility. Side effects are limited to LLM/Manager calls.

### A. `init_session`

**Side effects (Manager):**

- `GET /functions`
- `GET /capabilities/syntax`
- `GET /streams`

**Outputs:**

- `catalog_digest` populated
- `notes` appended with a short stream list summary

**Server optimization:**

- Cache function/syntax catalogs by TTL; still refresh stream list per session/turn.

### B. `route_intent`

**Side effects (LLM router model):** classify intent only.

**Inputs:**

- `user_input.text`
- `active_stream`
- list of available stream names (from Manager)

**Outputs:**

- `intent = list_streams | describe_stream | nl2sql | ask_user`
- optional `stream` and `question`

### C1. `handle_list_streams`

**Side effects (Manager):** `GET /streams`

**Outputs:**

- Return a list view (server) or print (CLI)
- Append a short summary note to `notes`/`history`

### C2. `handle_describe_stream`

**Side effects (Manager):** `GET /streams/describe/:name`

**Behavior:**

- If stream name missing: set `interrupt(type="need_stream")`.
- If success: update `active_stream`, `stream_schema`, append schema summary note.

### D. `ensure_active_stream` (for `nl2sql`)

**Behavior:**

- If `active_stream` is set: continue.
- Else if router provided `stream`: select it and load schema.
- Else: set `interrupt(type="need_stream", choices=[streams])`.

### E. `draft_preview_sql` (stream-friendly)

**Side effects (LLM preview model):**

- Produce *plain text* output:
  - either a single SQL statement; or
  - a single line `QUESTION: ...`

**Outputs:**

- `draft.preview_text`, and parsed `draft.sql` or `draft.questions`
- If question: set `interrupt(type="need_clarification")`

**Streaming:**

- Server can forward deltas via SSE/WebSocket.
- CLI prints deltas to stderr.

### F. `validate_sql_via_tmp_pipeline`

**Side effects (Manager):**

- `POST /pipelines` using a **temporary pipeline id**

**Outputs:**

- On success: `validation.tmp_pipeline_id` set; proceed to explain.
- On failure: `validation.error` set; route to repair.

**Id strategy (important for resume):**

- Prefer deterministic: `__nl2pipeline_tmp__{session_id}_{turn_id}`.
- This enables replay/resume and makes cleanup robust.

### G. `explain_tmp_pipeline`

**Side effects (Manager):**

- `GET /pipelines/:id/explain`

**Outputs:**

- `explain.pretty`
- `explain.pipeline_request_json` (sink config + `id=REPLACE_ME`)

### H. `cleanup_tmp_pipeline`

**Side effects (Manager):**

- `DELETE /pipelines/:id` (best-effort)

**Rationale:**

- Keep cleanup as its own node so recovery logic can always run it when a temp id is present.

### I. `interrupt_confirm`

**Behavior:**

- CLI asks `y/n` and collects feedback.
- Server returns `{sql, explain, pipeline_json, question}` and stops.

**Outputs:**

- If confirm: terminate the turn as “done”.
- If feedback: go to revise.

### J. `revise_sql_json` (structured repair loop)

**Side effects (LLM reasoning model):**

- JSON-mode output `{sql, questions, assumptions}`.

**Inputs:**

- previous SQL
- planner error message
- user feedback (if any)
- stream schema + digest constraints

**Outputs:**

- updated `draft.sql/questions/assumptions`
- increment `validation.attempt`, check `max_attempts`

---

## 4) Edges (control flow)

High-level:

- `init_session` → `route_intent`

Routing:

- `route_intent(list_streams)` → `handle_list_streams` → END
- `route_intent(describe_stream)` → `handle_describe_stream` → END (or interrupt)
- `route_intent(nl2sql)` → `ensure_active_stream` → `draft_preview_sql` → `validate_sql_via_tmp_pipeline`
- `route_intent(ask_user)` → `interrupt(need_clarification)` → END

Validation:

- `validate(success)` → `explain_tmp_pipeline` → `cleanup_tmp_pipeline` → `interrupt_confirm`
- `validate(fail)` → `revise_sql_json` → `validate_sql_via_tmp_pipeline` (until `max_attempts`)
- `attempts_exceeded` → `interrupt(need_clarification)` or `failed`

---

## 5) Checkpointing and resuming (server core)

### Checkpoint store

- SQLite/Postgres/Redis are all acceptable; key by `session_id`.
- Persist:
  - state payload
  - current node / graph position
  - timestamps

### When to checkpoint

At minimum:

- after any LLM call
- after `POST /pipelines` (temp pipeline id must be persisted)
- after `GET /explain`
- after `DELETE /pipelines` (cleanup outcome)

### Recovery rules

On resume:

- If `validation.tmp_pipeline_id` exists but we are not in a confirmed/done terminal state:
  - run `cleanup_tmp_pipeline` first (best-effort) to avoid leaking temp pipelines.

---

## 6) Observability (trace)

Record per-node execution:

- `node_name`
- `start_ts`, `end_ts`, `latency_ms`
- `ok / error`
- `attempt`
- LLM metadata: `model`, `temperature`, (tokens if available)
- Manager metadata: `method`, `path`, `status`

Server can expose a debug endpoint such as:

- `GET /sessions/:id/trace`

CLI can optionally display recent trace (e.g. `/trace`).

---

## 7) CLI and server integration pattern

### CLI driver

- Runs the graph synchronously.
- When hitting `interrupt`, collect input via terminal and continue under the same `session_id`.

### Server driver

- `POST /nl2pipeline/turn` advances the graph until `interrupt` or completion, then returns a view:
  - streams list, schema, SQL preview, explain, pipeline json, question, etc.
- Streaming channel (SSE/WebSocket) forwards deltas produced by `draft_preview_sql`.

---

## 8) Migration plan (small steps)

1) Keep the existing workflow as reference; define the LangGraph state schema and node boundaries.
2) Implement LangGraph version for the happy path (NL→SQL→validate→explain→cleanup→confirm).
3) Add interrupt/resume endpoints (server) and a checkpoint store.
4) Move CLI to run the same graph (optional), or keep CLI as a thin adapter over the graph engine.
5) Add full trace export and latency/token reporting.

---

## Decisions (confirmed)

1) **Session id ownership**: the **server** generates `session_id`.
2) **Temp pipeline id semantics**: on conflict, **generate a new temp pipeline id** and retry (do not rely on overwrite/409 semantics).
3) **History storage**: keep “**summary + last N messages**” only (token- and storage-friendly).
4) **Checkpoint store (v1)**: keep session state **in memory** first; design the workflow so a persistent store can be plugged in later.

