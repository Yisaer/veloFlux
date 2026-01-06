from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .manager_client import ApiError, ManagerClient
from .workflow import EventKind, PipelineCandidate, TurnContext, TurnInput, Workflow


@dataclass
class ReplState:
    active_stream: str
    stream_schema: Dict[str, Any]
    last_sql: Optional[str] = None
    last_explain: Optional[str] = None
    last_pipeline_json: Optional[Dict[str, Any]] = None


def _print_help() -> None:
    print(
        "\n".join(
            [
                "Commands:",
                "  /help",
                "  /streams",
                "  /use <stream_name>",
                "  /show sql",
                "  /show explain",
                "  /show pipeline",
                "  /exit",
                "",
                "Otherwise, type a natural-language requirement and press Enter.",
            ]
        )
    )


def _print_streams(streams: List[Dict[str, Any]]) -> None:
    for s in streams:
        print(str(s.get("name", "")))


def _select_stream_interactively(streams: List[Dict[str, Any]]) -> str:
    print("Available streams:", file=sys.stderr)
    for idx, s in enumerate(streams, start=1):
        name = s.get("name", "")
        cols = (((s.get("schema") or {}).get("columns")) or []) if isinstance(s, dict) else []
        sample_cols = [c.get("name", "") for c in cols[:5] if isinstance(c, dict)]
        suffix = f" cols={len(cols)} sample={','.join([x for x in sample_cols if x])}" if cols else ""
        print(f"  {idx}) {name}{suffix}", file=sys.stderr)

    while True:
        raw = input("Select stream by number or name: ").strip()
        if not raw:
            continue
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(streams):
                return str(streams[idx - 1].get("name", "")).strip()
        for s in streams:
            if str(s.get("name", "")).strip() == raw:
                return raw
        print("Invalid selection.", file=sys.stderr)


def _get_stream_schema(manager: ManagerClient, stream: str) -> Dict[str, Any]:
    desc = manager.describe_stream(stream)
    return (desc.get("spec") or {}).get("schema") or {}


def _render_result(result: PipelineCandidate) -> None:
    print("\nSQL:\n" + result.sql)
    if result.assumptions:
        print("\nAssumptions:")
        for a in result.assumptions:
            print(f"- {a}")
    print("\nEXPLAIN:\n" + result.explain_pretty)
    print(
        "\nPipeline JSON (manual apply):\n"
        + json.dumps(result.create_pipeline_request, indent=2, ensure_ascii=False)
    )


def run_repl(
    manager: ManagerClient,
    workflow: Workflow,
    initial_stream_name: str,
    check_max_attempts: int,
) -> int:
    streams = manager.list_streams()
    if not streams:
        raise ApiError("no streams found (create one first via Manager)")

    active_stream = initial_stream_name.strip()
    if not active_stream:
        if len(streams) == 1:
            active_stream = str(streams[0].get("name", "")).strip()
        else:
            active_stream = _select_stream_interactively(streams)

    schema = _get_stream_schema(manager, active_stream)
    state = ReplState(active_stream=active_stream, stream_schema=schema)
    workflow.seed_session(TurnContext(active_stream=state.active_stream, stream_schema=state.stream_schema))

    print("Type /help for commands. Enter requirements in plain text.", file=sys.stderr)

    while True:
        try:
            line = input("nl2pipeline> ").strip()
        except EOFError:
            print("", file=sys.stderr)
            break
        if not line:
            continue

        if line == "/exit":
            break
        if line == "/help":
            _print_help()
            continue
        if line == "/streams":
            _print_streams(manager.list_streams())
            continue
        if line.startswith("/use "):
            name = line[len("/use ") :].strip()
            streams = manager.list_streams()
            if not any(s.get("name") == name for s in streams):
                print(f"stream not found: {name}", file=sys.stderr)
                continue
            state.active_stream = name
            state.stream_schema = _get_stream_schema(manager, name)
            workflow.update_active_stream(
                TurnContext(active_stream=state.active_stream, stream_schema=state.stream_schema)
            )
            print(f"active stream set to {state.active_stream}", file=sys.stderr)
            continue
        if line == "/show sql":
            print(state.last_sql or "")
            continue
        if line == "/show explain":
            print(state.last_explain or "")
            continue
        if line == "/show pipeline":
            print(json.dumps(state.last_pipeline_json or {}, indent=2, ensure_ascii=False))
            continue

        prompt = line
        ctx = TurnContext(active_stream=state.active_stream, stream_schema=state.stream_schema)
        turn = TurnInput(prompt=prompt, max_attempts=check_max_attempts, previous_sql=state.last_sql)

        last_result: Optional[PipelineCandidate] = None
        needs_user_input: Optional[List[str]] = None
        final_error: Optional[str] = None

        for ev in workflow.run_turn(ctx, turn):
            if ev.kind == EventKind.NeedUserInput and ev.candidate is not None:
                needs_user_input = ev.candidate.questions
            if ev.kind == EventKind.Explained and ev.result is not None:
                last_result = ev.result
            if ev.kind == EventKind.Failed:
                final_error = ev.error

        if needs_user_input:
            print("Questions:", file=sys.stderr)
            for q in needs_user_input:
                print(f"- {q}", file=sys.stderr)
            continue

        if final_error and not last_result:
            print(f"Failed to produce a valid pipeline: {final_error}", file=sys.stderr)
            continue

        if not last_result:
            print("No result.", file=sys.stderr)
            continue

        _render_result(last_result)
        state.last_sql = last_result.sql
        state.last_explain = last_result.explain_pretty
        state.last_pipeline_json = last_result.create_pipeline_request

        feedback = input("\nIs this correct? (y=done / n=revise): ").strip().lower()
        if feedback in ("y", "yes"):
            continue

        revision_text = input("What should be changed? ").strip()
        if revision_text:
            state.last_sql = state.last_sql
            state.last_explain = state.last_explain
            revised_prompt = f"{prompt}\nUser feedback: {revision_text}"
            turn = TurnInput(
                prompt=revised_prompt,
                max_attempts=check_max_attempts,
                previous_sql=state.last_sql,
            )
            for ev in workflow.run_turn(ctx, turn):
                if ev.kind == EventKind.Explained and ev.result is not None:
                    last_result = ev.result
                if ev.kind == EventKind.Failed:
                    final_error = ev.error
            if last_result:
                _render_result(last_result)
                state.last_sql = last_result.sql
                state.last_explain = last_result.explain_pretty
                state.last_pipeline_json = last_result.create_pipeline_request

    return 0
