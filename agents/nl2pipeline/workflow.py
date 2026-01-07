from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from .chat_client import ChatCompletionsClient, LlmError
from .catalogs import CapabilitiesDigest
from .manager_client import ApiError, ManagerClient


class Phase(str, Enum):
    DraftSql = "draft_sql"
    ValidatePipeline = "validate_pipeline"
    ExplainPipeline = "explain_pipeline"
    Done = "done"
    Failed = "failed"


@dataclass(frozen=True)
class TurnContext:
    active_stream: str
    stream_schema: Dict[str, Any]


@dataclass(frozen=True)
class Candidate:
    sql: str
    questions: List[str]
    assumptions: List[str]


@dataclass(frozen=True)
class PipelineCandidate:
    sql: str
    explain_pretty: str
    create_pipeline_request: Dict[str, Any]
    assumptions: List[str]


@dataclass(frozen=True)
class TurnInput:
    prompt: str
    max_attempts: int
    previous_sql: Optional[str] = None


class EventKind(str, Enum):
    PhaseChanged = "phase_changed"
    CandidateGenerated = "candidate_generated"
    PlanningFailed = "planning_failed"
    Explained = "explained"
    NeedUserInput = "need_user_input"
    Failed = "failed"


@dataclass(frozen=True)
class WorkflowEvent:
    kind: EventKind
    phase: Optional[Phase] = None
    attempt: Optional[int] = None
    candidate: Optional[Candidate] = None
    error: Optional[str] = None
    result: Optional[PipelineCandidate] = None


def default_assistant_instructions() -> str:
    return (
        "You convert user requirements into SynapseFlow-valid SQL.\n"
        "Grounding rules:\n"
        "- Do not invent stream names, column names, column types, or function names.\n"
        "- Use only functions present in the provided function catalog.\n"
        "- Use only syntax constructs and expression operators marked supported/partial in the provided syntax catalog.\n"
        "Output rules:\n"
        "- Return ONLY valid JSON with keys: sql, questions, assumptions.\n"
        "- If required information is missing, put a question in questions[] instead of guessing.\n"
    )


def build_create_stream_request(
    name: str,
    broker_url: str,
    topic: str,
    qos: int,
    columns: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "name": name,
        "type": "mqtt",
        "props": {"broker_url": broker_url, "topic": topic, "qos": qos},
        "schema": {"type": "json", "props": {"columns": columns}},
        "decoder": {"type": "json", "props": {}},
        "shared": False,
    }


def build_create_pipeline_request(
    pipeline_id: str,
    sql: str,
    sink_broker_url: str,
    sink_topic: str,
    sink_qos: int,
) -> Dict[str, Any]:
    return {
        "id": pipeline_id,
        "sql": sql,
        "sinks": [
            {
                "type": "mqtt",
                "props": {
                    "broker_url": sink_broker_url,
                    "topic": sink_topic,
                    "qos": sink_qos,
                    "retain": False,
                },
                "encoder": {"type": "json", "props": {}},
            }
        ],
        "options": {"plan_cache": {"enabled": False}, "eventtime": {"enabled": False}},
    }


def _candidate_from_json(obj: Dict[str, Any]) -> Candidate:
    sql = str(obj.get("sql", "")).strip()
    if not sql:
        raise LlmError(f"LLM response missing sql: {obj}")
    questions = obj.get("questions", []) or []
    assumptions = obj.get("assumptions", []) or []
    return Candidate(
        sql=sql,
        questions=[str(x) for x in questions],
        assumptions=[str(x) for x in assumptions],
    )


class Workflow:
    """
    The core NL→SQL→(planning validate)→explain loop.

    This module intentionally avoids REPL/printing so the state machine remains readable and testable.
    """

    def __init__(
        self,
        manager: ManagerClient,
        llm: ChatCompletionsClient,
        llm_model: str,
        digest: CapabilitiesDigest,
        sink_broker_url: str,
        sink_topic: str,
        sink_qos: int,
        max_history_messages: int = 30,
        llm_json_mode: bool = True,
        llm_stream: bool = False,
    ) -> None:
        self.manager = manager
        self.llm = llm
        self.llm_model = llm_model
        self.digest = digest
        self.sink_broker_url = sink_broker_url
        self.sink_topic = sink_topic
        self.sink_qos = sink_qos
        self.max_history_messages = max_history_messages
        self.llm_json_mode = llm_json_mode
        self.llm_stream = llm_stream

        self._messages: List[Dict[str, str]] = []
        self._seeded = False

    def seed_session(self) -> None:
        if self._seeded:
            return
        self._messages = [
            {"role": "system", "content": default_assistant_instructions()},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "type": "context",
                        "capabilities_digest": self.digest.to_json(),
                    },
                    ensure_ascii=False,
                ),
            },
        ]
        self._seeded = True

    def update_active_stream(self, ctx: TurnContext) -> None:
        self._append_user_json(
            {
                "type": "active_stream_update",
                "active_stream": ctx.active_stream,
                "stream_schema": ctx.stream_schema,
            }
        )

    def _append_user_json(self, payload: Dict[str, Any]) -> None:
        self._messages.append(
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
        )
        self._trim_history()

    def add_context_note(self, note: str) -> None:
        """
        Add a short, user-visible context note into the chat history.

        Use this for deterministic meta-query results (e.g. list streams) so later turns
        can reference them without needing to re-ask or re-fetch.
        """
        if not note.strip():
            return
        self._append_user_json({"type": "context_note", "note": note.strip()})

    def _append_assistant_text(self, text: str) -> None:
        self._messages.append({"role": "assistant", "content": text})
        self._trim_history()

    def _trim_history(self) -> None:
        if self.max_history_messages <= 0:
            return
        if len(self._messages) <= self.max_history_messages:
            return
        # Keep system message and most recent messages.
        system = self._messages[0:1]
        tail = self._messages[-(self.max_history_messages - 1) :]
        self._messages = system + tail

    def run_turn(self, ctx: TurnContext, turn: TurnInput) -> Iterator[WorkflowEvent]:
        if not self._seeded:
            self.seed_session()

        previous_error: Optional[str] = None
        previous_sql = turn.previous_sql
        explain_summary: Optional[str] = None

        for attempt in range(1, max(1, turn.max_attempts) + 1):
            yield WorkflowEvent(kind=EventKind.PhaseChanged, phase=Phase.DraftSql, attempt=attempt)

            llm_payload = {
                "nl": turn.prompt,
                "active_stream": ctx.active_stream,
                "stream_schema": ctx.stream_schema,
                "previous_sql": previous_sql,
                "previous_error": previous_error,
                "explain_summary": explain_summary,
                "output_schema": {
                    "sql": "string (required)",
                    "questions": "string[] (optional)",
                    "assumptions": "string[] (optional)",
                },
            }
            self._append_user_json(llm_payload)
            try:
                progress_emitted = False

                def _on_delta(_piece: str) -> None:
                    nonlocal progress_emitted
                    if not progress_emitted:
                        progress_emitted = True
                    # Show minimal progress indicator, not raw JSON.
                    print(".", end="", file=sys.stderr, flush=True)

                parsed = self.llm.complete_json(
                    model=self.llm_model,
                    messages=self._messages,
                    temperature=0.0,
                    response_format_json=self.llm_json_mode,
                    stream=self.llm_stream,
                    on_stream_delta=_on_delta if self.llm_stream else None,
                )
                if self.llm_stream and progress_emitted:
                    print("", file=sys.stderr)
            except LlmError as e:
                yield WorkflowEvent(kind=EventKind.Failed, phase=Phase.Failed, error=str(e))
                return
            # Keep raw output in history (best-effort) for multi-turn consistency.
            self._append_assistant_text(json.dumps(parsed, ensure_ascii=False))
            candidate = _candidate_from_json(parsed)

            yield WorkflowEvent(
                kind=EventKind.CandidateGenerated,
                phase=Phase.DraftSql,
                attempt=attempt,
                candidate=candidate,
            )

            if candidate.questions:
                yield WorkflowEvent(
                    kind=EventKind.NeedUserInput,
                    phase=Phase.DraftSql,
                    attempt=attempt,
                    candidate=candidate,
                )
                return

            yield WorkflowEvent(
                kind=EventKind.PhaseChanged, phase=Phase.ValidatePipeline, attempt=attempt
            )

            temp_id = f"__nl2pipeline_tmp__{int(time.time())}"
            pipeline_req = build_create_pipeline_request(
                pipeline_id=temp_id,
                sql=candidate.sql,
                sink_broker_url=self.sink_broker_url,
                sink_topic=self.sink_topic,
                sink_qos=self.sink_qos,
            )
            try:
                self.manager.create_pipeline(pipeline_req)
            except ApiError as e:
                previous_error = str(e)
                previous_sql = candidate.sql
                yield WorkflowEvent(
                    kind=EventKind.PlanningFailed,
                    phase=Phase.ValidatePipeline,
                    attempt=attempt,
                    candidate=candidate,
                    error=previous_error,
                )
                continue

            yield WorkflowEvent(
                kind=EventKind.PhaseChanged, phase=Phase.ExplainPipeline, attempt=attempt
            )

            try:
                explain = self.manager.explain_pipeline(temp_id)
            finally:
                try:
                    self.manager.delete_pipeline(temp_id)
                except ApiError:
                    pass

            result = PipelineCandidate(
                sql=candidate.sql,
                explain_pretty=explain,
                create_pipeline_request=build_create_pipeline_request(
                    pipeline_id="REPLACE_ME",
                    sql=candidate.sql,
                    sink_broker_url=self.sink_broker_url,
                    sink_topic=self.sink_topic,
                    sink_qos=self.sink_qos,
                ),
                assumptions=candidate.assumptions,
            )
            yield WorkflowEvent(
                kind=EventKind.Explained,
                phase=Phase.ExplainPipeline,
                attempt=attempt,
                result=result,
            )
            yield WorkflowEvent(kind=EventKind.PhaseChanged, phase=Phase.Done, attempt=attempt)
            return

        yield WorkflowEvent(kind=EventKind.Failed, phase=Phase.Failed, error=previous_error)
