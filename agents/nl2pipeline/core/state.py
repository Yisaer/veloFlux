from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, TypedDict


Message = Dict[str, str]  # {"role": "system"|"user"|"assistant", "content": "..."}


InterruptType = Literal["need_stream", "need_clarification", "need_confirm"]


class Interrupt(TypedDict, total=False):
    type: InterruptType
    question: str
    choices: Any


class DraftState(TypedDict, total=False):
    preview_text: Optional[str]
    sql: Optional[str]
    assumptions: List[str]
    questions: List[str]


class ValidationState(TypedDict, total=False):
    tmp_pipeline_id: Optional[str]
    error: Optional[str]
    attempt: int
    max_attempts: int


class ExplainState(TypedDict, total=False):
    pretty: Optional[str]
    pipeline_request_json: Optional[Dict[str, Any]]


class HistoryState(TypedDict, total=False):
    summary: str
    tail: List[Message]
    max_tail: int


class TraceEntry(TypedDict, total=False):
    node: str
    ok: bool
    error: Optional[str]
    latency_ms: int


class State(TypedDict, total=False):
    session_id: str
    initialized: bool

    active_stream: Optional[str]
    stream_schema: Optional[Dict[str, Any]]
    available_streams: List[Dict[str, Any]]

    catalog_digest: Dict[str, Any]

    notes: List[str]
    history: HistoryState

    user_input: Dict[str, Any]

    last_requirement: Optional[str]
    pending_action: Optional[str]
    confirmed: bool
    intent: Optional[str]
    intent_stream: Optional[str]
    intent_question: Optional[str]

    draft: DraftState
    validation: ValidationState
    explain: ExplainState

    interrupt: Optional[Interrupt]
    trace: List[TraceEntry]


@dataclass(frozen=True)
class TurnRequest:
    text: str = ""
    confirm: Optional[bool] = None
    feedback: Optional[str] = None
    stream: bool = False


@dataclass(frozen=True)
class TurnResponse:
    session_id: str
    done: bool
    interrupt: Optional[Interrupt]
    intent: Optional[str]
    # Convenience fields for clients.
    sql: Optional[str]
    explain_pretty: Optional[str]
    pipeline_request_json: Optional[Dict[str, Any]]
    streams: Optional[List[Dict[str, Any]]]
    schema: Optional[Dict[str, Any]]
    trace: List[TraceEntry]


__all__ = [
    "ExplainState",
    "HistoryState",
    "Interrupt",
    "InterruptType",
    "Message",
    "State",
    "TraceEntry",
    "TurnRequest",
    "TurnResponse",
]
