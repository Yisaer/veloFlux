from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Dict, List

from .state import State, TraceEntry


@dataclass
class InMemorySessionStore:
    """
    v1 store: in-memory only.

    The workflow is designed so a persistent store (SQLite/Redis/Postgres) can be plugged in later.
    """

    _lock: threading.Lock
    _sessions: Dict[str, State]

    @classmethod
    def new(cls) -> "InMemorySessionStore":
        return cls(_lock=threading.Lock(), _sessions={})

    def create(self, initial_state: State) -> str:
        session_id = uuid.uuid4().hex
        initial_state = dict(initial_state)
        initial_state["session_id"] = session_id
        with self._lock:
            self._sessions[session_id] = initial_state
        return session_id

    def get(self, session_id: str) -> State:
        with self._lock:
            if session_id not in self._sessions:
                raise KeyError(f"unknown session_id: {session_id}")
            return dict(self._sessions[session_id])

    def save(self, session_id: str, state: State) -> None:
        state = dict(state)
        state["session_id"] = session_id
        with self._lock:
            if session_id not in self._sessions:
                raise KeyError(f"unknown session_id: {session_id}")
            self._sessions[session_id] = state

    def append_trace(self, session_id: str, entry: TraceEntry) -> None:
        with self._lock:
            if session_id not in self._sessions:
                raise KeyError(f"unknown session_id: {session_id}")
            state = self._sessions[session_id]
            trace = state.get("trace") or []
            if not isinstance(trace, list):
                trace = []
            trace.append(entry)
            state["trace"] = trace  # type: ignore[assignment]

    def list_trace(self, session_id: str) -> List[TraceEntry]:
        with self._lock:
            if session_id not in self._sessions:
                raise KeyError(f"unknown session_id: {session_id}")
            trace = self._sessions[session_id].get("trace") or []
            return list(trace) if isinstance(trace, list) else []


__all__ = ["InMemorySessionStore"]
