"""Codex adapter, backed by Codex's native app-server surface.

Codex app-server exposes both delivery channels Intercom needs:

    queue  - `turn/start` submits a user turn to a thread
    steer  - `turn/steer` {threadId, expectedTurnId, input} injects
             into the named turn and fails when it does not match the
             currently active turn. The schema's own words: "Required
             active turn id precondition. The request fails when it
             does not match the currently active turn." That is the
             same failure semantics as MSP `turn/steer`, so this
             adapter claims native steering for Codex — subject to the
             live verification recorded in docs/HARNESSES.md, which
             must show a steer accepted during a running turn and
             rejected after it ends.

A steer failure is reported as a rejection and is never retried as a
new turn: a new turn is a semantically different operation (ICSP-001
section 12).

Thread identity mapping mirrors the Muse adapter: Intercom session
ids and Codex thread ids live in different namespaces, so
`thread_starter` creates a thread for an Intercom session on first
use and the adapter caches the mapping. Without a starter the
Intercom id is used as the thread id (the unit-test path).
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from .harness import (
    MODE_NOTIFY,
    MODE_QUEUE,
    MODE_STEER,
    RECEIPT_DELIVERED,
    RECEIPT_INJECTED,
    RECEIPT_PRESENTED,
    RECEIPT_REJECTED,
    RECEIPT_UNKNOWN,
    BaseHarness,
    DeliveryResult,
    SessionState,
    UnsupportedOperation,
)


class CodexAdapter(BaseHarness):
    """Adapter for Codex, over its app-server surface."""

    name = "codex"
    version = "0.2.0"

    #: Codex natively supports queueing and active-turn steering.
    modes = (MODE_STEER, MODE_QUEUE, MODE_NOTIFY)
    live_verified_modes = (MODE_STEER,)

    def __init__(
        self,
        codex_client: Any = None,
        session_id: str = "",
        thread_starter: Optional[Callable[[str], str]] = None,
    ) -> None:
        super().__init__()
        self._codex = codex_client
        self._session_id = session_id
        self._thread_starter = thread_starter
        self._threads: dict[str, str] = {}
        self._turns: dict[str, str] = {}
        self._states: dict[str, str] = {}
        self._intercom_by_thread: dict[str, str] = {}
        self._notes: dict[str, list[str]] = {}
        on_notification = getattr(codex_client, "on_notification", None)
        if callable(on_notification):
            on_notification(self._handle_codex_notification)

    # -- thread identity mapping --------------------------------------------

    def thread(self, session_id: str) -> str:
        """Return the Codex thread id for an Intercom session id."""
        mapped = self._threads.get(session_id) or self._lookup_mapping(session_id)
        if mapped:
            self._threads[session_id] = mapped
            self._intercom_by_thread[mapped] = session_id
            return mapped
        if self._thread_starter is None:
            return session_id
        try:
            thread_id = self._thread_starter(session_id)
        except Exception as exc:
            raise UnsupportedOperation(f"codex thread/start failed: {exc}") from exc
        if not thread_id:
            raise UnsupportedOperation("codex thread/start returned no thread id")
        self._threads[session_id] = thread_id
        self._intercom_by_thread[thread_id] = session_id
        self._persist_mapping(session_id, thread_id)
        return thread_id

    def bind_session(self, session_id: str) -> None:
        self._session_id = session_id

    def start(self) -> None:
        start = getattr(self._codex, "start", None)
        if callable(start):
            start()

    def close(self) -> None:
        close = getattr(self._codex, "close", None)
        if callable(close):
            close()

    def note_turn(self, session_id: str, turn_id: str) -> None:
        if turn_id:
            self._turns[session_id] = turn_id

    def note_state(self, session_id: str, state: str) -> None:
        self._states[session_id] = state

    def _handle_codex_notification(self, method: str, params: Any) -> None:
        if not isinstance(params, dict):
            return
        thread_id = str(params.get("threadId") or "")
        session_id = self._intercom_by_thread.get(thread_id, thread_id)
        if method == "turn/started":
            turn = params.get("turn") or {}
            turn_id = str(turn.get("id") or params.get("turnId") or "")
            if turn_id:
                self.note_turn(session_id, turn_id)
            self.note_state(session_id, "running")
        elif method == "turn/completed":
            self._turns.pop(session_id, None)
            self.note_state(session_id, "idle")
        elif method == "thread/started":
            self.note_state(session_id, "idle")

    # -- adapter contract ---------------------------------------------------

    def state(self, session_id: str) -> SessionState:
        reported = self._states.get(session_id)
        turn_id = self._turns.get(session_id, "")
        if reported is None:
            return SessionState(
                state="unknown",
                turn_id=turn_id,
                safe_point=False,
                detail="the harness has not reported a state for this session",
            )
        return SessionState(
            state=reported,
            turn_id=turn_id,
            safe_point=bool(turn_id),
            detail="reported by the Codex app server",
        )

    def deliver(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        if self._codex is None:
            return DeliveryResult(
                status=RECEIPT_REJECTED,
                detail="no Codex app-server client is attached to this adapter",
            )
        text = _message_text(message)
        mode = (message.get("delivery") or {}).get("mode", MODE_QUEUE)
        try:
            thread_id = self.thread(session_id)
        except UnsupportedOperation as exc:
            return DeliveryResult(status=RECEIPT_REJECTED, detail=str(exc))

        if mode == MODE_STEER:
            turn_id = self._turns.get(session_id, "")
            if not turn_id:
                return DeliveryResult(
                    status=RECEIPT_REJECTED,
                    detail="no active turn is known for this session, so the message cannot be steered into one",
                )
            try:
                result = self._codex.call(
                    "turn/steer",
                    {
                        "threadId": thread_id,
                        "expectedTurnId": turn_id,
                        "input": [{"type": "text", "text": text}],
                    },
                )
            except Exception as exc:
                return DeliveryResult(status=RECEIPT_REJECTED, detail=f"codex turn/steer failed: {exc}")
            new_turn = (result or {}).get("turn") or {}
            if isinstance(new_turn, dict) and new_turn.get("id"):
                self.note_turn(session_id, str(new_turn["id"]))
            return DeliveryResult(
                status=RECEIPT_INJECTED,
                detail="injected into the active turn through Codex app-server turn/steer",
            )

        try:
            result = self._codex.call(
                "turn/start",
                {"threadId": thread_id, "input": [{"type": "text", "text": text}]},
            )
        except Exception as exc:
            return DeliveryResult(status=RECEIPT_REJECTED, detail=f"codex turn/start failed: {exc}")
        turn = (result or {}).get("turn") or {}
        if isinstance(turn, dict) and turn.get("id"):
            self.note_turn(session_id, str(turn["id"]))
            self.note_state(session_id, "running")
        return DeliveryResult(
            status=RECEIPT_DELIVERED,
            detail="submitted as a turn through Codex app-server turn/start",
        )

    def notify(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        text = _message_text(message)
        if not self._notify_durable(session_id, text, message):
            return DeliveryResult(
                status=RECEIPT_UNKNOWN,
                detail="notification could not be durably persisted; not presented",
            )
        return DeliveryResult(
            status=RECEIPT_PRESENTED,
            detail="recorded in the adapter host's operator surface; not injected into the model context",
        )

    def notes(self, session_id: str) -> list[str]:
        return list(self._notes.get(session_id, []))


def _message_text(message: dict[str, Any]) -> str:
    """Extract labeled peer text (provenance enforced)."""
    from .harness import render_peer_text

    return render_peer_text(message)


__all__ = ["CodexAdapter"]
