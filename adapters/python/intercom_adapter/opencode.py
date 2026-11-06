"""OpenCode adapter, backed by OpenCode's native ACP surface.

`opencode acp` serves the Agent Client Protocol (protocolVersion 1):
initialize, session/new, session/prompt, and session/update
notifications. That was verified against OpenCode 2.2.5 with the raw
client in acp_client.py (initialize and session/new need no inference;
session/prompt runs a real model turn).

What ACP gives this adapter, and what it does not:

    notify - native, via the adapter host's operator surface (presented)
    queue  - native: session/prompt submits the next user turn
    steer  - unsupported: ACP has no active-turn injection point

Claiming steer would be exactly the misrepresentation ICSP-001 section
12 forbids: a newly submitted prompt is not active-turn steering, so a
steer request is refused by the adapter host before it reaches this
adapter, and this adapter never submits one as a steer.

OpenCode also consumes MCP servers (its `mcp` command and
`opencode.json` config, or per-session `mcpServers` in ACP
session/new). For an OpenCode session that is given intercom-mcp that
way, the agent itself discovers peers and sends messages through the
MCP tools; this adapter is the complementary path for delivering a
message *into* a running OpenCode session from Intercom.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from .harness import (
    MODE_NOTIFY,
    MODE_QUEUE,
    MODE_STEER,
    RECEIPT_DELIVERED,
    RECEIPT_PRESENTED,
    RECEIPT_REJECTED,
    RECEIPT_UNKNOWN,
    BaseHarness,
    DeliveryResult,
    SessionState,
    UnsupportedOperation,
)


class OpenCodeAdapter(BaseHarness):
    """Adapter for OpenCode, over its ACP agent surface.

    The ACP client is injected so the caller decides the transport (an
    `opencode acp` subprocess, or a test fake). `session_starter`
    creates an ACP session for an Intercom session on first use; its
    argument is the Intercom session id and its return value is the
    ACP session id, because the two id namespaces are unrelated.
    """

    name = "opencode"
    version = "0.2.0"

    #: ACP offers a prompt channel but no active-turn injection point.
    modes = (MODE_QUEUE, MODE_NOTIFY)

    def __init__(
        self,
        acp_client: Any = None,
        session_id: str = "",
        session_starter: Optional[Callable[[str], str]] = None,
        cwd: str = "",
    ) -> None:
        super().__init__()
        self._acp = acp_client
        self._session_id = session_id
        self._session_starter = session_starter
        self._cwd = cwd
        self._acp_sessions: dict[str, str] = {}
        self._states: dict[str, str] = {}
        self._notes: dict[str, list[str]] = {}
        on_notification = getattr(acp_client, "on_notification", None)
        if callable(on_notification):
            on_notification(self._handle_acp_notification)

    # -- session identity mapping -------------------------------------------

    def acp_session(self, session_id: str) -> str:
        """Return the ACP session id for an Intercom session id."""
        mapped = self._acp_sessions.get(session_id) or self._lookup_mapping(session_id)
        if mapped:
            self._acp_sessions[session_id] = mapped
            return mapped
        if self._session_starter is None:
            return session_id
        try:
            acp_session_id = self._session_starter(session_id)
        except Exception as exc:
            raise UnsupportedOperation(f"opencode session/new failed: {exc}") from exc
        if not acp_session_id:
            raise UnsupportedOperation("opencode session/new returned no session id")
        self._acp_sessions[session_id] = acp_session_id
        self._persist_mapping(session_id, acp_session_id)
        return acp_session_id

    def bind_session(self, session_id: str) -> None:
        self._session_id = session_id

    def start(self) -> None:
        start = getattr(self._acp, "start", None)
        if callable(start):
            start()

    def close(self) -> None:
        close = getattr(self._acp, "close", None)
        if callable(close):
            close()

    def note_state(self, session_id: str, state: str) -> None:
        self._states[session_id] = state

    def _handle_acp_notification(self, method: str, params: Any) -> None:
        if not isinstance(params, dict) or method != "session/update":
            return
        # ACP session/update carries the ACP session id; reverse-map it
        # to the Intercom session id the adapter host polls for.
        acp_session_id = str(params.get("sessionId") or "")
        session_id = next(
            (k for k, v in self._acp_sessions.items() if v == acp_session_id),
            acp_session_id,
        )
        update = params.get("update") or {}
        kind = str(update.get("sessionUpdate") or "")
        if kind:
            # Any update means the agent is producing output: running.
            # The terminal prompt result flips this back to idle in
            # deliver(), which owns the blocking call.
            self._states[session_id] = "running"

    # -- adapter contract ---------------------------------------------------

    def state(self, session_id: str) -> SessionState:
        reported = self._states.get(session_id)
        if reported is None:
            return SessionState(
                state="unknown",
                detail="the harness has not reported a state for this session",
            )
        return SessionState(
            state=reported,
            safe_point=reported == "idle",
            detail="reported by the OpenCode ACP agent",
        )

    def deliver(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        """Submit a message as the next turn through ACP session/prompt."""
        if self._acp is None:
            return DeliveryResult(
                status=RECEIPT_REJECTED,
                detail="no ACP client is attached to this adapter",
            )
        text = _message_text(message)
        try:
            acp_session_id = self.acp_session(session_id)
        except UnsupportedOperation as exc:
            return DeliveryResult(status=RECEIPT_REJECTED, detail=str(exc))
        self._states[session_id] = "running"
        try:
            self._acp.prompt(acp_session_id, text)
        except Exception as exc:
            self._states[session_id] = "idle"
            return DeliveryResult(status=RECEIPT_REJECTED, detail=f"ACP prompt failed: {exc}")
        self._states[session_id] = "idle"
        return DeliveryResult(
            status=RECEIPT_DELIVERED,
            detail="submitted as the next turn through ACP session/prompt",
        )

    def notify(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        """Record a notification without injecting it into the model's context."""
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


__all__ = ["OpenCodeAdapter", "MODE_STEER", "RECEIPT_UNKNOWN"]
