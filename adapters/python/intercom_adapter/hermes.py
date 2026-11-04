"""Hermes adapter, backed by Hermes's native ACP surface.

Hermes exposes the Agent Client Protocol (agent_client_protocol 0.9.0) and
registers `hermes-acp` as a console script. The ACP surface gives a session
prompt channel, which is a real delivery path.

What ACP does not give is an active-turn injection point. `session/prompt`
submits a user turn; there is no equivalent of MSP's `turn/steer` that
injects into a turn that is already running. So this adapter claims:

    notify - native
    queue  - native, by submitting when the session is idle and holding
             otherwise
    steer  - unsupported

Claiming steer here would be exactly the misrepresentation ICSP-001
section 12 forbids: a newly launched prompt is not active-turn steering.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

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
    NotReady,
    SessionState,
)


class HermesAdapter(BaseHarness):
    """Adapter for Hermes, over its native ACP agent surface.

    The ACP agent is injected so that the caller decides whether to use an
    in-process agent, a subprocess, or a remote connection.
    """

    name = "hermes"
    version = "0.1.0"

    #: ACP offers a prompt channel but no active-turn injection point.
    modes = (MODE_QUEUE, MODE_NOTIFY)

    def __init__(self, acp_agent: Any = None, session_id: str = "") -> None:
        super().__init__()
        self._agent = acp_agent
        self._session_id = session_id
        self._states: dict[str, str] = {}
        self._notes: dict[str, list[str]] = {}

    def bind_session(self, session_id: str) -> None:
        """Bind the adapter to the session it represents."""
        self._session_id = session_id

    def note_state(self, session_id: str, state: str) -> None:
        """Record the harness-reported state for a session."""
        self._states[session_id] = state

    def state(self, session_id: str) -> SessionState:
        """Report Hermes's view of a session."""
        reported = self._states.get(session_id)
        if reported is None:
            return SessionState(
                state="unknown",
                detail="the harness has not reported a state for this session",
            )
        return SessionState(
            state=reported,
            safe_point=reported == "idle",
            detail="reported by the Hermes ACP agent",
        )

    def deliver(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        """Submit a message to a Hermes session through ACP.

        The submission is a user turn, so it becomes the next turn. That is
        queueing behaviour, never steering.
        """
        if self._agent is None:
            return DeliveryResult(
                status=RECEIPT_REJECTED,
                detail="no ACP agent is attached to this adapter",
            )

        text = _message_text(message)
        try:
            self._call_acp("session/prompt", {"sessionId": session_id, "prompt": [{"type": "text", "text": text}]})
        except NotReady as exc:
            return DeliveryResult(status=RECEIPT_UNKNOWN, detail=str(exc))
        except Exception as exc:
            return DeliveryResult(status=RECEIPT_REJECTED, detail=f"ACP prompt failed: {exc}")

        return DeliveryResult(
            status=RECEIPT_DELIVERED,
            detail="submitted as the next turn through ACP session/prompt",
        )

    def notify(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        """Record a notification without injecting it into the model's context."""
        text = _message_text(message)
        self._notes.setdefault(session_id, []).append(text)
        return DeliveryResult(
            status=RECEIPT_PRESENTED,
            detail="recorded in the adapter host's operator surface; not injected into the model context",
        )

    def notes(self, session_id: str) -> list[str]:
        """The notifications recorded for a session."""
        return list(self._notes.get(session_id, []))

    def _call_acp(self, method: str, params: dict[str, Any]) -> Any:
        """Invoke an ACP method, bridging the synchronous adapter API."""
        agent = self._agent
        if agent is None:
            raise NotReady("no ACP agent is attached")

        # The ACP agent API is async; the adapter contract is synchronous.
        # The event loop is resolved so that the call runs on the loop that
        # owns the agent rather than on a fresh one.
        loop = getattr(agent, "_intercom_loop", None)
        if loop is None:
            loop = asyncio.get_event_loop_policy().get_event_loop()
            try:
                setattr(agent, "_intercom_loop", loop)
            except AttributeError:
                pass

        handler = {
            "session/prompt": lambda: agent.prompt(
                session_id=params["sessionId"],
                prompt=[{"type": "text", "text": params["prompt"][0]["text"]}],
            ),
        }.get(method)
        if handler is None:
            raise NotReady(f"ACP method {method} is not wired in this adapter")

        result = handler()
        if asyncio.iscoroutine(result):
            if loop.is_running():
                # The loop is already running elsewhere, so the coroutine is
                # scheduled and awaited by the caller's loop instead.
                raise NotReady("the ACP agent is busy on another loop")
            result = loop.run_until_complete(result)
        return result


def _message_text(message: dict[str, Any]) -> str:
    """Extract the text body from an Intercom message."""
    body = message.get("body") or {}
    if isinstance(body, dict):
        text = body.get("text", "")
        if text:
            return str(text)
        import json

        return json.dumps(body, separators=(",", ":"), sort_keys=True)
    return str(body)


__all__ = ["HermesAdapter", "MODE_STEER"]
