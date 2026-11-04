"""Muse adapter, backed by Muse's native MSP surface.

Muse is the one locally installed harness that exposes a genuine native
active-turn injection point: MSP's `turn/steer` injects input into a named
turn and fails if that turn is no longer active. That is exactly the
semantics Intercom's STEER mode requires, so this adapter can claim native
steering honestly (ICSP-001 sections 5.2 and 12).

The MSP contract used here was taken from the schema that `muse schema
generate-json-schema` exports, which is the binary's own authority on its
wire format:

    turn/start   TurnStartParams  {commandId, sessionId, input[], ifBusy}
    turn/steer   TurnSteerParams  {commandId, sessionId, expectedTurnId, input[]}
    turn/unqueue TurnUnqueueParams {commandId, sessionId, turnId}

Intercom does not claim wire-level MSP compatibility. This adapter speaks
MSP to Muse and Intercom to the Go core; a separate bridge would be needed
for MSP conformance claims (ICSP-001 section 17).
"""

from __future__ import annotations

import json
from typing import Any, Optional

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
    NotReady,
    SessionState,
    UnsupportedOperation,
)

# MSP IfBusy dispositions.
IF_BUSY_QUEUE = "queue"
IF_BUSY_STEER = "steer"
IF_BUSY_REPLACE = "replace"


class MuseAdapter(BaseHarness):
    """Adapter for Muse Code, over its native MSP surface.

    The MSP client is injected rather than constructed here so that the
    transport (a `muse serve` subprocess over stdio, or an existing client)
    stays the caller's decision.
    """

    name = "muse"
    version = "0.1.0"

    #: Muse natively supports all three delivery modes. Steering maps onto
    #: MSP `turn/steer`, queueing onto `turn/start` with ifBusy=queue, and
    #: notification onto the session inbox.
    modes = (MODE_STEER, MODE_QUEUE, MODE_NOTIFY)

    def __init__(self, msp_client: Any, session_id: str = "") -> None:
        super().__init__()
        self._msp = msp_client
        self._session_id = session_id
        # The active turn per session, learned from turn notifications and
        # from turn/start and turn/steer results.
        self._turns: dict[str, str] = {}
        self._states: dict[str, str] = {}
        # Operator-visible notifications, keyed by session.
        self._notes: dict[str, list[str]] = {}

    # -- MSP plumbing -----------------------------------------------------

    def _msp_call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Invoke an MSP method and return its result object."""
        try:
            result = self._msp.call(method, params)
        except Exception as exc:  # the MSP client raises its own errors
            raise UnsupportedOperation(f"muse {method} failed: {exc}") from exc
        return result if isinstance(result, dict) else {}

    def bind_session(self, session_id: str) -> None:
        """Bind the adapter to the session it represents."""
        self._session_id = session_id

    def note_turn(self, session_id: str, turn_id: str) -> None:
        """Record the active turn for a session.

        The Go core learns about turns from MSP turn notifications relayed by
        the adapter host.
        """
        if turn_id:
            self._turns[session_id] = turn_id

    def note_state(self, session_id: str, state: str) -> None:
        """Record the harness-reported state for a session."""
        self._states[session_id] = state

    # -- adapter contract -------------------------------------------------

    def state(self, session_id: str) -> SessionState:
        """Report Muse's view of a session.

        The state comes from what Muse actually reported. A value inferred
        by the adapter is labelled as such, so the Go core can distinguish a
        native state from an inferred one.
        """
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
            detail="reported by the Muse session host",
        )

    def deliver(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        """Deliver a message to a Muse session.

        Steering uses MSP `turn/steer`, which fails when the named turn is no
        longer active. That failure is reported as a rejection rather than
        being retried as a new turn, because a new turn would be a
        semantically different operation (ICSP-001 section 12).
        """
        text = _message_text(message)
        mode = (message.get("delivery") or {}).get("mode", MODE_QUEUE)
        command_id = message.get("commandId") or message.get("command_id") or ""

        if mode == MODE_STEER:
            turn_id = self._turns.get(session_id, "")
            if not turn_id:
                return DeliveryResult(
                    status=RECEIPT_REJECTED,
                    detail=(
                        "no active turn is known for this session, so the message "
                        "cannot be steered into one"
                    ),
                )
            try:
                result = self._msp_call(
                    "turn/steer",
                    {
                        "commandId": command_id,
                        "sessionId": session_id,
                        "expectedTurnId": turn_id,
                        "input": [{"type": "text", "text": text}],
                    },
                )
            except UnsupportedOperation as exc:
                return DeliveryResult(status=RECEIPT_REJECTED, detail=str(exc))
            self.note_turn(session_id, result.get("turnId", turn_id))
            return DeliveryResult(
                status=RECEIPT_INJECTED,
                detail="injected into the active turn through MSP turn/steer",
            )

        # Queue: MSP turn/start with ifBusy=queue keeps the running turn
        # unaffected.
        try:
            result = self._msp_call(
                "turn/start",
                {
                    "commandId": command_id,
                    "sessionId": session_id,
                    "input": [{"type": "text", "text": text}],
                    "ifBusy": IF_BUSY_QUEUE,
                },
            )
        except UnsupportedOperation as exc:
            return DeliveryResult(status=RECEIPT_REJECTED, detail=str(exc))

        disposition = result.get("disposition", "")
        if disposition == IF_BUSY_QUEUE:
            return DeliveryResult(
                status=RECEIPT_DELIVERED,
                detail="queued through MSP turn/start with ifBusy=queue",
            )
        turn_id = result.get("turnId", "")
        if turn_id:
            self.note_turn(session_id, turn_id)
        return DeliveryResult(
            status=RECEIPT_DELIVERED,
            detail=f"submitted through MSP turn/start (disposition {disposition or 'unknown'})",
        )

    def notify(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        """Present a message without injecting it into the model's context.

        The MSP surface this adapter speaks has no notification-only method.
        `session/sideChat` is not a substitute: it opens a new side session
        and runs a model turn in it, which is a semantically different
        operation from a notification and would misrepresent the delivery
        (ICSP-001 section 12).

        So the notification is recorded in the adapter host, which is an
        operator-visible surface, and reported as "presented". It is never
        reported as "injected", because nothing was injected.
        """
        text = _message_text(message)
        self._notes.setdefault(session_id, []).append(text)
        return DeliveryResult(
            status=RECEIPT_PRESENTED,
            detail=(
                "recorded in the adapter host's operator surface; "
                "not injected into the model context"
            ),
        )

    def notes(self, session_id: str) -> list[str]:
        """The notifications recorded for a session."""
        return list(self._notes.get(session_id, []))


def _message_text(message: dict[str, Any]) -> str:
    """Extract the text body from an Intercom message."""
    body = message.get("body") or {}
    if isinstance(body, dict):
        text = body.get("text", "")
        if text:
            return str(text)
        # A structured body is rendered compactly rather than dropped.
        return json.dumps(body, separators=(",", ":"), sort_keys=True)
    return str(body)


__all__ = ["MuseAdapter", "NotReady"]
