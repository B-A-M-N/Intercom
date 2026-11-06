"""A scripted MCP consumer: an MCP client for intercom-mcp, plus the
harness adapter that mirrors the Go server's honest capability set.

This exists for one reason: an MCP-consuming agent (Claude, Codex,
OpenCode, Muse) participates through the MCP tools during its own
turns, and there has to be a way to exercise that exact path —
peers_list, message_send, inbox_list, message_ack, message_reply,
message_status — without a model in the loop. This consumer is that
path, driven by a script instead of a model. It proves the MCP
surface a real agent would use, including the receipt trail: claim
via inbox_list (presented), then message_ack with handled.

It is NOT a delivery adapter for a live agent. A live MCP agent must
not also run a Python adapter host polling the same inbox: two
claimants would race for the same lease. Live agents participate via
MCP alone; this consumer is for verification and for scripted
participants.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from .harness import (
    MODE_NOTIFY,
    MODE_QUEUE,
    MODE_STEER,
    RECEIPT_DELIVERED,
    RECEIPT_PRESENTED,
    RECEIPT_REJECTED,
    BaseHarness,
    DeliveryResult,
    SessionState,
)
from .ids import harness_command_id
from .stdio_rpc import RPCError, StdioRPCClient


class MCPError(RuntimeError):
    """An MCP call failed."""


class MCPClient:
    """Minimal MCP stdio client for intercom-mcp (tools only)."""

    def __init__(self, argv: list[str], env: Optional[dict[str, str]] = None, cwd: Optional[str] = None) -> None:
        self._rpc = StdioRPCClient(argv, env=env, cwd=cwd)
        self._started = False
        self.server_info: dict[str, Any] = {}
        self.protocol_version = ""

    def start(self) -> None:
        if self._started:
            return
        self._rpc.start()
        result = self._call(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "intercom_scripted_consumer", "version": "0.2.0"},
            },
        )
        if not isinstance(result, dict):
            raise MCPError(f"unexpected initialize result: {result!r}")
        self.server_info = result.get("serverInfo") or {}
        self.protocol_version = str(result.get("protocolVersion") or "")
        self._rpc.notify("notifications/initialized", {})
        self._started = True

    def close(self) -> None:
        self._rpc.close()
        self._started = False

    def __enter__(self) -> "MCPClient":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _call(self, method: str, params: Any, timeout: float = 30.0) -> Any:
        try:
            return self._rpc.call(method, params, timeout=timeout)
        except RPCError as exc:
            raise MCPError(f"mcp {method} failed: {exc}") from exc

    def tools_list(self) -> list[dict[str, Any]]:
        result = self._call("tools/list", {})
        tools = result.get("tools") if isinstance(result, dict) else None
        return list(tools or [])

    def tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Call one tool and return its parsed JSON payload.

        intercom-mcp returns the daemon result as a text content block;
        this parses it back. Tool-level errors (isError) raise MCPError
        carrying the server's text, so a test cannot mistake an error
        result for a success payload.
        """
        result = self._call("tools/call", {"name": name, "arguments": arguments})
        if not isinstance(result, dict):
            raise MCPError(f"tool {name} returned a non-object result: {result!r}")
        if result.get("isError"):
            raise MCPError(f"tool {name} isError: {_tool_text(result)}")
        text = _tool_text(result)
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except ValueError:
            return {"text": text}
        return parsed if isinstance(parsed, dict) else {"value": parsed}


def _tool_text(result: dict[str, Any]) -> str:
    content = result.get("content") or []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            return str(block.get("text", ""))
    return ""


class MCPHarnessAdapter(BaseHarness):
    """Harness adapter for a scripted MCP participant.

    Its capability set must match what intercom-mcp registers for the
    same session (notify/queue native, steer unsupported). deliver()
    claims the message through inbox_list and reports presented —
    claim is presentation, not injection, and never handled: handled
    is reported only by ack_handled(), after the participant has
    actually acted on the message.
    """

    name = "mcp-scripted"
    version = "0.2.0"
    modes = (MODE_NOTIFY, MODE_QUEUE)

    def __init__(self, mcp_client: Any = None) -> None:
        super().__init__()
        self._mcp = mcp_client
        self._states: dict[str, str] = {"*": "idle"}

    def start(self) -> None:
        start = getattr(self._mcp, "start", None)
        if callable(start):
            start()

    def close(self) -> None:
        close = getattr(self._mcp, "close", None)
        if callable(close):
            close()

    def state(self, session_id: str) -> SessionState:
        return SessionState(
            state="idle",
            safe_point=True,
            detail="scripted MCP participant; inbox claims are its next-turn channel",
        )

    def deliver(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        if self._mcp is None:
            return DeliveryResult(
                status=RECEIPT_REJECTED,
                detail="no MCP client is attached to this adapter",
            )
        try:
            inbox = self._mcp.tool("inbox_list", {})
        except Exception as exc:
            return DeliveryResult(status=RECEIPT_REJECTED, detail=f"inbox_list failed: {exc}")
        messages = inbox.get("messages") or []
        wanted = message.get("messageId") or message.get("message_id") or ""
        if wanted and not any(m.get("messageId") == wanted for m in messages):
            return DeliveryResult(
                status=RECEIPT_REJECTED,
                detail=f"message {wanted} was not in the claimed inbox",
            )
        return DeliveryResult(
            status=RECEIPT_PRESENTED,
            detail="claimed through MCP inbox_list and presented to the participant; not injected into a model context",
        )

    def ack_handled(self, message_id: str, detail: str = "") -> None:
        """Report that the participant acted on a claimed message."""
        if self._mcp is None:
            raise MCPError("no MCP client is attached to this adapter")
        self._mcp.tool("message_ack", {"messageId": message_id, "status": "handled", "detail": detail})

    def notify(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        # A notify-mode message is in the same durable inbox; claiming
        # it presents it without any model turn.
        return self.deliver(session_id, message)

    def send(self, to_session: str, text: str, mode: str = "notify", command_id: str = "") -> dict[str, Any]:
        if self._mcp is None:
            raise MCPError("no MCP client is attached to this adapter")
        return self._mcp.tool(
            "message_send",
            {
                "commandId": command_id or ("cmd_" + harness_command_id("")),
                "to": to_session,
                "text": text,
                "mode": mode,
            },
        )


__all__ = ["MCPClient", "MCPError", "MCPHarnessAdapter", "MODE_STEER", "RECEIPT_DELIVERED"]
