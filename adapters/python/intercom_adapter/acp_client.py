"""A raw ACP client over stdio, for ACP agents (OpenCode, Hermes).

ACP is newline-delimited JSON-RPC 2.0 with `protocolVersion: 1`. The
client here is deliberately raw rather than built on the reference
`agent_client_protocol` package: the adapter host must run anywhere the
Intercom Python package runs, and the reference package is an optional
Hermes-venv dependency, not a package dependency.

The wire shape was verified against `opencode acp` (OpenCode 2.2.5):

    initialize    {protocolVersion: 1, clientCapabilities: {},
                   clientInfo: null}
                  A non-null clientInfo object is rejected by OpenCode
                  (no response at all), so the client sends null.
    initialized   notification
    session/new   {cwd, mcpServers: []} -> {sessionId, configOptions}
                  Slow on OpenCode (model catalog load, ~10s), so the
                  default timeout for this call is generous.
    session/prompt {sessionId, prompt: [{type: "text", text}]}
                  Blocks until the turn completes. That blocking submit
                  of a user turn is queue behaviour, never steering:
                  ACP has no active-turn injection point.
    session/update notification (agent state and content progress)

Server-initiated requests (session/request_permission and friends) are
answered through the request handler. The default handler refuses
permissions with a cancelled outcome rather than auto-approving them:
approval authority belongs to the session owner, not to this adapter
(ICSP-001 section 11).
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from .stdio_rpc import RPCError, StdioRPCClient

__all__ = ["ACPClient", "ACPError", "create_acp_client"]


class ACPError(RuntimeError):
    """An ACP call failed on the wire."""


class ACPClient:
    """ACP client over an ACP agent subprocess (stdio transport)."""

    def __init__(
        self,
        argv: list[str],
        env: Optional[dict[str, str]] = None,
        cwd: Optional[str] = None,
        permission_handler: Optional[Callable[[str, Any], Any]] = None,
    ) -> None:
        self._rpc = StdioRPCClient(argv, env=env, cwd=cwd)
        self._permission_handler = permission_handler
        self._started = False
        self.agent_info: dict[str, Any] = {}
        self.agent_capabilities: dict[str, Any] = {}

    # -- lifecycle --------------------------------------------------------

    def start(self) -> None:
        """Spawn the agent and complete initialize/initialized."""
        if self._started:
            return
        self._rpc.start()
        self._rpc.on_request(self._handle_server_request)
        result = self.call(
            "initialize",
            {"protocolVersion": 1, "clientCapabilities": {}, "clientInfo": None},
            timeout=30.0,
        )
        if not isinstance(result, dict) or result.get("protocolVersion") != 1:
            raise ACPError(f"unexpected initialize result: {result!r}")
        self.agent_info = result.get("agentInfo") or {}
        self.agent_capabilities = result.get("agentCapabilities") or {}
        self._rpc.notify("initialized", {})
        self._started = True

    def close(self) -> None:
        self._rpc.close()
        self._started = False

    def __enter__(self) -> "ACPClient":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- notifications ----------------------------------------------------

    def on_notification(self, handler: Callable[[str, Any], None]) -> None:
        self._rpc.on_notification(handler)

    # -- calls ------------------------------------------------------------

    def call(self, method: str, params: Any, timeout: float = 60.0) -> Any:
        try:
            return self._rpc.call(method, params, timeout=timeout)
        except RPCError as exc:
            raise ACPError(f"acp {method} failed: {exc}") from exc

    def new_session(self, cwd: str, mcp_servers: Optional[list[dict[str, Any]]] = None, timeout: float = 60.0) -> str:
        """Create a session and return the agent's session id."""
        result = self.call(
            "session/new",
            {"cwd": cwd, "mcpServers": list(mcp_servers or [])},
            timeout=timeout,
        )
        session_id = result.get("sessionId") if isinstance(result, dict) else None
        if not session_id:
            raise ACPError(f"session/new returned no session id: {result!r}")
        return str(session_id)

    def prompt(self, session_id: str, text: str, timeout: float = 300.0) -> Any:
        """Submit a user turn and block until it completes.

        This is the HermesAdapter agent interface as well: `prompt`
        with a `session_id` keyword. It submits the *next* turn; it
        cannot inject into a running one.
        """
        return self.call(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": text}]},
            timeout=timeout,
        )

    # -- server requests ----------------------------------------------------

    def _handle_server_request(self, method: str, params: Any) -> Any:
        if method == "session/request_permission":
            if self._permission_handler is not None:
                return self._permission_handler(method, params)
            # Refuse rather than auto-approve: this adapter holds no
            # approval authority over the session it bridges.
            return {"outcome": {"outcome": "cancelled"}}
        raise RPCError(-32601, f"method {method!r} not found")


def create_acp_client(
    argv: list[str],
    env: Optional[dict[str, str]] = None,
    cwd: Optional[str] = None,
    permission_handler: Optional[Callable[[str, Any], Any]] = None,
) -> ACPClient:
    """Build (but do not start) an ACP client for an agent command."""
    return ACPClient(argv, env=env, cwd=cwd, permission_handler=permission_handler)
