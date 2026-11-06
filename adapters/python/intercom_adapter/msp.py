"""A real MSP client: `muse serve` over stdio.

The wire contract here was read from the schema that
`muse schema generate-json-schema` exports for the installed binary
(Muse 1.4.14), not from documentation:

    initialize     {protocolVersion, clientInfo{name, version}}
                   clientInfo.name must match ^[a-z0-9_]+$ (SS1.4.1)
    initialized    notification, required before any other method
    session/start  {commandId (UUIDv7), workspaceRoot, config?, ...}
                   -> {session: {sessionId, status, activeTurnId, ...}}
    turn/start     {commandId (UUIDv7), sessionId, input[], ifBusy}
                   -> {commandId, disposition, startedNewTurn, status, turnId}
    turn/steer     {commandId, sessionId, expectedTurnId, input[]}
                   -> {commandId, status, turnId}; fails when the named
                   turn is no longer the active turn
    turn/completed notification {sessionId, turnId, terminal, ...}

`session/start` also accepts `config.mcpServers`: native MCP servers
added only to that session's runtime. That is how a Muse session can be
given intercom-mcp without touching any global harness configuration.

Provider selection is a host launch flag (`--provider echo|meta|local`),
never a config rewrite. The echo provider needs no credentials and no
inference, which is what makes protocol-level verification of this
client possible without spending anything.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Optional

from .ids import uuid7
from .stdio_rpc import RPCError, StdioRPCClient

__all__ = ["MSPClient", "MSPError", "create_msp_client"]


class MSPError(RuntimeError):
    """An MSP call failed on the wire."""


class MSPClient:
    """MSP client over a `muse serve` subprocess (stdio transport)."""

    def __init__(self, argv: list[str], env: Optional[dict[str, str]] = None, cwd: Optional[str] = None) -> None:
        self._rpc = StdioRPCClient(argv, env=env, cwd=cwd)
        self._started = False

    # -- lifecycle --------------------------------------------------------

    def start(self) -> None:
        """Spawn `muse serve` and complete initialize/initialized."""
        if self._started:
            return
        self._rpc.start()
        result = self.call(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "clientInfo": {"name": "intercom_adapter", "version": "0.2.0"},
            },
        )
        if not isinstance(result, dict) or "serverInfo" not in result:
            raise MSPError(f"unexpected initialize result: {result!r}")
        self._rpc.notify("initialized", {})
        self._started = True

    def close(self) -> None:
        self._rpc.close()
        self._started = False

    def __enter__(self) -> "MSPClient":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- notifications ----------------------------------------------------

    def on_notification(self, handler: Callable[[str, Any], None]) -> None:
        self._rpc.on_notification(handler)

    # -- calls ------------------------------------------------------------

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Invoke an MSP method and return its result object.

        This is the interface MuseAdapter was written against: a failure
        raises, so the adapter can report a rejection instead of
        inventing a result.
        """
        try:
            result = self._rpc.call(method, params)
        except RPCError as exc:
            raise MSPError(f"muse {method} failed: {exc}") from exc
        return result if isinstance(result, dict) else {}

    def start_session(
        self,
        workspace_root: str,
        mcp_servers: Optional[dict[str, Any]] = None,
        model_id: Optional[str] = None,
        provider_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create a session and return its session object.

        `mcp_servers` is the per-session `config.mcpServers` map, e.g.
        {"intercom": {"transport": "stdio", "command": "...", "args": [...]}}.
        """
        params: dict[str, Any] = {
            "commandId": uuid7(),
            "workspaceRoot": workspace_root,
        }
        if mcp_servers:
            params["config"] = {"mcpServers": mcp_servers}
        if model_id:
            params["modelId"] = model_id
        if provider_id:
            params["providerId"] = provider_id
        result = self.call("session/start", params)
        session = result.get("session")
        if not isinstance(session, dict) or not session.get("sessionId"):
            raise MSPError(f"session/start returned no session: {result!r}")
        return session


def create_msp_client(
    muse_bin: str = "muse",
    provider: str = "echo",
    model: Optional[str] = None,
    extra_args: Optional[list[str]] = None,
    env: Optional[dict[str, str]] = None,
    cwd: Optional[str] = None,
) -> MSPClient:
    """Build (but do not start) an MSP client for a Muse wrapper binary.

    `muse_bin` is the wrapper to launch (`muse`, `musefi`, `openmuse`,
    ...): each wrapper is the same Muse binary with a different
    XDG_CONFIG_HOME profile, so launching the wrapper is what selects
    the profile. The provider is passed as a serve flag for this
    process only and never rewrites the profile's settings.
    """
    argv = [muse_bin, "serve", "--provider", provider, "--no-session-log"]
    if model:
        argv += ["--model", model]
    if extra_args:
        argv += list(extra_args)
    merged_env = dict(os.environ)
    merged_env["MUSE_NO_AUTO_UPDATE"] = "1"
    if env:
        merged_env.update(env)
    return MSPClient(argv, env=merged_env, cwd=cwd)
