"""A real Codex app-server client: `codex app-server --stdio`.

The wire contract was read from the schema that
`codex app-server generate-json-schema` exports for the installed
binary (Codex 0.166.0), and verified live without any inference:

    initialize     {clientInfo{name, version}, capabilities}
                   -> {userAgent, codexHome, platformFamily, platformOs}
    initialized    notification
    thread/start   {cwd, approvalPolicy, sandbox, ephemeral, config?, ...}
                   -> {thread: {id, status: {type: "idle"}, ...}}
                   No model call: a thread can be started with no
                   inference spend at all.
    turn/start     {threadId, input: [{type: "text", text}], ...}
                   -> {turn: {id, ...}}; runs a model turn
    turn/steer     {threadId, expectedTurnId, input: [...]}
                   expectedTurnId: "Required active turn id
                   precondition. The request fails when it does not
                   match the currently active turn." That is a genuine
                   active-turn injection point with the same failure
                   semantics Intercom's STEER requires.
    turn/completed notification {threadId, turn: {id, status, ...}}

Wrapper note (verified, and important): `harvardcodex app-server`
does not reach this protocol. The harvardcodex launcher selects
`~/.harvardcodex/bin/codex-workflow` when it exists, and that build
has no app-server subcommand at all (`--stdio` is rejected; a bare
`app-server` falls through to the TUI and dies on a non-terminal
stdin). The stock `codex` binary with CODEX_HOME pointed at the
harvardcodex profile speaks app-server normally. The adapter
therefore launches the stock binary and selects the profile through
CODEX_HOME, which is a per-process environment choice, not a rewrite
of any global configuration.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from .stdio_rpc import RPCError, StdioRPCClient

__all__ = ["CodexClient", "CodexError", "create_codex_client"]


class CodexError(RuntimeError):
    """A Codex app-server call failed on the wire."""


class CodexClient:
    """Codex app-server client over a `codex app-server --stdio` subprocess."""

    def __init__(self, argv: list[str], env: Optional[dict[str, str]] = None, cwd: Optional[str] = None) -> None:
        self._rpc = StdioRPCClient(argv, env=env, cwd=cwd)
        self._started = False
        self.user_agent = ""

    # -- lifecycle --------------------------------------------------------

    def start(self) -> None:
        """Spawn app-server and complete initialize/initialized."""
        if self._started:
            return
        self._rpc.start()
        result = self.call(
            "initialize",
            {"clientInfo": {"name": "intercom_adapter", "version": "0.2.0"}, "capabilities": {}},
        )
        if not isinstance(result, dict) or "codexHome" not in result:
            raise CodexError(f"unexpected initialize result: {result!r}")
        self.user_agent = str(result.get("userAgent") or "")
        self._rpc.notify("initialized", {})
        self._started = True

    def close(self) -> None:
        self._rpc.close()
        self._started = False

    def __enter__(self) -> "CodexClient":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- notifications ----------------------------------------------------

    def on_notification(self, handler: Callable[[str, Any], None]) -> None:
        self._rpc.on_notification(handler)

    # -- calls ------------------------------------------------------------

    def call(self, method: str, params: dict[str, Any], timeout: float = 60.0) -> Any:
        try:
            return self._rpc.call(method, params, timeout=timeout)
        except RPCError as exc:
            raise CodexError(f"codex {method} failed: {exc}") from exc

    def start_thread(
        self,
        cwd: str,
        approval_policy: str = "never",
        sandbox: str = "read-only",
        ephemeral: bool = True,
        config: Optional[dict[str, Any]] = None,
        model: Optional[str] = None,
        model_provider: Optional[str] = None,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        """Start a thread and return the thread object (no inference)."""
        params: dict[str, Any] = {
            "cwd": cwd,
            "approvalPolicy": approval_policy,
            "sandbox": sandbox,
            "ephemeral": ephemeral,
        }
        if config:
            params["config"] = config
        if model:
            params["model"] = model
        if model_provider:
            params["modelProvider"] = model_provider
        result = self.call("thread/start", params, timeout=timeout)
        thread = result.get("thread") if isinstance(result, dict) else None
        if not isinstance(thread, dict) or not thread.get("id"):
            raise CodexError(f"thread/start returned no thread: {result!r}")
        return thread


def create_codex_client(
    codex_bin: str = "codex",
    codex_home: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
    cwd: Optional[str] = None,
) -> CodexClient:
    """Build (but do not start) a Codex app-server client.

    `codex_bin` must be a stock Codex binary with an app-server
    subcommand. `codex_home` selects the profile (CODEX_HOME) for this
    process only; passing the harvardcodex profile is how the
    FreeInference-backed configuration is used without going through
    the harvardcodex launcher's broken app-server path.
    """
    import os

    argv = [codex_bin, "app-server", "--stdio"]
    merged_env = dict(os.environ)
    if codex_home:
        merged_env["CODEX_HOME"] = codex_home
    if env:
        merged_env.update(env)
    return CodexClient(argv, env=merged_env, cwd=cwd)
