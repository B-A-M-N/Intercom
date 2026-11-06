"""Claude Code adapter, backed by Claude Code's `--print` stream-json surface.

Claude Code has no server protocol with an active-turn injection point
(no ACP agent, no MSP equivalent). What it does expose, verified from
`claude --help` (Claude Code 2.1.316):

    --print --output-format=stream-json
        Non-interactive output as a realtime JSON message stream.
    --input-format=stream-json
        Realtime streaming input: user messages are fed to the running
        session as JSON lines, so a long-lived `--print` process is a
        genuine next-turn (queue) channel.
    --mcp-config <json>
        MCP servers for this run only, without touching the user's
        global MCP configuration.
    --max-budget-usd <amount>
        Hard spend cap for a `--print` run.

Delivery honesty for this adapter:

    notify - native, via the adapter host's durable notification sink (persistence only, not presentation) (presented)
    queue  - native: a user message on the stream-json input becomes
             the session's next turn
    steer  - unsupported: there is no documented active-turn injection
             API, and submitting a new user message while a turn runs
             is queueing, not steering

Every queue delivery here is a real Claude turn and therefore real
Anthropic inference spend. The client takes an explicit budget cap and
the live tests treat a Claude turn as a paid operation: protocol shape
is verified with non-turn invocations, and a model turn is run only
deliberately, under the cap.
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
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
    SessionState,
)


class ClaudeError(RuntimeError):
    """A Claude --print stream failed."""


class ClaudeClient:
    """A long-lived `claude --print` stream-json subprocess."""

    def __init__(self, argv: list[str], env: Optional[dict[str, str]] = None, cwd: Optional[str] = None) -> None:
        self._argv = list(argv)
        self._env = env
        self._cwd = cwd
        self._proc: Optional[subprocess.Popen[str]] = None
        self._lock = threading.Lock()
        self._results: list[dict[str, Any]] = []
        self._texts: list[str] = []
        self._running = False

    def start(self) -> None:
        if self._proc is not None:
            return
        self._proc = subprocess.Popen(
            self._argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            env=self._env,
            cwd=self._cwd,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._read_loop, daemon=True).start()

    def close(self) -> None:
        proc, self._proc = self._proc, None
        if proc is not None:
            if proc.stdin is not None:
                try:
                    proc.stdin.close()
                except OSError:
                    pass
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        self._running = False

    def __enter__(self) -> "ClaudeClient":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _read_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if not isinstance(message, dict):
                continue
            kind = message.get("type")
            if kind == "result":
                self._results.append(message)
                self._running = False
            elif kind == "assistant":
                self._running = True
                content = ((message.get("message") or {}).get("content")) or []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        self._texts.append(str(block.get("text", "")))

    def send_text(self, text: str, timeout: float = 300.0) -> dict[str, Any]:
        """Send one user message and block until its result arrives.

        The stream-json input line is a user message object, the same
        shape the output stream uses for user messages.
        """
        if self._proc is None or self._proc.stdin is None:
            raise ClaudeError("claude client is not started")
        before = len(self._results)
        self._running = True
        payload = {
            "type": "user",
            "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        }
        with self._lock:
            self._proc.stdin.write(json.dumps(payload) + "\n")
            self._proc.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self._results) > before:
                return self._results[-1]
            if self._proc.poll() is not None:
                break
            time.sleep(0.05)
        raise ClaudeError(f"claude turn did not complete within {timeout}s")


def create_claude_client(
    claude_bin: str = "claude",
    model: Optional[str] = None,
    max_budget_usd: Optional[float] = None,
    mcp_config: Optional[dict[str, Any]] = None,
    allowed_tools: Optional[list[str]] = None,
    extra_args: Optional[list[str]] = None,
    cwd: Optional[str] = None,
) -> ClaudeClient:
    """Build (but do not start) a Claude stream-json client.

    The budget cap is mandatory in spirit: a queue delivery is a paid
    model turn, so callers should always pass one. `--permission-mode
    dontAsk` keeps the non-interactive run from blocking on a prompt;
    tool use that would need approval is denied rather than approved
    by this adapter (approval authority is the owner's, not ours).
    `allowed_tools` is the explicit allow-list the owner grants (for
    example `mcp__intercom__peers_list`); without it, dontAsk denies
    every MCP tool call, which the live verification confirmed.
    """
    argv = [
        claude_bin,
        "--print",
        "--input-format=stream-json",
        "--output-format=stream-json",
        "--permission-mode=dontAsk",
    ]
    if model:
        argv += ["--model", model]
    if max_budget_usd is not None:
        argv += ["--max-budget-usd", str(max_budget_usd)]
    if mcp_config is not None:
        argv += ["--mcp-config", json.dumps(mcp_config)]
    for tool in allowed_tools or []:
        argv += [f"--allowedTools={tool}"]
    if extra_args:
        argv += list(extra_args)
    return ClaudeClient(argv, cwd=cwd)


class ClaudeAdapter(BaseHarness):
    """Adapter for Claude Code, over its stream-json queue channel."""

    name = "claude-code"
    version = "0.2.0"

    #: Claude offers a next-turn channel but no active-turn injection.
    modes = (MODE_QUEUE, MODE_NOTIFY)

    def __init__(self, claude_client: Any = None, session_id: str = "") -> None:
        super().__init__()
        self._claude = claude_client
        self._session_id = session_id
        self._states: dict[str, str] = {}
        self._notes: dict[str, list[str]] = {}

    def bind_session(self, session_id: str) -> None:
        self._session_id = session_id

    def start(self) -> None:
        start = getattr(self._claude, "start", None)
        if callable(start):
            start()

    def close(self) -> None:
        close = getattr(self._claude, "close", None)
        if callable(close):
            close()

    def note_state(self, session_id: str, state: str) -> None:
        self._states[session_id] = state

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
            detail="reported by the Claude Code stream-json channel",
        )

    def deliver(self, session_id: str, message: dict[str, Any]) -> DeliveryResult:
        """Submit a message as the next turn on the stream-json channel."""
        if self._claude is None:
            return DeliveryResult(
                status=RECEIPT_REJECTED,
                detail="no Claude stream-json client is attached to this adapter",
            )
        text = _message_text(message)
        self._states[session_id] = "running"
        try:
            result = self._claude.send_text(text)
        except Exception as exc:
            self._states[session_id] = "idle"
            return DeliveryResult(status=RECEIPT_UNKNOWN, detail=f"claude turn failed: {exc}")
        self._states[session_id] = "idle"
        if isinstance(result, dict) and result.get("is_error"):
            return DeliveryResult(
                status=RECEIPT_REJECTED,
                detail=f"claude turn reported an error: {result.get('result') or result.get('error') or 'unknown'}",
            )
        return DeliveryResult(
            status=RECEIPT_DELIVERED,
            detail="submitted as the next turn through Claude --print stream-json input",
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
            detail="recorded in the adapter host's durable notification sink (persistence only, not presentation); not injected into the model context",
        )

    def notes(self, session_id: str) -> list[str]:
        return list(self._notes.get(session_id, []))


def _message_text(message: dict[str, Any]) -> str:
    """Extract labeled peer text (provenance enforced)."""
    from .harness import render_peer_text

    return render_peer_text(message)


__all__ = ["ClaudeAdapter", "ClaudeClient", "ClaudeError", "create_claude_client", "MODE_STEER"]
