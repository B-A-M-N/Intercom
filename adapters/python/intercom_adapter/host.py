"""The adapter host process.

The adapter host is the bridge between the Go core and one harness. It:

- connects to the Go host over the local socket,
- registers its session and its honest capability set,
- polls the Go host's session inbox and delivers messages to the harness,
- reports the resulting receipts back to the Go core.

The adapter host has no write path to the durable store. It reports
observations; the Go core decides what is true (ICSP-001 section 17).
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Optional

from .client import IntercomClient, IntercomError, wait_for_socket
from .harness import BaseHarness, DeliveryResult, RECEIPT_UNKNOWN

#: The ACP extension namespace.
#:
#: Section 8.1 requires a collision-resistant reverse-DNS namespace
#: controlled by the implementer to be selected before a stable ACP
#: extension is published. The specification's examples use the provisional
#: "_intercom/" prefix; this implementation uses a reverse-DNS namespace.
EXTENSION_NAMESPACE = "dev.bamn.intercom"

#: The underscore-prefixed method prefix ACP routes to extension handlers.
EXTENSION_PREFIX = f"_{EXTENSION_NAMESPACE}/"


@dataclass
class HostConfig:
    """Configuration for one adapter host."""

    socket_path: str
    harness: str
    session_id: str
    alias: str = ""
    principal: str = ""
    poll_interval: float = 0.5
    once: bool = False
    session_secret: str = ""
    secret_file: str = ""
    state_dir: str = ""


class AdapterHost:
    """Runs one harness adapter against the Go core."""

    def __init__(self, cfg: HostConfig, harness: BaseHarness, client: Optional[IntercomClient] = None) -> None:
        self.cfg = cfg
        self.harness = harness
        self.client = client or IntercomClient(cfg.socket_path)
        self._running = True
        # The instance identity is minted per process. A restart produces a
        # new instance id while the session identity is preserved.
        self.instance_id = _mint_instance_id()
        # Resolve the client-held resume secret (explicit > env > file).
        self.session_secret = cfg.session_secret or os.environ.get("INTERCOM_SESSION_SECRET", "")
        self._secret_file = cfg.secret_file or _default_secret_file(cfg.session_id)
        self._delegation_token: str = ""
        self._delegation_file: str = ""
        if not self.session_secret:
            self.session_secret = _load_secret_file(self._secret_file)
        # State dir for native-session mappings + notifications (harness-owned).
        self.state_dir = cfg.state_dir or os.environ.get(
            "INTERCOM_STATE_DIR",
            os.path.join(os.path.expanduser("~/.local/share/intercom/harness-state"), cfg.session_id),
        )
        try:
            os.makedirs(self.state_dir, exist_ok=True)
        except OSError:
            pass
        # Hand the durable store to the harness when it supports it.
        if hasattr(harness, "set_state_dir"):
            try:
                harness.set_state_dir(self.state_dir)  # type: ignore[attr-defined]
            except Exception:
                pass

    def stop(self) -> None:
        """Ask the host loop to exit."""
        self._running = False

    def heartbeat_once(self) -> None:
        """Report liveness for this session (best effort).

        The daemon marks a session disconnected after 90s without a
        heartbeat, so the host loop beats at a third of that window.
        Failures are silent; the next beat retries.
        """
        try:
            self.client.call(
                f"{EXTENSION_PREFIX}heartbeat",
                {"sessionId": self.cfg.session_id},
                timeout=5.0,
            )
        except Exception:
            pass

    # -- registration ------------------------------------------------------

    def register(self) -> dict[str, Any]:
        """Register this session with the Go core.

        The capability set reported here is the harness's honest one. The Go
        core stores it and serves it to every peer, so a peer's view of this
        harness's abilities is exactly what the harness claimed.

        The instance identity is minted per process, so a restart produces a
        new instance id while the session identity is preserved (ICSP-001
        section 3).
        """
        # The host states what it needs before it depends on it. The
        # negotiation fails safely if the host cannot satisfy those needs,
        # rather than the adapter discovering it later (ICSP-001 section
        # 8.3).
        hello = self.client.call(
            f"{EXTENSION_PREFIX}hello",
            {
                "require": list(self.required_capabilities()),
                "clientInfo": {
                    "name": self.harness.identity[0],
                    "version": self.harness.identity[1],
                },
            },
        )

        payload = {
            "sessionId": self.cfg.session_id,
            "instanceId": self.instance_id,
            "alias": self.cfg.alias or self.cfg.session_id,
            "harness": self.harness.identity[0],
            "harnessVersion": self.harness.identity[1],
            "capabilities": [c.to_dict() for c in self.harness.capabilities],
            "deliveryModes": self.harness.delivery_modes,
            "sessionSecret": self.session_secret,
        }
        if self.cfg.principal:
            payload["principalId"] = self.cfg.principal

        reg_result = self.client.call(f"{EXTENSION_PREFIX}session/register", payload)
        issued = (reg_result or {}).get("sessionSecret", "")
        if issued:
            # First creation: persist the issued secret for authorized resume.
            # A persistence failure is fatal: without it the session can
            # never be resumed, so fail loudly with an actionable path.
            self.session_secret = issued
            try:
                _save_secret_file(self._secret_file, issued)
            except OSError as exc:
                raise RuntimeError(
                    f"failed to persist session secret for {self.cfg.session_id!r} "
                    f"to {self._secret_file!r}: {exc}. Fix the file path/permissions "
                    "and restart; the daemon has already enrolled the secret."
                ) from exc
        # Mint a scoped delegation token for embedded MCP consumers.
        # The agent subprocess never receives the unrestricted resume secret.
        self._delegation_token = ""
        try:
            deleg = self.client.call(
                f"{EXTENSION_PREFIX}session/delegate",
                {"sessionId": self.cfg.session_id},
            )
            self._delegation_token = (deleg or {}).get("delegationToken", "")
            if self._delegation_token:
                # Write the scoped token to a 0600 file only; never export it
                # process-wide or via argv where ps can read it.
                self._delegation_file = _default_delegation_file(self.cfg.session_id)
                _save_secret_file(self._delegation_file, self._delegation_token)
                os.environ.pop("INTERCOM_MCP_DELEGATION_TOKEN", None)
        except Exception:
            self._delegation_token = ""

        return {
            "harness": self.harness.identity[0],
            "version": self.harness.identity[1],
            "sessionId": self.cfg.session_id,
            "instanceId": self.instance_id,
            "alias": self.cfg.alias or self.cfg.session_id,
            "capabilities": payload["capabilities"],
            "deliveryModes": self.harness.delivery_modes,
            "specVersion": hello.get("specVersion", ""),
            "profiles": hello.get("profiles", []),
        }

    def required_capabilities(self) -> list[str]:
        """The capabilities this adapter host cannot operate without.

        A host needs peer discovery, the ability to send, and a durable
        command ledger. It does not require steering, because a harness
        that cannot steer still serves the other delivery modes.
        """
        return [
            "peer.discovery",
            "peer.send",
            "command.durable",
            "session.observe",
        ]

    # -- delivery ----------------------------------------------------------

    def deliver_pending(self) -> int:
        """Deliver every message waiting in this session's inbox.

        The Go core owns the inbox. The adapter only asks what is pending and
        reports what it did.
        """
        delivered = 0
        result = self.client.call(
            f"{EXTENSION_PREFIX}message/listPending", {"sessionId": self.cfg.session_id}
        )
        for message in result.get("messages", []):
            outcome = self._deliver_one(message)
            self._report(message, outcome)
            delivered += 1
        return delivered

    def _deliver_one(self, message: dict[str, Any]) -> DeliveryResult:
        """Deliver one message to the harness with provenance enforced."""
        mode = (message.get("delivery") or {}).get("mode", "queue")
        # Provenance is mandatory at the injection boundary: queue/steer
        # messages are labeled before they reach the harness.
        if mode in ("queue", "steer"):
            message = _ensure_provenance(message)
        # Lease renewal: a long harness operation must not lose its claim.
        stop_renew = self._start_renewal(message)
        try:
            if mode == "notify":
                return self.harness.notify(self.cfg.session_id, message)
            if mode == "steer" and not self.harness.supports("steer"):
                # The harness cannot steer. Refuse explicitly rather than
                # silently substituting a different operation.
                return DeliveryResult(
                    status="rejected",
                    detail=(
                        f"the {self.harness.identity[0]} harness does not support "
                        "active-turn steering"
                    ),
                )
            return self.harness.deliver(self.cfg.session_id, message)
        except Exception as exc:
            return DeliveryResult(status=RECEIPT_UNKNOWN, detail=f"delivery raised: {exc}")
        finally:
            if stop_renew is not None:
                stop_renew()

    def _start_renewal(self, message: dict[str, Any]):
        """Renew the claim lease while a long delivery runs. Returns stop fn."""
        token = message.get("claimToken", "")
        gen = message.get("claimGeneration", 0)
        if not token or not gen or gen <= 0:
            return None
        import threading

        stop = threading.Event()

        def renew_loop() -> None:
            while not stop.wait(10.0):
                if not self._running:
                    break
                try:
                    self.client.call(
                        f"{EXTENSION_PREFIX}message/renew",
                        {
                            "messageId": message.get("messageId", ""),
                            "claimToken": token,
                            "claimGeneration": gen,
                        },
                        timeout=5.0,
                    )
                except Exception:
                    # A failed renewal is retryable next tick; a superseded
                    # claim will be rejected at report time (fencing).
                    pass

        thread = threading.Thread(target=renew_loop, daemon=True)
        thread.start()
        return stop.set

    def _report(self, message: dict[str, Any], outcome: DeliveryResult) -> None:
        """Report a delivery outcome back to the Go core, fenced by claim."""
        try:
            self.client.call(
                f"{EXTENSION_PREFIX}message/report",
                {
                    "messageId": message.get("messageId", ""),
                    "status": outcome.status,
                    "detail": outcome.detail,
                    "claimToken": message.get("claimToken", ""),
                    "claimGeneration": message.get("claimGeneration", 0),
                },
            )
        except (IntercomError, OSError):
            # The core will classify the message as unknown on its own; a
            # failed report must not stop the loop.
            pass

    # -- loop --------------------------------------------------------------

    def run(self) -> int:
        """Run the adapter host until stopped."""
        # A harness that owns a subprocess (muse serve, opencode acp, ...)
        # is started before registration and closed afterwards. Adapters
        # without a lifecycle are unaffected.
        start = getattr(self.harness, "start", None)
        if callable(start):
            start()
        self.client.connect()
        # Heartbeat on its own thread: a blocking delivery (a long
        # model turn) must not let presence lapse mid-turn.
        import threading

        hb_stop = threading.Event()

        def beat_loop() -> None:
            while not hb_stop.wait(10.0):
                if not self._running:
                    break
                self.heartbeat_once()

        hb_thread = threading.Thread(target=beat_loop, daemon=True)
        hb_thread.start()
        try:
            info = self.register()
            print(
                json.dumps(
                    {"event": "registered", "sessionId": self.cfg.session_id, **info},
                    separators=(",", ":"),
                ),
                flush=True,
            )

            while self._running:
                try:
                    count = self.deliver_pending()
                except (IntercomError, OSError, RuntimeError, TimeoutError) as exc:
                    count = 0
                    if self.cfg.once:
                        print(f"adapter host: {exc}", file=sys.stderr, flush=True)
                        return 1
                    # Connection lost (daemon restart, idle disconnect):
                    # reconnect and re-register (resume with the same
                    # session secret) before polling again.
                    print(f"adapter host: connection lost ({exc}); reconnecting", file=sys.stderr, flush=True)
                    for attempt in range(5):
                        try:
                            self.client.reconnect()
                            self.instance_id = _mint_instance_id()
                            self.register()
                            break
                        except Exception:
                            if attempt == 4:
                                print("adapter host: reconnect failed", file=sys.stderr, flush=True)
                            time.sleep(1.0)
                    time.sleep(self.cfg.poll_interval)
                    continue
                if self.cfg.once:
                    return 0
                # Beat well inside the daemon's 90s disconnect window
                # for the --once / short-lived path; the thread above
                # covers the long-running loop.
                self.heartbeat_once()
                if count == 0:
                    time.sleep(self.cfg.poll_interval)
        finally:
            hb_stop.set()
            self.client.close()
            close = getattr(self.harness, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
        return 0


def _ensure_provenance(message: dict[str, Any]) -> dict[str, Any]:
    """Ensure a queue/steer message carries the labeled peer rendering."""
    from .harness import render_peer_text

    message = dict(message)
    body = message.get("body") or {}
    if not isinstance(body, dict):
        body = {"type": "text", "text": str(body)}
    if body.get("type") == "peer-message" and body.get("text"):
        return message
    labeled = render_peer_text(message)
    message["body"] = {"type": "peer-message", "text": labeled, "data": body.get("data", {})}
    return message


def _default_secret_file(session_id: str) -> str:
    safe = "".join(c for c in session_id if c.isalnum() or c in "-_")
    return os.path.join(
        os.path.expanduser("~/.local/share/intercom/session-secrets"), f"{safe}.txt"
    )


def _default_delegation_file(session_id: str) -> str:
    safe = "".join(c for c in session_id if c.isalnum() or c in "-_")
    return os.path.join(
        os.path.expanduser("~/.local/share/intercom/session-secrets"), f"{safe}.mcp-token"
    )


def _load_secret_file(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read().strip()
    except OSError:
        return ""


def _save_secret_file(path: str, secret: str) -> None:
    """Persist the session secret atomically; failures are loud.

    A silently dropped secret means the next restart cannot resume the
    session, so any filesystem error raises instead of passing.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(secret + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def _mint_instance_id() -> str:
    """Mint an instance identifier for this process.

    UUIDv7 is time-ordered, so the identifier sorts by creation without a
    coordination round trip. Python 3.11 has no uuid7, so the layout is
    built here: 48 bits of Unix milliseconds, version 7, 12 bits of
    monotonic counter, then random bits.
    """
    import os
    import time
    import uuid

    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    counter = int.from_bytes(os.urandom(2), "big")
    rand = os.urandom(8)
    value = (
        (ms << 80)
        | (0x7 << 76)
        | ((counter & 0xFFF) << 64)
        | (0b10 << 62)
        | int.from_bytes(rand, "big") & ((1 << 62) - 1)
    )
    return "ins_" + str(uuid.UUID(int=value))


def main(argv: Optional[list[str]] = None) -> int:
    """Entry point for the adapter host."""
    parser = argparse.ArgumentParser(description="Intercom adapter host")
    parser.add_argument("--socket", required=True, help="path to the Intercom host socket")
    parser.add_argument("--harness", required=True, help="harness adapter to run")
    parser.add_argument("--session", required=True, help="session identity this adapter represents")
    parser.add_argument("--alias", default="", help="human-readable alias")
    parser.add_argument("--session-secret", default="", help="client-held resume secret for --session (env INTERCOM_SESSION_SECRET or secret file override)")
    parser.add_argument("--secret-file", default="", help="path for the persisted session secret (default: ~/.local/share/intercom/session-secrets/<session>.txt)")
    parser.add_argument("--state-dir", default="", help="durable harness state dir for native-session mappings and notifications")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="inbox poll interval in seconds")
    parser.add_argument("--once", action="store_true", help="drain the inbox once and exit")
    parser.add_argument("--workspace", default="", help="harness session workspace root (default: cwd)")
    parser.add_argument("--muse-bin", default="muse", help="Muse wrapper binary to launch (muse, musefi, openmuse, ...)")
    parser.add_argument("--muse-provider", default="echo", help="Muse serve provider for this process (echo, meta, local)")
    parser.add_argument("--muse-model", default="", help="Muse model id for non-echo providers")
    parser.add_argument("--codex-bin", default="codex", help="stock Codex binary for app-server")
    parser.add_argument("--codex-home", default="", help="CODEX_HOME profile for the Codex app-server process")
    parser.add_argument("--acp-command", default="", help="ACP agent command, space-separated (default per harness)")
    parser.add_argument("--claude-bin", default="claude", help="Claude Code binary")
    parser.add_argument("--claude-model", default="", help="Claude model for queue turns")
    parser.add_argument("--claude-budget-usd", type=float, default=0.0, help="spend cap per Claude run in USD (0 = uncapped; prefer a cap)")
    parser.add_argument("--claude-allowed-tools", default="", help="comma-separated explicit tool allow-list for Claude (e.g. mcp__intercom__peers_list)")
    parser.add_argument("--intercom-mcp-bin", default="", help="intercom-mcp binary, to give the harness session an MCP server")
    args = parser.parse_args(argv)

    options = {
        "workspace": args.workspace,
        "muse_bin": args.muse_bin,
        "muse_provider": args.muse_provider,
        "muse_model": args.muse_model or None,
        "codex_bin": args.codex_bin,
        "codex_home": args.codex_home or None,
        "acp_command": args.acp_command or None,
        "claude_bin": args.claude_bin,
        "claude_model": args.claude_model or None,
        "claude_budget_usd": args.claude_budget_usd or None,
        "claude_allowed_tools": [t for t in args.claude_allowed_tools.split(",") if t] or None,
        "intercom_mcp_bin": args.intercom_mcp_bin or None,
        "intercom_socket": args.socket,
        "intercom_session": args.session,
        "intercom_alias": args.alias,
    }
    harness = build_harness(args.harness, options)
    if harness is None:
        print(f"adapter host: unknown harness {args.harness!r}", file=sys.stderr)
        return 2

    cfg = HostConfig(
        socket_path=args.socket,
        harness=args.harness,
        session_id=args.session,
        alias=args.alias,
        poll_interval=args.poll_interval,
        once=args.once,
        session_secret=args.session_secret,
        secret_file=args.secret_file,
        state_dir=args.state_dir,
    )

    host = AdapterHost(cfg, harness)

    def handle_signal(signum: int, frame: Any) -> None:
        host.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if not wait_for_socket(args.socket, timeout=10.0):
        print(f"adapter host: {args.socket} did not become available", file=sys.stderr)
        return 1

    return host.run()


def _intercom_mcp_server(
    options: dict[str, Any], harness_name: str, fallback_session: str
) -> Optional[dict[str, Any]]:
    """Build the per-session MCP server entry for intercom-mcp, if asked.

    The entry gives the harness session itself the Intercom MCP tools
    (peers_list, message_send, inbox_list, ...), bound to the same
    Intercom session identity the adapter host represents, so the agent
    can discover and message peers during its own turns.
    """
    binary = options.get("intercom_mcp_bin")
    socket_path = options.get("intercom_socket")
    if not binary or not socket_path:
        return None
    session = options.get("intercom_session") or fallback_session
    alias = options.get("intercom_alias") or ""
    delegation_file = options.get("intercom_delegation_file") or _default_delegation_file(session)
    delegation_token = options.get("intercom_delegation_token") or ""
    args = [
        "--socket", socket_path,
        "--session", session,
        "--alias", alias,
        "--harness", harness_name,
    ]
    if delegation_file and os.path.isfile(delegation_file):
        args.append("--attach")
        args.append("--delegation-token-file")
        args.append(delegation_file)
    elif delegation_token:
        args.append("--attach")
        args.append("--delegation-token")
        args.append(delegation_token)
    return {
        "transport": "stdio",
        "command": binary,
        "args": args,
    }


def build_harness(name: str, options: Optional[dict[str, Any]] = None) -> Optional[BaseHarness]:
    """Build the named harness adapter with a real client.

    Every adapter here is constructed with its real transport client
    (an MSP subprocess, an ACP subprocess, a Codex app-server, a
    Claude stream-json process, or intercom-mcp itself). Clients are
    constructed but not started; AdapterHost.run() starts and closes
    them. Session starters create harness sessions lazily, on first
    delivery, so registration never spends inference.
    """
    options = dict(options or {})
    workspace = options.get("workspace") or os.getcwd()

    if name == "muse":
        from .msp import create_msp_client
        from .muse import MuseAdapter

        client = create_msp_client(
            muse_bin=options.get("muse_bin") or "muse",
            provider=options.get("muse_provider") or "echo",
            model=options.get("muse_model"),
            cwd=workspace,
        )

        def starter(intercom_session: str) -> str:
            mcp = _intercom_mcp_server(options, "muse", intercom_session)
            session = client.start_session(
                workspace_root=workspace,
                mcp_servers={"intercom": mcp} if mcp else None,
            )
            return str(session["sessionId"])

        return MuseAdapter(client, session_starter=starter)

    if name == "opencode":
        from .acp_client import create_acp_client
        from .opencode import OpenCodeAdapter

        command = options.get("acp_command") or "opencode acp"
        client = create_acp_client(list(command.split()), cwd=workspace)

        def starter(intercom_session: str) -> str:
            mcp_servers: list[dict[str, Any]] = []
            mcp = _intercom_mcp_server(options, "opencode", intercom_session)
            if mcp:
                mcp_servers.append(
                    {
                        "name": "intercom",
                        "command": mcp["command"],
                        "args": mcp["args"],
                        "env": [],
                    }
                )
            return client.new_session(cwd=workspace, mcp_servers=mcp_servers)

        return OpenCodeAdapter(client, session_starter=starter, cwd=workspace)

    if name == "hermes":
        from .acp_client import create_acp_client
        from .hermes import HermesAdapter

        command = options.get("acp_command") or "hermes-acp"
        client = create_acp_client(list(command.split()), cwd=workspace)

        def starter(intercom_session: str) -> str:
            return client.new_session(cwd=workspace)

        return HermesAdapter(client, session_starter=starter)

    if name == "codex":
        from .codex import CodexAdapter
        from .codex_client import create_codex_client

        client = create_codex_client(
            codex_bin=options.get("codex_bin") or "codex",
            codex_home=options.get("codex_home"),
            cwd=workspace,
        )

        def starter(intercom_session: str) -> str:
            config: Optional[dict[str, Any]] = None
            mcp = _intercom_mcp_server(options, "codex", intercom_session)
            if mcp:
                config = {
                    "mcp_servers": {
                        "intercom": {"command": mcp["command"], "args": mcp["args"]}
                    }
                }
            thread = client.start_thread(cwd=workspace, config=config)
            return str(thread["id"])

        return CodexAdapter(client, thread_starter=starter)

    if name == "claude":
        from .claude import ClaudeAdapter, create_claude_client

        mcp_config = None
        mcp = _intercom_mcp_server(options, "claude-code", options.get("intercom_session") or "")
        if mcp:
            mcp_config = {
                "mcpServers": {
                    "intercom": {
                        "type": "stdio",
                        "command": mcp["command"],
                        "args": mcp["args"],
                    }
                }
            }
        client = create_claude_client(
            claude_bin=options.get("claude_bin") or "claude",
            model=options.get("claude_model"),
            max_budget_usd=options.get("claude_budget_usd"),
            allowed_tools=options.get("claude_allowed_tools"),
            mcp_config=mcp_config,
            cwd=workspace,
        )
        return ClaudeAdapter(client)

    if name == "mcp":
        from .mcp_consumer import MCPClient, MCPHarnessAdapter

        binary = options.get("intercom_mcp_bin") or "intercom-mcp"
        argv = [
            binary,
            "--socket", options.get("intercom_socket") or "",
            "--session", options.get("intercom_session") or "",
            "--alias", options.get("intercom_alias") or "",
            "--harness", "mcp-scripted",
        ]
        return MCPHarnessAdapter(MCPClient(argv, cwd=workspace))

    return None


if __name__ == "__main__":
    raise SystemExit(main())
