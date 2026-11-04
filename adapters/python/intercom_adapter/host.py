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

    def stop(self) -> None:
        """Ask the host loop to exit."""
        self._running = False

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
        hello = self.client.call(f"{EXTENSION_PREFIX}hello")

        payload = {
            "sessionId": self.cfg.session_id,
            "instanceId": self.instance_id,
            "alias": self.cfg.alias or self.cfg.session_id,
            "harness": self.harness.identity[0],
            "harnessVersion": self.harness.identity[1],
            "capabilities": [c.to_dict() for c in self.harness.capabilities],
            "deliveryModes": self.harness.delivery_modes,
        }
        if self.cfg.principal:
            payload["principalId"] = self.cfg.principal

        self.client.call(f"{EXTENSION_PREFIX}session/register", payload)

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
        """Deliver one message to the harness."""
        mode = (message.get("delivery") or {}).get("mode", "queue")
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

    def _report(self, message: dict[str, Any], outcome: DeliveryResult) -> None:
        """Report a delivery outcome back to the Go core."""
        try:
            self.client.call(
                f"{EXTENSION_PREFIX}message/report",
                {
                    "messageId": message.get("messageId", ""),
                    "status": outcome.status,
                    "detail": outcome.detail,
                },
            )
        except (IntercomError, OSError):
            # The core will classify the message as unknown on its own; a
            # failed report must not stop the loop.
            pass

    # -- loop --------------------------------------------------------------

    def run(self) -> int:
        """Run the adapter host until stopped."""
        self.client.connect()
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
                except (IntercomError, OSError) as exc:
                    count = 0
                    if self.cfg.once:
                        print(f"adapter host: {exc}", file=sys.stderr, flush=True)
                        return 1
                if self.cfg.once:
                    return 0
                if count == 0:
                    time.sleep(self.cfg.poll_interval)
        finally:
            self.client.close()
        return 0


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
    parser.add_argument("--poll-interval", type=float, default=0.5, help="inbox poll interval in seconds")
    parser.add_argument("--once", action="store_true", help="drain the inbox once and exit")
    args = parser.parse_args(argv)

    harness = build_harness(args.harness)
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


def build_harness(name: str) -> Optional[BaseHarness]:
    """Build the named harness adapter.

    The adapter for a harness that needs a live client is constructed with a
    client by its own entry point; this factory covers the harnesses that
    can be constructed without one.
    """
    if name == "muse":
        from .muse import MuseAdapter

        return MuseAdapter(_MuseClientProxy())
    if name == "hermes":
        from .hermes import HermesAdapter

        return HermesAdapter(None)
    return None


class _MuseClientProxy:
    """A minimal MSP client proxy.

    The real MSP client is created by the Muse-specific entry point, which
    owns the `muse serve` subprocess. This proxy keeps the adapter
    constructible for capability inspection.
    """

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("the Muse MSP client is not connected")


if __name__ == "__main__":
    raise SystemExit(main())
