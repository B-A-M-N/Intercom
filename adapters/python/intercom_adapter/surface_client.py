"""Surface Bridge driver for existing UIs (TUI, IDE, web).

Thin NDJSON client for intercom-surface. It renders peer-neighbour events
without claiming the agent inbox. Truth rule: a durable write alone never
means presented; only surface/present can move the receipt to presented.

Truth table:
  delivered  Durably accepted; human may not have seen it.
  presented  A connected visual surface confirmed rendering.
  handled    A human or recipient explicitly acted.
  unknown    Presentation or handling could not be established.
"""

from __future__ import annotations

import json
import subprocess
from typing import Any, Iterator, Optional


class SurfaceError(RuntimeError):
    """A surface bridge request failed."""


class SurfaceBridge:
    """NDJSON client for intercom-surface (stdio)."""

    def __init__(self, argv: list[str]) -> None:
        self._argv = argv
        self._proc: Optional[subprocess.Popen] = None
        self._next_id = 0

    def start(self) -> None:
        if self._proc is not None:
            return
        self._proc = subprocess.Popen(
            self._argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def close(self) -> None:
        if self._proc is not None:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=5)
            except Exception:
                pass
            self._proc = None

    def _request(self, op: str, args: dict[str, Any]) -> dict[str, Any]:
        self.start()
        assert self._proc and self._proc.stdin and self._proc.stdout
        self._next_id += 1
        req = {"op": op, "id": self._next_id, "args": args}
        self._proc.stdin.write(json.dumps(req) + "\n")
        self._proc.stdin.flush()
        # interleaved display events ({"event":"display"}) may arrive first.
        resp = None
        while True:
            line = self._proc.stdout.readline()
            if not line:
                err = self._proc.stderr.read() if self._proc.stderr else ""
                raise SurfaceError(f"intercom-surface exited: {err.strip()[-500:]}")
            parsed = json.loads(line)
            if parsed.get("event") == "display":
                continue
            if parsed.get("op") != op or parsed.get("id") != self._next_id:
                continue
            resp = parsed
            break
        if resp.get("isError"):
            raise SurfaceError(f"{op} failed: {resp.get('error')}")
        result = resp.get("result")
        return result if isinstance(result, dict) else {"result": result}

    def replay(self, cursor: int = 0, limit: int = 100) -> tuple[list[dict[str, Any]], int]:
        """Fetch displayable events after cursor. Non-destructive."""
        out = self._request("surface/replay", {"afterPosition": cursor, "limit": limit})
        events = out.get("events") or []
        return list(events), int(out.get("cursor") or cursor)

    def present(self, message_id: str) -> dict[str, Any]:
        """Confirm actual rendering. This is the sole source of presented."""
        return self._request("surface/present", {"messageId": message_id})

    def reply(self, message_id: str, text: str, command_id: str = "") -> dict[str, Any]:
        """Submit an authorized human reply (requires reply scope)."""
        args: dict[str, Any] = {"messageId": message_id, "text": text}
        if command_id:
            args["commandId"] = command_id
        return self._request("surface/reply", args)

    def display_events(self, cursor: int = 0, limit: int = 100) -> Iterator[dict[str, Any]]:
        """Yield display events once (polling underlying replay)."""
        events, next_cursor = self.replay(cursor, limit)
        for ev in events:
            yield {"event": "display", "message": ev, "cursor": next_cursor}


def render_status_line(message: dict[str, Any]) -> str:
    """Thin TUI/IDE status-line renderer: one line, never claims."""
    body = (message.get("body") or {}).get("text") or ""
    return f"{message.get('from','peer')}: {body} [{message.get('status','unknown')}]"
