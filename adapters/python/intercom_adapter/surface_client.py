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
import queue
import threading
import subprocess
from collections import deque
from typing import Any, Callable, Iterator, Optional


class SurfaceError(RuntimeError):
    """A surface bridge request failed."""


class SurfaceBridge:
    """NDJSON client for intercom-surface (stdio)."""

    def __init__(self, argv: list[str]) -> None:
        self._argv = argv
        self._proc: Optional[subprocess.Popen] = None
        self._next_id = 0
        self._events: queue.Queue = queue.Queue()
        self._reader_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._pending: dict[int, queue.Queue] = {}
        self._cursor = 0

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
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

    def _read_loop(self) -> None:
        assert self._proc and self._proc.stdout
        for line in self._proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except Exception:
                continue
            if parsed.get("event") == "display":
                self._events.put(parsed)
                continue
            rid = parsed.get("id")
            try:
                iid = int(rid)
            except Exception:
                continue
            with self._lock:
                q = self._pending.get(iid)
            if q is not None:
                q.put(parsed)

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
        assert self._proc and self._proc.stdin
        with self._lock:
            self._next_id += 1
            rid = self._next_id
            q: queue.Queue = queue.Queue()
            self._pending[rid] = q
        req = {"op": op, "id": rid, "args": args}
        self._proc.stdin.write(json.dumps(req) + "\n")
        self._proc.stdin.flush()
        try:
            resp = q.get(timeout=30)
        except queue.Empty:
            raise SurfaceError(f"{op} timed out")
        finally:
            with self._lock:
                self._pending.pop(rid, None)
        if resp.get("isError"):
            raise SurfaceError(f"{op} failed: {resp.get('error')}")
        result = resp.get("result")
        return result if isinstance(result, dict) else {"result": result}

    def replay(self, cursor: int = 0, limit: int = 100) -> tuple[list[dict[str, Any]], int]:
        """Fetch displayable events after cursor. Non-destructive."""
        out = self._request("surface/replay", {"afterPosition": cursor, "limit": limit})
        events = out.get("events") or []
        self._cursor = int(out.get("cursor") or cursor)
        return list(events), self._cursor

    def present(self, message_id: str) -> dict[str, Any]:
        """Confirm actual rendering. This is the sole source of presented."""
        return self._request("surface/present", {"messageId": message_id})

    def reply(self, message_id: str, text: str, command_id: str = "") -> dict[str, Any]:
        """Submit an authorized human reply (requires reply scope).

        command_id must be provided by the caller and retained across
        retries so a lost response does not create a second reply. If
        omitted, a stable id is generated once per (message_id,text) pair
        and retained for the life of this bridge instance.
        """
        from intercom_adapter.ids import uuid7
        if not command_id:
            if not hasattr(self, "_reply_ids"):
                self._reply_ids = {}  # type: ignore[attr-defined]
            key = (message_id, text)
            if key not in self._reply_ids:  # type: ignore[attr-defined]
                self._reply_ids[key] = uuid7()  # type: ignore[attr-defined]
            command_id = self._reply_ids[key]  # type: ignore[attr-defined]
        return self._request("surface/reply", {"messageId": message_id, "text": text, "commandId": command_id})

    def drain_events(self) -> list[dict[str, Any]]:
        out = []
        while True:
            try:
                out.append(self._events.get_nowait())
            except queue.Empty:
                break
        return out

    def poll_events(self, timeout: float = 1.0) -> Optional[dict[str, Any]]:
        try:
            return self._events.get(timeout=timeout)
        except queue.Empty:
            return None

    def subscribe(self, on_event: Callable[[dict[str, Any]], None], stop: threading.Event, timeout: float = 1.0) -> int:
        """Consume display events continuously into callback. Returns count."""
        count = 0
        while not stop.is_set():
            ev = self.poll_events(timeout)
            if ev is not None:
                on_event(ev)
                count += 1
        return count

    def display_events(self, cursor: int = 0, limit: int = 100) -> Iterator[dict[str, Any]]:
        """Yield display events once (polling underlying replay)."""
        events, next_cursor = self.replay(cursor, limit)
        for ev in events:
            yield {"event": "display", "message": ev, "cursor": next_cursor}


def render_status_line(message: dict[str, Any]) -> str:
    """Thin TUI/IDE status-line renderer: one line, never claims."""
    body = (message.get("body") or {}).get("text") or ""
    return f"{message.get('from','peer')}: {body} [{message.get('status','unknown')}]"
