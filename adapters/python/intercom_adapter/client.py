"""JSON-RPC client for the Intercom host.

This is a deliberately small, dependency-free client so that the adapter
host runs anywhere Python runs, without needing an ACP SDK. It speaks the
same newline-delimited JSON-RPC 2.0 framing as the Go transport.
"""

from __future__ import annotations

import json
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


class IntercomError(RuntimeError):
    """A JSON-RPC error returned by the Intercom host."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"intercom error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


@dataclass
class PendingCall:
    """One in-flight request."""

    event: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: Optional[dict] = None


class IntercomClient:
    """A JSON-RPC client over the local Intercom socket.

    The socket is a Unix domain socket, so the connection is authenticated by
    the kernel's peer credentials. The client never asserts its own identity
    in the protocol payload.
    """

    def __init__(self, socket_path: str) -> None:
        self._socket_path = socket_path
        self._sock: Optional[socket.socket] = None
        self._next_id = 0
        self._pending: dict[int, PendingCall] = {}
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._reader: Optional[threading.Thread] = None
        self._closed = False
        self._notifications: list[Callable[[str, Any], None]] = []

    # -- connection -------------------------------------------------------

    def connect(self, timeout: float = 10.0) -> None:
        """Open the socket and start the reader thread."""
        if self._sock is not None:
            return
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(self._socket_path)
        # Long-lived reads must not inherit the connect timeout: restore
        # blocking behavior so an idle connection does not die after ~10s.
        sock.settimeout(None)
        # The host frames on newlines, so the socket is a byte stream.
        self._sock = sock
        self._closed = False
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def is_connected(self) -> bool:
        """Whether this client believes its socket is live."""
        return self._sock is not None and not self._closed

    def reconnect(self, timeout: float = 10.0) -> None:
        """Close and reopen the socket (caller re-registers after this)."""
        self.close()
        # close() sets _closed; connect() resets it on success.
        self.connect(timeout=timeout)

    def close(self) -> None:
        """Close the connection and fail every in-flight call."""
        self._closed = True
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        with self._lock:
            for pending in self._pending.values():
                pending.error = {"code": -32000, "message": "connection closed"}
                pending.event.set()
            self._pending.clear()

    def __enter__(self) -> "IntercomClient":
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- notifications ----------------------------------------------------

    def on_notification(self, handler: Callable[[str, Any], None]) -> None:
        """Register a handler for server-initiated notifications."""
        self._notifications.append(handler)

    # -- requests ---------------------------------------------------------

    def call(self, method: str, params: Any = None, timeout: float = 30.0) -> Any:
        """Send a request and wait for its response."""
        with self._lock:
            self._next_id += 1
            request_id = self._next_id
            pending = PendingCall()
            self._pending[request_id] = pending

        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params

        try:
            self._send(payload)
        except Exception:
            with self._lock:
                self._pending.pop(request_id, None)
            raise

        if not pending.event.wait(timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            raise TimeoutError(f"intercom call {method!r} timed out after {timeout}s")

        with self._lock:
            self._pending.pop(request_id, None)

        if pending.error is not None:
            raise IntercomError(
                pending.error.get("code", -32000),
                pending.error.get("message", "unknown error"),
                pending.error.get("data"),
            )
        return pending.result

    def notify(self, method: str, params: Any = None) -> None:
        """Send a notification, which expects no response."""
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        self._send(payload)

    # -- internals --------------------------------------------------------

    def _send(self, payload: dict) -> None:
        if self._sock is None:
            raise RuntimeError("intercom client is not connected")
        data = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
        # Heartbeat and delivery threads share this client: serialize whole
        # frames so concurrent calls never interleave bytes.
        with self._write_lock:
            if self._sock is None:
                raise RuntimeError("intercom client is not connected")
            self._sock.sendall(data)

    def _read_loop(self) -> None:
        buffer = b""
        while not self._closed:
            try:
                if self._sock is None:
                    break
                chunk = self._sock.recv(65536)
            except OSError:
                break
            if not chunk:
                break
            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    message = json.loads(line.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    continue
                self._dispatch(message)
        # Wake any waiter blocked on a connection that just died.
        self.close()

    def _dispatch(self, message: dict) -> None:
        if "id" in message and ("result" in message or "error" in message):
            request_id = message.get("id")
            with self._lock:
                pending = self._pending.get(request_id)
            if pending is None:
                return
            if "error" in message and message["error"] is not None:
                pending.error = message["error"]
            else:
                pending.result = message.get("result")
            pending.event.set()
            return

        method = message.get("method")
        if method:
            for handler in list(self._notifications):
                try:
                    handler(method, message.get("params"))
                except Exception:  # a broken handler must not kill the reader
                    pass


class StdioIntercomClient:
    """A JSON-RPC client over a subprocess's stdin and stdout.

    This is the adapter-host transport: an Intercom adapter process talks to
    the Go host over stdio, which keeps the host's Unix socket private to the
    local host process.
    """

    def __init__(self, argv: list[str], env: Optional[dict[str, str]] = None) -> None:
        self._proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            env=env,
            text=True,
            bufsize=1,
        )
        self._next_id = 0
        self._pending: dict[int, PendingCall] = {}
        self._lock = threading.Lock()
        self._closed = False
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def close(self) -> None:
        self._closed = True
        if self._proc.stdin is not None:
            try:
                self._proc.stdin.close()
            except OSError:
                pass
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()

    def call(self, method: str, params: Any = None, timeout: float = 30.0) -> Any:
        with self._lock:
            self._next_id += 1
            request_id = self._next_id
            pending = PendingCall()
            self._pending[request_id] = pending

        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params

        assert self._proc.stdin is not None
        self._proc.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self._proc.stdin.flush()

        if not pending.event.wait(timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            raise TimeoutError(f"intercom call {method!r} timed out after {timeout}s")

        with self._lock:
            self._pending.pop(request_id, None)

        if pending.error is not None:
            raise IntercomError(
                pending.error.get("code", -32000),
                pending.error.get("message", "unknown error"),
                pending.error.get("data"),
            )
        return pending.result

    def _read_loop(self) -> None:
        assert self._proc.stdout is not None
        for line in self._proc.stdout:
            if self._closed:
                break
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except ValueError:
                continue
            request_id = message.get("id")
            if request_id is None:
                continue
            with self._lock:
                pending = self._pending.get(request_id)
            if pending is None:
                continue
            if message.get("error"):
                pending.error = message["error"]
            else:
                pending.result = message.get("result")
            pending.event.set()


def wait_for_socket(path: str, timeout: float = 10.0) -> bool:
    """Wait until the Intercom socket accepts connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(path)
            sock.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False
