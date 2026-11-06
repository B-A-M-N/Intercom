"""A newline-delimited JSON-RPC 2.0 client over a subprocess's stdio.

This is the transport every real harness client in this package shares
(Muse MSP, Codex app-server, ACP agents such as OpenCode and Hermes).
All three speak the same framing: one JSON object per line on stdin,
responses and notifications interleaved on stdout, and server-initiated
requests that the client must answer (for example ACP permission
requests). The framing was verified against each binary rather than
assumed: initialize/initialized, session or thread creation, and turn
submission were all driven over exactly this transport.

The client never invents a result: a failed call raises RPCError with
the wire error, and a dead process fails its in-flight calls instead of
leaving them hanging.
"""

from __future__ import annotations

import json
import subprocess
import threading
from typing import Any, Callable, Optional


class RPCError(RuntimeError):
    """A JSON-RPC error returned by the harness process."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"rpc error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class StdioRPCClient:
    """JSON-RPC 2.0 client over a child process's stdin and stdout."""

    def __init__(
        self,
        argv: list[str],
        env: Optional[dict[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> None:
        self._argv = list(argv)
        self._env = env
        self._cwd = cwd
        self._proc: Optional[subprocess.Popen[str]] = None
        self._next_id = 0
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._pending: dict[Any, tuple[threading.Event, dict[str, Any]]] = {}
        self._closed = False
        self._reader: Optional[threading.Thread] = None
        self._notifications: list[Callable[[str, Any], None]] = []
        self._request_handler: Optional[Callable[[str, Any], Any]] = None

    # -- lifecycle --------------------------------------------------------

    def start(self) -> None:
        """Spawn the harness process and start the reader thread."""
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
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def close(self) -> None:
        """Close stdin and terminate the child, failing in-flight calls."""
        if self._closed:
            return
        self._closed = True
        proc, self._proc = self._proc, None
        if proc is not None:
            if proc.stdin is not None:
                try:
                    proc.stdin.close()
                except OSError:
                    pass
            if proc.stdout is not None:
                try:
                    proc.stdout.close()
                except OSError:
                    pass
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        with self._lock:
            pending, self._pending = self._pending, {}
        for event, slot in pending.values():
            slot["error"] = {"code": -32000, "message": "harness process closed"}
            event.set()

    def __enter__(self) -> "StdioRPCClient":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- handlers ---------------------------------------------------------

    def on_notification(self, handler: Callable[[str, Any], None]) -> None:
        """Register a handler for server-initiated notifications."""
        self._notifications.append(handler)

    def on_request(self, handler: Callable[[str, Any], Any]) -> None:
        """Register the handler for server-initiated requests.

        The handler returns the result object; raising RPCError returns
        that error on the wire. With no handler registered a server
        request is answered method-not-found, which is what keeps a
        client that cannot answer approvals operable rather than hung.
        """
        self._request_handler = handler

    # -- requests ---------------------------------------------------------

    def call(self, method: str, params: Any = None, timeout: float = 60.0) -> Any:
        """Send a request and return its result, or raise RPCError."""
        if self._proc is None:
            raise RuntimeError("stdio RPC client is not started")
        with self._lock:
            self._next_id += 1
            request_id = self._next_id
            event = threading.Event()
            slot: dict[str, Any] = {}
            self._pending[request_id] = (event, slot)

        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        self._write(payload)

        if not event.wait(timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            raise TimeoutError(f"rpc call {method!r} timed out after {timeout}s")
        with self._lock:
            self._pending.pop(request_id, None)
        if slot.get("error") is not None:
            error = slot["error"]
            raise RPCError(
                int(error.get("code", -32000)),
                str(error.get("message", "unknown error")),
                error.get("data"),
            )
        return slot.get("result")

    def notify(self, method: str, params: Any = None) -> None:
        """Send a notification, which expects no response."""
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        self._write(payload)

    # -- internals --------------------------------------------------------

    def _write(self, payload: dict[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise RuntimeError("stdio RPC client is not started")
        data = json.dumps(payload, separators=(",", ":")) + "\n"
        with self._write_lock:
            self._proc.stdin.write(data)
            self._proc.stdin.flush()

    def _read_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        for line in proc.stdout:
            if self._closed:
                break
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except ValueError:
                continue
            self._dispatch(message)
        self.close()

    def _dispatch(self, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        method = message.get("method")

        # A response to one of our calls.
        if request_id is not None and ("result" in message or "error" in message):
            with self._lock:
                pending = self._pending.get(request_id)
            if pending is None:
                return
            event, slot = pending
            if message.get("error") is not None:
                slot["error"] = message["error"]
            else:
                slot["result"] = message.get("result")
            event.set()
            return

        if method is None:
            return

        # A server-initiated request: it carries an id and expects a
        # response on the same stream.
        if request_id is not None:
            if self._request_handler is None:
                self._respond(
                    request_id,
                    error={"code": -32601, "message": f"method {method!r} not found"},
                )
                return
            try:
                result = self._request_handler(method, message.get("params"))
            except RPCError as exc:
                self._respond(
                    request_id,
                    error={"code": exc.code, "message": exc.message, "data": exc.data},
                )
            except Exception as exc:  # a broken handler must not kill the reader
                self._respond(
                    request_id,
                    error={"code": -32603, "message": f"internal error: {exc}"},
                )
            else:
                self._respond(request_id, result=result)
            return

        # A notification.
        for handler in list(self._notifications):
            try:
                handler(method, message.get("params"))
            except Exception:
                pass

    def _respond(self, request_id: Any, result: Any = None, error: Any = None) -> None:
        if self._closed:
            return
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result
        try:
            self._write(payload)
        except RuntimeError:
            pass


__all__ = ["RPCError", "StdioRPCClient"]
