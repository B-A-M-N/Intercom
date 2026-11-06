"""Live protocol tests against the real harness binaries.

These tests drive each harness's real wire protocol — Muse MSP,
Codex app-server, OpenCode ACP — and assert the shapes the adapters
rely on. They are deliberately inference-free:

- Muse runs with `--provider echo`: initialize, session/start, and
  a turn/start whose echo reply is asserted. No credentials, no spend.
- Codex runs initialize + thread/start (ephemeral): starting a thread
  makes no model call.
- OpenCode runs initialize + session/new: creating a session makes
  no model call. (It is slow, ~10s, because OpenCode loads its model
  catalog.)

A test skips when its binary is not installed, rather than pretending
to have verified anything. Real model turns are exercised separately
in the live cross-harness verification (docs/HARNESSES.md) and are
not part of this suite, so the suite never spends inference.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from intercom_adapter.acp_client import create_acp_client  # noqa: E402
from intercom_adapter.codex_client import create_codex_client  # noqa: E402
from intercom_adapter.ids import uuid7  # noqa: E402
from intercom_adapter.msp import create_msp_client  # noqa: E402


def _which(*names: str) -> str | None:
    for name in names:
        found = shutil.which(name)
        if found:
            return found
    return None


class TestLiveMuseMSP(unittest.TestCase):
    """Muse MSP over `muse serve --provider echo` (via the musefi wrapper)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.muse_bin = _which("musefi", "muse", "openmuse")
        if cls.muse_bin is None:
            raise unittest.SkipTest("no Muse wrapper binary on PATH")
        cls.workspace = tempfile.mkdtemp(prefix="ic-muse-live-")

    def test_echo_turn_completes(self) -> None:
        client = create_msp_client(
            muse_bin=self.muse_bin, provider="echo", cwd=self.workspace
        )
        client.start()
        self.addCleanup(client.close)

        completed: list[dict] = []
        texts: list[str] = []
        done = threading.Event()

        def on_notification(method: str, params: object) -> None:
            if not isinstance(params, dict):
                return
            if method == "turn/completed":
                completed.append(params)
                done.set()
            if method == "item/delta" and params.get("field") == "text":
                texts.append(str(params.get("delta", "")))
            if method == "item/completed":
                item = params.get("item") or {}
                if isinstance(item, dict) and item.get("kind") == "agentMessage":
                    texts.append(str(item.get("text", "")))

        client.on_notification(on_notification)

        session = client.start_session(workspace_root=self.workspace)
        session_id = session["sessionId"]
        self.assertEqual(session.get("providerId"), "echo")
        self.assertEqual(session.get("status"), "idle")

        result = client.call(
            "turn/start",
            {
                "commandId": uuid7(),
                "sessionId": session_id,
                "input": [{"type": "text", "text": "hello echo probe"}],
            },
        )
        self.assertIn(result.get("turnId"), (session_id, result.get("turnId")))
        self.assertTrue(result.get("turnId"))

        self.assertTrue(done.wait(timeout=30), "echo turn did not complete")
        self.assertTrue(completed, "no turn/completed notification")
        joined = "".join(texts)
        self.assertIn("echo: hello echo probe", joined)

    def test_steer_with_a_wrong_turn_is_rejected_by_muse(self) -> None:
        """The failure semantics steer honesty depends on, live."""
        client = create_msp_client(
            muse_bin=self.muse_bin, provider="echo", cwd=self.workspace
        )
        client.start()
        self.addCleanup(client.close)
        session = client.start_session(workspace_root=self.workspace)
        with self.assertRaises(Exception) as ctx:
            client.call(
                "turn/steer",
                {
                    "commandId": uuid7(),
                    "sessionId": session["sessionId"],
                    "expectedTurnId": uuid7(),
                    "input": [{"type": "text", "text": "steer into nothing"}],
                },
            )
        self.assertIn("turn/steer", str(ctx.exception))


class TestLiveCodexAppServer(unittest.TestCase):
    """Codex app-server: initialize + thread/start, no inference."""

    def test_initialize_and_thread_start(self) -> None:
        codex_bin = _which("codex")
        if codex_bin is None:
            raise unittest.SkipTest("codex binary not on PATH")
        workspace = tempfile.mkdtemp(prefix="ic-codex-live-")
        client = create_codex_client(codex_bin=codex_bin, cwd=workspace)
        client.start()
        self.addCleanup(client.close)
        # The userAgent echoes our clientInfo name and the Codex version,
        # e.g. "intercom_adapter/0.166.0 (...)".
        self.assertIn("intercom_adapter/", client.user_agent)

        thread = client.start_thread(cwd=workspace)
        self.assertTrue(thread.get("id"))
        status = thread.get("status") or {}
        self.assertEqual(status.get("type"), "idle")


class TestLiveOpenCodeACP(unittest.TestCase):
    """OpenCode ACP: initialize + session/new, no inference."""

    def test_initialize_and_session_new(self) -> None:
        opencode_bin = _which("opencode")
        if opencode_bin is None:
            raise unittest.SkipTest("opencode binary not on PATH")
        workspace = tempfile.mkdtemp(prefix="ic-oc-live-")
        client = create_acp_client([opencode_bin, "acp"], cwd=workspace)
        client.start()
        self.addCleanup(client.close)
        self.assertEqual(client.agent_info.get("name"), "OpenCode")

        session_id = client.new_session(cwd=workspace, timeout=90.0)
        self.assertTrue(session_id.startswith("ses_"))


if __name__ == "__main__":
    unittest.main()
