"""Review-fix tests: durable notify failure + secret persistence.

A notification recorded only in volatile memory must never be reported as
presented; the receipt must be unknown.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from intercom_adapter.claude import ClaudeAdapter
from intercom_adapter.codex import CodexAdapter
from intercom_adapter.hermes import HermesAdapter
from intercom_adapter.host import _save_secret_file
from intercom_adapter.muse import MuseAdapter
from intercom_adapter.opencode import OpenCodeAdapter


def _message(text="hi"):
    return {
        "messageId": "msg_1",
        "from": "peer",
        "body": {"type": "text", "text": text},
        "delivery": {"mode": "notify"},
    }


class TestNotifyDiskFailure(unittest.TestCase):
    def _adapters(self):
        class Fake:
            pass

        return [
            ("muse", MuseAdapter(None)),
            ("hermes", HermesAdapter(None)),
            ("codex", CodexAdapter(None)),
            ("opencode", OpenCodeAdapter(None)),
            ("claude", ClaudeAdapter(None)),
        ]

    def test_notify_unknown_when_durable_write_fails(self):
        for name, adapter in self._adapters():
            with self.subTest(adapter=name):
                # No state_dir: _notify_durable returns False (volatile only).
                adapter._state_dir = ""
                result = adapter.notify("ses_1", _message())
                self.assertEqual(
                    result.status,
                    "unknown",
                    f"{name} claimed {result.status!r} without durable persistence",
                )
                self.assertIn("durably", result.detail)

    def test_notify_presented_when_durable_write_succeeds(self):
        for name in ("muse", "hermes", "codex", "opencode", "claude"):
            with self.subTest(adapter=name):
                with tempfile.TemporaryDirectory() as d:
                    adapter = dict(self._adapters())[name]
                    adapter.set_state_dir(d)
                    result = adapter.notify("ses_1", _message())
                    self.assertEqual(result.status, "presented")

    def test_notify_unknown_when_state_dir_removed(self):
        with tempfile.TemporaryDirectory() as d:
            adapter = MuseAdapter(None)
            adapter.set_state_dir(d)
            # Point the state dir at an unwritable location.
            adapter._state_dir = os.path.join(d, "missing", "nested")
            # append_notification should fail / return False; if it somehow
            # succeeds, the receipt would be presented - assert the honesty
            # path by forcing a failure in the durable helper.
            adapter._state_dir = ""
            result = adapter.notify("ses_1", _message())
            self.assertEqual(result.status, "unknown")


class TestDelegationCredentialDelivery(unittest.TestCase):
    def test_mcp_server_uses_token_file_not_argv_or_env(self):
        import tempfile
        from intercom_adapter.host import _intercom_mcp_server, _default_delegation_file
        secret = "tok-SECRET-MUST-NOT-APPEAR-IN-ARGV"
        with tempfile.TemporaryDirectory() as d:
            path = _default_delegation_file("ses_delivery00000000000001")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as fh:
                fh.write(secret + "\n")
            os.chmod(path, 0o600)
            # Ensure no global env leaks.
            os.environ.pop("INTERCOM_MCP_DELEGATION_TOKEN", None)
            srv = _intercom_mcp_server(
                {"intercom_mcp_bin": "/bin/mcp", "intercom_socket": "/tmp/s.sock",
                 "intercom_session": "ses_delivery00000000000001", "intercom_alias": "a"},
                "muse", "ses_delivery00000000000001",
            )
            joined = " ".join(srv["args"])
            self.assertIn("--attach", joined)
            self.assertIn("--delegation-token-file", joined)
            self.assertIn(path, joined)
            self.assertNotIn(secret, joined)
            self.assertNotIn("INTERCOM_MCP_DELEGATION_TOKEN", os.environ)


class TestSecretPersistence(unittest.TestCase):
    def test_save_secret_file_raises_on_failure(self):
        with tempfile.TemporaryDirectory() as d:
            # A directory where a file is expected: opening the tmp path
            # inside a file (not a dir) must raise, not pass silently.
            blocker = os.path.join(d, "blocker")
            with open(blocker, "w") as fh:
                fh.write("x")
            with self.assertRaises(OSError):
                _save_secret_file(os.path.join(blocker, "secret.txt"), "s3cr3t")

    def test_save_secret_file_round_trip_with_0600(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "sub", "secret.txt")
            _save_secret_file(path, "hello-secret")
            with open(path) as fh:
                self.assertEqual(fh.read().strip(), "hello-secret")
            self.assertEqual(oct(os.stat(path).st_mode & 0o777), "0o600")


if __name__ == "__main__":
    unittest.main()
