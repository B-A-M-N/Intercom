"""Unit tests for the new harness adapters and the shared clients.

These tests use fakes for the harness side: they pin the capability
honesty rules (what each adapter may claim) and the wire shapes the
adapters produce. They deliberately do not validate the harness wire
protocols — that is what the live tests do, against the real binaries,
with no inference spend (see test_live_protocols.py).
"""

from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from intercom_adapter.claude import ClaudeAdapter  # noqa: E402
from intercom_adapter.codex import CodexAdapter  # noqa: E402
from intercom_adapter.harness import (  # noqa: E402
    MODE_NOTIFY,
    MODE_QUEUE,
    MODE_STEER,
    RECEIPT_DELIVERED,
    RECEIPT_INJECTED,
    RECEIPT_PRESENTED,
    RECEIPT_REJECTED,
    SUPPORT_NATIVE,
    SUPPORT_UNSUPPORTED,
)
from intercom_adapter.ids import as_uuid7, harness_command_id, uuid7  # noqa: E402
from intercom_adapter.mcp_consumer import MCPHarnessAdapter  # noqa: E402
from intercom_adapter.muse import MuseAdapter  # noqa: E402
from intercom_adapter.opencode import OpenCodeAdapter  # noqa: E402
from intercom_adapter.stdio_rpc import RPCError, StdioRPCClient  # noqa: E402


class TestIDs(unittest.TestCase):
    def test_uuid7_shape(self) -> None:
        value = uuid7()
        parsed = as_uuid7(value)
        self.assertEqual(parsed, value)

    def test_harness_command_id_preserves_intercom_uuid7(self) -> None:
        inner = uuid7()
        self.assertEqual(harness_command_id(f"cmd_{inner}"), inner)
        # Idempotent: the same Intercom command maps to the same
        # harness command, so a retried delivery does not mint a second
        # harness-side idempotency key.
        self.assertEqual(
            harness_command_id(f"cmd_{inner}"),
            harness_command_id(f"cmd_{inner}"),
        )

    def test_harness_command_id_mints_for_opaque_ids(self) -> None:
        minted = harness_command_id("cmd_1")
        self.assertEqual(as_uuid7(minted), minted)


class TestStdioRPCClient(unittest.TestCase):
    """The shared transport, against a tiny in-process fake server."""

    FAKE_SERVER = r"""
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    if "id" not in msg:
        continue
    if msg["method"] == "fail/now":
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"error":{"code":-32042,"message":"nope","data":{"k":1}}}), flush=True)
    elif msg["method"] == "notify/me":
        print(json.dumps({"jsonrpc":"2.0","method":"server/note","params":{"hello":"world"}}), flush=True)
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{"ok":True}}), flush=True)
    else:
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{"echo":msg.get("params")}}), flush=True)
"""

    def make_client(self) -> StdioRPCClient:
        client = StdioRPCClient([sys.executable, "-c", self.FAKE_SERVER])
        client.start()
        self.addCleanup(client.close)
        return client

    def test_call_round_trip(self) -> None:
        client = self.make_client()
        self.assertEqual(client.call("ping", {"a": 1}), {"echo": {"a": 1}})

    def test_error_raises_with_code(self) -> None:
        client = self.make_client()
        with self.assertRaises(RPCError) as ctx:
            client.call("fail/now", {})
        self.assertEqual(ctx.exception.code, -32042)
        self.assertIn("nope", str(ctx.exception))

    def test_notification_dispatch(self) -> None:
        client = self.make_client()
        seen: list[tuple[str, object]] = []
        client.on_notification(lambda method, params: seen.append((method, params)))
        client.call("notify/me", {})
        self.assertIn(("server/note", {"hello": "world"}), seen)


class TestCodexAdapter(unittest.TestCase):
    class FakeCodex:
        def __init__(self, fail=()):
            self.calls: list[tuple[str, dict]] = []
            self._fail = set(fail)
            self._handlers = []

        def on_notification(self, handler):
            self._handlers.append(handler)

        def call(self, method, params, timeout=60.0):
            self.calls.append((method, params))
            if method in self._fail:
                raise RuntimeError(f"{method} failed")
            if method == "turn/steer":
                return {"turn": {"id": params["expectedTurnId"]}}
            if method == "turn/start":
                return {"turn": {"id": "turn_new"}}
            return {}

    def test_claims_native_steer_and_queue(self) -> None:
        caps = {c.id: c for c in CodexAdapter(self.FakeCodex()).capabilities}
        self.assertEqual(caps["delivery.steer"].support, SUPPORT_NATIVE)
        self.assertEqual(caps["delivery.queue"].support, SUPPORT_NATIVE)

    def test_steer_uses_expected_turn_id(self) -> None:
        codex = self.FakeCodex()
        adapter = CodexAdapter(codex)
        adapter.note_turn("ses_x", "turn_1")
        result = adapter.deliver(
            "ses_x",
            {"delivery": {"mode": MODE_STEER}, "body": {"text": "steer me"}},
        )
        self.assertEqual(result.status, RECEIPT_INJECTED)
        method, params = codex.calls[0]
        self.assertEqual(method, "turn/steer")
        self.assertEqual(params["expectedTurnId"], "turn_1")
        self.assertTrue(params["input"][0]["text"].startswith("[PEER MESSAGE"))
        self.assertIn("steer me", params["input"][0]["text"])

    def test_steer_without_a_turn_is_refused_without_a_call(self) -> None:
        codex = self.FakeCodex()
        adapter = CodexAdapter(codex)
        result = adapter.deliver(
            "ses_x",
            {"delivery": {"mode": MODE_STEER}, "body": {"text": "x"}},
        )
        self.assertEqual(result.status, RECEIPT_REJECTED)
        self.assertEqual(codex.calls, [])

    def test_steer_failure_is_rejected_not_retried_as_a_turn(self) -> None:
        codex = self.FakeCodex(fail={"turn/steer"})
        adapter = CodexAdapter(codex)
        adapter.note_turn("ses_x", "turn_1")
        result = adapter.deliver(
            "ses_x",
            {"delivery": {"mode": MODE_STEER}, "body": {"text": "x"}},
        )
        self.assertEqual(result.status, RECEIPT_REJECTED)
        self.assertEqual([m for m, _ in codex.calls], ["turn/steer"])

    def test_queue_uses_turn_start(self) -> None:
        codex = self.FakeCodex()
        adapter = CodexAdapter(codex)
        result = adapter.deliver(
            "ses_x",
            {"delivery": {"mode": MODE_QUEUE}, "body": {"text": "later"}},
        )
        self.assertEqual(result.status, RECEIPT_DELIVERED)
        self.assertEqual(codex.calls[0][0], "turn/start")

    def test_notify_is_presented_not_injected(self) -> None:
        import tempfile
        adapter = CodexAdapter(self.FakeCodex())
        adapter.set_state_dir(tempfile.mkdtemp(prefix="ic-test-"))
        result = adapter.notify("ses_x", {"body": {"text": "FYI"}})
        self.assertEqual(result.status, RECEIPT_PRESENTED)
        self.assertNotEqual(result.status, RECEIPT_INJECTED)


class TestOpenCodeAdapter(unittest.TestCase):
    class FakeACP:
        def __init__(self):
            self.prompts: list[tuple[str, str]] = []
            self._handlers = []

        def on_notification(self, handler):
            self._handlers.append(handler)

        def prompt(self, session_id, text, timeout=300.0):
            self.prompts.append((session_id, text))
            return {"stopReason": "end_turn"}

    def test_steer_is_unsupported_with_a_limitation(self) -> None:
        caps = {c.id: c for c in OpenCodeAdapter(self.FakeACP()).capabilities}
        self.assertEqual(caps["delivery.steer"].support, SUPPORT_UNSUPPORTED)
        self.assertTrue(caps["delivery.steer"].limitations)
        self.assertEqual(caps["delivery.queue"].support, SUPPORT_NATIVE)

    def test_queue_prompts_and_reports_delivered(self) -> None:
        acp = self.FakeACP()
        adapter = OpenCodeAdapter(acp)
        result = adapter.deliver(
            "ses_x",
            {"delivery": {"mode": MODE_QUEUE}, "body": {"text": "next turn"}},
        )
        self.assertEqual(result.status, RECEIPT_DELIVERED)
        self.assertEqual(len(acp.prompts), 1)
        self.assertEqual(acp.prompts[0][0], "ses_x")
        self.assertTrue(acp.prompts[0][1].startswith("[PEER MESSAGE"))
        self.assertIn("next turn", acp.prompts[0][1])

    def test_deliver_without_a_client_is_refused(self) -> None:
        adapter = OpenCodeAdapter(None)
        result = adapter.deliver("ses_x", {"body": {"text": "x"}})
        self.assertEqual(result.status, RECEIPT_REJECTED)

    def test_session_starter_maps_ids(self) -> None:
        acp = self.FakeACP()
        adapter = OpenCodeAdapter(acp, session_starter=lambda sid: f"acp-{sid}")
        result = adapter.deliver("ses_x", {"body": {"text": "hi"}})
        self.assertEqual(result.status, RECEIPT_DELIVERED)
        self.assertEqual(len(acp.prompts), 1)
        self.assertEqual(acp.prompts[0][0], "acp-ses_x")
        self.assertIn("hi", acp.prompts[0][1])


class TestClaudeAdapter(unittest.TestCase):
    class FakeClaude:
        def __init__(self):
            self.sent: list[str] = []

        def send_text(self, text, timeout=300.0):
            self.sent.append(text)
            return {"type": "result", "is_error": False}

    def test_steer_is_unsupported_with_a_limitation(self) -> None:
        caps = {c.id: c for c in ClaudeAdapter(self.FakeClaude()).capabilities}
        self.assertEqual(caps["delivery.steer"].support, SUPPORT_UNSUPPORTED)
        self.assertTrue(caps["delivery.steer"].limitations)

    def test_queue_sends_a_user_message(self) -> None:
        claude = self.FakeClaude()
        adapter = ClaudeAdapter(claude)
        result = adapter.deliver("ses_x", {"body": {"text": "next turn"}})
        self.assertEqual(result.status, RECEIPT_DELIVERED)
        self.assertEqual(len(claude.sent), 1)
        self.assertTrue(claude.sent[0].startswith("[PEER MESSAGE"))
        self.assertIn("next turn", claude.sent[0])

    def test_notify_is_presented_not_injected(self) -> None:
        import tempfile
        adapter = ClaudeAdapter(self.FakeClaude())
        adapter.set_state_dir(tempfile.mkdtemp(prefix="ic-test-"))
        result = adapter.notify("ses_x", {"body": {"text": "FYI"}})
        self.assertEqual(result.status, RECEIPT_PRESENTED)
        self.assertNotEqual(result.status, RECEIPT_INJECTED)


class TestMuseSessionMapping(unittest.TestCase):
    class FakeMSP:
        def __init__(self):
            self.calls: list[tuple[str, dict]] = []

        def call(self, method, params):
            self.calls.append((method, params))
            return {"turnId": "turn_1", "status": "accepted", "disposition": "started", "startedNewTurn": True}

    def test_starter_maps_intercom_session_to_msp_session(self) -> None:
        msp = self.FakeMSP()
        adapter = MuseAdapter(msp, session_starter=lambda sid: "msp-session-1")
        result = adapter.deliver(
            "ses_intercom000000000000000001",
            {"commandId": "cmd_1", "delivery": {"mode": MODE_QUEUE}, "body": {"text": "hi"}},
        )
        self.assertEqual(result.status, RECEIPT_DELIVERED)
        method, params = msp.calls[0]
        self.assertEqual(method, "turn/start")
        self.assertEqual(params["sessionId"], "msp-session-1")
        # MSP requires a UUIDv7 commandId on the wire.
        self.assertEqual(as_uuid7(params["commandId"]), params["commandId"])

    def test_starter_failure_is_a_rejection_not_a_guess(self) -> None:
        def broken(_sid: str) -> str:
            raise RuntimeError("muse is down")

        adapter = MuseAdapter(self.FakeMSP(), session_starter=broken)
        result = adapter.deliver("ses_x", {"body": {"text": "hi"}})
        self.assertEqual(result.status, RECEIPT_REJECTED)


class TestMCPHarnessAdapter(unittest.TestCase):
    class FakeMCP:
        def __init__(self):
            self.calls: list[tuple[str, dict]] = []

        def tool(self, name, arguments):
            self.calls.append((name, arguments))
            if name == "inbox_list":
                return {"messages": [{"messageId": "msg_1", "body": {"text": "hi"}}]}
            return {}

    def test_capabilities_match_the_mcp_server_registration(self) -> None:
        # intercom-mcp registers notify/queue native and steer
        # unsupported for the same session; the scripted harness must
        # advertise exactly that, or peer routing would disagree.
        caps = {c.id: c for c in MCPHarnessAdapter(self.FakeMCP()).capabilities}
        self.assertEqual(caps["delivery.notify"].support, SUPPORT_NATIVE)
        self.assertEqual(caps["delivery.queue"].support, SUPPORT_NATIVE)
        self.assertEqual(caps["delivery.steer"].support, SUPPORT_UNSUPPORTED)
        self.assertTrue(caps["delivery.steer"].limitations)

    def test_deliver_claims_and_reports_presented_not_handled(self) -> None:
        mcp = self.FakeMCP()
        adapter = MCPHarnessAdapter(mcp)
        result = adapter.deliver("ses_x", {"messageId": "msg_1", "body": {"text": "hi"}})
        self.assertEqual(result.status, RECEIPT_PRESENTED)
        self.assertNotEqual(result.status, "handled")
        self.assertNotEqual(result.status, RECEIPT_INJECTED)
        # Handled is reported only through ack_handled, after the
        # participant has acted — never by the claim itself.
        adapter.ack_handled("msg_1", "acted")
        self.assertIn(("message_ack", {"messageId": "msg_1", "status": "handled", "detail": "acted"}), mcp.calls)

    def test_deliver_for_a_message_not_in_the_inbox_is_refused(self) -> None:
        adapter = MCPHarnessAdapter(self.FakeMCP())
        result = adapter.deliver("ses_x", {"messageId": "msg_other", "body": {"text": "hi"}})
        self.assertEqual(result.status, RECEIPT_REJECTED)


    def test_qualification_never_claims_steer_without_implementation(self) -> None:
        from intercom_adapter.hermes import HermesAdapter
        from intercom_adapter.muse import MuseAdapter
        from intercom_adapter.codex import CodexAdapter

        for cls, kwargs in ((MuseAdapter, {"msp_client": object()}), (CodexAdapter, {"codex_client": object()})):
            rows = {r["capability"]: r for r in cls(**kwargs).qualifications()}
            steer = rows["delivery.steer"]
            self.assertTrue(steer["advertised"] and steer["implemented"])
            self.assertTrue(steer["liveVerified"])

        rows = {r["capability"]: r for r in HermesAdapter().qualifications()}
        steer = rows["delivery.steer"]
        self.assertFalse(steer["advertised"] or steer["implemented"] or steer["liveVerified"])


if __name__ == "__main__":
    unittest.main()

    def test_qualification_never_claims_steer_without_implementation(self) -> None:
        from intercom_adapter.hermes import HermesAdapter
        from intercom_adapter.muse import MuseAdapter
        from intercom_adapter.codex import CodexAdapter

        # Muse/Codex steer: advertised + implemented + liveVerified.
        for cls, kwargs in ((MuseAdapter, {"msp_client": object()}), (CodexAdapter, {"codex_client": object()})):
            rows = {r["capability"]: r for r in cls(**kwargs).qualifications()}
            steer = rows["delivery.steer"]
            self.assertTrue(steer["advertised"] and steer["implemented"])
            self.assertTrue(steer["liveVerified"])

        # Hermes steer: not advertised, not implemented, not liveVerified.
        rows = {r["capability"]: r for r in HermesAdapter().qualifications()}
        steer = rows["delivery.steer"]
        self.assertFalse(steer["advertised"] or steer["implemented"] or steer["liveVerified"])
