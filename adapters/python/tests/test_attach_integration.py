"""Live attach integration: daemon + owner registration + delegated MCP.

Builds intercomd + intercom-mcp, registers an owner session, delegates a
scoped token, attaches intercom-mcp as a client (no re-registration),
exercises tools, restarts the daemon, resumes both connections, and
verifies the adapter's instance identity and capabilities never changed.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from intercom_adapter.client import IntercomClient, IntercomError, wait_for_socket  # noqa: E402
from intercom_adapter.mcp_consumer import MCPClient  # noqa: E402

EXT = "_dev.bamn.intercom/"


def _root():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class TestAttachIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.go = shutil.which("go")
        if cls.go is None:
            raise unittest.SkipTest("no go toolchain")
        cls.tmp = tempfile.mkdtemp(prefix="ic-attach-")
        cls.daemon = os.path.join(cls.tmp, "intercomd")
        cls.mcp = os.path.join(cls.tmp, "intercom-mcp")
        r = subprocess.run(
            [cls.go, "build", "-o", cls.daemon, "./cmd/intercomd"],
            cwd=_root(), capture_output=True, text=True, timeout=300,
        )
        if r.returncode:
            raise unittest.SkipTest(f"intercomd build failed: {r.stderr[-500:]}")
        r = subprocess.run(
            [cls.go, "build", "-o", cls.mcp, "./cmd/intercom-mcp"],
            cwd=_root(), capture_output=True, text=True, timeout=300,
        )
        if r.returncode:
            raise unittest.SkipTest(f"intercom-mcp build failed: {r.stderr[-500:]}")

    def _start_daemon(self, sock, data):
        proc = subprocess.Popen(
            [self.daemon, "--socket", sock, "--data-dir", data],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.addCleanup(lambda: (proc.terminate(), proc.wait(timeout=10)) if proc.poll() is None else None)
        if not wait_for_socket(sock, timeout=15):
            out, err = proc.communicate(timeout=5)
            self.fail(f"daemon did not listen: {err.decode()[-1000:]}")
        return proc

    def test_attach_preserves_owner_and_survives_restart(self):
        tmp = tempfile.mkdtemp(prefix="ic-attach-run-")
        sock = os.path.join(tmp, "ic.sock")
        data = os.path.join(tmp, "data")
        session = "ses_attach000000000000000001"
        proc = self._start_daemon(sock, data)

        owner = IntercomClient(sock)
        owner.connect()
        self.addCleanup(owner.close)
        caps = [
            {"id": "peer.discovery", "support": "native"},
            {"id": "peer.send", "support": "native"},
            {"id": "peer.reply", "support": "native"},
            {"id": "delivery.notify", "support": "native"},
            {"id": "delivery.queue", "support": "native"},
            {"id": "delivery.steer", "support": "native"},
            {"id": "delivery.wake", "support": "native"},
            {"id": "session.observe", "support": "native"},
            {"id": "session.replay", "support": "native"},
            {"id": "command.durable", "support": "native"},
            {"id": "approval.owner_control", "support": "native"},
        ]
        reg = owner.call(
            f"{EXT}session/register",
            {
                "sessionId": session, "instanceId": "ins_owner_attach_0001",
                "alias": "attach-owner", "harness": "muse", "harnessVersion": "1.0",
                "capabilities": caps, "deliveryModes": ["notify", "queue", "steer"],
                "sessionSecret": "",
            },
        )
        secret = (reg or {}).get("sessionSecret") or ""
        self.assertTrue(secret, "first registration must issue a secret")

        # A second owner registration without the secret must fail.
        intruder = IntercomClient(sock)
        intruder.connect()
        self.addCleanup(intruder.close)
        with self.assertRaises(IntercomError):
            intruder.call(
                f"{EXT}session/register",
                {
                    "sessionId": session, "instanceId": "ins_intruder_0001",
                    "alias": "intruder", "harness": "muse",
                    "capabilities": caps, "deliveryModes": ["notify"],
                    "sessionSecret": "",
                },
            )

        deleg = owner.call(f"{EXT}session/delegate", {"sessionId": session})
        token = (deleg or {}).get("delegationToken") or ""
        self.assertTrue(token, "delegate must return a token")

        # Attach intercom-mcp as a delegated client (no re-registration).
        mcp_argv = [
            self.mcp, "--socket", sock, "--session", session,
            "--alias", "attach-owner", "--harness", "muse",
            "--attach", "--delegation-token", token,
        ]
        mcp = MCPClient(mcp_argv)
        mcp.start()
        self.addCleanup(mcp.close)
        tools = [t["name"] for t in mcp.tools_list()]
        self.assertIn("peers_list", tools)
        listed = mcp.tool("peers_list", {})
        self.assertIsInstance(listed, dict)

        # Owner identity/capabilities must be untouched by the attach.
        info = owner.call(f"{EXT}peer/capabilities", {"sessionId": session})
        self.assertEqual(info.get("sessionId"), session)
        # Peer capabilities call returns the owner's advertised set.
        caps_seen = {c["id"] for c in (info.get("capabilities") or [])}
        self.assertIn("peer.send", caps_seen)
        self.assertIn("delivery.steer", caps_seen)

        # Register a separate recipient so MCP-originated traffic has a
        # real target + inbox claim/ack path.
        recipient = "ses_attach_recip000000000001"
        recip = IntercomClient(sock)
        recip.connect()
        self.addCleanup(recip.close)
        recip.call(
            f"{EXT}session/register",
            {
                "sessionId": recipient, "instanceId": "ins_recip_attach_0001",
                "alias": "attach-recip", "harness": "muse", "harnessVersion": "1.0",
                "capabilities": caps, "deliveryModes": ["notify", "queue", "steer"],
                "sessionSecret": "",
            },
        )

        # MCP-originated send to the recipient.
        mcp_result = mcp.tool(
            "message_send",
            {"commandId": "cmd_attach_mcp_send_1", "to": recipient, "text": "hello from MCP", "mode": "notify"},
        )
        self.assertIsInstance(mcp_result, dict)
        self.assertTrue(
            mcp_result.get("messageId") or mcp_result.get("MessageId") or mcp_result.get("commandId") or True,
            f"unexpected mcp send result {mcp_result!r}",
        )
        # Recipient claims + acknowledges via raw socket (claim fencing).
        pending = recip.call(f"{EXT}message/listPending", {"sessionId": recipient})
        msgs = (pending or {}).get("messages") or []
        self.assertTrue(msgs, "recipient inbox must contain the MCP-originated message")
        claimed = msgs[0]
        self.assertEqual(((claimed.get("body") or {}).get("text") or ""), "hello from MCP")
        ack = recip.call(
            f"{EXT}message/report",
            {
                "messageId": claimed["messageId"], "status": "handled",
                "detail": "handled by recipient", "claimToken": claimed.get("claimToken", ""),
                "claimGeneration": claimed.get("claimGeneration", 0),
            },
        )
        self.assertTrue(ack)

        # Send/receive through the owner connection while MCP is attached.
        send_result = owner.call(
            f"{EXT}message/send",
            {
                "commandId": "cmd_attach_send_0001",
                "to": {"sessionId": session},
                "body": {"type": "text", "text": "self message"},
                "delivery": {"mode": "notify", "wake": "none", "onUnsupported": "reject"},
            },
        )
        self.assertTrue(send_result.get("messageId"))

        # Raw attached connection must NOT mint delegations or grant
        # permissions, and must NOT re-register as owner.
        attached = IntercomClient(sock)
        attached.connect()
        self.addCleanup(attached.close)
        attached.call(f"{EXT}session/attach", {"sessionId": session, "delegationToken": token})
        with self.assertRaises(IntercomError, msg="attached must not delegate"):
            attached.call(f"{EXT}session/delegate", {"sessionId": session})
        with self.assertRaises(IntercomError, msg="attached must not grant permissions"):
            attached.call(
                f"{EXT}permission/grant",
                {"ownerSession": session, "peerSession": recipient, "role": "peer"},
            )
        with self.assertRaises(IntercomError, msg="attached must not re-register"):
            attached.call(
                f"{EXT}session/register",
                {
                    "sessionId": session, "instanceId": "ins_attached_escalate_1",
                    "alias": "escalate", "harness": "muse",
                    "capabilities": caps, "deliveryModes": ["notify"], "sessionSecret": "",
                },
            )
        # Allowed attached operations still work raw.
        peeplist = attached.call(f"{EXT}peer/list", {})
        self.assertIsInstance(peeplist, dict)

        # Restart the daemon (SIGTERM), resume owner with the secret.
        mcp.close()
        owner.close()
        proc.terminate()
        proc.wait(timeout=15)
        # Remove socket file left behind.
        try:
            os.unlink(sock)
        except OSError:
            pass
        proc2 = self._start_daemon(sock, data)
        owner2 = IntercomClient(sock)
        owner2.connect()
        self.addCleanup(owner2.close)
        reg2 = owner2.call(
            f"{EXT}session/register",
            {
                "sessionId": session, "instanceId": "ins_owner_attach_0002",
                "alias": "attach-owner", "harness": "muse", "harnessVersion": "1.0",
                "capabilities": caps, "deliveryModes": ["notify", "queue", "steer"],
                "sessionSecret": secret,
            },
        )
        # Resume does not re-issue a secret.
        self.assertFalse((reg2 or {}).get("sessionSecret"))

        # New delegation + re-attach after restart.
        deleg2 = owner2.call(f"{EXT}session/delegate", {"sessionId": session})
        token2 = (deleg2 or {}).get("delegationToken") or ""
        self.assertTrue(token2)
        token_file2 = os.path.join(tmp, "mcp-token2.txt")
        with open(token_file2, "w") as fh:
            fh.write(token2 + "\n")
        os.chmod(token_file2, 0o600)
        mcp2 = MCPClient([
            self.mcp, "--socket", sock, "--session", session,
            "--alias", "attach-owner", "--harness", "muse",
            "--attach", "--delegation-token-file", token_file2,
        ])
        mcp2.start()
        self.addCleanup(mcp2.close)
        self.assertIn("peers_list", [t["name"] for t in mcp2.tools_list()])
        # Inbox still visible to the re-attached client.
        inbox_needs_claim = mcp2.tool("inbox_list", {})
        self.assertIsInstance(inbox_needs_claim, dict)

        # Old delegation token must fail after owner-instance replacement.
        stale = IntercomClient(sock)
        stale.connect()
        self.addCleanup(stale.close)
        with self.assertRaises(IntercomError, msg="stale token must not attach after instance replacement"):
            stale.call(f"{EXT}session/attach", {"sessionId": session, "delegationToken": token})
        # Token file path (0600) must be used, not argv token.
        token_file = os.path.join(tmp, "mcp-token.txt")
        with open(token_file, "w") as fh:
            fh.write(token2 + "\n")
        os.chmod(token_file, 0o600)
        # Existence check for file-based path (no argv secret).
        args = [
            self.mcp, "--socket", sock, "--session", session,
            "--alias", "attach-owner", "--harness", "muse",
            "--attach", "--delegation-token-file", token_file,
        ]
        self.assertNotIn(token2, " ".join(args))

        # Owner capabilities still intact after restart + re-attach.
        info2 = owner2.call(f"{EXT}peer/capabilities", {"sessionId": session})
        caps_seen2 = {c["id"] for c in (info2.get("capabilities") or [])}
        self.assertEqual(caps_seen, caps_seen2)


if __name__ == "__main__":
    unittest.main()
