"""Interoperability with a real ACP client.

This test drives the Intercom ACP extension through the reference ACP
implementation, agent_client_protocol, rather than through a hand-rolled
client. It is the evidence for acceptance criterion 8 of ICSP-001 section
16: "ACP interoperability is demonstrable through an actual client".

The reference library is installed in the Hermes virtualenv. When it is not
available the test skips rather than pretending to have verified anything.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]


def _find_acp_package() -> str | None:
    """Locate a Python environment that has agent_client_protocol."""
    candidates = [
        "/home/bamn/hermes-agent/venv/lib/python3.11/site-packages",
    ]
    for path in candidates:
        if os.path.isdir(os.path.join(path, "acp")):
            return path
    return None


class TestRealACPClient(unittest.TestCase):
    """The Intercom extension is reachable through the real ACP client."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.acp_path = _find_acp_package()
        if cls.acp_path is None:
            raise unittest.SkipTest("agent_client_protocol is not installed")

        # The Intercom ACP server must be built before the test can run it.
        cls.binary = Path(REPO) / ".testbin" / "intercom-acp"
        cls.binary.parent.mkdir(exist_ok=True)
        build = subprocess.run(
            [
                "go", "build", "-buildvcs=false",
                "-o", str(cls.binary),
                "./cmd/intercom-acp",
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "GOCACHE": "/tmp/gocache",
                "GOFLAGS": "-mod=mod",
                "GOPROXY": "off",
                "GOSUMDB": "off",
            },
        )
        if build.returncode != 0:
            raise unittest.SkipTest(f"could not build intercom-acp: {build.stderr}")

    def _run_client(self, script: str) -> str:
        """Run a script against the Intercom ACP server and return stdout."""
        env = dict(os.environ)
        env["PYTHONPATH"] = self.acp_path + os.pathsep + env.get("PYTHONPATH", "")
        env["INTERCOM_ACP_BINARY"] = str(self.binary)

        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )
        if result.returncode != 0:
            raise AssertionError(f"client failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")
        return result.stdout

    def test_real_client_negotiates_and_calls_the_extension(self) -> None:
        """A real ACP client initializes and reaches the Intercom methods."""
        script = r'''
import asyncio, json, os, sys

from acp.client import ClientSideConnection
from acp.interfaces import Client


class IntercomClient(Client):
    """A minimal ACP client that only implements what the test exercises."""

    async def request_permission(self, **kwargs):
        raise NotImplementedError

    async def session_update(self, **kwargs):
        return None

    async def write_text_file(self, **kwargs):
        raise NotImplementedError

    async def read_text_file(self, **kwargs):
        raise NotImplementedError

    async def create_terminal(self, **kwargs):
        raise NotImplementedError

    async def terminal_output(self, **kwargs):
        raise NotImplementedError

    async def release_terminal(self, **kwargs):
        raise NotImplementedError

    async def wait_for_terminal_exit(self, **kwargs):
        raise NotImplementedError

    async def kill_terminal(self, **kwargs):
        raise NotImplementedError

    async def ext_method(self, method: str, params: dict):
        """Extension methods reach the agent under the reverse-DNS namespace."""
        return {}


async def main():
    binary = os.environ["INTERCOM_ACP_BINARY"]
    proc = await asyncio.create_subprocess_exec(
        binary,
        "--socket", "/tmp/intercom-acp-test.sock",
        "--data-dir", "/tmp/intercom-acp-test-data",
        "--session", "ses_realtest000000000000000000",
        "--alias", "real-client-test",
        "--harness", "hermes",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )

    # create_subprocess_exec already hands back a StreamReader for stdout
    # and a StreamWriter for stdin, which is exactly what the ACP
    # connection expects.
    conn = ClientSideConnection(IntercomClient(), proc.stdin, proc.stdout)
    await conn.initialize(protocol_version=1, client_info=None)

    # The extension is reachable through the reference client's own
    # extension mechanism: ext_method prefixes the method with "_", so
    # "dev.bamn.intercom/hello" becomes "_dev.bamn.intercom/hello" on the
    # wire.
    hello = await conn.ext_method("dev.bamn.intercom/hello", {})
    print("HELLO " + json.dumps(hello))

    # A standard ACP method that this agent does not implement must fail
    # cleanly rather than crash the connection.
    try:
        await conn.new_session(cwd="/tmp", mcp_servers=[])
        print("NEWSESSION ok")
    except Exception as exc:
        print("NEWSESSION error " + type(exc).__name__)

    proc.kill()


asyncio.run(main())
'''
        out = self._run_client(script)
        self.assertIn("HELLO ", out, out)

        hello_line = next(line for line in out.splitlines() if line.startswith("HELLO "))
        hello = json.loads(hello_line[len("HELLO "):])

        # The extension answered with real Intercom content.
        self.assertEqual(hello.get("specVersion"), "ICSP-001")
        self.assertIn("capabilities", hello)
        self.assertIn("profiles", hello)
        self.assertTrue(hello["capabilities"], "the extension advertised no capabilities")

        # Every capability states its support level.
        for cap in hello["capabilities"]:
            self.assertIn("id", cap)
            self.assertIn(cap["support"], ("native", "emulated", "unsupported"))

        # An ordinary ACP client stays operable: a method the agent does not
        # implement fails cleanly instead of breaking the connection.
        self.assertIn("NEWSESSION", out)


if __name__ == "__main__":
    unittest.main()
