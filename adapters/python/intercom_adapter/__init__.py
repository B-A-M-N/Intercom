"""Intercom adapter host.

The Python side of Intercom exists for one reason: agent harnesses expose
their native surfaces in Python, and those surfaces are where the quirks live.

The division of responsibility is deliberate and strict:

- The Go core owns durability, identity, authorization, and delivery truth.
  It is the only component that writes the store.
- The Python adapter owns harness quirks. It translates a harness's native
  surface into the Intercom adapter contract and reports capability
  honestly.

An adapter must never claim a capability it does not have. A harness that
cannot accept a message during an active turn must not report a newly
launched prompt as successful active-turn steering (ICSP-001 sections 1.1
and 12).
"""

__all__ = [
    "acp_client",
    "claude",
    "client",
    "codex",
    "codex_client",
    "harness",
    "hermes",
    "host",
    "ids",
    "mcp_consumer",
    "msp",
    "muse",
    "opencode",
    "stdio_rpc",
]
