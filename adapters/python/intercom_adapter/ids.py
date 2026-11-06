"""Identifier helpers shared by the harness adapters.

Several harnesses (Muse MSP, Codex app-server) require UUIDv7 command
identities on the wire: a `cmd_`-prefixed Intercom command id is not a
bare UUID and is rejected as invalid params. These helpers mint and
translate identifiers without inventing a second idempotency domain:
translating an Intercom command id strips its prefix when the remainder
is a UUIDv7, so a retried Intercom command maps to the same harness
command id.
"""

from __future__ import annotations

import os
import time
import uuid


def uuid7() -> str:
    """Mint a UUIDv7 string (time-ordered, version 7)."""
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    counter = int.from_bytes(os.urandom(2), "big") & 0xFFF
    rand = int.from_bytes(os.urandom(8), "big") & ((1 << 62) - 1)
    value = (ms << 80) | (0x7 << 76) | (counter << 64) | (0b10 << 62) | rand
    return str(uuid.UUID(int=value))


def as_uuid7(value: str) -> str | None:
    """Return value (or its unprefixed remainder) if it is a UUIDv7."""
    candidates = [value]
    if "_" in value:
        candidates.append(value.split("_", 1)[1])
    for candidate in candidates:
        try:
            parsed = uuid.UUID(candidate)
        except (ValueError, TypeError, AttributeError):
            continue
        if parsed.version == 7:
            return str(parsed)
    return None


def harness_command_id(intercom_command_id: str) -> str:
    """Translate an Intercom command id to a harness-acceptable UUIDv7.

    A `cmd_<uuidv7>` id maps to the same UUIDv7, preserving idempotency
    across a retried delivery. Anything else mints a fresh UUIDv7: the
    harness never sees an identifier it cannot parse.
    """
    derived = as_uuid7(intercom_command_id or "")
    return derived if derived is not None else uuid7()


__all__ = ["uuid7", "as_uuid7", "harness_command_id"]
