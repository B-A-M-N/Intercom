"""Tests for the adapter host and the capability honesty rules.

These are the Python half of the conformance suite. They test the adapter
contract from section 12 of ICSP-001: an adapter must report what its
harness genuinely supports, and it must never substitute a different
operation for one the harness cannot perform.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from intercom_adapter.harness import (  # noqa: E402
    MODE_NOTIFY,
    MODE_QUEUE,
    MODE_STEER,
    RECEIPT_PRESENTED,
    RECEIPT_REJECTED,
    SUPPORT_EMULATED,
    SUPPORT_NATIVE,
    SUPPORT_UNSUPPORTED,
    BaseHarness,
    DeliveryResult,
    SessionState,
)
from intercom_adapter.hermes import HermesAdapter  # noqa: E402
from intercom_adapter.muse import MuseAdapter  # noqa: E402


class RecordingHarness(BaseHarness):
    """A synthetic harness that records what it was asked to do."""

    name = "recording"
    version = "9.9.9"
    modes = (MODE_NOTIFY, MODE_QUEUE, MODE_STEER)

    def __init__(self) -> None:
        super().__init__()
        self.delivered: list[dict] = []
        self.notified: list[dict] = []

    def state(self, session_id: str) -> SessionState:
        return SessionState(state="idle", safe_point=True)

    def deliver(self, session_id: str, message: dict) -> DeliveryResult:
        self.delivered.append(message)
        return DeliveryResult(status="delivered", detail="recorded")

    def notify(self, session_id: str, message: dict) -> DeliveryResult:
        self.notified.append(message)
        return DeliveryResult(status=RECEIPT_PRESENTED, detail="presented")


class TestCapabilityHonesty(unittest.TestCase):
    """An adapter must report its harness's real abilities."""

    def test_capabilities_reflect_declared_modes(self) -> None:
        caps = {c.id: c for c in RecordingHarness().capabilities}

        self.assertEqual(caps["delivery.steer"].support, SUPPORT_NATIVE)
        self.assertEqual(caps["delivery.queue"].support, SUPPORT_NATIVE)
        self.assertEqual(caps["delivery.notify"].support, SUPPORT_NATIVE)

    def test_absent_mode_is_reported_unsupported(self) -> None:
        caps = {c.id: c for c in HermesAdapter(None).capabilities}

        # ACP offers a prompt channel but no active-turn injection point.
        self.assertEqual(caps["delivery.steer"].support, SUPPORT_UNSUPPORTED)
        self.assertTrue(caps["delivery.steer"].limitations)

    def test_unsupported_capability_has_a_limitation(self) -> None:
        caps = {c.id: c for c in HermesAdapter(None).capabilities}
        for cap in caps.values():
            if cap.support == SUPPORT_UNSUPPORTED:
                self.assertTrue(
                    cap.limitations,
                    f"{cap.id} is unsupported but reports no limitation",
                )

    def test_every_capability_states_a_support_level(self) -> None:
        for harness in (RecordingHarness(), HermesAdapter(None), MuseAdapter(None)):
            for cap in harness.capabilities:
                self.assertIn(
                    cap.support,
                    (SUPPORT_NATIVE, SUPPORT_EMULATED, SUPPORT_UNSUPPORTED),
                    f"{harness.name}: {cap.id} has an invalid support level",
                )

    def test_downgrade_prefers_the_closest_fidelity(self) -> None:
        self.assertEqual(RecordingHarness().downgrade(MODE_STEER), MODE_STEER)
        self.assertEqual(HermesAdapter(None).downgrade(MODE_STEER), MODE_QUEUE)

        class NotifyOnly(BaseHarness):
            name = "notify-only"
            modes = (MODE_NOTIFY,)

            def state(self, session_id: str) -> SessionState:
                return SessionState()

            def deliver(self, session_id: str, message: dict) -> DeliveryResult:
                return DeliveryResult(status="unknown")

            def notify(self, session_id: str, message: dict) -> DeliveryResult:
                return DeliveryResult(status=RECEIPT_PRESENTED)

        notify_only = NotifyOnly()
        self.assertEqual(notify_only.downgrade(MODE_STEER), MODE_NOTIFY)
        self.assertEqual(notify_only.downgrade(MODE_QUEUE), MODE_NOTIFY)

    def test_downgrade_fails_when_nothing_is_available(self) -> None:
        class Nothing(BaseHarness):
            name = "nothing"
            modes = ()

            def state(self, session_id: str) -> SessionState:
                return SessionState()

            def deliver(self, session_id: str, message: dict) -> DeliveryResult:
                return DeliveryResult(status="unknown")

            def notify(self, session_id: str, message: dict) -> DeliveryResult:
                return DeliveryResult(status="unknown")

        self.assertIsNone(Nothing().downgrade(MODE_STEER))


class TestHermesAdapter(unittest.TestCase):
    """The Hermes adapter reflects what ACP can actually do."""

    def test_steer_is_not_claimed(self) -> None:
        adapter = HermesAdapter(None)
        self.assertFalse(adapter.supports(MODE_STEER))
        self.assertNotIn(MODE_STEER, adapter.delivery_modes)

    def test_notify_never_claims_injection(self) -> None:
        adapter = HermesAdapter(None)
        result = adapter.notify("ses_x", {"body": {"type": "text", "text": "hi"}})
        self.assertEqual(result.status, RECEIPT_PRESENTED)
        self.assertNotEqual(result.status, "injected")
        self.assertIn("not injected", result.detail)

    def test_deliver_without_an_agent_is_refused(self) -> None:
        adapter = HermesAdapter(None)
        result = adapter.deliver("ses_x", {"body": {"type": "text", "text": "hi"}})
        self.assertEqual(result.status, RECEIPT_REJECTED)


class TestMuseAdapter(unittest.TestCase):
    """The Muse adapter uses MSP's native turn surface."""

    class FakeMSP:
        """A fake MSP client that records calls."""

        def __init__(self, steer_result=None, start_result=None, fail=()):
            self.calls: list[tuple[str, dict]] = []
            self._steer_result = steer_result or {"turnId": "turn_1", "status": "accepted"}
            self._start_result = start_result or {
                "turnId": "turn_2",
                "status": "accepted",
                "disposition": "queue",
                "startedNewTurn": False,
            }
            self._fail = set(fail)

        def call(self, method: str, params: dict):
            self.calls.append((method, params))
            if method in self._fail:
                raise RuntimeError(f"{method} failed")
            if method == "turn/steer":
                return self._steer_result
            if method == "turn/start":
                return self._start_result
            return {}

    def test_steer_uses_msp_turn_steer(self) -> None:
        msp = self.FakeMSP()
        adapter = MuseAdapter(msp)
        adapter.note_turn("ses_x", "turn_1")

        result = adapter.deliver(
            "ses_x",
            {
                "commandId": "cmd_1",
                "delivery": {"mode": MODE_STEER},
                "body": {"type": "text", "text": "steer me"},
            },
        )

        self.assertEqual(result.status, "injected")
        method, params = msp.calls[0]
        self.assertEqual(method, "turn/steer")
        self.assertEqual(params["expectedTurnId"], "turn_1")
        self.assertEqual(params["input"][0]["text"], "steer me")

    def test_steer_without_a_turn_is_refused(self) -> None:
        msp = self.FakeMSP()
        adapter = MuseAdapter(msp)

        result = adapter.deliver(
            "ses_x",
            {"commandId": "cmd_1", "delivery": {"mode": MODE_STEER}, "body": {"text": "x"}},
        )

        self.assertEqual(result.status, RECEIPT_REJECTED)
        self.assertEqual(msp.calls, [], "no MSP call should be made without a turn")

    def test_queue_uses_turn_start_with_queue_disposition(self) -> None:
        msp = self.FakeMSP()
        adapter = MuseAdapter(msp)

        result = adapter.deliver(
            "ses_x",
            {"commandId": "cmd_2", "delivery": {"mode": MODE_QUEUE}, "body": {"text": "later"}},
        )

        self.assertEqual(result.status, "delivered")
        method, params = msp.calls[0]
        self.assertEqual(method, "turn/start")
        self.assertEqual(params["ifBusy"], "queue")

    def test_steer_failure_is_rejected_not_retried_as_a_new_turn(self) -> None:
        msp = self.FakeMSP(fail={"turn/steer"})
        adapter = MuseAdapter(msp)
        adapter.note_turn("ses_x", "turn_1")

        result = adapter.deliver(
            "ses_x",
            {"commandId": "cmd_3", "delivery": {"mode": MODE_STEER}, "body": {"text": "x"}},
        )

        self.assertEqual(result.status, RECEIPT_REJECTED)
        # Exactly one call: the adapter must not fall back to turn/start,
        # because a new turn is not steering.
        self.assertEqual(len(msp.calls), 1)
        self.assertEqual(msp.calls[0][0], "turn/steer")

    def test_notify_does_not_use_a_model_turn(self) -> None:
        msp = self.FakeMSP()
        adapter = MuseAdapter(msp)

        result = adapter.notify("ses_x", {"body": {"type": "text", "text": "FYI"}})

        self.assertEqual(result.status, RECEIPT_PRESENTED)
        self.assertNotEqual(result.status, "injected")
        self.assertEqual(msp.calls, [], "notification must not drive a model turn")
        self.assertEqual(adapter.notes("ses_x"), ["FYI"])

    def test_muse_claims_native_steering(self) -> None:
        caps = {c.id: c for c in MuseAdapter(self.FakeMSP()).capabilities}
        self.assertEqual(caps["delivery.steer"].support, SUPPORT_NATIVE)


if __name__ == "__main__":
    unittest.main()
