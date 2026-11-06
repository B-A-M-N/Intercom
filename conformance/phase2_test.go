// Package conformance - Phase 2 durability and delivery tests.
package conformance

import (
	"testing"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

// Test_Phase2_AtomicAdmissionRetryReturnsOriginal: identical retry after
// atomic admission returns the original message id and does not duplicate.
func Test_Phase2_AtomicAdmissionRetryReturnsOriginal(t *testing.T) {
	e := newEnv(t)
	first, err := e.send(e.aliceSender(), e.bobID, "atomic", "cmd_phase2000000000000000001", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	// Command, message, receipt and event must all exist together.
	cmd, err := e.store.GetCommand(e.ctx, first.CommandID)
	if err != nil {
		t.Fatalf("command: %v", err)
	}
	if cmd.MessageID != first.MessageID {
		t.Errorf("command message link = %q, want %q", cmd.MessageID, first.MessageID)
	}
	if _, err := e.store.GetMessage(e.ctx, first.MessageID); err != nil {
		t.Fatalf("message missing after admission: %v", err)
	}
	receipts, err := e.node.Receipts(e.ctx, first.MessageID)
	if err != nil || len(receipts) == 0 || receipts[0].Status != protocol.ReceiptAccepted {
		t.Errorf("acceptance receipt missing: %v %v", receipts, err)
	}

	second, err := e.send(e.aliceSender(), e.bobID, "atomic", "cmd_phase2000000000000000001", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if second.MessageID != first.MessageID || !second.Replayed {
		t.Errorf("retry = %+v, want original %q replayed", second, first.MessageID)
	}
	// Conflicting reuse fails.
	if _, err := e.send(e.aliceSender(), e.bobID, "different", "cmd_phase2000000000000000001", protocol.DeliveryNotify); err == nil {
		t.Error("conflicting commandId reuse was allowed")
	}
	// Cross-sender reuse with same commandId is also a conflict (fingerprint
	// binds sender), not a replay.
	if _, err := e.send(e.bobSender(), e.aliceID, "atomic", "cmd_phase2000000000000000001", protocol.DeliveryNotify); err == nil {
		t.Error("cross-sender commandId reuse was treated as replay")
	}
}

// Test_Phase2_InboxClaimPreventsRePresentation: a claimed message is not
// handed out again until lease expiry or acknowledgement; acknowledgement
// clears it permanently.
func Test_Phase2_InboxClaimPreventsRePresentation(t *testing.T) {
	e := newEnv(t)
	// Use an offline session so delivery stays pending (no live adapter).
	offline := "ses_offline2000000000000000000"
	if err := e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{
		SessionID: offline, Alias: "offline2", InstanceID: "ins_offline2000000000000000",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := e.send(e.aliceSender(), offline, "claim me", "cmd_phase2000000000000000002", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	first, err := e.node.PendingInbox(e.ctx, offline, 10)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %d, err %v", len(first), err)
	}
	second, err := e.node.PendingInbox(e.ctx, offline, 10)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 0 {
		t.Errorf("claimed message re-presented: %d messages", len(second))
	}
	// Acknowledge via report: it must never be presented again.
	if err := e.node.ReportDelivery(e.ctx, res.MessageID, protocol.ReceiptPresented, "presented"); err != nil {
		t.Fatalf("report: %v", err)
	}
	third, err := e.node.PendingInbox(e.ctx, offline, 10)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if len(third) != 0 {
		t.Errorf("acknowledged message re-presented: %d", len(third))
	}
	// After lease expiry a still-pending message would be reclaimable;
	// simulate by expiring the lease in the store and re-claiming a new one.
	res2, err := e.send(e.aliceSender(), offline, "claim two", "cmd_phase2000000000000000003", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send2: %v", err)
	}
	claimed, err := e.store.ClaimInbox(e.ctx, offline, offline, time.Nanosecond, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim with tiny lease = %d %v", len(claimed), err)
	}
	time.Sleep(2 * time.Millisecond)
	reclaimed, err := e.store.ClaimInbox(e.ctx, offline, offline, time.Second, 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	found := false
	for _, m := range reclaimed {
		if m.MessageID == res2.MessageID {
			found = true
		}
	}
	if !found {
		t.Error("message not reclaimable after lease expiry")
	}
}

// Test_Phase2_PendingCommandsLimitEnforced: MaxPendingCommands is enforced
// at admission, not merely configured.
func Test_Phase2_PendingCommandsLimitEnforced(t *testing.T) {
	e := newEnv(t)
	// Use store-level AdmitMessage with a tiny limit for determinism.
	offline := "ses_offline2000000000000000000"
	if err := e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{
		SessionID: offline, Alias: "offline3", InstanceID: "ins_offline3000000000000000",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Fill one pending command via a normal send to the offline session
	// (stays executing/pending). Then a second admission with limit 1 must fail.
	if _, err := e.send(e.aliceSender(), offline, "one", "cmd_phase2000000000000000004", protocol.DeliveryNotify); err != nil {
		t.Fatalf("send one: %v", err)
	}
	// Direct store admission with MaxPendingCommands=1 must now be rejected.
	fp, err := store.Fingerprint("message/send", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	_, _, err = e.store.AdmitMessage(e.ctx,
		store.CommandRecord{CommandID: "cmd_phase2000000000000000005", PrincipalID: e.node.PrincipalID(), TargetSession: offline, Operation: "message/send", State: protocol.CommandAccepted},
		fp,
		protocol.Message{MessageID: "msg_phase20000000000000000005", CommandID: "cmd_phase2000000000000000005", From: e.aliceID, To: offline, Body: protocol.MessageBody{Type: "text", Text: "two"}, Delivery: protocol.Delivery{Mode: protocol.DeliveryNotify}, CreatedAt: time.Now().UTC()},
		e.node.PrincipalID(),
		protocol.EventEnvelope{EventID: "evt_phase200000000000000005", SessionID: offline, EventType: protocol.EventMessageAccepted, Timestamp: time.Now().UTC()},
		store.AdmissionLimits{MaxPendingCommands: 1, MaxInboxMessages: 1000, MaxDatabaseBytes: 0},
	)
	if err == nil {
		t.Error("MaxPendingCommands was not enforced")
	}
}

// Test_Phase2_RetentionPreservesPending: retention must not purge pending
// or claimed messages even when old.
func Test_Phase2_RetentionPreservesPending(t *testing.T) {
	e := newEnv(t)
	offline := "ses_offline2000000000000000000"
	// Already registered in previous tests? Each test has fresh env; register again.
	if err := e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{
		SessionID: offline, Alias: "offline4", InstanceID: "ins_offline4000000000000000",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := e.send(e.aliceSender(), offline, "keep me", "cmd_phase2000000000000000006", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	// Purge directly with a far-future cutoff and assert pending survives.
	purged, err := e.store.PurgeMessagesOlderThan(e.ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != 0 {
		t.Errorf("purged %d pending messages, want 0", purged)
	}
	if _, err := e.store.GetMessage(e.ctx, res.MessageID); err != nil {
		t.Errorf("pending message was purged: %v", err)
	}
}
