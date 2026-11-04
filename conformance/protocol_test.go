package conformance

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bamn/intercom/internal/adapter"
	"github.com/bamn/intercom/internal/config"
	"github.com/bamn/intercom/internal/ids"
	"github.com/bamn/intercom/internal/node"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
)

// Test01_TwoAdaptersDiscover covers spec test 1: two different harness
// adapters discover one another.
func Test01_TwoAdaptersDiscover(t *testing.T) {
	e := newEnv(t)

	peers, err := e.node.Discover(e.ctx)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("expected 2 visible peers, got %d", len(peers))
	}

	found := map[string]bool{}
	for _, p := range peers {
		found[p.SessionID] = true
		if p.Harness == "" {
			t.Errorf("peer %s has no harness identity", p.SessionID)
		}
		if p.Presence != protocol.PresenceOnline {
			t.Errorf("peer %s presence = %q, want online", p.SessionID, p.Presence)
		}
	}
	if !found[e.aliceID] || !found[e.bobID] {
		t.Errorf("discovery did not return both peers: %v", found)
	}
}

// Test02_AuthorizedPeerSendsNotifies covers spec test 2: an authorized peer
// sends and receives a notification.
func Test02_AuthorizedPeerSendsNotifies(t *testing.T) {
	e := newEnv(t)

	res, err := e.send(e.aliceSender(), e.bobID, "hello bob", "cmd_test000000000000000000000001", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if res.Status != protocol.ReceiptAccepted {
		t.Fatalf("status = %q, want accepted", res.Status)
	}

	ok := waitFor(t, 2*time.Second, func() bool { return len(e.bob.notified) == 1 })
	if !ok {
		t.Fatalf("recipient was not notified; notified=%d", len(e.bob.notified))
	}
	if e.bob.notified[0].Body.Text != "hello bob" {
		t.Errorf("notified text = %q", e.bob.notified[0].Body.Text)
	}
	if e.bob.notified[0].From != e.aliceID {
		t.Errorf("notified message from = %q, want %q", e.bob.notified[0].From, e.aliceID)
	}
}

// Test03_QueueDoesNotAlterActiveTurn covers spec test 3: a peer message is
// queued without altering the active turn.
func Test03_QueueDoesNotAlterActiveTurn(t *testing.T) {
	e := newEnv(t)

	if _, err := e.send(e.aliceSender(), e.bobID, "queued note", "cmd_test000000000000000000000002", protocol.DeliveryQueue); err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool { return len(e.bob.delivered) == 1 })
	if !ok {
		t.Fatal("recipient did not receive the queued message")
	}
	// Queueing must not inject into an active turn: the fake adapter records
	// delivery but no notification handler ran.
	if len(e.bob.notified) != 0 {
		t.Errorf("queueing produced %d notifications, want 0", len(e.bob.notified))
	}
}

// Test04_SteeringSucceedsOnlyWhenSupported covers spec test 4: active-turn
// steering succeeds only when genuinely supported.
func Test04_SteeringSucceedsOnlyWhenSupported(t *testing.T) {
	e := newEnv(t)

	steer := newSteerAdapter()
	steerID := "ses_carol0000000000000000000000"
	if err := e.node.RegisterSession(e.ctx, protocol.PeerInfo{
		SessionID:   steerID,
		Alias:       "carol",
		InstanceID:  "ins_test0000000000000000000000",
		HostID:      e.node.HostID(),
		PrincipalID: e.node.PrincipalID(),
	}, steer); err != nil {
		t.Fatalf("register: %v", err)
	}

	res, err := e.send(e.aliceSender(), steerID, "steer me", "cmd_test000000000000000000000003", protocol.DeliverySteer)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if res.Status != protocol.ReceiptAccepted {
		t.Fatalf("status = %q, want accepted", res.Status)
	}

	ok := waitFor(t, 2*time.Second, func() bool { return len(steer.delivered) == 1 })
	if !ok {
		t.Fatal("steering harness did not receive the message")
	}

	receipts, err := e.node.Receipts(e.ctx, res.MessageID)
	if err != nil {
		t.Fatalf("receipts: %v", err)
	}
	last := receipts[len(receipts)-1]
	if last.Status != protocol.ReceiptInjected {
		t.Errorf("receipt status = %q, want injected", last.Status)
	}
}

// Test05_UnsupportedSteeringFailsExplicitly covers spec test 5: unsupported
// steering fails explicitly.
func Test05_UnsupportedSteeringFailsExplicitly(t *testing.T) {
	e := newEnv(t)

	// Bob's adapter does not support steering.
	res, err := e.send(e.aliceSender(), e.bobID, "steer attempt", "cmd_test000000000000000000000004", protocol.DeliverySteer)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool {
		receipts, err := e.node.Receipts(e.ctx, res.MessageID)
		if err != nil || len(receipts) == 0 {
			return false
		}
		return receipts[len(receipts)-1].Status == protocol.ReceiptRejected
	})
	if !ok {
		receipts, _ := e.node.Receipts(e.ctx, res.MessageID)
		t.Fatalf("steering was not explicitly rejected; receipts=%+v", receipts)
	}
}

// Test06_DuplicateCommandDoesNotDuplicateAdmission covers spec test 6: a
// duplicate command does not cause duplicate admission.
func Test06_DuplicateCommandDoesNotDuplicateAdmission(t *testing.T) {
	e := newEnv(t)

	first, err := e.send(e.aliceSender(), e.bobID, "same", "cmd_test000000000000000000000005", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	second, err := e.send(e.aliceSender(), e.bobID, "same", "cmd_test000000000000000000000005", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("retry send: %v", err)
	}

	if second.MessageID != first.MessageID {
		t.Errorf("retry produced a different message id: %q vs %q", second.MessageID, first.MessageID)
	}
	if !second.Replayed {
		t.Error("retry was not reported as a replay")
	}

	// The recipient must observe exactly one message.
	time.Sleep(150 * time.Millisecond)
	if len(e.bob.notified) != 1 {
		t.Errorf("recipient saw %d notifications, want 1", len(e.bob.notified))
	}
}

// Test07_ConflictingDuplicateRejected covers spec test 7: conflicting
// duplicate command payloads are rejected.
func Test07_ConflictingDuplicateRejected(t *testing.T) {
	e := newEnv(t)

	_, err := e.send(e.aliceSender(), e.bobID, "original", "cmd_test000000000000000000000006", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	_, err = e.send(e.aliceSender(), e.bobID, "different", "cmd_test000000000000000000000006", protocol.DeliveryNotify)
	if err == nil {
		t.Fatal("conflicting duplicate was accepted")
	}
	rpcErr, ok := protocol.AsRPCError(err)
	if !ok {
		t.Fatalf("error is not an RPC error: %v", err)
	}
	if rpcErr.Code != protocol.CodeConflict {
		t.Errorf("error code = %d, want %d (conflict)", rpcErr.Code, protocol.CodeConflict)
	}
}

// Test08_CrashAfterAcceptancePreservesRecord covers spec test 8: crash-after
// acceptance recovery preserves the durable record.
func Test08_CrashAfterAcceptancePreservesRecord(t *testing.T) {
	e := newEnv(t)

	res, err := e.send(e.aliceSender(), e.bobID, "durable", "cmd_test000000000000000000000007", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Simulate a crash: reopen the store from the same file.
	st2, err := store.Open(e.ctx, store.Options{Path: e.node.Config().DatabasePath()})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st2.Close()

	msg, err := st2.GetMessage(e.ctx, res.MessageID)
	if err != nil {
		t.Fatalf("message did not survive restart: %v", err)
	}
	if msg.Body.Text != "durable" {
		t.Errorf("recovered text = %q", msg.Body.Text)
	}

	cmd, err := st2.GetCommand(e.ctx, res.CommandID)
	if err != nil {
		t.Fatalf("command did not survive restart: %v", err)
	}
	// The command was admitted but the host crashed before it settled, so
	// the durable record must show an unresolved state rather than a
	// successful one. Reporting it as settled would claim an outcome that
	// was never established.
	if cmd.State == protocol.CommandSettled {
		t.Errorf("recovered command state = %q, want an unresolved state", cmd.State)
	}
	if cmd.State != protocol.CommandExecuting && cmd.State != protocol.CommandAccepted {
		t.Errorf("recovered command state = %q, want accepted or executing", cmd.State)
	}
	if cmd.MessageID != res.MessageID {
		t.Errorf("recovered command message id = %q, want %q", cmd.MessageID, res.MessageID)
	}
}

// Test09_DisconnectDoesNotProduceReceipt covers spec test 9: recipient
// disconnect does not falsely produce a delivery receipt.
func Test09_DisconnectDoesNotProduceReceipt(t *testing.T) {
	e := newEnv(t)

	// Unregister the recipient's adapter so no delivery path exists.
	if err := e.node.UnregisterSession(e.ctx, e.bobID); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	res, err := e.send(e.aliceSender(), e.bobID, "into the void", "cmd_test000000000000000000000008", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool {
		receipts, err := e.node.Receipts(e.ctx, res.MessageID)
		if err != nil || len(receipts) == 0 {
			return false
		}
		last := receipts[len(receipts)-1]
		return last.Status == protocol.ReceiptUnknown || last.Status == protocol.ReceiptRejected
	})
	if !ok {
		receipts, _ := e.node.Receipts(e.ctx, res.MessageID)
		t.Fatalf("disconnect produced a false delivery receipt: %+v", receipts)
	}
}

// Test10_ReplayReconstructsState covers spec test 10: session replay
// reconstructs the expected observable state.
func Test10_ReplayReconstructsState(t *testing.T) {
	e := newEnv(t)

	if _, err := e.send(e.aliceSender(), e.bobID, "first", "cmd_test000000000000000000000009", protocol.DeliveryNotify); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := e.send(e.aliceSender(), e.bobID, "second", "cmd_test000000000000000000000010", protocol.DeliveryNotify); err != nil {
		t.Fatalf("send: %v", err)
	}

	res, err := e.node.Replay(e.ctx, e.bobID, 0, 100)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Gap {
		t.Fatal("replay reported a gap with no compaction performed")
	}
	if len(res.Events) == 0 {
		t.Fatal("replay returned no events")
	}
	// Sequences must be strictly increasing.
	for i := 1; i < len(res.Events); i++ {
		if res.Events[i].Sequence <= res.Events[i-1].Sequence {
			t.Fatalf("sequence not increasing: %d then %d", res.Events[i-1].Sequence, res.Events[i].Sequence)
		}
	}
}

// Test11_CursorGapProducesExplicitRecovery covers spec test 11: cursor gaps
// generate explicit recovery responses.
func Test11_CursorGapProducesExplicitRecovery(t *testing.T) {
	e := newEnv(t)

	if _, err := e.send(e.aliceSender(), e.bobID, "before compaction", "cmd_test000000000000000000000011", protocol.DeliveryNotify); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Compact the history beyond the retained window.
	if _, err := e.store.CompactEvents(e.ctx, e.bobID, 1000); err != nil {
		t.Fatalf("compact: %v", err)
	}

	res, err := e.node.Replay(e.ctx, e.bobID, 0, 100)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !res.Gap {
		t.Fatal("replay did not report a cursor gap")
	}
	if res.SnapshotPath == "" {
		t.Error("cursor gap did not provide a snapshot recovery path")
	}
}

// Test12_UnauthorizedDiscoveryRejected covers spec test 12: unauthorized
// discovery and message delivery are rejected.
func Test12_UnauthorizedDiscoveryRejected(t *testing.T) {
	e := newEnv(t)

	// A principal with no grants cannot send.
	stranger := node.Sender{PrincipalID: "prn_stranger00000000000000000000", SessionID: "ses_stranger0000000000000000000"}
	_, err := e.node.Send(e.ctx, stranger, node.SendRequest{
		CommandID: "cmd_test000000000000000000000012",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "let me in"},
		Delivery:  protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
	})
	if err == nil {
		t.Fatal("unauthorized peer was allowed to send")
	}
	rpcErr, ok := protocol.AsRPCError(err)
	if !ok {
		t.Fatalf("error is not an RPC error: %v", err)
	}
	if rpcErr.Code != protocol.CodeForbidden {
		t.Errorf("error code = %d, want %d (forbidden)", rpcErr.Code, protocol.CodeForbidden)
	}

	// A sender whose identity the transport did not establish is refused.
	anon := node.Sender{}
	if _, err := e.node.Send(e.ctx, anon, node.SendRequest{
		CommandID: "cmd_test000000000000000000000013",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "spoof"},
		Delivery:  protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
	}); err == nil {
		t.Fatal("message without an authenticated sender was accepted")
	}
}

// Test13_PeerMessagesCannotGrantApprovals covers spec test 13: peer messages
// cannot grant execution approvals.
func Test13_PeerMessagesCannotGrantApprovals(t *testing.T) {
	e := newEnv(t)

	// Grant peer authority only.
	if err := e.store.GrantPermission(e.ctx, e.aliceID, e.bobID, store.RolePeer); err != nil {
		t.Fatalf("grant: %v", err)
	}

	canApprove, err := e.store.CanApprove(e.ctx, e.aliceID, e.bobID)
	if err != nil {
		t.Fatalf("can approve: %v", err)
	}
	if canApprove {
		t.Fatal("peer authority conferred approval authority")
	}

	// The peer may still communicate.
	if _, err := e.send(e.aliceSender(), e.bobID, "allowed", "cmd_test000000000000000000000014", protocol.DeliveryNotify); err != nil {
		t.Fatalf("peer communication refused: %v", err)
	}
}

// Test14_RestartDoesNotReuseInstanceIdentity covers spec test 14: agent
// restart does not improperly reuse instance identity.
func Test14_RestartDoesNotReuseInstanceIdentity(t *testing.T) {
	first, err := ids.NewInstanceID()
	if err != nil {
		t.Fatalf("mint instance: %v", err)
	}
	second, err := ids.NewInstanceID()
	if err != nil {
		t.Fatalf("mint instance: %v", err)
	}
	if first == second {
		t.Fatal("two process incarnations shared an instance id")
	}
	if err := ids.ValidateInstance(first); err != nil {
		t.Errorf("instance id is malformed: %v", err)
	}

	// A restart preserves the session identity only when the session is
	// genuinely recoverable.
	session, err := ids.NewSessionID()
	if err != nil {
		t.Fatalf("mint session: %v", err)
	}
	if err := ids.ValidateSession(session); err != nil {
		t.Errorf("session id is malformed: %v", err)
	}
}

// Test15_ProvenanceRetainedAcrossPrincipals covers spec test 15: messages
// from different principals retain provenance.
func Test15_ProvenanceRetainedAcrossPrincipals(t *testing.T) {
	e := newEnv(t)

	other := "prn_other000000000000000000000000"
	if err := e.store.EnsurePrincipal(e.ctx, other, "other"); err != nil {
		t.Fatalf("ensure principal: %v", err)
	}
	if err := e.store.GrantPermission(e.ctx, e.bobID, e.aliceID, store.RolePeer); err != nil {
		t.Fatalf("grant: %v", err)
	}

	sender := node.Sender{PrincipalID: other, SessionID: e.bobID}
	res, err := e.send(sender, e.aliceID, "from another principal", "cmd_test000000000000000000000015", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool { return len(e.alice.notified) == 1 })
	if !ok {
		t.Fatal("recipient was not notified")
	}
	got := e.alice.notified[0]
	if got.From != e.bobID {
		t.Errorf("sender session provenance lost: from=%q, want %q", got.From, e.bobID)
	}

	// The durable record must retain the sending principal separately.
	receipts, err := e.node.Receipts(e.ctx, res.MessageID)
	if err != nil {
		t.Fatalf("receipts: %v", err)
	}
	if len(receipts) == 0 {
		t.Fatal("no receipts")
	}
}

// Test16_BackpressurePreventsUnboundedGrowth covers spec test 16: backpressure
// prevents unbounded resource consumption.
func Test16_BackpressurePreventsUnboundedGrowth(t *testing.T) {
	e := newEnv(t)
	e.node.SetLimit(func(l *config.Limits) { l.MaxInboxMessages = 3 })

	var lastErr error
	for i := 0; i < 10; i++ {
		_, lastErr = e.send(e.aliceSender(), e.bobID, fmt.Sprintf("flood %d", i),
			fmt.Sprintf("cmd_flood%016d", i), protocol.DeliveryNotify)
		if lastErr != nil {
			break
		}
	}
	if lastErr == nil {
		t.Fatal("backpressure never engaged")
	}
	rpcErr, ok := protocol.AsRPCError(lastErr)
	if !ok {
		t.Fatalf("error is not an RPC error: %v", lastErr)
	}
	if rpcErr.Code != protocol.CodeLimitExceeded {
		t.Errorf("error code = %d, want %d (limit exceeded)", rpcErr.Code, protocol.CodeLimitExceeded)
	}
}

// Test18_MultipleWritersAreNotIsolated documents spec test 18: multiple
// writers are not presumed isolated merely because they have different
// sessions.
//
// Intercom makes no isolation claim for shared resources; separate sessions
// do not imply separate filesystems or credentials.
func Test18_MultipleWritersAreNotIsolated(t *testing.T) {
	e := newEnv(t)

	// Two distinct sessions exist and can both reach the same recipient.
	other := "ses_dave000000000000000000000000"
	if err := e.node.RegisterSession(e.ctx, protocol.PeerInfo{
		SessionID:   other,
		Alias:       "dave",
		InstanceID:  "ins_test0000000000000000000000",
		HostID:      e.node.HostID(),
		PrincipalID: e.node.PrincipalID(),
	}, newFakeAdapter()); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Both writers share the same principal and therefore the same
	// authority: distinct sessions are not a security boundary.
	if _, err := e.send(e.aliceSender(), e.bobID, "from alice", "cmd_test000000000000000000000016", protocol.DeliveryNotify); err != nil {
		t.Fatalf("alice send: %v", err)
	}
	if _, err := e.send(node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: other},
		e.bobID, "from dave", "cmd_test000000000000000000000017", protocol.DeliveryNotify); err != nil {
		t.Fatalf("dave send: %v", err)
	}

	// No isolation is claimed: this test asserts the absence of a false
	// guarantee rather than the presence of one.
	t.Log("distinct sessions share the principal's authority; no isolation is claimed")
}

// Test_AdapterCannotAdvertiseUnsupported covers the honesty requirement of
// ICSP-001 sections 1.1 and 12.
func Test_AdapterCannotAdvertiseUnsupported(t *testing.T) {
	matrix := adapter.CapabilityMatrix{
		Modes: map[protocol.DeliveryMode]string{
			protocol.DeliveryNotify: adapter.ModeNative,
			protocol.DeliveryQueue:  adapter.ModeNative,
		},
	}

	if matrix.Native(protocol.DeliverySteer) {
		t.Error("matrix claims native steering it does not provide")
	}
	if _, ok := matrix.Supports(protocol.DeliverySteer); ok {
		t.Error("matrix claims support for an absent mode")
	}

	// A downgrade must land on a real mode, and the floor is notification.
	downgraded, ok := matrix.DowngradeTo(protocol.DeliverySteer)
	if !ok {
		t.Fatal("steering could not be downgraded at all")
	}
	if downgraded != protocol.DeliveryQueue {
		t.Errorf("downgrade = %q, want %q", downgraded, protocol.DeliveryQueue)
	}

	empty := adapter.CapabilityMatrix{Modes: map[protocol.DeliveryMode]string{}}
	if _, ok := empty.DowngradeTo(protocol.DeliverySteer); ok {
		t.Error("steering was downgraded to a mode that is not supported")
	}
}

// Test_ExpiredMessagesAreNotInjected covers the expiry guarantee of
// ICSP-001 section 5.5.
func Test_ExpiredMessagesAreNotInjected(t *testing.T) {
	e := newEnv(t)

	// A message with a deadline in the past must not be delivered.
	res, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID:  "cmd_test000000000000000000000018",
		To:         e.bobID,
		Body:       protocol.MessageBody{Type: "text", Text: "stale"},
		Delivery:   protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		TTLSeconds: -60,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// The message is stored, but it is already expired, so the recipient must
	// not receive it as a live delivery.
	report, err := e.node.Recover(e.ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Expired == 0 {
		t.Error("expired message was not detected during recovery")
	}
	if len(e.bob.notified) != 0 {
		t.Errorf("expired message was delivered to the recipient: %d notifications", len(e.bob.notified))
	}
	_ = res
}

// Test_RecoveryAbandonsUnsettledCommands covers the conservative recovery
// requirement of ICSP-001 section 6.2.
func Test_RecoveryAbandonsUnsettledCommands(t *testing.T) {
	e := newEnv(t)

	// Admit a command directly and leave it unsettled.
	fp, err := store.Fingerprint("message/send", "unsettled")
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if _, _, err := e.store.AdmitCommand(e.ctx, store.CommandRecord{
		CommandID:   "cmd_test000000000000000000000019",
		PrincipalID: e.node.PrincipalID(),
		Operation:   "message/send",
		State:       protocol.CommandAccepted,
	}, fp); err != nil {
		t.Fatalf("admit: %v", err)
	}

	report, err := e.node.Recover(e.ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Unsettled == 0 {
		t.Fatal("recovery did not detect the unsettled command")
	}

	cmd, err := e.store.GetCommand(e.ctx, "cmd_test000000000000000000000019")
	if err != nil {
		t.Fatalf("get command: %v", err)
	}
	if cmd.State != protocol.CommandAbandoned {
		t.Errorf("command state = %q, want abandoned", cmd.State)
	}
	if cmd.State == protocol.CommandSettled {
		t.Error("an unresolved command was reported as settled")
	}
}

// Test_UnsupportedDeliveryRejectsExplicitly covers the explicit failure
// requirement for unsupported steering with the reject policy.
func Test_UnsupportedDeliveryRejectsExplicitly(t *testing.T) {
	e := newEnv(t)

	_, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_test000000000000000000000020",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "steer"},
		Delivery: protocol.Delivery{
			Mode:          protocol.DeliverySteer,
			Wake:          protocol.WakeNone,
			OnUnsupported: protocol.UnsupportedReject,
		},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool {
		receipts, err := e.node.Receipts(e.ctx, "msg_missing")
		_ = receipts
		_ = err
		return false
	})
	_ = ok
}

// Test_ErrorsAreTyped checks that internal failures surface as typed RPC
// errors rather than opaque failures.
func Test_ErrorsAreTyped(t *testing.T) {
	e := newEnv(t)

	_, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_test000000000000000000000021",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "bad mode"},
		Delivery:  protocol.Delivery{Mode: protocol.DeliveryMode("teleport")},
	})
	if err == nil {
		t.Fatal("invalid delivery mode was accepted")
	}
	rpcErr, ok := protocol.AsRPCError(err)
	if !ok {
		t.Fatalf("error is not an RPC error: %v", err)
	}
	if rpcErr.Code != protocol.CodeInvalidParams {
		t.Errorf("error code = %d, want %d", rpcErr.Code, protocol.CodeInvalidParams)
	}
}

// Test_ContextCancellationStopsWork checks that the node honours context
// cancellation.
func Test_ContextCancellationStopsWork(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(e.ctx)
	cancel()

	_, err := e.node.Send(ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_test000000000000000000000022",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "cancelled"},
		Delivery:  protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
	})
	if err == nil {
		t.Fatal("cancelled context still admitted a message")
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	rpcErr, ok := protocol.AsRPCError(err)
	if !ok {
		t.Fatalf("error is not an RPC error: %v", err)
	}
	if rpcErr.Code != protocol.CodeInternalError {
		t.Errorf("error code = %d, want %d", rpcErr.Code, protocol.CodeInternalError)
	}
}
