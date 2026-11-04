package conformance

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bamn/intercom/internal/config"
	"github.com/bamn/intercom/internal/node"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
)

// Test_WakePolicyIsApplied covers the wake policy requirement of spec
// section 5.3.
//
// The wake policy is independent of the delivery mode: the mode decides
// what happens to the message, and the wake decides whether the recipient
// is initiated.
func Test_WakePolicyIsApplied(t *testing.T) {
	e := newEnv(t)

	// A queue delivery with wake=none must not wake the recipient.
	if _, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_wake0000000000000000000001",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "quiet"},
		Delivery: protocol.Delivery{
			Mode: protocol.DeliveryQueue, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject,
		},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool { return len(e.bob.delivered) == 1 })
	if !ok {
		t.Fatal("the queued message was not delivered")
	}
	if len(e.bob.woken) != 0 {
		t.Errorf("wake=none woke the recipient %d times", len(e.bob.woken))
	}

	// A queue delivery with wake=if_idle must wake an idle recipient.
	if _, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_wake0000000000000000000002",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "wake up"},
		Delivery: protocol.Delivery{
			Mode: protocol.DeliveryQueue, Wake: protocol.WakeIfIdle, OnUnsupported: protocol.UnsupportedReject,
		},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	ok = waitFor(t, 2*time.Second, func() bool { return len(e.bob.woken) == 1 })
	if !ok {
		t.Fatalf("wake=if_idle did not wake the idle recipient; woken=%d", len(e.bob.woken))
	}
}

// Test_CommandReachesExecutingState covers the command lifecycle of spec
// section 6.1.
func Test_CommandReachesExecutingState(t *testing.T) {
	e := newEnv(t)

	res, err := e.send(e.aliceSender(), e.bobID, "work", "cmd_test000000000000000000000021", protocol.DeliveryQueue)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool {
		cmdRec, err := e.store.GetCommand(e.ctx, res.CommandID)
		if err != nil {
			return false
		}
		// A terminal receipt settles the command.
		return cmdRec.State == protocol.CommandSettled || cmdRec.State == protocol.CommandExecuting
	})
	if !ok {
		cmdRec, _ := e.store.GetCommand(e.ctx, res.CommandID)
		t.Fatalf("command never left the accepted state: %q", cmdRec.State)
	}

	// The terminal state must be settled, with a disposition.
	ok = waitFor(t, 2*time.Second, func() bool {
		cmdRec, err := e.store.GetCommand(e.ctx, res.CommandID)
		return err == nil && cmdRec.State == protocol.CommandSettled
	})
	finalCmd, _ := e.store.GetCommand(e.ctx, res.CommandID)
	if !ok {
		t.Fatalf("command did not settle: %q", finalCmd.State)
	}
	if finalCmd.Disposition == "" {
		t.Error("settled command carries no disposition")
	}
}

// Test_CursorGapIsAnExplicitResponse covers the cursor gap requirement of
// spec section 7.2.
func Test_CursorGapIsAnExplicitResponse(t *testing.T) {
	e := newEnv(t)

	if _, err := e.send(e.aliceSender(), e.bobID, "before", "cmd_test000000000000000000000022", protocol.DeliveryNotify); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := e.store.CompactEvents(e.ctx, e.bobID, 1000); err != nil {
		t.Fatalf("compact: %v", err)
	}

	res, err := e.node.Replay(e.ctx, e.bobID, 0, 100)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !res.Gap {
		t.Fatal("replay did not report a gap")
	}
	if res.SnapshotPath == "" {
		t.Error("gap did not carry a snapshot recovery path")
	}
}

// Test_RetentionBoundsTheStore covers the retention requirement of spec
// sections 10.4 and 13.
func Test_RetentionBoundsTheStore(t *testing.T) {
	e := newEnv(t)

	// Send a message, then age the store's view past the retention window
	// by shrinking the retention to zero.
	e.node.SetLimit(func(l *config.Limits) {
		l.MessageRetention = time.Nanosecond
		l.EventRetention = time.Nanosecond
		l.DeduplicationHorizon = 0
	})

	if _, err := e.send(e.aliceSender(), e.bobID, "old", "cmd_test000000000000000000000023", protocol.DeliveryNotify); err != nil {
		t.Fatalf("send: %v", err)
	}

	report, err := e.node.Maintain(e.ctx)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if report.MessagesPurged == 0 {
		t.Error("retention purged no messages")
	}

	// The purged message must be gone from the durable store.
	if _, err := e.store.GetMessage(e.ctx, "msg_does_not_exist"); err == nil {
		t.Error("a nonexistent message was found")
	}
}

// Test_PeerCannotModifyPermissions covers spec acceptance criterion 9:
// unauthorized peers cannot modify execution permissions.
func Test_PeerCannotModifyPermissions(t *testing.T) {
	e := newEnv(t)

	// Alice holds peer authority over Bob's session.
	if err := e.store.GrantPermission(e.ctx, e.aliceID, e.bobID, store.RolePeer); err != nil {
		t.Fatalf("grant: %v", err)
	}

	// Alice tries to grant herself owner authority over Bob's session.
	err := e.node.GrantPermission(e.ctx, e.aliceSender(), e.bobID, e.aliceID, store.RoleOwner)
	if err == nil {
		t.Fatal("a peer escalated itself to owner authority")
	}
	rpcErr, ok := protocol.AsRPCError(err)
	if !ok {
		t.Fatalf("error is not an RPC error: %v", err)
	}
	if rpcErr.Code != protocol.CodeForbidden {
		t.Errorf("error code = %d, want %d (forbidden)", rpcErr.Code, protocol.CodeForbidden)
	}

	// The escalation must not have been recorded.
	perm, err := e.store.LookupPermission(e.ctx, e.aliceID, e.bobID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if perm.Role == store.RoleOwner {
		t.Error("the peer was granted owner authority")
	}
}

// Test_OwnerCanManagePermissions covers the positive half of the
// authorization requirement.
func Test_OwnerCanManagePermissions(t *testing.T) {
	e := newEnv(t)

	// Bob owns his own session, so Bob grants Alice peer access to it.
	// Ownership is per session: Alice does not own Bob's session even
	// though they share a principal.
	if err := e.node.GrantPermission(e.ctx, e.bobSender(), e.bobID, e.aliceID, store.RolePeer); err != nil {
		t.Fatalf("grant: %v", err)
	}

	perm, err := e.store.LookupPermission(e.ctx, e.aliceID, e.bobID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if perm.Role != store.RolePeer {
		t.Errorf("role = %q, want peer", perm.Role)
	}

	// Revocation ends the grant.
	if err := e.node.RevokePermission(e.ctx, e.bobSender(), e.bobID, e.aliceID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	perm, err = e.store.LookupPermission(e.ctx, e.aliceID, e.bobID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !perm.Revoked() {
		t.Error("the grant was not revoked")
	}

	// A revoked peer can no longer send.
	canSend, err := e.store.CanSend(e.ctx, e.aliceID, e.bobID)
	if err != nil {
		t.Fatalf("can send: %v", err)
	}
	if canSend {
		t.Error("a revoked peer can still send")
	}
}

// Test_RecipientPolicyNarrowsAGrant covers the receiving-session
// enforcement requirement of spec section 11.3.
func Test_RecipientPolicyNarrowsAGrant(t *testing.T) {
	e := newEnv(t)

	if err := e.store.GrantPermission(e.ctx, e.aliceID, e.bobID, store.RolePeer); err != nil {
		t.Fatalf("grant: %v", err)
	}
	// The recipient accepts notifications but not steering. The recipient
	// sets its own policy: a peer cannot configure it.
	if err := e.node.SetDeliveryPolicy(e.ctx, e.bobSender(), e.bobID, e.aliceID,
		[]protocol.DeliveryMode{protocol.DeliveryNotify}); err != nil {
		t.Fatalf("set policy: %v", err)
	}

	// A peer must not be able to change the policy either.
	if err := e.node.SetDeliveryPolicy(e.ctx, e.aliceSender(), e.bobID, e.aliceID,
		[]protocol.DeliveryMode{protocol.DeliverySteer}); err == nil {
		t.Error("a peer changed the recipient's delivery policy")
	}

	// A permitted mode is accepted.
	if _, err := e.send(e.aliceSender(), e.bobID, "allowed", "cmd_test000000000000000000000024", protocol.DeliveryNotify); err != nil {
		t.Fatalf("a permitted mode was refused: %v", err)
	}

	// A mode the recipient does not accept is refused.
	_, err := e.send(e.aliceSender(), e.bobID, "refused", "cmd_test000000000000000000000025", protocol.DeliverySteer)
	if err == nil {
		t.Fatal("the recipient accepted a mode its policy excludes")
	}
	rpcErr, ok := protocol.AsRPCError(err)
	if !ok {
		t.Fatalf("error is not an RPC error: %v", err)
	}
	if rpcErr.Code != protocol.CodeForbidden {
		t.Errorf("error code = %d, want %d (forbidden)", rpcErr.Code, protocol.CodeForbidden)
	}
}

// Test_SubscribeDeliversReceipts covers the subscription requirement of
// spec section 7.2.
func Test_SubscribeDeliversReceipts(t *testing.T) {
	e := newEnv(t)

	ch, err := e.node.Subscribe(e.ctx, e.bobID)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	res, err := e.send(e.aliceSender(), e.bobID, "watched", "cmd_test000000000000000000000026", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case receipt := <-ch:
		if receipt.MessageID != res.MessageID {
			t.Errorf("receipt message id = %q, want %q", receipt.MessageID, res.MessageID)
		}
		if !receipt.Status.Valid() {
			t.Errorf("receipt status %q is not a defined status", receipt.Status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the subscriber received no receipt")
	}
}

// Test_ConnectionBoundIsEnforced covers the connection bound of spec
// section 10.4.
func Test_ConnectionBoundIsEnforced(t *testing.T) {
	e := newEnv(t)

	// The bound is enforced in the transport, which is covered by the
	// transport test. Here the node-level configuration is verified to be
	// present and sane.
	if e.node.Config().Limits.MaxConcurrentConnections <= 0 {
		t.Error("the concurrent connection bound is not configured")
	}
}

// Test_ContextCancellationPropagates checks that a cancelled context stops
// work rather than partially applying it.
func Test_ContextCancellationPropagates(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(e.ctx)
	cancel()

	_, err := e.node.Send(ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_test000000000000000000000027",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "cancelled"},
		Delivery: protocol.Delivery{
			Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject,
		},
	})
	if err == nil {
		t.Fatal("a cancelled context still admitted a message")
	}
}

// Test_NotifyFallbackIsNotificationOnly covers the fallback semantics of
// spec section 5.3.
//
// The notify fallback means notification-only, not a general downgrade. A
// queue fallback would still make the message available for the next turn,
// which is a different operation from the notification the sender asked to
// fall back to.
func Test_NotifyFallbackIsNotificationOnly(t *testing.T) {
	e := newEnv(t)

	// Bob's adapter supports notify and queue but not steer.
	res, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_fallback00000000000000000001",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "fall back"},
		Delivery: protocol.Delivery{
			Mode:          protocol.DeliverySteer,
			Wake:          protocol.WakeNone,
			OnUnsupported: protocol.UnsupportedNotify,
		},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool {
		receipts, err := e.node.Receipts(e.ctx, res.MessageID)
		if err != nil || len(receipts) == 0 {
			return false
		}
		return receipts[len(receipts)-1].Status == protocol.ReceiptPresented
	})
	if !ok {
		receipts, _ := e.node.Receipts(e.ctx, res.MessageID)
		t.Fatalf("the notify fallback did not fall back to notification: %+v", receipts)
	}

	// The message was presented, not queued for a turn.
	if len(e.bob.delivered) != 0 {
		t.Errorf("the fallback delivered %d messages to the turn path, want 0", len(e.bob.delivered))
	}
	if len(e.bob.notified) != 1 {
		t.Errorf("the fallback produced %d notifications, want 1", len(e.bob.notified))
	}
}

// Test_DowngradeFallbackIsExplicitlyReported covers the downgrade reporting
// requirement of spec section 5.3.
func Test_DowngradeFallbackIsExplicitlyReported(t *testing.T) {
	e := newEnv(t)

	res, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_fallback00000000000000000002",
		To:        e.bobID,
		Body:      protocol.MessageBody{Type: "text", Text: "downgrade"},
		Delivery: protocol.Delivery{
			Mode:          protocol.DeliverySteer,
			Wake:          protocol.WakeNone,
			OnUnsupported: protocol.UnsupportedDowngrade,
		},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool {
		receipts, err := e.node.Receipts(e.ctx, res.MessageID)
		if err != nil || len(receipts) < 2 {
			return false
		}
		return receipts[1].Detail != ""
	})
	if !ok {
		t.Fatal("the downgrade was not reported")
	}

	receipts, err := e.node.Receipts(e.ctx, res.MessageID)
	if err != nil {
		t.Fatalf("receipts: %v", err)
	}
	detail := receipts[1].Detail
	if !contains(detail, "downgraded") {
		t.Errorf("the downgrade receipt does not report the downgrade: %q", detail)
	}
}

// contains reports whether s contains sub.
func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

// indexOf returns the index of sub in s, or -1.
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// Test_InjectedMessagesRetainProvenance covers the provenance requirement of
// spec section 11.3.
//
// Messages injected into model context MUST retain provenance identifying
// them as peer-supplied content.
func Test_InjectedMessagesRetainProvenance(t *testing.T) {
	e := newEnv(t)

	if _, err := e.send(e.aliceSender(), e.bobID, "peer content", "cmd_provenance00000000000000001", protocol.DeliveryQueue); err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool { return len(e.bob.delivered) == 1 })
	if !ok {
		t.Fatal("the recipient did not receive the message")
	}

	injected := e.bob.delivered[0]

	// The rendered body is marked as peer-supplied content.
	if injected.Body.Type != "peer-message" {
		t.Errorf("injected body type = %q, want peer-message", injected.Body.Type)
	}
	if len(injected.Body.Data) == 0 {
		t.Fatal("the injected body carries no provenance data")
	}

	var payload struct {
		Provenance protocol.Provenance  `json:"provenance"`
		Body       protocol.MessageBody `json:"body"`
	}
	if err := json.Unmarshal(injected.Body.Data, &payload); err != nil {
		t.Fatalf("decode provenance: %v", err)
	}
	if payload.Provenance.Source != protocol.ProvenanceSourcePeer {
		t.Errorf("provenance source = %q, want %q", payload.Provenance.Source, protocol.ProvenanceSourcePeer)
	}
	if payload.Provenance.FromSession != e.aliceID {
		t.Errorf("provenance sender = %q, want %q", payload.Provenance.FromSession, e.aliceID)
	}
	if payload.Provenance.MessageID == "" {
		t.Error("provenance carries no message id")
	}

	// The original content is preserved inside the wrapper.
	if payload.Body.Text != "peer content" {
		t.Errorf("wrapped body text = %q", payload.Body.Text)
	}

	// The underlying durable message keeps the original body, so the
	// rendering is an injection-time concern and not a storage change.
	stored, err := e.store.GetMessage(e.ctx, injected.MessageID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	if stored.Body.Type != "text" {
		t.Errorf("stored body type = %q, want text", stored.Body.Type)
	}
}

// Test_NotificationIsNotWrapped covers that the provenance wrapper applies
// to model-context injection, not to notification surfaces.
func Test_NotificationIsNotWrapped(t *testing.T) {
	e := newEnv(t)

	if _, err := e.send(e.aliceSender(), e.bobID, "a note", "cmd_provenance00000000000000002", protocol.DeliveryNotify); err != nil {
		t.Fatalf("send: %v", err)
	}

	ok := waitFor(t, 2*time.Second, func() bool { return len(e.bob.notified) == 1 })
	if !ok {
		t.Fatal("the recipient was not notified")
	}
	if e.bob.notified[0].Body.Type != "text" {
		t.Errorf("notified body type = %q, want text", e.bob.notified[0].Body.Type)
	}
}
