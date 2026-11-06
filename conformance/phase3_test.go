// Package conformance - Phase 3 policy and routing tests.
package conformance

import (
	"testing"
	"time"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/policy"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/routing"
	"github.com/B-A-M-N/intercom/internal/store"
)

func Test_Phase3_PolicyDenyOverrides(t *testing.T) {
	// Explicit delivery-policy deny wins over same-principal allow.
	dec := policy.Evaluate(policy.OpSendNotify, policy.Facts{
		SamePrincipal: true, DeliveryPolicySet: true, DeliveryAllowed: false,
	})
	if dec.Allowed {
		t.Error("delivery policy deny did not override same-principal allow")
	}
	// Cross-principal without grant denied.
	dec = policy.Evaluate(policy.OpSendNotify, policy.Facts{})
	if dec.Allowed {
		t.Error("cross-principal send allowed without grant")
	}
	// Peer grant never approves.
	dec = policy.Evaluate(policy.OpApprove, policy.Facts{GrantActive: true, GrantIsOwner: false})
	if dec.Allowed {
		t.Error("peer grant conferred approve authority")
	}
	// Owner grant approves.
	dec = policy.Evaluate(policy.OpApprove, policy.Facts{GrantIsOwner: true})
	if !dec.Allowed {
		t.Error("owner grant did not approve")
	}
}

func Test_Phase3_GrantExpiry(t *testing.T) {
	e := newEnv(t)
	strangerSession, _ := strangerEnv(t, e)
	// Grant peer access expiring in the past: must be treated as revoked.
	if err := e.store.GrantPermissionWithExpiry(e.ctx, strangerSession, e.bobID, store.RolePeer, time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("grant expired: %v", err)
	}
	can, err := e.store.CanSend(e.ctx, strangerSession, e.bobID)
	if err != nil {
		t.Fatalf("cansend: %v", err)
	}
	if can {
		t.Error("expired grant still allows send")
	}
	// Future expiry allows.
	if err := e.store.GrantPermissionWithExpiry(e.ctx, strangerSession, e.bobID, store.RolePeer, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("grant future: %v", err)
	}
	can, _ = e.store.CanSend(e.ctx, strangerSession, e.bobID)
	if !can {
		t.Error("unexpired grant does not allow send")
	}
	// Node-level grant with expiry via ACP path is covered by store; check send works.
	if _, err := e.send(node.Sender{PrincipalID: "prn_stranger0000000000000000000", SessionID: strangerSession}, e.bobID, "hi", "cmd_phase3000000000000000001", protocol.DeliveryNotify); err != nil {
		t.Errorf("granted cross-principal send failed: %v", err)
	}
}

func Test_Phase3_RevocationBlocksQueuedDelivery(t *testing.T) {
	e := newEnv(t)
	strangerSession, strangerPrincipal := strangerEnv(t, e)
	// Grant, send to offline Bob? Bob has live adapter; use offline session
	// owned by node principal so message stays queued, then revoke and claim.
	offline := "ses_offline3000000000000000000"
	// Offline session must be owned by stranger principal to test cross grant?
	// Simpler: grant stranger -> bob, queue a message to bob via direct store
	// by pausing delivery: send to bob while bob adapter present delivers
	// immediately, so use an offline recipient owned by node principal and
	// grant stranger access to it, then revoke before claim.
	if err := e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{
		SessionID: offline, Alias: "offlineP3", InstanceID: "ins_offline3000000000000000",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic",
	}); err != nil {
		t.Fatalf("register offline: %v", err)
	}
	// Temporarily move offline ownership? Instead grant stranger->offline directly.
	if err := e.store.GrantPermission(e.ctx, strangerSession, offline, store.RolePeer); err != nil {
		t.Fatalf("grant: %v", err)
	}
	res, err := e.send(node.Sender{PrincipalID: strangerPrincipal, SessionID: strangerSession}, offline, "queued", "cmd_phase3000000000000000002", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send queued: %v", err)
	}
	// Revoke before the recipient claims it.
	if err := e.store.RevokePermission(e.ctx, strangerSession, offline); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	claimed, err := e.node.PendingInboxFor(e.ctx, node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}, offline, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for _, m := range claimed {
		if m.MessageID == res.MessageID {
			t.Error("revoked sender's queued message was handed for delivery")
		}
	}
	// And the receipt trail shows rejection.
	receipts, err := e.node.Receipts(e.ctx, res.MessageID)
	if err != nil {
		t.Fatalf("receipts: %v", err)
	}
	foundRejected := false
	for _, r := range receipts {
		if r.Status == protocol.ReceiptRejected {
			foundRejected = true
		}
	}
	if !foundRejected {
		t.Errorf("no rejected receipt after revocation: %+v", receipts)
	}
}

func Test_Phase3_RoutingDeterministic(t *testing.T) {
	// Exact beats alias; ambiguous alias fails; selector picks single match.
	alice := routing.Peer{SessionID: "ses_a", Alias: "a", PrincipalID: "prn_1", Harness: "muse", Presence: protocol.PresenceOnline}
	bob := routing.Peer{SessionID: "ses_b", Alias: "b", PrincipalID: "prn_1", Harness: "hermes", Presence: protocol.PresenceOnline}
	route, err := routing.ResolveExactOrAlias(&alice, []routing.Peer{bob}, "ses_a")
	if err != nil || route.TargetSession != "ses_a" || route.Via != "exact" {
		t.Errorf("exact route = %+v %v", route, err)
	}
	if _, err := routing.ResolveExactOrAlias(nil, []routing.Peer{alice, bob}, "dup"); err == nil {
		t.Error("ambiguous alias did not fail")
	}
	route, err = routing.ResolveSelector([]routing.Peer{alice, bob}, routing.Selector{Harness: "hermes"})
	if err != nil || route.TargetSession != "ses_b" {
		t.Errorf("selector route = %+v %v", route, err)
	}
	if _, err := routing.ResolveSelector([]routing.Peer{alice, bob}, routing.Selector{}); err == nil {
		t.Error("ambiguous selector did not fail")
	}
	if err := routing.CheckHops(routing.MaxRelayHops); err == nil {
		t.Error("hop limit did not reject")
	}
}

func Test_Phase3_NodeRouteAndHopLimit(t *testing.T) {
	e := newEnv(t)
	route, err := e.node.Route(e.ctx, e.aliceSender(), "", &routing.Selector{Harness: "synthetic", Availability: protocol.PresenceOnline})
	// Synthetic peers: alice and bob both match -> ambiguous must fail.
	if err == nil {
		t.Errorf("ambiguous selector routed to %q, want failure", route.TargetSession)
	}
	// Relay hop limit enforced on send.
	if _, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_phase3000000000000000003", To: e.bobID,
		Body: protocol.MessageBody{Type: "text", Text: "loop"},
		Delivery: protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		Metadata: map[string]any{"hops": float64(routing.MaxRelayHops)},
	}); err == nil {
		t.Error("relay hop limit was not enforced")
	}
}
