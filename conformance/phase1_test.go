// Package conformance - Phase 1 authority and identity negative tests.
package conformance

import (
	"testing"

	"github.com/B-A-M-N/intercom/internal/adapter/acp"
	"github.com/B-A-M-N/intercom/internal/config"
	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

// strangerEnv registers a session owned by a different principal.
func strangerEnv(t *testing.T, e *env) (strangerSession, strangerPrincipal string) {
	t.Helper()
	strangerPrincipal = "prn_stranger0000000000000000000"
	strangerSession = "ses_stranger000000000000000000"
	if err := e.store.EnsurePrincipal(e.ctx, strangerPrincipal, "stranger"); err != nil {
		t.Fatalf("ensure principal: %v", err)
	}
	if err := e.store.RegisterSession(e.ctx, protocol.PeerInfo{
		SessionID: strangerSession, Alias: "stranger", InstanceID: "ins_stranger0000000000000000",
		HostID: e.node.HostID(), PrincipalID: strangerPrincipal, Harness: "synthetic",
	}); err != nil {
		t.Fatalf("register stranger: %v", err)
	}
	return strangerSession, strangerPrincipal
}

func expectForbidden(t *testing.T, name string, rpcErr *protocol.RPCError) {
	t.Helper()
	if rpcErr == nil {
		t.Fatalf("%s: expected forbidden error, got success", name)
	}
	if rpcErr.Code != protocol.CodeForbidden && rpcErr.Code != protocol.CodeUnauthorized {
		t.Errorf("%s: error code = %d, want forbidden/unauthorized", name, rpcErr.Code)
	}
}

// Test_Phase1_SessionTakeoverRejected: supplying an existing sessionId
// under a different principal must not take ownership.
func Test_Phase1_SessionTakeoverRejected(t *testing.T) {
	e := newEnv(t)
	err := e.store.RegisterSession(e.ctx, protocol.PeerInfo{
		SessionID: e.bobID, Alias: "bob", InstanceID: "ins_attacker0000000000000000",
		HostID: e.node.HostID(), PrincipalID: "prn_attacker000000000000000000", Harness: "synthetic",
	})
	if err == nil {
		t.Fatal("session takeover by supplying an existing sessionId was allowed")
	}
	rec, getErr := e.store.GetSession(e.ctx, e.bobID)
	if getErr != nil {
		t.Fatalf("get bob: %v", getErr)
	}
	if rec.PrincipalID != e.node.PrincipalID() {
		t.Errorf("bob principal changed to %q", rec.PrincipalID)
	}
}

// Test_Phase1_AliasCollisionRejected: a second session claiming an
// existing alias fails explicitly.
func Test_Phase1_AliasCollisionRejected(t *testing.T) {
	e := newEnv(t)
	err := e.store.RegisterSession(e.ctx, protocol.PeerInfo{
		SessionID: "ses_aliasx0000000000000000000000", Alias: "bob", InstanceID: "ins_aliasx000000000000000000",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic",
	})
	if err == nil {
		t.Fatal("alias collision was allowed")
	}
}

// Test_Phase1_CrossPrincipalInboxAndReportForbidden: a stranger cannot
// read Bob's inbox or forge a handled receipt for Bob's message.
func Test_Phase1_CrossPrincipalInboxAndReportForbidden(t *testing.T) {
	e := newEnv(t)
	strangerSession, strangerPrincipal := strangerEnv(t, e)

	res, err := e.send(e.aliceSender(), e.bobID, "secret", "cmd_phase1000000000000000001", protocol.DeliveryNotify)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	srv := acp.NewServer(e.node, strangerPrincipal, strangerSession)
	_, rpcErr := srv.HandleValue(e.ctx, protocol.MethodMessageListPending, map[string]any{"sessionId": e.bobID})
	expectForbidden(t, "listPending", rpcErr)

	_, rpcErr = srv.HandleValue(e.ctx, protocol.MethodMessageReport, map[string]any{
		"messageId": res.MessageID, "status": "handled", "detail": "forged",
	})
	expectForbidden(t, "report", rpcErr)

	// The forged receipt must not exist.
	receipts, err := e.node.Receipts(e.ctx, res.MessageID)
	if err != nil {
		t.Fatalf("receipts: %v", err)
	}
	for _, r := range receipts {
		if r.Status == protocol.ReceiptHandled {
			t.Error("forged handled receipt was recorded")
		}
	}
}

// Test_Phase1_CrossPrincipalDiscoveryAndObserveForbidden: discovery must
// not leak sessions outside the caller's principal without a grant.
func Test_Phase1_CrossPrincipalDiscoveryAndObserveForbidden(t *testing.T) {
	e := newEnv(t)
	strangerSession, strangerPrincipal := strangerEnv(t, e)
	srv := acp.NewServer(e.node, strangerPrincipal, strangerSession)

	listResult, rpcErr := srv.HandleValue(e.ctx, protocol.MethodPeerList, nil)
	if rpcErr != nil {
		t.Fatalf("peer/list: %v", rpcErr)
	}
	list := listResult.(acp.PeerListResult)
	for _, p := range list.Peers {
		if p.SessionID == e.bobID || p.SessionID == e.aliceID {
			t.Errorf("cross-principal discovery leaked session %s", p.SessionID)
		}
	}

	_, rpcErr = srv.HandleValue(e.ctx, protocol.MethodPeerResolve, map[string]any{"sessionId": e.bobID})
	expectForbidden(t, "resolve", rpcErr)

	_, rpcErr = srv.HandleValue(e.ctx, protocol.MethodPeerCapabilities, map[string]any{"sessionId": e.bobID})
	expectForbidden(t, "capabilities", rpcErr)

	_, rpcErr = srv.HandleValue(e.ctx, protocol.MethodSessionReplay, map[string]any{"sessionId": e.bobID})
	expectForbidden(t, "replay", rpcErr)

	_, rpcErr = srv.HandleValue(e.ctx, protocol.MethodSessionSubscribe, map[string]any{"sessionId": e.bobID})
	expectForbidden(t, "subscribe", rpcErr)

	_, rpcErr = srv.HandleValue(e.ctx, protocol.MethodMessageStatus, map[string]any{"messageId": "msg_doesnotexist000000000000"})
	// Unknown message for a stranger must not succeed.
	if rpcErr == nil {
		t.Error("status for unknown message succeeded for stranger")
	}
}

// Test_Phase1_HeartbeatCannotTargetOtherSession: heartbeat ignores a
// supplied sessionId and only reports the caller's bound session.
func Test_Phase1_HeartbeatCannotTargetOtherSession(t *testing.T) {
	e := newEnv(t)
	// Bob starts disconnected in the store view; Alice heartbeating with
	// Bob's id in params must not mark Bob online.
	if err := e.store.SetPresence(e.ctx, e.bobID, protocol.PresenceDisconnected); err != nil {
		t.Fatalf("set presence: %v", err)
	}
	srv := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)
	if _, rpcErr := srv.HandleValue(e.ctx, protocol.MethodHeartbeat, map[string]any{"sessionId": e.bobID}); rpcErr != nil {
		t.Fatalf("heartbeat: %v", rpcErr)
	}
	presence, err := e.node.Presence(e.ctx, e.bobID)
	if err != nil {
		t.Fatalf("presence: %v", err)
	}
	if presence == protocol.PresenceOnline {
		t.Error("Alice's heartbeat marked Bob online")
	}
	alicePresence, err := e.node.Presence(e.ctx, e.aliceID)
	if err != nil {
		t.Fatalf("alice presence: %v", err)
	}
	if alicePresence != protocol.PresenceOnline {
		t.Errorf("alice presence = %q, want online (own heartbeat)", alicePresence)
	}
}

// Test_Phase1_SenderCannotClaimRegisteredSessionOfAnotherPrincipal:
// Node.Send with a registered session id but a different principal fails.
func Test_Phase1_SenderCannotClaimRegisteredSessionOfAnotherPrincipal(t *testing.T) {
	e := newEnv(t)
	_, err := e.node.Send(e.ctx, node.Sender{PrincipalID: "prn_attacker000000000000000000", SessionID: e.bobID},
		node.SendRequest{
			CommandID: "cmd_phase1000000000000000002", To: e.aliceID,
			Body: protocol.MessageBody{Type: "text", Text: "spoof"},
			Delivery: protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		})
	if err == nil {
		t.Fatal("send claiming another principal's session was allowed")
	}
}

// Test_Phase1_HostAndPrincipalStableAcrossRestart: reopening the same
// store reuses host and principal identity.
func Test_Phase1_HostAndPrincipalStableAcrossRestart(t *testing.T) {
	e := newEnv(t)
	host1, principal1 := e.node.HostID(), e.node.PrincipalID()
	// Close and reopen the store at the same path, as a daemon restart does.
	if err := e.store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	cfg := config.Default()
	cfg.DataDir = e.node.Config().DataDir
	cfg.SocketPath = e.node.Config().SocketPath
	// Deliberately leave PrincipalID/HostID empty: identity must load
	// from the store, not be minted fresh.
	cfg.PrincipalID = ""
	st2, err := store.Open(e.ctx, store.Options{Path: cfg.DatabasePath()})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	n2, err := node.New(e.ctx, node.Options{Config: cfg, Store: st2})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if n2.HostID() != host1 {
		t.Errorf("host changed across restart: %q -> %q", host1, n2.HostID())
	}
	// Principal was explicitly configured in newEnv, so it is stable too.
	if n2.PrincipalID() != principal1 {
		t.Errorf("principal changed across restart: %q -> %q", principal1, n2.PrincipalID())
	}
}
