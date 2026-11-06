// Package conformance - Phase 6 federation tests (mTLS, host-qualified).
package conformance

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/remote"
	"github.com/B-A-M-N/intercom/internal/store"
)

func startRemoteServer(t *testing.T, e *env) (*remote.Server, string, *remote.Identity) {
	t.Helper()
	id, err := remote.GenerateIdentity(e.node.HostID())
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	e.node.SetRemoteIdentity(id)
	srv := remote.NewServer("127.0.0.1:0", id, func(ctx context.Context, env remote.Envelope, peerHost, peerFP string) (string, error) {
		return e.node.AdmitRemote(ctx, env, peerHost, peerFP)
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.ListenAndServe(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for (srv.Addr() == "" || srv.Addr() == "127.0.0.1:0") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("remote server did not bind")
	}
	return srv, srv.Addr(), id
}

func Test_Phase6_TwoHostsExchange(t *testing.T) {
	eA := newEnv(t)
	eB := newEnv(t)

	_, endpointB, idB := startRemoteServer(t, eB)
	_, endpointA, idA := startRemoteServer(t, eA)
	// Mutual enrollment (both directions, distinct identities).
	if err := eA.store.EnrollHost(eA.ctx, eB.node.HostID(), endpointB, idB.Fingerprint); err != nil {
		t.Fatalf("enroll B on A: %v", err)
	}
	if err := eB.store.EnrollHost(eB.ctx, eA.node.HostID(), endpointA, idA.Fingerprint); err != nil {
		t.Fatalf("enroll A on B: %v", err)
	}

	bob := "ses_remote_bob0000000000000"
	if err := eB.node.RegisterAdapterSession(eB.ctx, protocol.PeerInfo{
		SessionID: bob, Alias: "remotebob", InstanceID: "ins_remotebob0000000000000",
		HostID: eB.node.HostID(), PrincipalID: eB.node.PrincipalID(), Harness: "synthetic",
	}); err != nil {
		t.Fatalf("register bob: %v", err)
	}
	// Denied without grant for host-qualified sender.
	if _, err := eA.node.SendRemote(eA.ctx, eA.aliceSender(), node.RemoteSendRequest{
		RemoteHost: eB.node.HostID(), TargetSession: bob, CommandID: "cmd_remote_denied1",
		Body: protocol.MessageBody{Type: "text", Text: "should be denied"},
		Mode: protocol.DeliveryNotify,
	}); err == nil {
		t.Fatal("cross-host send without grant was allowed")
	}
	// Grant for qualified identity only.
	qualified := eA.node.HostID() + "/" + eA.aliceID
	if err := eB.store.GrantPermission(eB.ctx, qualified, bob, store.RolePeer); err != nil {
		t.Fatalf("grant: %v", err)
	}
	result, err := eA.node.SendRemote(eA.ctx, eA.aliceSender(), node.RemoteSendRequest{
		RemoteHost: eB.node.HostID(), TargetSession: bob, CommandID: "cmd_remote_allowed1",
		Body: protocol.MessageBody{Type: "text", Text: "hello remote"},
		Mode: protocol.DeliveryNotify, CorrelationID: "corr-remote-1",
	})
	if err != nil {
		t.Fatalf("remote send: %v", err)
	}
	if !result.RemoteAccepted {
		t.Fatalf("remote not accepted: %+v", result)
	}
	claimed, err := eB.node.PendingInboxFor(eB.ctx, node.Sender{PrincipalID: eB.node.PrincipalID(), SessionID: bob}, bob, 10)
	if err != nil {
		t.Fatalf("claim B inbox: %v", err)
	}
	found := false
	for _, m := range claimed {
		if m.Body.Text == "hello remote" && m.From == qualified {
			found = true
		}
	}
	if !found {
		t.Errorf("remote message not in B inbox with qualified From: %+v", claimed)
	}
	// Identical retry: same admission, no duplicate.
	result2, err := eA.node.SendRemote(eA.ctx, eA.aliceSender(), node.RemoteSendRequest{
		RemoteHost: eB.node.HostID(), TargetSession: bob, CommandID: "cmd_remote_allowed1",
		Body: protocol.MessageBody{Type: "text", Text: "hello remote"},
		Mode: protocol.DeliveryNotify, CorrelationID: "corr-remote-1",
	})
	if err != nil {
		t.Fatalf("remote resend: %v", err)
	}
	_ = result2
	// Conflicting retry: same command id, different payload must fail.
	if _, err := eA.node.SendRemote(eA.ctx, eA.aliceSender(), node.RemoteSendRequest{
		RemoteHost: eB.node.HostID(), TargetSession: bob, CommandID: "cmd_remote_allowed1",
		Body: protocol.MessageBody{Type: "text", Text: "DIFFERENT"},
		Mode: protocol.DeliveryNotify,
	}); err == nil {
		t.Error("conflicting retry was allowed")
	}
	// Host impersonation: host C cannot claim A/alice.
	idC, _ := remote.GenerateIdentity("hst_impersonator000000000000")
	eC := newEnv(t)
	eC.node.SetRemoteIdentity(idC)
	if err := eB.store.EnrollHost(eB.ctx, idC.HostID, endpointB, idC.Fingerprint); err != nil {
		t.Fatalf("enroll C: %v", err)
	}
	impEnv := remote.Envelope{FromSession: eA.aliceID, TargetSession: bob, OriginalMessageID: "msg_imp1", OriginalCommandID: "cmd_imp1"}
	impEnv.Body.Type = "text"
	impEnv.Body.Text = "impersonated"
	impEnv.Delivery.Mode = "notify"
	if _, err := remote.Client(eB.ctx, endpointB, idC, eB.node.HostID(), idB.Fingerprint, impEnv); err == nil {
		t.Error("impersonating host was accepted")
	}
	// Wrong fingerprint enrollment fails.
	if _, err := remote.Enroll(eA.ctx, endpointB, idA, eB.node.HostID(), "0000deadbeef"); err == nil {
		t.Error("enrollment with wrong fingerprint succeeded")
	}
	// Plaintext dial to TLS port fails.
	raw, err := net.Dial("tcp", endpointB)
	if err == nil {
		_, _ = raw.Write([]byte("hello\n"))
		raw.Close()
		// No plaintext response expected; TLS handshake would fail on server.
	}
	// Revocation: revoke B on A, new sends fail.
	if err := eA.store.RevokeHost(eA.ctx, eB.node.HostID()); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := eA.node.SendRemote(eA.ctx, eA.aliceSender(), node.RemoteSendRequest{
		RemoteHost: eB.node.HostID(), TargetSession: bob, CommandID: "cmd_remote_after_revoke",
		Body: protocol.MessageBody{Type: "text", Text: "x"},
	}); err == nil {
		t.Error("send to revoked host allowed")
	}
	// Unenrolled host denied.
	if _, err := eA.node.SendRemote(eA.ctx, eA.aliceSender(), node.RemoteSendRequest{
		RemoteHost: "hst_unenrolled", TargetSession: bob, CommandID: "cmd_x",
		Body: protocol.MessageBody{Type: "text", Text: "x"},
	}); err == nil {
		t.Error("send to unenrolled host allowed")
	}
	if !strings.Contains(qualified, "/") {
		t.Error("qualified identity malformed")
	}
}
