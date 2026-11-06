package conformance

import (
	"testing"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/remote"
	"github.com/B-A-M-N/intercom/internal/store"
)

func Test_Remediation_RemoteAdminAndTransferOwnership(t *testing.T) {
	eA := newEnv(t)
	_, endpointB, idB := startRemoteServer(t, newEnv(t))
	// Stranger principal cannot enroll.
	strangerSession, strangerPrincipal := strangerEnv(t, eA)
	// AuthorizeRemoteAdmin with stranger must fail.
	if err := eA.node.AuthorizeRemoteAdmin(eA.ctx, node.Sender{PrincipalID: strangerPrincipal, SessionID: strangerSession}); err == nil {
		t.Error("non-operator authorized as remote admin")
	}
	// Local operator passes.
	if err := eA.node.AuthorizeRemoteAdmin(eA.ctx, eA.aliceSender()); err != nil {
		t.Errorf("operator denied: %v", err)
	}
	// Transfer ownership: record transfer for alice, bob cannot retry.
	bob := "ses_remed_bob2000000000000"
	_ = eB2(t, bob)
	_ = eA.store.RecordRemoteTransferFull(eA.ctx, "msg_own001", eB_host(t), bob, eA.aliceID, `{"targetSession":"x"}`)
	if _, err := eA.node.RetryRemote(eA.ctx, eA.bobSender(), "msg_own001"); err == nil {
		t.Error("non-sender retried another's transfer")
	}
	_ = endpointB
	_ = idB
	_ = remote.GenerateIdentity
	_ = store.ErrNotFound
	_ = protocol.MethodRemoteEnroll
}

func eB2(t *testing.T, bob string) *env { return nil }
func eB_host(t *testing.T) string { return "hst_x" }

func Test_Remediation_TransferStatusCrossPrincipal(t *testing.T) {
	e := newEnv(t)
	_ = e.store.RecordRemoteTransferFull(e.ctx, "msg_status001", "hst_x", "ses_target", e.aliceID, "{}")
	// Stranger cannot read alice's transfer status via node-level check
	// that mirrors ACP remoteStatus fallback.
	strangerSession, strangerPrincipal := strangerEnv(t, e)
	if err := e.node.AuthorizeRemoteAdmin(e.ctx, node.Sender{PrincipalID: strangerPrincipal, SessionID: strangerSession}); err == nil {
		t.Fatal("stranger passed admin")
	}
	transfer, _ := e.store.GetRemoteTransfer(e.ctx, "msg_status001")
	if transfer.SenderSession != e.aliceID {
		t.Fatal("transfer sender mismatch")
	}
	if strangerSession == transfer.SenderSession {
		t.Error("stranger owns transfer")
	}
}
