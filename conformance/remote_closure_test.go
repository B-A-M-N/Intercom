package conformance

// remote_closure_test.go: narrow closure proofs for the three
// remote-delivery defects found in review:
//  1. RetryRemote must check ownership BEFORE the acked shortcut.
//  2. RecordRemoteTransferFull must reject command-ID reuse with
//     different content (ErrConflict), while identical retries stay
//     idempotent.
//  3. ResumeRemoteTransfers (daemon restart recovery) must classify
//     pending transfers: deliverable ones are attempted, corrupt or
//     expired ones are terminally classified, nothing is silently
//     dropped or resent with recreated content.

import (
	"testing"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/remote"
	"github.com/B-A-M-N/intercom/internal/store"
)

func Test_RemoteClosure_RetryOwnershipBeforeAck(t *testing.T) {
	eA := newEnv(t)
	bob := "ses_closure_bob00000000000"
	_ = eA.store.RecordRemoteTransferFull(eA.ctx, "msg_closure_ack", eB_host(t), bob, eA.aliceID, `{"targetSession":"x"}`)
	_ = eA.store.SetRemoteTransfer(eA.ctx, "msg_closure_ack", "acked", "remote-id-1", 1)
	// Non-owner must be forbidden even though the transfer is acked
	// (the ack shortcut previously ran before the ownership check).
	if _, err := eA.node.RetryRemote(eA.ctx, eA.bobSender(), "msg_closure_ack"); err == nil {
		t.Fatal("non-owner retry of acked transfer must be forbidden")
	}
	// Owner gets the acked outcome without a resend.
	res, err := eA.node.RetryRemote(eA.ctx, eA.aliceSender(), "msg_closure_ack")
	if err != nil {
		t.Fatalf("owner retry of acked: %v", err)
	}
	if !res.RemoteAccepted || res.Attempts != 1 {
		t.Fatalf("unexpected acked result: %+v", res)
	}
}

func Test_RemoteClosure_TransferIDContentConflict(t *testing.T) {
	e := newEnv(t)
	if err := e.store.RecordRemoteTransferFull(e.ctx, "msg_closure_1", "hst_x", "ses_t", e.aliceID, "env-1"); err != nil {
		t.Fatalf("first record: %v", err)
	}
	// Identical retry is idempotent.
	if err := e.store.RecordRemoteTransferFull(e.ctx, "msg_closure_1", "hst_x", "ses_t", e.aliceID, "env-1"); err != nil {
		t.Fatalf("identical retry must succeed: %v", err)
	}
	// Different content under the same ID is a conflict.
	if err := e.store.RecordRemoteTransferFull(e.ctx, "msg_closure_1", "hst_x", "ses_t", e.aliceID, "env-DIFFERENT"); err != store.ErrConflict {
		t.Fatalf("different envelope must conflict, got %v", err)
	}
	if err := e.store.RecordRemoteTransferFull(e.ctx, "msg_closure_1", "hst_OTHER", "ses_t", e.aliceID, "env-1"); err != store.ErrConflict {
		t.Fatalf("different host must conflict, got %v", err)
	}
	// Original record untouched.
	tr, err := e.store.GetRemoteTransfer(e.ctx, "msg_closure_1")
	if err != nil || tr.Envelope != "env-1" || tr.RemoteHost != "hst_x" {
		t.Fatalf("original mutated: %+v err=%v", tr, err)
	}
}

func Test_RemoteClosure_ResumeClassifiesPending(t *testing.T) {
	eA := newEnv(t)
	// Corrupt envelope: classified expired at recovery, terminally.
	_ = eA.store.RecordRemoteTransferFull(eA.ctx, "msg_resume_bad", eB_host(t), "ses_t", eA.aliceID, "{not-json")
	// Pending transfer to an unknown/untrusted host: marked failed,
	// never silently dropped, never acked.
	_ = eA.store.RecordRemoteTransferFull(eA.ctx, "msg_resume_host", "hst_unknown", "ses_t", eA.aliceID,
		`{"originHost":"x","originFingerprint":"fp","fromPrincipal":"p","fromSession":"ses_x","originalMessageId":"msg_resume_host","originalCommandId":"c1","targetSession":"ses_t","correlationId":"","hops":0,"body":{"type":"text","text":"hi"},"delivery":{"mode":"notify"}}`)
	func() {
		id, err := remote.GenerateIdentity(eA.node.HostID())
		if err != nil {
			t.Skipf("identity: %v", err)
		}
		eA.node.SetRemoteIdentity(id)

		terminal, err := eA.node.ResumeRemoteTransfers(eA.ctx)
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		if terminal < 1 {
			t.Fatalf("expected at least the corrupt transfer terminally classified, got %d", terminal)
		}
		bad, _ := eA.store.GetRemoteTransfer(eA.ctx, "msg_resume_bad")
		if bad.Status != "expired" {
			t.Fatalf("corrupt envelope must be expired, got %q", bad.Status)
		}
		hostTr, _ := eA.store.GetRemoteTransfer(eA.ctx, "msg_resume_host")
		if hostTr.Status != "failed" {
			t.Fatalf("untrusted host transfer must be failed, got %q", hostTr.Status)
		}
	}()
}

func Test_RemoteClosure_ResumeRequiresIdentity(t *testing.T) {
	e := newEnv(t)
	_ = e.store.RecordRemoteTransferFull(e.ctx, "msg_resume_noid", "hst_x", "ses_t", e.aliceID, `{}`)
	if _, err := e.node.ResumeRemoteTransfers(e.ctx); err == nil {
		t.Fatal("resume without remote identity must fail explicitly")
	}
	_ = node.MaxRemoteAttempts
}
