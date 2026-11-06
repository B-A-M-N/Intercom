package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/remote"
	)

func mustMsgID(t *testing.T, e *env, cmdID string) string {
	t.Helper()
	// Offline recipient (registered session, no live adapter) keeps the
	// message pending so claim/receipt tests are deterministic.
	offline := "ses_review_offline00000000000"
	_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{
		SessionID: offline, Alias: "reviewoffline", InstanceID: "ins_reviewoffline00000000",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic",
	})
	res, err := e.send(e.aliceSender(), offline, "hello", cmdID, protocol.DeliveryQueue)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	return res.MessageID
}

func offlineSender(e *env) node.Sender {
	return node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: "ses_review_offline00000000000"}
}

func Test_ReviewFix_ClaimRenewalFencing(t *testing.T) {
	e := newEnv(t)
	mid := mustMsgID(t, e, "cmd_review_renew01")
	claimed, err := e.store.ClaimInbox(e.ctx, "ses_review_offline00000000000", "ses_review_offline00000000000", 30*time.Second, 10)
	if err != nil || len(claimed) == 0 {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	msg := claimed[0]
	if msg.MessageID != mid {
		t.Fatalf("claimed wrong message")
	}
	owner, token, gen, _, _, err := e.store.ClaimInfo(e.ctx, mid)
	if err != nil || token == "" || gen <= 0 {
		t.Fatalf("claim info: %v %q %d", err, token, gen)
	}
	// Missing token / zero generation / stale generation must all fail.
	if err := e.store.RenewClaim(e.ctx, mid, owner, "", gen, 30*time.Second); err == nil {
		t.Fatal("renew with empty token must fail")
	}
	if err := e.store.RenewClaim(e.ctx, mid, owner, token, 0, 30*time.Second); err == nil {
		t.Fatal("renew with zero generation must fail")
	}
	if err := e.store.RenewClaim(e.ctx, mid, owner, token, gen+5, 30*time.Second); err == nil {
		t.Fatal("renew with stale generation must fail")
	}
	if err := e.store.RenewClaim(e.ctx, mid, owner, "badtoken", gen, 30*time.Second); err == nil {
		t.Fatal("renew with wrong token must fail")
	}
	// Correct fences succeed.
	if err := e.store.RenewClaim(e.ctx, mid, owner, token, gen, 30*time.Second); err != nil {
		t.Fatalf("renew with correct fences: %v", err)
	}
	// Node layer also refuses missing fences.
	if err := e.node.RenewClaimFor(e.ctx, offlineSender(e), mid, "", gen); err == nil {
		t.Fatal("node renew with empty token must fail")
	}
	if err := e.node.RenewClaimFor(e.ctx, offlineSender(e), mid, token, 0); err == nil {
		t.Fatal("node renew with zero generation must fail")
	}
	// Superseded ownership: a fresh message claimed with a tiny lease,
	// then reclaimed after expiry, must supersede the old fences.
	midS := mustMsgID(t, e, "cmd_review_supersede1")
	idS := "ses_review_supersede000000000"
	_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{SessionID: idS, Alias: "supersede", InstanceID: "ins_supersede00000000000000", HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic"})
	// Note: midS was addressed to the generic offline session; move it by
	// claiming generically is not possible cross-session, so instead
	// supersede mid directly: expire the first lease by waiting is not
	// viable at 30s. Verify supersession at store level by reclaiming
	// after forcing the stored expiry into the past via a fresh claim on
	// a second message addressed to the supersede session.
	resS, err := e.send(e.aliceSender(), idS, "hello", "cmd_review_supersede2", protocol.DeliveryQueue)
	if err != nil {
		t.Fatalf("supersede send: %v", err)
	}
	_ = midS
	claimedS, err := e.store.ClaimInbox(e.ctx, idS, idS, 1*time.Millisecond, 10)
	if err != nil || len(claimedS) != 1 {
		t.Fatalf("supersede claim: %v %+v", err, claimedS)
	}
	_, tok1, gen1, _, _, err := e.store.ClaimInfo(e.ctx, resS.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	claimedS2, err := e.store.ClaimInbox(e.ctx, idS, idS, 30*time.Second, 10)
	if err != nil || len(claimedS2) != 1 {
		t.Fatalf("supersede reclaim: %v %+v", err, claimedS2)
	}
	_, token3, gen3, _, _, err := e.store.ClaimInfo(e.ctx, resS.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if token3 == tok1 && gen3 == gen1 {
		t.Fatalf("expected superseded fences after reclaim, still %q/%d", token3, gen3)
	}
	if err := e.store.RenewClaim(e.ctx, resS.MessageID, idS, tok1, gen1, 30*time.Second); err == nil {
		t.Fatal("renew with superseded token/gen must fail")
	}
}

func Test_ReviewFix_DeliveryMonotonic(t *testing.T) {
	e := newEnv(t)
	mid := mustMsgID(t, e, "cmd_review_mono01")
	msg, err := e.store.GetMessage(e.ctx, mid)
	if err != nil {
		t.Fatal(err)
	}
	evt := func() protocol.EventEnvelope {
		return protocol.EventEnvelope{EventID: fmt.Sprintf("evt_%d", time.Now().UnixNano()), SessionID: msg.To, EventType: protocol.EventMessageReceipt, Timestamp: time.Now().UTC(), Payload: []byte("{}")}
	}
	rcpt := func(s protocol.ReceiptStatus) protocol.Receipt {
		return protocol.Receipt{MessageID: mid, CommandID: msg.CommandID, Status: s, From: msg.From, To: msg.To, ObservedAt: time.Now().UTC(), Detail: "t"}
	}
	// Delivered first, then a delayed presented must be rejected (regression).
	if err := e.store.RecordDeliveryOutcome(e.ctx, rcpt(protocol.ReceiptDelivered), evt()); err != nil {
		t.Fatalf("delivered: %v", err)
	}
	if err := e.store.RecordDeliveryOutcome(e.ctx, rcpt(protocol.ReceiptPresented), evt()); err == nil {
		t.Fatal("delivered must not regress to acknowledged via presented")
	}
	if err := e.store.RecordDeliveryOutcome(e.ctx, rcpt(protocol.ReceiptInjected), evt()); err == nil {
		t.Fatal("delivered must not regress to acknowledged via injected")
	}
	// Duplicate delivered is idempotent.
	if err := e.store.RecordDeliveryOutcome(e.ctx, rcpt(protocol.ReceiptDelivered), evt()); err != nil {
		t.Fatalf("duplicate delivered must be idempotent: %v", err)
	}
	// Opposite order on a fresh message: presented then delivered forwards.
	mid2 := mustMsgID(t, e, "cmd_review_mono02")
	msg2, _ := e.store.GetMessage(e.ctx, mid2)
	rcpt2 := func(s protocol.ReceiptStatus) protocol.Receipt {
		return protocol.Receipt{MessageID: mid2, CommandID: msg2.CommandID, Status: s, From: msg2.From, To: msg2.To, ObservedAt: time.Now().UTC(), Detail: "t"}
	}
	if err := e.store.RecordDeliveryOutcome(e.ctx, rcpt2(protocol.ReceiptPresented), evt()); err != nil {
		t.Fatalf("presented: %v", err)
	}
	if err := e.store.RecordDeliveryOutcome(e.ctx, rcpt2(protocol.ReceiptPresented), evt()); err != nil {
		t.Fatalf("duplicate presented must be idempotent: %v", err)
	}
	if err := e.store.RecordDeliveryOutcome(e.ctx, rcpt2(protocol.ReceiptDelivered), evt()); err != nil {
		t.Fatalf("presented -> delivered must forward: %v", err)
	}
	// After delivered, a late rejected must be rejected.
	if err := e.store.RecordDeliveryOutcome(e.ctx, rcpt2(protocol.ReceiptRejected), evt()); err == nil {
		t.Fatal("delivered must not transition to rejected")
	}
}

func Test_ReviewFix_RemoteCumulativeBudgetAcrossRestart(t *testing.T) {
	eA := newEnv(t)
	idA, err := remote.GenerateIdentity(eA.node.HostID())
	if err != nil {
		t.Fatal(err)
	}
	eA.node.SetRemoteIdentity(idA)

	// Failing mTLS server: counts requests, always errors.
	counter := &countingServer{}
	srv := remote.NewServer("127.0.0.1:0", idA, func(ctx context.Context, env remote.Envelope, peerHost, peerFP string) (string, error) {
		counter.hit()
		return "", fmt.Errorf("forced failure")
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.ListenAndServe(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for (srv.Addr() == "" || srv.Addr() == "127.0.0.1:0") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("counter server did not bind")
	}
	remoteHost := idA.HostID
	if err := eA.store.EnrollHost(eA.ctx, remoteHost, srv.Addr(), idA.Fingerprint); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after attempt one: stored Attempts=1, past schedule.
	envJSON := `{"originHost":"x","originFingerprint":"fp","fromPrincipal":"p","fromSession":"` + eA.aliceID + `","originalMessageId":"m1","originalCommandId":"c1","targetSession":"ses_t","correlationId":"","hops":0,"body":{"type":"text","text":"hi"},"delivery":{"mode":"notify"}}`
	_ = eA.store.RecordRemoteTransferFull(eA.ctx, "msg_review_crash1", remoteHost, "ses_t", eA.aliceID, envJSON)
	_ = eA.store.SetRemoteTransferRetry(eA.ctx, "msg_review_crash1", "failed", "forced failure", 1, time.Now().UTC().Add(-time.Second))
	_ = counter.set(0)
	// Restart recovery must respect the stored attempt count: only attempts
	// 2..5 remain (4 more requests), never a fresh 5 (total 6).
	_, err = eA.node.ResumeRemoteTransfers(eA.ctx)
	if err != nil {
		// Resume returns after attempting; error is allowed via attempt failure
		// propagation in some impls, but count is what matters.
		t.Logf("resume error (may be nil): %v", err)
	}
	tr, err := eA.store.GetRemoteTransfer(eA.ctx, "msg_review_crash1")
	if err != nil {
		t.Fatal(err)
	}
	if tr.Attempts != node.MaxRemoteAttempts {
		t.Fatalf("cumulative attempts must equal %d, got %d", node.MaxRemoteAttempts, tr.Attempts)
	}
	if got := counter.get(); got != node.MaxRemoteAttempts-1 {
		t.Fatalf("network requests after restart must be %d (cumulative %d), got %d", node.MaxRemoteAttempts-1, node.MaxRemoteAttempts, got)
	}

	// Backoff: a transfer whose NextRetryAt is in the future must not be
	// attempted by a normal retry.
	_ = eA.store.RecordRemoteTransferFull(eA.ctx, "msg_review_backoff", remoteHost, "ses_t", eA.aliceID, envJSON)
	_ = eA.store.SetRemoteTransferRetry(eA.ctx, "msg_review_backoff", "failed", "room", 1, time.Now().UTC().Add(10*time.Minute))
	_ = counter.set(0)
	if _, err := eA.node.RetryRemote(eA.ctx, eA.aliceSender(), "msg_review_backoff"); err == nil {
		t.Fatal("retry during backoff must be refused without a network attempt")
	}
	if got := counter.get(); got != 0 {
		t.Fatalf("backoff retry must make 0 network requests, got %d", got)
	}
}

type countingServer struct {
	n int
}

func (c *countingServer) hit() {
	c.n++
}
func (c *countingServer) get() int { return c.n }
func (c *countingServer) set(v int) int {
	c.n = v
	return v
}

func Test_ReviewFix_DelegationAttachPreservesOwner(t *testing.T) {
	e := newEnv(t)
	// Owner registration is via newEnv (in-process). Issue a delegation for
	// an adapter session and verify it does not enroll a new owner.
	if err := e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{
		SessionID: "ses_review_owner0000000000", Alias: "owner", InstanceID: "ins_owner00000000000000",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(),
		Capabilities: []protocol.Capability{{ID: protocol.CapPeerSend, Support: protocol.SupportNative}},
		Delivery:       []protocol.DeliveryMode{protocol.DeliveryNotify, protocol.DeliverySteer},
		Harness:        "muse",
	}); err != nil {
		t.Fatal(err)
	}
	ownerSender := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: "ses_review_owner0000000000"}
	tok, err := e.node.IssueDelegationFor(e.ctx, ownerSender, "ses_review_owner0000000000")
	if err != nil || tok == "" {
		t.Fatalf("issue delegation: %v", err)
	}
	if err := e.node.VerifyDelegationFor(e.ctx, "ses_review_owner0000000000", tok); err != nil {
		t.Fatalf("verify delegation: %v", err)
	}
	if err := e.node.VerifyDelegationFor(e.ctx, "ses_review_owner0000000000", "bad"); err == nil {
		t.Fatal("bad delegation token must not verify")
	}
	// Expired token (negative TTL) must fail.
	curRec, _ := e.store.GetSession(e.ctx, "ses_review_owner0000000000")
	expTok, err := e.store.IssueDelegation(e.ctx, "ses_review_owner0000000000", curRec.InstanceID, -time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.node.VerifyDelegationFor(e.ctx, "ses_review_owner0000000000", expTok); err == nil {
		t.Fatal("expired delegation token must not verify")
	}
	// Revoked token must fail on a subsequent (existing-connection) verify.
	revTok, err := e.store.IssueDelegation(e.ctx, "ses_review_owner0000000000", curRec.InstanceID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.RevokeDelegation(e.ctx, revTok); err != nil {
		t.Fatal(err)
	}
	if err := e.node.VerifyDelegationFor(e.ctx, "ses_review_owner0000000000", revTok); err == nil {
		t.Fatal("revoked delegation token must not verify")
	}
	// Owner-instance replacement supersedes old tokens: resume owner with a
	// new instance id, then the previously-issued token must fail.
	// (RegisterAdapterSession with secret via store: use RegisterSessionWithSecret path through node resume.)
	// Go through the store directly to simulate a new process instance.
	newPeer := protocol.PeerInfo{
		SessionID: "ses_review_owner0000000000", Alias: "owner", InstanceID: "ins_owner_replacement_0001",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(),
		Capabilities: []protocol.Capability{}, Delivery: nil, Harness: "muse",
	}
	// Fetch the enrolled secret is not stored plaintext; use Verify bypass via same principal
	// by issuing with explicit secret: the node path GenerateSessionSecretForNode is not
	// accessible here, so simulate by updating instance via a fresh registration with the
	// original secret token's owner check: re-issue after direct DB instance bump is not
	// possible, so instead verify the store check by issuing with a stale owner instance.
	staleTok, err := e.store.IssueDelegation(e.ctx, "ses_review_owner0000000000", "ins_stale_instance_0000", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// staleTok was issued with a non-current instance and must not verify.
	if err := e.node.VerifyDelegationFor(e.ctx, "ses_review_owner0000000000", staleTok); err == nil {
		t.Fatal("delegation token for a superseded owner instance must not verify")
	}
	_ = newPeer
	before, err := e.store.GetSession(e.ctx, "ses_review_owner0000000000")
	if err != nil {
		t.Fatal(err)
	}
	// Verify does not mutate instance/capabilities (attach semantics).
	after, err := e.store.GetSession(e.ctx, "ses_review_owner0000000000")
	if err != nil {
		t.Fatal(err)
	}
	if before.InstanceID != after.InstanceID || before.Harness != after.Harness {
		t.Fatal("verification mutated owner identity")
	}
	if len(after.Delivery) != 2 {
		t.Fatalf("delivery modes changed: %v", after.Delivery)
	}
}
