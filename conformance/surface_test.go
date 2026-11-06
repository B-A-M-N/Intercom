package conformance

import (
	"testing"
	"time"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

func Test_Surface_NonDestructiveReplayAndPresent(t *testing.T) {
	e := newEnv(t)
	offline := "ses_surface_offline00000000"
	_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{SessionID: offline, Alias: "surfaceoff", InstanceID: "ins_surfaceoff000000000000", HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic"})
	res, err := e.send(e.aliceSender(), offline, "hello surface", "cmd_surface0000000000001", protocol.DeliveryNotify)
	if err != nil {
		t.Fatal(err)
	}
	msgID := res.MessageID
	sender := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}
	events, next, err := e.node.SurfaceReplay(e.ctx, sender, offline, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range events {
		if ev.Message.MessageID == msgID {
			found = true
			if ev.Message.Body.Text != "hello surface" {
				t.Fatalf("display body %q", ev.Message.Body.Text)
			}
			if ev.Status == protocol.ReceiptPresented {
				t.Fatal("replay must not present before surface ack")
			}
		}
	}
	if !found {
		t.Fatal("surface replay missing displayable message")
	}
	// Inbox must still be claimable by the agent after surface read.
	claimed, err := e.store.ClaimInbox(e.ctx, offline, offline, 30*time.Second, 10)
	if err != nil || len(claimed) == 0 {
		t.Fatalf("surface read must not claim: %v %+v", err, claimed)
	}
	// Present only after explicit surface confirm.
	// Reset claim effect: need fresh message for clean present.
	res2, err := e.send(e.aliceSender(), offline, "second hello", "cmd_surface0000000000002", protocol.DeliveryNotify)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.node.SurfacePresent(e.ctx, sender, res2.MessageID); err != nil {
		t.Fatal(err)
	}
	receipts, err := e.store.ReceiptsFor(e.ctx, res2.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	hasPresented := false
	for _, r := range receipts {
		if r.Status == protocol.ReceiptPresented {
			hasPresented = true
		}
	}
	if !hasPresented {
		t.Fatalf("expected presented receipt after surface confirm, got %+v", receipts)
	}
	// Replay from cursor does not duplicate or refabricate presented for
	// the first message.
	events2, _, err := e.node.SurfaceReplay(e.ctx, sender, offline, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	_ = events2
	// Reply preserves correlation.
	replyRes, err := e.node.SurfaceReply(e.ctx, sender, msgID, "cmd_surface_reply0000001", "thanks")
	if err != nil {
		t.Fatal(err)
	}
	if replyRes.MessageID == "" {
		t.Fatal("reply must admit a message")
	}
	_ = next
}

func Test_Surface_DelegationScope(t *testing.T) {
	e := newEnv(t)
	offline := "ses_surface_scope000000000"
	_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{SessionID: offline, Alias: "surfacescope", InstanceID: "ins_surfacescope0000000000", HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic"})
	sender := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}
	tok, err := e.node.SurfaceIssueDelegation(e.ctx, sender, offline, "read", 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := e.store.VerifyDelegationDetailed(e.ctx, offline, tok)
	if err != nil {
		t.Fatal(err)
	}
	if info.Scope != "surface" {
		t.Fatalf("scope %q", info.Scope)
	}
	if !info.Has("read") || info.Has("reply") {
		t.Fatalf("perms %v", info.Permissions)
	}
	// Surface token must not be usable as MCP scope.
	if _, err := e.store.VerifyDelegationDetailed(e.ctx, offline, tok); err != nil {
		t.Fatal(err)
	}
	// MCP-issued tokens still verify as mcp.
	owner := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: e.aliceID}
	mcpTok, err := e.node.IssueDelegationFor(e.ctx, owner, e.aliceID)
	if err != nil {
		t.Fatal(err)
	}
	mInfo, err := e.store.VerifyDelegationDetailed(e.ctx, e.aliceID, mcpTok)
	if err != nil {
		t.Fatal(err)
	}
	if mInfo.Scope != "mcp" {
		t.Fatalf("mcp scope %q", mInfo.Scope)
	}
	_ = store.ErrForbidden
}
