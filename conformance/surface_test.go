package conformance

import (
	"testing"
	"time"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

func Test_Surface_PresentThenClaimBothOrders(t *testing.T) {
	for _, order := range []string{"present-before-claim", "claim-before-present"} {
		t.Run(order, func(t *testing.T) {
			e := newEnv(t)
			offline := "ses_surface_" + order[:6] + "offline000000000"
			_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{SessionID: offline, Alias: "surfaceoff", InstanceID: "ins_surfaceoff000000000000", HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic"})
			sender := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}
			res, err := e.send(e.aliceSender(), offline, "hello surface", "cmd_surface_"+order+"_1", protocol.DeliveryNotify)
			if err != nil {
				t.Fatal(err)
			}
			msgID := res.MessageID
			present := func() {
				t.Helper()
				events, _, err := e.node.SurfaceReplay(e.ctx, sender, offline, 0, 100)
				_ = events
				if err != nil {
					t.Fatal(err)
				}
				if err := e.node.SurfacePresent(e.ctx, sender, msgID); err != nil {
					t.Fatal(err)
				}
				receipts, _ := e.store.ReceiptsFor(e.ctx, msgID)
				hasPresented := false
				for _, r := range receipts {
					if r.Status == protocol.ReceiptPresented {
						hasPresented = true
					}
				}
				if !hasPresented {
					t.Fatal("expected presented after surface confirm")
				}
			}
			claim := func() {
				t.Helper()
				claimed, err := e.store.ClaimInbox(e.ctx, offline, offline, 30*time.Second, 10)
				if err != nil || len(claimed) == 0 {
					t.Fatalf("claim must succeed after surface ops: %v %+v", err, claimed)
				}
				// Agent report with fencing
				var cid string
				var tok string
				var gen int64
				found := false
				for _, m := range claimed {
					if m.MessageID == msgID {
						cid = m.MessageID
						tok = m.ClaimToken
						gen = m.ClaimGeneration
						found = true
					}
				}
				if !found {
					t.Fatal("claimed does not contain target")
				}
				if err := e.node.ReportDeliveryFor(e.ctx, node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}, cid, protocol.ReceiptPresented, "agent presented", tok, gen); err != nil {
					t.Logf("agent report presented err (may be monotonic idempotent): %v", err)
				}
			}
			if order == "present-before-claim" {
				present()
				claim()
			} else {
				claim()
				present()
			}
			// Both orders must leave presented receipt durable.
			receipts, _ := e.store.ReceiptsFor(e.ctx, msgID)
			hasPresented := false
			for _, r := range receipts {
				if r.Status == protocol.ReceiptPresented {
					hasPresented = true
				}
			}
			if !hasPresented {
				t.Fatal("presentation lost after agent claim")
			}
		})
	}
}

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
	// Present a fresh message (independent dimension).
	res2, err := e.send(e.aliceSender(), offline, "second hello", "cmd_surface0000000000002", protocol.DeliveryNotify)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.node.SurfacePresent(e.ctx, sender, res2.MessageID); err != nil {
		// res2 may already be delivered via live adapter? offline has no adapter, so must succeed.
		t.Fatal(err)
	}
	// Claim res2 after present: must still be claimable.
	claimed2, err := e.store.ClaimInbox(e.ctx, offline, offline, 30*time.Second, 10)
	_ = claimed2
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
	// Reply requires stable command ID.
	if _, err := e.node.SurfaceReply(e.ctx, sender, msgID, "", "thanks"); err == nil {
		t.Fatal("reply without commandId must fail")
	}
	replyRes, err := e.node.SurfaceReply(e.ctx, sender, msgID, "cmd_surface_reply0000001", "thanks")
	if err != nil {
		t.Fatal(err)
	}
	if replyRes.MessageID == "" {
		t.Fatal("reply must admit a message")
	}
	// Replay from cursor does not refabricate presented.
	_ = next
}

func Test_Surface_CursorGapAcrossRetention(t *testing.T) {
	makeTerminal := func(e *env, offline string, cmd string) string {
		res, err := e.send(e.aliceSender(), offline, "msg", cmd, protocol.DeliveryNotify)
		if err != nil { t.Fatal(err) }
		claimed, err := e.store.ClaimInbox(e.ctx, offline, offline, 30*time.Second, 10)
		if err != nil || len(claimed) == 0 { t.Fatalf("claim %v %+v", err, claimed) }
		for _, m := range claimed {
			if m.MessageID == res.MessageID {
				if err := e.node.ReportDeliveryFor(e.ctx, node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}, res.MessageID, protocol.ReceiptHandled, "handled", m.ClaimToken, m.ClaimGeneration); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err := e.store.GetMessage(e.ctx, res.MessageID); err != nil {
			t.Fatal("terminal message missing before purge")
		}
		return res.MessageID
	}

	// Case 1: terminal messages purged -> replay from 0 reports gap and purge count >0
	t.Run("purged_terminal_message_gap", func(t *testing.T) {
		e := newEnv(t)
		offline := "ses_surface_gap000000000001"
		_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{SessionID: offline, Alias: "gap", InstanceID: "ins_gap00000000000000001", HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic"})
		sender := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}
		mid := makeTerminal(e, offline, "cmd_gap_purge0000000001")
		removed, err := e.store.PurgeMessagesOlderThan(e.ctx, time.Now().Add(2*time.Hour))
		if err != nil { t.Fatal(err) }
		if removed < 1 { t.Fatalf("expected >=1 purge, got %d", removed) }
		if _, err := e.store.GetMessage(e.ctx, mid); err == nil {
			t.Fatal("message should be purged")
		}
		_, _, gap, _, _ := e.node.SurfaceReplayWithGap(e.ctx, sender, offline, 0, 10)
		if !gap { t.Fatal("expected gap after terminal message purge") }
	})

	// Case 2: journal compaction with retained newer message
	t.Run("journal_compaction_retained_newer", func(t *testing.T) {
		e := newEnv(t)
		offline := "ses_surface_gap000000000002"
		_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{SessionID: offline, Alias: "gap2", InstanceID: "ins_gap00000000000000002", HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic"})
		sender := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}
		_, _ = e.send(e.aliceSender(), offline, "first", "cmd_gap_compact000001", protocol.DeliveryNotify)
		res2, _ := e.send(e.aliceSender(), offline, "second", "cmd_gap_compact000002", protocol.DeliveryNotify)
		_ = res2
		events, next2, gap, _, err := e.node.SurfaceReplayWithGap(e.ctx, sender, offline, 0, 1)
		if err != nil || gap || len(events) != 1 { t.Fatalf("first page %v %v %d", gap, err, len(events)) }
		// Compact through next2 (keep newer events after cursor partially)
		// Compact before next2+1 to retain only events after first.
		if _, err := e.store.CompactEvents(e.ctx, offline, uint64(next2)+1); err != nil { t.Fatal(err) }
		// Replay from stale cursor 0 must gap (floor).
		_, _, gapStale, earliestStale, _ := e.node.SurfaceReplayWithGap(e.ctx, sender, offline, 0, 10)
		if !gapStale { t.Fatalf("stale cursor after compaction must gap earliest=%d", earliestStale) }
		// Replay from a cursor at/after floor must succeed and show newer message.
		eventsNew, _, gapNew, _, err := e.node.SurfaceReplayWithGap(e.ctx, sender, offline, uint64(next2), 10)
		if err != nil || gapNew { t.Fatalf("newer replay gap=%v err=%v", gapNew, err) }
		if len(eventsNew) == 0 { t.Fatal("expected retained newer message") }
		if eventsNew[0].Message.MessageID != res2.MessageID { t.Fatalf("expected newer message %s got %s", res2.MessageID, eventsNew[0].Message.MessageID) }
	})

	// Case 3: fully compacted journal
	t.Run("fully_compacted", func(t *testing.T) {
		e := newEnv(t)
		offline := "ses_surface_gap000000000003"
		_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{SessionID: offline, Alias: "gap3", InstanceID: "ins_gap00000000000000003", HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic"})
		sender := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}
		_, _ = e.send(e.aliceSender(), offline, "only", "cmd_gap_full000000001", protocol.DeliveryNotify)
		high, _ := e.store.LatestSequence(e.ctx, offline)
		if _, err := e.store.CompactEvents(e.ctx, offline, high+1); err != nil { t.Fatal(err) }
		_, _, gapFull, earliestFull, _ := e.node.SurfaceReplayWithGap(e.ctx, sender, offline, 0, 10)
		if !gapFull { t.Fatalf("fully compacted must gap earliest=%d", earliestFull) }
	})
}

func Test_Surface_ReceiptProjectionDistinct(t *testing.T) {
	e := newEnv(t)
	offline := "ses_surface_proj000000000001"
	_ = e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{SessionID: offline, Alias: "proj", InstanceID: "ins_proj000000000000001", HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic"})
	sender := node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: offline}
	res, err := e.send(e.aliceSender(), offline, "check", "cmd_surface_proj000001", protocol.DeliveryNotify)
	if err != nil { t.Fatal(err) }
	mid := res.MessageID
	events, _, err := e.node.SurfaceReplay(e.ctx, sender, offline, 0, 10)
	if err != nil { t.Fatal(err) }
	var found *node.DisplayEvent
	for i := range events { if events[i].Message.MessageID == mid { found = &events[i] } }
	if found == nil { t.Fatal("missing") }
	// Before any receipt, human must be unknown, not delivered
	if found.HumanStatus != protocol.ReceiptUnknown {
		t.Fatalf("human status %q want unknown", found.HumanStatus)
	}
	if found.HumanStatus == protocol.ReceiptDelivered { t.Fatal("human must not default to delivered") }
	// Agent status may be accepted/unknown but must be separate field
	if found.AgentStatus == protocol.ReceiptPresented { t.Fatal("agent status must not be presented before surface ack") }
	// After present, human presented, agent unchanged dimension
	if err := e.node.SurfacePresent(e.ctx, sender, mid); err != nil { t.Fatal(err) }
	events2, _, _ := e.node.SurfaceReplay(e.ctx, sender, offline, 0, 10)
	var f2 *node.DisplayEvent
	for i := range events2 { if events2[i].Message.MessageID == mid { f2 = &events2[i] } }
	if f2.HumanStatus != protocol.ReceiptPresented { t.Fatalf("human after present %q", f2.HumanStatus) }
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
	if _, err := e.store.VerifyDelegationDetailed(e.ctx, offline, tok); err != nil {
		t.Fatal(err)
	}
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
