// Package conformance - Phase 4 discovery and conversation tests.
package conformance

import (
	"testing"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
)

func Test_Phase4_DiscoveryCarriesTrustAndLastSeen(t *testing.T) {
	e := newEnv(t)
	peers, err := e.node.DiscoverFor(e.ctx, e.aliceSender())
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(peers) == 0 {
		t.Fatal("no peers discovered")
	}
	for _, p := range peers {
		if p.TrustDomain == "" {
			t.Errorf("peer %s has empty trustDomain", p.SessionID)
		}
		// LastSeen is populated from last_seen (heartbeat/registration).
		if p.LastSeen.IsZero() {
			t.Errorf("peer %s has zero lastSeen", p.SessionID)
		}
		if p.SessionID == "" || p.InstanceID == "" || p.HostID == "" || p.PrincipalID == "" {
			t.Errorf("peer missing identity fields: %+v", p)
		}
	}
}

func Test_Phase4_ConversationLifecycle(t *testing.T) {
	e := newEnv(t)
	conv, err := e.node.CreateConversation(e.ctx, e.aliceSender(), "cnv_phase400000000000000001", []string{e.aliceID, e.bobID}, "chat")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if conv.ID != "cnv_phase400000000000000001" || conv.Status != "open" {
		t.Errorf("conversation = %+v", conv)
	}
	// Send inside the conversation.
	if _, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_phase4000000000000000001", To: e.bobID,
		Body: protocol.MessageBody{Type: "text", Text: "hello in conv"},
		Delivery: protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		ConversationID: conv.ID, CorrelationID: conv.ID,
	}); err != nil {
		t.Fatalf("send in conv: %v", err)
	}
	// Participant history reads it (idempotent replay: same result twice).
	h1, err := e.node.ConversationHistory(e.ctx, e.bobSender(), conv.ID, 10)
	if err != nil || len(h1) != 1 {
		t.Fatalf("history = %d %v", len(h1), err)
	}
	h2, err := e.node.ConversationHistory(e.ctx, e.bobSender(), conv.ID, 10)
	if err != nil || len(h2) != 1 || h2[0].MessageID != h1[0].MessageID {
		t.Errorf("replay not idempotent: %+v vs %+v", h1, h2)
	}
	// Non-participant cannot read history.
	other := "ses_other40000000000000000000"
	if err := e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{
		SessionID: other, Alias: "other4", InstanceID: "ins_other400000000000000000",
		HostID: e.node.HostID(), PrincipalID: e.node.PrincipalID(), Harness: "synthetic",
	}); err != nil {
		t.Fatalf("register other: %v", err)
	}
	if _, err := e.node.ConversationHistory(e.ctx, node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: other}, conv.ID, 10); err == nil {
		t.Error("non-participant read conversation history")
	}
	// Non-participant cannot send into the conversation.
	if _, err := e.node.Send(e.ctx, node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: other}, node.SendRequest{
		CommandID: "cmd_phase4000000000000000002", To: e.bobID,
		Body: protocol.MessageBody{Type: "text", Text: "intrude"},
		Delivery: protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		ConversationID: conv.ID,
	}); err == nil {
		t.Error("non-participant sent into conversation")
	}
	// Close, then sends are rejected but history remains readable.
	if err := e.node.CloseConversation(e.ctx, e.aliceSender(), conv.ID); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_phase4000000000000000003", To: e.bobID,
		Body: protocol.MessageBody{Type: "text", Text: "after close"},
		Delivery: protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		ConversationID: conv.ID,
	}); err == nil {
		t.Error("send into closed conversation was allowed")
	}
	if _, err := e.node.ConversationHistory(e.ctx, e.bobSender(), conv.ID, 10); err != nil {
		t.Errorf("history unreadable after close: %v", err)
	}
}

func Test_Phase4_JsonBodyRequiresData(t *testing.T) {
	e := newEnv(t)
	if _, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_phase4000000000000000004", To: e.bobID,
		Body: protocol.MessageBody{Type: "json"},
		Delivery: protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
	}); err == nil {
		t.Error("json body without data was accepted")
	}
	if _, err := e.node.Send(e.ctx, e.aliceSender(), node.SendRequest{
		CommandID: "cmd_phase4000000000000000005", To: e.bobID,
		Body: protocol.MessageBody{Type: "json", Data: []byte(`{"k":1}`)},
		Delivery: protocol.Delivery{Mode: protocol.DeliveryNotify, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
	}); err != nil {
		t.Errorf("json body with data was rejected: %v", err)
	}
}
