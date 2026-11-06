// Package conformance implements the required protocol tests from
// ICSP-001 section 14.
//
// Each test is numbered to match the specification's list so that coverage
// can be checked against the spec directly. Tests distinguish deterministic
// protocol qualification from live agent interoperability.
package conformance

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/B-A-M-N/intercom/internal/adapter"
	"github.com/B-A-M-N/intercom/internal/config"
	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

// fakeAdapter is a synthetic agent harness used for deterministic protocol
// qualification.
//
// It records what it was asked to do so that tests can assert on delivery
// behaviour without running a real agent.
type fakeAdapter struct {
	mu        sync.Mutex
	harness   string
	version   string
	modes     []protocol.DeliveryMode
	capabil   []protocol.Capability
	state     adapter.SessionState
	delivered []protocol.Message
	notified  []protocol.Message
	steerable bool
	woken     []protocol.Message
}

func (f *fakeAdapter) notifiedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.notified)
}
func (f *fakeAdapter) notifiedMsg(i int) protocol.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.notified[i]
}
func (f *fakeAdapter) deliveredCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.delivered) }
func (f *fakeAdapter) deliveredMsg(i int) protocol.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.delivered[i]
}
func (f *fakeAdapter) wokenCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.woken) }
func (f *fakeAdapter) wokenMsg(i int) protocol.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.woken[i]
}

func newFakeAdapter(modes ...protocol.DeliveryMode) *fakeAdapter {
	if len(modes) == 0 {
		modes = []protocol.DeliveryMode{protocol.DeliveryNotify, protocol.DeliveryQueue}
	}
	return &fakeAdapter{
		harness:   "synthetic",
		version:   "0.1.0",
		modes:     modes,
		steerable: false,
		state:     adapter.SessionState{State: protocol.SessionIdle, SafePoint: true},
	}
}

func (f *fakeAdapter) Identity() (string, string) { return f.harness, f.version }

func (f *fakeAdapter) Capabilities() []protocol.Capability { return f.capabil }

func (f *fakeAdapter) SupportedDeliveryModes() []protocol.DeliveryMode { return f.modes }

func (f *fakeAdapter) State(ctx context.Context, sessionID string) (adapter.SessionState, error) {
	return f.state, nil
}

func (f *fakeAdapter) Deliver(ctx context.Context, sessionID string, msg protocol.Message) (adapter.DeliveryResult, error) {
	f.mu.Lock()
	f.delivered = append(f.delivered, msg)
	f.mu.Unlock()
	return adapter.DeliveryResult{
		Status: protocol.ReceiptDelivered,
		Detail: "queued for the next eligible turn",
	}, nil
}

func (f *fakeAdapter) Notify(ctx context.Context, sessionID string, msg protocol.Message) (adapter.DeliveryResult, error) {
	f.mu.Lock()
	f.notified = append(f.notified, msg)
	f.mu.Unlock()
	return adapter.DeliveryResult{
		Status: protocol.ReceiptPresented,
		Detail: "presented by the notification handler",
	}, nil
}

func (f *fakeAdapter) Wake(ctx context.Context, sessionID string, msg protocol.Message) (adapter.DeliveryResult, error) {
	f.mu.Lock()
	f.woken = append(f.woken, msg)
	f.mu.Unlock()
	return adapter.DeliveryResult{
		Status: protocol.ReceiptHandled,
		Detail: "recipient woken to act on the message",
	}, nil
}

// steerAdapter is a synthetic harness that genuinely supports active-turn
// steering.
type steerAdapter struct {
	*fakeAdapter
}

func newSteerAdapter() *steerAdapter {
	f := newFakeAdapter(protocol.DeliveryNotify, protocol.DeliveryQueue, protocol.DeliverySteer)
	f.steerable = true
	return &steerAdapter{fakeAdapter: f}
}

func (s *steerAdapter) Deliver(ctx context.Context, sessionID string, msg protocol.Message) (adapter.DeliveryResult, error) {
	s.mu.Lock()
	s.delivered = append(s.delivered, msg)
	s.mu.Unlock()
	if !s.steerable {
		return adapter.DeliveryResult{}, adapter.ErrUnsupported
	}
	return adapter.DeliveryResult{
		Status: protocol.ReceiptInjected,
		Detail: "injected into the active turn at a safe point",
	}, nil
}

// env is a fully wired node with two synthetic peers.
type env struct {
	t       *testing.T
	node    *node.Node
	store   *store.Store
	alice   *fakeAdapter
	bob     *fakeAdapter
	aliceID string
	bobID   string
	ctx     context.Context
}

// newEnv builds a node with two registered synthetic peers.
func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = shortSocketDir(t) + "/intercom.sock"
	cfg.Alias = "test-host"
	cfg.PrincipalID = "prn_test000000000000000000000000000"
	cfg.Limits.PerPeerRatePerMinute = 10000

	st, err := store.Open(ctx, store.Options{Path: cfg.DatabasePath()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	n, err := node.New(ctx, node.Options{Config: cfg, Store: st})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	alice := newFakeAdapter()
	bob := newFakeAdapter()
	aliceID := "ses_alice00000000000000000000000"
	bobID := "ses_bob000000000000000000000000"

	for _, peer := range []struct {
		id  string
		ad  adapter.Adapter
		ali string
	}{
		{aliceID, alice, "alice"},
		{bobID, bob, "bob"},
	} {
		err := n.RegisterSession(ctx, protocol.PeerInfo{
			SessionID:   peer.id,
			Alias:       peer.ali,
			InstanceID:  "ins_test0000000000000000000000",
			HostID:      n.HostID(),
			PrincipalID: n.PrincipalID(),
		}, peer.ad)
		if err != nil {
			t.Fatalf("register session: %v", err)
		}
	}

	return &env{
		t: t, node: n, store: st,
		alice: alice, bob: bob,
		aliceID: aliceID, bobID: bobID,
		ctx: ctx,
	}
}

// aliceSender returns the authenticated sender identity for Alice.
func (e *env) aliceSender() node.Sender {
	return node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: e.aliceID}
}

// bobSender returns the authenticated sender identity for Bob.
func (e *env) bobSender() node.Sender {
	return node.Sender{PrincipalID: e.node.PrincipalID(), SessionID: e.bobID}
}

// send submits a text message and returns the result.
func (e *env) send(from node.Sender, to, text, commandID string, mode protocol.DeliveryMode) (node.SendResult, error) {
	return e.node.Send(e.ctx, from, node.SendRequest{
		CommandID: commandID,
		To:        to,
		Body:      protocol.MessageBody{Type: "text", Text: text},
		Delivery: protocol.Delivery{
			Mode:          mode,
			Wake:          protocol.WakeNone,
			OnUnsupported: protocol.UnsupportedReject,
		},
	})
}

// waitFor polls until cond holds or the deadline expires.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
