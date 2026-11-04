// Package node implements the Intercom session core and peer communication
// plane.
//
// The node owns durability and delivery truth. Adapters own harness quirks
// and never decide durability outcomes.
package node

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bamn/intercom/internal/adapter"
	"github.com/bamn/intercom/internal/config"
	"github.com/bamn/intercom/internal/ids"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
)

// ErrNotFound is returned when a referenced session does not exist.
var ErrNotFound = errors.New("node: not found")

// ErrForbidden is returned when policy refuses an operation.
var ErrForbidden = errors.New("node: forbidden")

// ErrLimitExceeded is returned when a configured resource bound is hit.
var ErrLimitExceeded = errors.New("node: limit exceeded")

// Node is one Intercom host.
type Node struct {
	cfg   config.Config
	store *store.Store

	mu          sync.RWMutex
	adapters    map[string]adapter.Adapter // keyed by session id
	hostID      string
	principalID string

	// rate limiting state, keyed by peer session
	rateMu   sync.Mutex
	rateSeen map[string][]time.Time

	// observers receive delivery events
	obsMu     sync.RWMutex
	observers map[string][]chan protocol.Receipt

	// streams serialize delivery within each sender-recipient pair, so
	// ordering is preserved within one ordered stream.
	streamMu sync.Mutex
	streams  map[streamKey]chan protocol.Message
}

// Options configure a Node.
type Options struct {
	Config config.Config
	Store  *store.Store
}

// New creates a node and establishes its host identity.
func New(ctx context.Context, opts Options) (*Node, error) {
	if opts.Store == nil {
		return nil, errors.New("node: store is required")
	}
	if err := opts.Config.Validate(); err != nil {
		return nil, err
	}

	n := &Node{
		cfg:         opts.Config,
		store:       opts.Store,
		adapters:    make(map[string]adapter.Adapter),
		rateSeen:    make(map[string][]time.Time),
		observers:   make(map[string][]chan protocol.Receipt),
		principalID: opts.Config.PrincipalID,
	}

	if n.principalID == "" {
		pid, err := ids.NewPrincipalID()
		if err != nil {
			return nil, err
		}
		n.principalID = pid
	}

	hostID, err := ids.NewHostID()
	if err != nil {
		return nil, err
	}
	n.hostID = hostID

	if err := opts.Store.EnsurePrincipal(ctx, n.principalID, opts.Config.Alias); err != nil {
		return nil, err
	}
	if err := opts.Store.EnsureHost(ctx, n.hostID, n.principalID, opts.Config.Alias); err != nil {
		return nil, err
	}
	return n, nil
}

// HostID returns this node's host identity.
func (n *Node) HostID() string { return n.hostID }

// PrincipalID returns this node's authenticated principal.
func (n *Node) PrincipalID() string { return n.principalID }

// principalIDFor returns the authenticated principal behind a session.
//
// The principal is looked up from the peer registry rather than assumed, so
// the provenance of an injected message names the principal that actually
// sent it.
func (n *Node) principalIDFor(ctx context.Context, sessionID string) string {
	rec, err := n.store.GetSession(ctx, sessionID)
	if err != nil {
		return ""
	}
	return rec.PrincipalID
}

// Config returns the node's configuration.
func (n *Node) Config() config.Config { return n.cfg }

// SetLimit updates one resource bound at runtime.
//
// The limits are mutable at runtime so that an operator can tighten a bound
// on a running host. Exhaustion still results in explicit backpressure or
// rejection rather than unbounded growth.
func (n *Node) SetLimit(set func(*config.Limits)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	set(&n.cfg.Limits)
}

// Store exposes the durable store.
func (n *Node) Store() *store.Store { return n.store }

// RegisterSession registers a session and its adapter with this node.
func (n *Node) RegisterSession(ctx context.Context, peer protocol.PeerInfo, ad adapter.Adapter) error {
	if peer.SessionID == "" {
		return errors.New("node: session id is required")
	}
	if peer.PrincipalID == "" {
		peer.PrincipalID = n.principalID
	}
	if peer.HostID == "" {
		peer.HostID = n.hostID
	}
	if peer.InstanceID == "" {
		return errors.New("node: instance id is required")
	}
	if peer.Presence == "" {
		peer.Presence = protocol.PresenceOnline
	}
	if ad != nil {
		peer.Harness, peer.HarnessVersion = ad.Identity()
		peer.Capabilities = ad.Capabilities()
		peer.Delivery = ad.SupportedDeliveryModes()
	}

	if err := n.store.RegisterSession(ctx, peer); err != nil {
		return err
	}
	if ad != nil {
		n.mu.Lock()
		n.adapters[peer.SessionID] = ad
		n.mu.Unlock()
	}

	ev, err := n.store.AppendEvent(ctx, protocol.EventEnvelope{
		EventID:   mustID("evt"),
		SessionID: peer.SessionID,
		EventType: protocol.EventSessionCreated,
		Timestamp: time.Now().UTC(),
		Payload:   mustJSON(peer),
	}, "")
	if err != nil {
		return err
	}
	_ = ev
	return n.store.Audit(ctx, n.principalID, peer.SessionID, "session.registered", peer.Harness)
}

// Adapter returns the adapter bound to a session.
func (n *Node) Adapter(sessionID string) (adapter.Adapter, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	ad, ok := n.adapters[sessionID]
	return ad, ok
}

// UnregisterSession removes a session's adapter.
//
// The logical session survives: a host disconnect alone SHALL NOT
// automatically terminate the logical session.
func (n *Node) UnregisterSession(ctx context.Context, sessionID string) error {
	n.mu.Lock()
	delete(n.adapters, sessionID)
	n.mu.Unlock()
	return n.store.SetPresence(ctx, sessionID, protocol.PresenceDisconnected)
}

// Discover lists the peers visible to this node's principal.
func (n *Node) Discover(ctx context.Context) ([]protocol.PeerInfo, error) {
	peers, err := n.store.ListPeers(ctx, n.principalID, n.cfg.Limits.MaxReplayBatch)
	if err != nil {
		return nil, err
	}
	out := make([]protocol.PeerInfo, 0, len(peers))
	for _, p := range peers {
		out = append(out, p.PeerInfo)
	}
	return out, nil
}

// Resolve maps an alias or session identifier to its canonical peer record.
func (n *Node) Resolve(ctx context.Context, aliasOrID string) (protocol.PeerInfo, error) {
	rec, err := n.store.ResolveSession(ctx, aliasOrID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.PeerInfo{}, ErrNotFound
		}
		return protocol.PeerInfo{}, err
	}
	return rec.PeerInfo, nil
}

// Capabilities returns the effective capabilities of a peer.
func (n *Node) Capabilities(ctx context.Context, sessionID string) ([]protocol.Capability, error) {
	rec, err := n.store.GetSession(ctx, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return rec.Capabilities, nil
}

// Presence returns a peer's last observed presence state.
func (n *Node) Presence(ctx context.Context, sessionID string) (protocol.PresenceState, error) {
	rec, err := n.store.GetSession(ctx, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.PresenceUnknown, ErrNotFound
		}
		return protocol.PresenceUnknown, err
	}
	return rec.Presence, nil
}
