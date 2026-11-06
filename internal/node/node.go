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

	"github.com/B-A-M-N/intercom/internal/adapter"
	"github.com/B-A-M-N/intercom/internal/config"
	"github.com/B-A-M-N/intercom/internal/ids"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/remote"
	"github.com/B-A-M-N/intercom/internal/routing"
	"github.com/B-A-M-N/intercom/internal/store"
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

	// sessionSecrets holds in-process resume secrets for sessions
	// registered through this Node (trusted local adapter path).
	// Wire (ACP) registrations must present the client-held secret;
	// this map never satisfies an ACP resume.
	secretMu       sync.Mutex
	sessionSecrets map[string]string

	// rate limiting state, keyed by peer session
	rateMu   sync.Mutex
	rateSeen map[string][]time.Time

	// observers receive delivery events
	obsMu     sync.RWMutex
	observers map[string][]chan protocol.Receipt

	// streams serialize delivery within each sender-recipient pair, so
	// ordering is preserved within one ordered stream. Bounded: at most
	// maxStreams streams, each with a bounded queue, on the daemon-owned
	// lifecycle context (not the RPC request context).
	streamMu     sync.Mutex
	streams      map[streamKey]chan protocol.Message
	lifecycleCtx context.Context
	lifecycleCancel context.CancelFunc

	// remoteIdentity is this host's TLS identity for federation.
	remoteIdentity *remote.Identity
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
		cfg:            opts.Config,
		store:          opts.Store,
		adapters:       make(map[string]adapter.Adapter),
		rateSeen:       make(map[string][]time.Time),
		observers:      make(map[string][]chan protocol.Receipt),
		sessionSecrets: make(map[string]string),
		principalID:    opts.Config.PrincipalID,
		hostID:         opts.Config.HostID,
	}
	n.lifecycleCtx, n.lifecycleCancel = context.WithCancel(ctx)

	// Host and principal identity are stable across restarts: reuse the
	// identity already persisted in this store, mint only on first boot.
	// An explicitly configured identity that conflicts with the persisted
	// host is rejected rather than silently switching principals.
	existingHost, existingPrincipal, found, err := opts.Store.ExistingIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if found {
		if n.hostID == "" {
			n.hostID = existingHost
		} else if n.hostID != existingHost {
			return nil, errors.New("node: configured hostId conflicts with persisted host identity")
		}
		if n.principalID == "" {
			n.principalID = existingPrincipal
		} else if existingPrincipal != "" && n.principalID != existingPrincipal {
			return nil, errors.New("node: configured principalId conflicts with persisted host principal")
		}
		if n.principalID == "" {
			n.principalID = existingPrincipal
		}
	}
	if n.principalID == "" {
		pid, err := ids.NewPrincipalID()
		if err != nil {
			return nil, err
		}
		n.principalID = pid
	}
	if n.hostID == "" {
		hostID, err := ids.NewHostID()
		if err != nil {
			return nil, err
		}
		n.hostID = hostID
	}

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


// Close stops the daemon-owned dispatcher lifecycle. Queued-but-undelivered
// messages remain in the durable inbox and are claimable after restart.
func (n *Node) Close() {
	if n.lifecycleCancel != nil {
		n.lifecycleCancel()
	}
}

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

// inProcessSecret returns (or mints and remembers) the resume secret for
// a session registered through the trusted in-process path. The secret is
// held only in this Node's memory; a different Node/process sharing the
// store has no secret and cannot resume the session.
func (n *Node) inProcessSecret(sessionID, supplied string) string {
	if supplied != "" {
		n.secretMu.Lock()
		n.sessionSecrets[sessionID] = supplied
		n.secretMu.Unlock()
		return supplied
	}
	n.secretMu.Lock()
	defer n.secretMu.Unlock()
	if s, ok := n.sessionSecrets[sessionID]; ok {
		return s
	}
	s := store.GenerateSessionSecretForNode()
	n.sessionSecrets[sessionID] = s
	return s
}

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
	// Authoritative trust domain: assigned by the host, never client-supplied.
	peer.TrustDomain = "local:" + n.hostID
	peer.SessionSecret = n.inProcessSecret(peer.SessionID, peer.SessionSecret)
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

// authenticatedSender validates that a Sender carries a transport-established
// identity and, if its session is registered, that the session's owning
// principal matches. An unregistered ephemeral session (e.g. CLI) may act
// only as itself; it may not claim a registered session of another principal.
func (n *Node) validateSender(ctx context.Context, from Sender) error {
	if from.PrincipalID == "" || from.SessionID == "" {
		return protocol.Errorf(protocol.CodeUnauthorized, "the sender identity could not be established by the transport")
	}
	rec, err := n.store.GetSession(ctx, from.SessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if rec.PrincipalID != "" && rec.PrincipalID != from.PrincipalID {
		return protocol.Errorf(protocol.CodeForbidden, "session %q is owned by a different principal", from.SessionID)
	}
	return nil
}

// visibleTo reports whether sender may discover/observe target under the
// default policy: same trusted local principal, self, or an explicit grant.
func (n *Node) visibleTo(ctx context.Context, sender Sender, target store.PeerRecord) (bool, error) {
	if sender.SessionID == target.SessionID {
		return true, nil
	}
	if sender.PrincipalID != "" && target.PrincipalID != "" && sender.PrincipalID == target.PrincipalID {
		return true, nil
	}
	if sender.SessionID == "" {
		return false, nil
	}
	canSend, err := n.store.CanSend(ctx, sender.SessionID, target.SessionID)
	if err != nil {
		return false, err
	}
	if canSend {
		return true, nil
	}
	return n.store.CanApprove(ctx, sender.SessionID, target.SessionID)
}

// AuthorizeObserve enforces visibility for discovery/observe operations.
func (n *Node) AuthorizeObserve(ctx context.Context, sender Sender, sessionID string) error {
	if err := n.validateSender(ctx, sender); err != nil {
		return err
	}
	rec, err := n.store.GetSession(ctx, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Errorf(protocol.CodeForbidden, "session is not visible to this principal")
		}
		return err
	}
	ok, err := n.visibleTo(ctx, sender, rec)
	if err != nil {
		return err
	}
	if !ok {
		return protocol.Errorf(protocol.CodeForbidden, "session %q is not visible to this principal", sessionID)
	}
	return nil
}

// AuthorizeOwn enforces recipient ownership for inbox/report operations:
// only the session itself or a holder of owner authority over it.
func (n *Node) AuthorizeOwn(ctx context.Context, sender Sender, sessionID string) error {
	if err := n.validateSender(ctx, sender); err != nil {
		return err
	}
	if sender.SessionID == sessionID {
		return nil
	}
	if sender.SessionID == "" {
		return protocol.Errorf(protocol.CodeForbidden, "only the owning session may perform this operation")
	}
	ok, err := n.store.CanApprove(ctx, sender.SessionID, sessionID)
	if err != nil {
		return err
	}
	if !ok {
		return protocol.Errorf(protocol.CodeForbidden, "only the owning session may perform this operation")
	}
	return nil
}

// DiscoverFor lists peers visible to a specific authenticated sender.
func (n *Node) DiscoverFor(ctx context.Context, sender Sender) ([]protocol.PeerInfo, error) {
	peers, _, err := n.DiscoverForPage(ctx, sender, 0, "")
	return peers, err
}

// DiscoverPage is one cursor page of discovery.
type DiscoverPage struct {
	Peers      []protocol.PeerInfo
	NextCursor string
	HasMore    bool
}

// DiscoverForPage lists visible peers with deterministic cursor pagination.
// Cursor is the session_id of the last returned peer; empty starts at the
// beginning. Limit <= 0 uses the configured replay batch bound.
func (n *Node) DiscoverForPage(ctx context.Context, sender Sender, limit int, cursor string) ([]protocol.PeerInfo, string, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = n.cfg.Limits.MaxReplayBatch
	}
	if limit > n.cfg.Limits.MaxReplayBatch*4 {
		limit = n.cfg.Limits.MaxReplayBatch * 4
	}
	// Over-fetch until we have a full visible page or exhaust the registry.
	// Visibility filtering happens here, so invisible peers never leak.
	var out []protocol.PeerInfo
	after := cursor
	for len(out) < limit {
		batch, err := n.store.ListAllPeersAfter(ctx, limit*2+1, after)
		if err != nil {
			return nil, "", err
		}
		if len(batch) == 0 {
			break
		}
		exhausted := len(batch) < limit*2+1
		for _, p := range batch {
			ok, err := n.visibleTo(ctx, sender, p)
			if err != nil {
				return nil, "", err
			}
			if ok {
				out = append(out, p.PeerInfo)
				if len(out) >= limit {
					break
				}
			}
		}
		after = batch[len(batch)-1].SessionID
		if len(out) >= limit || exhausted {
			break
		}
	}
	next := ""
	if len(out) > 0 {
		next = out[len(out)-1].SessionID
	}
	return out, next, nil
}

// ResolveFor resolves and authorizes a peer for a specific sender.
// Ambiguous aliases fail explicitly instead of picking an arbitrary peer.
func (n *Node) ResolveFor(ctx context.Context, sender Sender, aliasOrID string) (protocol.PeerInfo, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return protocol.PeerInfo{}, err
	}
	// Exact session id first.
	if rec, err := n.store.GetSession(ctx, aliasOrID); err == nil {
		if err := n.AuthorizeObserve(ctx, sender, rec.SessionID); err != nil {
			return protocol.PeerInfo{}, err
		}
		return rec.PeerInfo, nil
	}
	matches, err := n.store.ListByAlias(ctx, aliasOrID)
	if err != nil {
		return protocol.PeerInfo{}, err
	}
	if len(matches) == 0 {
		return protocol.PeerInfo{}, ErrNotFound
	}
	if len(matches) > 1 {
		return protocol.PeerInfo{}, protocol.Errorf(protocol.CodeConflict, "alias %q is ambiguous: it identifies %d sessions", aliasOrID, len(matches))
	}
	if err := n.AuthorizeObserve(ctx, sender, matches[0].SessionID); err != nil {
		return protocol.PeerInfo{}, err
	}
	return matches[0].PeerInfo, nil
}

// Route resolves a peer by exact id, alias, or capability selector, with
// visibility filtering and an explanation. Ambiguity fails explicitly.
// It pages through the entire registry (batch size capped per query) so
// eligible peers beyond any single fetch are never silently omitted.
func (n *Node) Route(ctx context.Context, sender Sender, target string, sel *routing.Selector) (routing.Route, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return routing.Route{}, err
	}
	batchSize := n.cfg.Limits.MaxReplayBatch * 4
	if batchSize <= 0 || batchSize > 512 {
		batchSize = 512
	}
	// Visibility-filter first: routing must not leak invisible peers.
	var visible []routing.Peer
	var exact *routing.Peer
	var aliasMatches []routing.Peer
	after := ""
	for {
		batch, err := n.store.ListAllPeersAfter(ctx, batchSize, after)
		if err != nil {
			return routing.Route{}, err
		}
		if len(batch) == 0 {
			break
		}
		for _, rec := range batch {
			ok, err := n.visibleTo(ctx, sender, rec)
			if err != nil {
				return routing.Route{}, err
			}
			if !ok {
				continue
			}
			rp := routing.Peer{SessionID: rec.SessionID, Alias: rec.Alias, PrincipalID: rec.PrincipalID, Harness: rec.Harness, Presence: rec.Presence, Capabilities: rec.Capabilities, Delivery: rec.Delivery}
			visible = append(visible, rp)
			if rec.SessionID == target {
				cp := rp
				exact = &cp
			}
			if target != "" && rec.Alias == target {
				aliasMatches = append(aliasMatches, rp)
			}
		}
		after = batch[len(batch)-1].SessionID
		if len(batch) < batchSize {
			break
		}
	}
	if sel != nil {
		return routing.ResolveSelector(visible, *sel)
	}
	route, err := routing.ResolveExactOrAlias(exact, aliasMatches, target)
	if err != nil {
		if errors.Is(err, routing.ErrAmbiguous) {
			return routing.Route{}, protocol.Errorf(protocol.CodeConflict, "%v", err)
		}
		return routing.Route{}, ErrNotFound
	}
	return route, nil
}

// PendingInboxFor returns a session's inbox only to its owner.
//
// Messages that may enter model context (queue/steer) are returned with the
// canonical labeled peer-content rendering, so the out-of-process path
// carries the same provenance as the in-process path. Notify messages are
// operator notifications and are returned unwrapped.
func (n *Node) PendingInboxFor(ctx context.Context, sender Sender, sessionID string, limit int) ([]protocol.Message, error) {
	if err := n.AuthorizeOwn(ctx, sender, sessionID); err != nil {
		return nil, err
	}
	claimed, err := n.PendingInbox(ctx, sessionID, limit)
	if err != nil {
		return nil, err
	}
	// Revocation recheck at claim time: queued work from a sender whose
	// grant has ended must not be handed for injection.
	out := make([]protocol.Message, 0, len(claimed))
	for _, m := range claimed {
		if err := n.recheckDelivery(ctx, m, m.Delivery.Mode); err != nil {
			_ = n.recordReceipt(ctx, m, protocol.ReceiptRejected, err.Error())
			continue
		}
		if m.Delivery.Mode == protocol.DeliveryQueue || m.Delivery.Mode == protocol.DeliverySteer {
			provenance := protocol.PeerProvenance(m, n.principalIDFor(ctx, m.From))
			m.Body = provenance.RenderAsPeerContent(m.Body)
		}
		out = append(out, m)
	}
	if out == nil {
		out = []protocol.Message{}
	}
	return out, nil
}

// ReportDeliveryFor records a receipt only for the message's recipient owner.
// Terminal acks must present the current claim token/generation when the
// message is claimed, so a superseded claimant cannot acknowledge.
func (n *Node) ReportDeliveryFor(ctx context.Context, sender Sender, messageID string, status protocol.ReceiptStatus, detail string, claimToken string, claimGeneration int64) error {
	if err := n.validateSender(ctx, sender); err != nil {
		return err
	}
	msg, err := n.store.GetMessage(ctx, messageID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		return err
	}
	if err := n.AuthorizeOwn(ctx, sender, msg.To); err != nil {
		return err
	}
	// Claim-owner validation: a claimed message may only be reported by
	// its lease holder (or, after expiry, by the recipient owner).
	owner, token, gen, state, expires, err := n.store.ClaimInfo(ctx, messageID)
	if err == nil && state == store.InboxClaimed && owner != "" && owner != sender.SessionID {
		if expires.IsZero() || time.Now().UTC().Before(expires) {
			return protocol.Errorf(protocol.CodeForbidden, "message %q is claimed by another session", messageID)
		}
	}
	if err == nil && state == store.InboxClaimed && token != "" {
		if claimToken == "" || claimToken != token || claimGeneration != gen {
			return protocol.Errorf(protocol.CodeForbidden, "message %q claim is superseded; stale acknowledgement rejected", messageID)
		}
	}
	return n.ReportDelivery(ctx, messageID, status, detail)
}

// RenewClaimFor renews the caller's claim lease (fenced by token/generation).
func (n *Node) RenewClaimFor(ctx context.Context, sender Sender, messageID, token string, generation int64) error {
	if err := n.validateSender(ctx, sender); err != nil {
		return err
	}
	if token == "" || generation <= 0 {
		return protocol.Errorf(protocol.CodeInvalidParams, "claimToken and claimGeneration are required for renewal")
	}
	msg, err := n.store.GetMessage(ctx, messageID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		return err
	}
	if err := n.AuthorizeOwn(ctx, sender, msg.To); err != nil {
		return err
	}
	if err := n.store.RenewClaim(ctx, messageID, sender.SessionID, token, generation, store.DefaultClaimLease); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return protocol.Errorf(protocol.CodeForbidden, "claim for %q is superseded", messageID)
		}
		return err
	}
	return nil
}

// MessageStatusFor returns receipts only to a party or a visible peer.
func (n *Node) MessageStatusFor(ctx context.Context, sender Sender, messageID string) ([]protocol.Receipt, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return nil, err
	}
	msg, err := n.store.GetMessage(ctx, messageID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		return nil, err
	}
	if sender.SessionID == msg.From || sender.SessionID == msg.To {
		return n.store.ReceiptsFor(ctx, messageID)
	}
	// Same-principal parties may inspect status (CLI peer model); a
	// cross-principal caller needs visibility of either endpoint.
	for _, endpoint := range []string{msg.From, msg.To} {
		if err := n.AuthorizeObserve(ctx, sender, endpoint); err == nil {
			return n.store.ReceiptsFor(ctx, messageID)
		}
	}
	return nil, protocol.Errorf(protocol.CodeForbidden, "message %q is not visible to this principal", messageID)
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
