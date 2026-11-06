// Package routing implements deterministic peer routing separate from
// delivery execution.
//
// Resolution order: exact canonical session id, then explicit alias
// (ambiguous aliases fail), then capability selector (harness / capability
// / availability / trust domain). Ambiguity always fails explicitly with a
// structured reason; an arbitrary agent is never selected.
package routing

import (
	"errors"
	"fmt"

	"github.com/B-A-M-N/intercom/internal/protocol"
)

// ErrNotFound means no route matches.
var ErrNotFound = errors.New("routing: no matching peer")

// ErrAmbiguous means a selector matched more than one peer.
var ErrAmbiguous = errors.New("routing: ambiguous selector")

// Peer is the routing view of a session (subset of store.PeerRecord).
type Peer struct {
	SessionID   string
	Alias       string
	PrincipalID string
	Harness     string
	Presence    protocol.PresenceState
	Capabilities []protocol.Capability
	Delivery    []protocol.DeliveryMode
}

// Selector requests a peer by attributes instead of identity.
type Selector struct {
	Harness     string
	Capability  protocol.CapabilityID
	Availability protocol.PresenceState // empty = any except disconnected/unknown? see Match
	TrustPrincipal string // if set, only peers of this principal
}

// Route is an explainable routing decision.
type Route struct {
	TargetSession string
	Via           string // exact | alias | selector
	Explain       string
}

// Match reports whether a peer satisfies a selector.
func (s Selector) Match(p Peer) bool {
	if s.Harness != "" && p.Harness != s.Harness {
		return false
	}
	if s.TrustPrincipal != "" && p.PrincipalID != s.TrustPrincipal {
		return false
	}
	if s.Availability != "" && p.Presence != s.Availability {
		return false
	}
	if s.Capability != "" {
		found := false
		for _, c := range p.Capabilities {
			if c.ID == s.Capability && c.Support != protocol.SupportUnsupported {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// ResolveExactOrAlias resolves exact id first, then alias. Alias ambiguity
// is an explicit error, never a first-match pick.
func ResolveExactOrAlias(exact *Peer, aliasMatches []Peer, target string) (Route, error) {
	if exact != nil {
		return Route{TargetSession: exact.SessionID, Via: "exact",
			Explain: fmt.Sprintf("exact canonical session id %q", exact.SessionID)}, nil
	}
	if len(aliasMatches) == 0 {
		return Route{}, ErrNotFound
	}
	if len(aliasMatches) > 1 {
		return Route{}, fmt.Errorf("%w: alias %q identifies %d sessions", ErrAmbiguous, target, len(aliasMatches))
	}
	return Route{TargetSession: aliasMatches[0].SessionID, Via: "alias",
		Explain: fmt.Sprintf("alias %q -> %q", target, aliasMatches[0].SessionID)}, nil
}

// ResolveSelector picks the single peer matching a selector.
func ResolveSelector(peers []Peer, sel Selector) (Route, error) {
	var matches []Peer
	for _, p := range peers {
		if sel.Match(p) {
			matches = append(matches, p)
		}
	}
	if len(matches) == 0 {
		return Route{}, ErrNotFound
	}
	if len(matches) > 1 {
		return Route{}, fmt.Errorf("%w: selector matched %d peers", ErrAmbiguous, len(matches))
	}
	return Route{TargetSession: matches[0].SessionID, Via: "selector",
		Explain: fmt.Sprintf("selector harness=%q capability=%q availability=%q -> %q",
			sel.Harness, sel.Capability, sel.Availability, matches[0].SessionID)}, nil
}

// MaxRelayHops bounds forwarded/relayed messages. Route history is carried
// in message metadata under "routeHistory" and hop count under "hops".
const MaxRelayHops = 5

// CheckHops rejects relay loops: hops at or beyond the limit fail.
func CheckHops(hops int) error {
	if hops >= MaxRelayHops {
		return fmt.Errorf("routing: relay hop limit %d exceeded (loop prevention)", MaxRelayHops)
	}
	return nil
}
