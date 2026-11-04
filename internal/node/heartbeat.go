package node

import (
	"context"
	"time"

	"github.com/bamn/intercom/internal/protocol"
)

// HeartbeatInterval is how often a peer is expected to report liveness.
//
// The presence states distinguish "disconnected" from "terminated": a
// disconnected peer means the transport or heartbeat was lost, not that the
// agent stopped (ICSP-001 section 4).
const HeartbeatInterval = 30 * time.Second

// HeartbeatTimeout is how long a peer may go silent before its presence
// becomes disconnected.
//
// It is several heartbeat intervals, so a transient delay does not flap the
// presence state.
const HeartbeatTimeout = 3 * HeartbeatInterval

// Heartbeat records that a peer is alive.
func (n *Node) Heartbeat(ctx context.Context, sessionID string) error {
	if err := n.store.SetPresence(ctx, sessionID, protocol.PresenceOnline); err != nil {
		return err
	}
	return n.store.Audit(ctx, n.principalID, sessionID, "presence.heartbeat", "")
}

// SweepPresence marks peers that have gone silent as disconnected.
//
// A host disconnect alone SHALL NOT automatically terminate the logical
// session (ICSP-001 section 7.3), so this changes presence only. The
// session remains registered and its durable state remains readable.
func (n *Node) SweepPresence(ctx context.Context) (int64, error) {
	return n.store.MarkStaleDisconnected(ctx, time.Now().UTC().Add(-HeartbeatTimeout))
}

// StartHeartbeat runs the presence sweep on an interval until the context is
// cancelled.
func (n *Node) StartHeartbeat(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = HeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// A failed sweep is retried on the next tick.
			_, _ = n.SweepPresence(ctx)
		}
	}
}
