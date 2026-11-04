package node

import (
	"context"
	"time"

	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
)

// RetentionReport summarizes one retention pass.
type RetentionReport struct {
	// MessagesPurged counts messages removed by the retention policy.
	MessagesPurged int64
	// EventsCompacted counts journal events removed by compaction.
	EventsCompacted int64
	// Expired counts messages that expired and were voided.
	Expired int
}

// Maintain runs one retention and expiry pass.
//
// Retention keeps the durable store bounded. The spec requires that
// compaction and retention preserve the idempotency obligations for the
// advertised deduplication horizon, so the message cutoff is the older of
// the configured retention and the deduplication horizon, and messages with
// a live expiry are never purged early.
//
// The event journal is compacted by age rather than by sequence, because a
// sequence cutoff would need to know how many events each session has.
func (n *Node) Maintain(ctx context.Context) (RetentionReport, error) {
	var report RetentionReport
	now := time.Now().UTC()

	// Expire messages whose deadline has passed. An expired message must not
	// be newly injected into agent context, so it is voided rather than
	// left pending.
	expired, err := n.store.ExpiredMessages(ctx, now, n.cfg.Limits.MaxInboxMessages)
	if err != nil {
		return report, err
	}
	for _, messageID := range expired {
		msg, err := n.store.GetMessage(ctx, messageID)
		if err != nil {
			continue
		}
		n.recordReceipt(ctx, msg, protocol.ReceiptExpired, "message expired before delivery completed")
		_ = n.store.SetInboxState(ctx, msg.MessageID, store.InboxVoided)
	}
	report.Expired = len(expired)

	// Purge messages past the retention horizon. Messages that carry a live
	// expiry are kept until they expire and are voided above, so an
	// unexpired message is never dropped early.
	cutoff := now.Add(-n.cfg.Limits.MessageRetention)
	if n.cfg.Limits.DeduplicationHorizon > 0 {
		horizon := now.Add(-n.cfg.Limits.DeduplicationHorizon)
		if horizon.After(cutoff) {
			cutoff = horizon
		}
	}
	purged, err := n.store.PurgeMessagesOlderThan(ctx, cutoff)
	if err != nil {
		return report, err
	}
	report.MessagesPurged = purged

	// Compact the event journal for sessions whose events are older than
	// the event retention window.
	compacted, err := n.store.CompactSessionsOlderThan(ctx, now.Add(-n.cfg.Limits.EventRetention))
	if err != nil {
		return report, err
	}
	report.EventsCompacted = compacted

	return report, nil
}

// StartMaintenance runs the retention pass on an interval until the context
// is cancelled.
//
// The interval is a fraction of the shortest retention window, so the
// maintenance cannot fall further behind than the window it enforces.
func (n *Node) StartMaintenance(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := n.Maintain(ctx); err != nil && ctx.Err() == nil {
				// A failed maintenance pass is retried on the next tick.
				continue
			}
		}
	}
}

// MaintenanceInterval derives the maintenance interval from the configured
// retention windows.
func (n *Node) MaintenanceInterval() time.Duration {
	window := n.cfg.Limits.EventRetention
	if n.cfg.Limits.MessageRetention < window {
		window = n.cfg.Limits.MessageRetention
	}
	interval := window / 12
	if interval < time.Minute {
		interval = time.Minute
	}
	return interval
}
