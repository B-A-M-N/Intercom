package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bamn/intercom/internal/ids"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
)

// SessionState returns the normalized state and recovery classification for a
// session (ICSP-001 sections 7.2 and 7.3).
func (n *Node) SessionState(ctx context.Context, sessionID string) (protocol.Snapshot, error) {
	rec, err := n.store.GetSession(ctx, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Snapshot{}, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", sessionID)
		}
		return protocol.Snapshot{}, err
	}

	seq, err := n.store.LatestSequence(ctx, sessionID)
	if err != nil {
		return protocol.Snapshot{}, err
	}
	pending, err := n.store.CountPendingInbox(ctx, sessionID)
	if err != nil {
		return protocol.Snapshot{}, err
	}

	recovery, err := n.classifyRecovery(ctx, sessionID)
	if err != nil {
		return protocol.Snapshot{}, err
	}

	return protocol.Snapshot{
		SessionID: sessionID,
		State:     rec.State,
		Sequence:  seq,
		UpdatedAt: rec.LastSeen,
		Pending:   pending,
		Recovery:  string(recovery),
	}, nil
}

// classifyRecovery determines how a session's state was reconstructed.
//
// The classification distinguishes native session state from observations
// inferred by the adapter (ICSP-001 section 7.3), so a caller can tell a
// harness-reported state from a reconstructed one.
func (n *Node) classifyRecovery(ctx context.Context, sessionID string) (protocol.RecoveryClassification, error) {
	rec, err := n.store.GetSession(ctx, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.RecoveryUnknown, nil
		}
		return protocol.RecoveryUnknown, err
	}

	// A live adapter reporting a state means the native harness is the
	// authority for that state.
	if ad, ok := n.Adapter(sessionID); ok {
		state, err := ad.State(ctx, sessionID)
		if err == nil && state.State != "" {
			return protocol.RecoveryClean, nil
		}
	}

	switch rec.State {
	case protocol.SessionCreated, protocol.SessionIdle, protocol.SessionRunning,
		protocol.SessionWaitingApproval, protocol.SessionEnded:
		return protocol.RecoveryClean, nil
	case protocol.SessionInterrupted:
		return protocol.RecoveryInterrupted, nil
	case protocol.SessionUnavailable:
		return protocol.RecoveryUnknown, nil
	default:
		return protocol.RecoveryUnknown, nil
	}
}

// Snapshot returns a coherent state projection of a session.
func (n *Node) Snapshot(ctx context.Context, sessionID string) (protocol.Snapshot, error) {
	return n.SessionState(ctx, sessionID)
}

// Replay returns journal events after a cursor.
//
// When the requested history has been compacted, the response carries an
// explicit cursor-gap indication and a snapshot-recovery path. It MUST NOT
// silently omit missing history while claiming continuous replay.
func (n *Node) Replay(ctx context.Context, sessionID string, afterCursor uint64, limit int) (store.ReplayResult, error) {
	if _, err := n.store.GetSession(ctx, sessionID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.ReplayResult{}, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", sessionID)
		}
		return store.ReplayResult{}, err
	}
	if limit <= 0 || limit > n.cfg.Limits.MaxReplayBatch {
		limit = n.cfg.Limits.MaxReplayBatch
	}
	return n.store.Replay(ctx, sessionID, afterCursor, limit)
}

// SetSessionState updates a session's state and journals the change.
func (n *Node) SetSessionState(ctx context.Context, sessionID string, state protocol.SessionState) error {
	if !state.Valid() {
		return protocol.Errorf(protocol.CodeInvalidParams, "invalid session state %q", state)
	}
	if err := n.store.SetSessionState(ctx, sessionID, state); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", sessionID)
		}
		return err
	}
	_, err := n.store.AppendEvent(ctx, protocol.EventEnvelope{
		EventID:   mustID("evt"),
		SessionID: sessionID,
		EventType: protocol.EventSessionState,
		Timestamp: time.Now().UTC(),
		Payload:   mustJSON(map[string]any{"state": state}),
	}, "")
	return err
}

// Recover reconstructs command intake state after a restart
// (ICSP-001 section 6.2).
//
// Recovery is deliberately conservative: a command that was admitted but
// never settled is marked abandoned rather than silently re-executed, because
// re-execution could duplicate effects.
func (n *Node) Recover(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport

	unsettled, err := n.store.UnsettledCommands(ctx)
	if err != nil {
		return report, err
	}
	report.Unsettled = len(unsettled)

	for _, cmd := range unsettled {
		reason := "admitted without settlement; outcome not established"
		if err := n.store.AbandonCommand(ctx, cmd.CommandID, reason); err != nil {
			return report, err
		}
		report.Abandoned = append(report.Abandoned, cmd.CommandID)
	}

	// Mark every session of this host as disconnected. A host disconnect
	// alone does not terminate the logical session.
	marked, err := n.store.MarkAllDisconnected(ctx, n.hostID)
	if err != nil {
		return report, err
	}
	report.MarkedDisconnected = marked

	// Expire messages whose deadline passed while the host was down.
	expired, err := n.store.ExpiredMessages(ctx, time.Now().UTC(), n.cfg.Limits.MaxInboxMessages)
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

	return report, nil
}

// RecoveryReport summarizes a recovery pass.
type RecoveryReport struct {
	// Unsettled counts commands that were admitted without settlement.
	Unsettled int
	// Abandoned lists the command ids marked abandoned.
	Abandoned []string
	// MarkedDisconnected counts sessions marked disconnected.
	MarkedDisconnected int64
	// Expired counts messages expired during recovery.
	Expired int
}

// String renders the report.
func (r RecoveryReport) String() string {
	return fmt.Sprintf("recovered: %d unsettled (%d abandoned), %d sessions disconnected, %d messages expired",
		r.Unsettled, len(r.Abandoned), r.MarkedDisconnected, r.Expired)
}

// Reply sends a correlated reply to the sender of a message.
func (n *Node) Reply(ctx context.Context, from Sender, messageID, text string, correlationID string) (SendResult, error) {
	orig, err := n.store.GetMessage(ctx, messageID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		return SendResult{}, err
	}
	if correlationID == "" {
		correlationID = orig.CorrelationID
	}
	return n.Send(ctx, from, SendRequest{
		CommandID:     mustCommandID(),
		To:            orig.From,
		Body:          protocol.MessageBody{Type: "text", Text: text},
		Delivery:      protocol.Delivery{Mode: protocol.DeliveryQueue, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		ReplyTo:       messageID,
		CorrelationID: correlationID,
	})
}

// mustCommandID mints a command identifier.
func mustCommandID() string {
	id, err := ids.NewCommandID()
	if err != nil {
		return "cmd_unknown"
	}
	return id
}
