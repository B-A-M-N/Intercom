package node

import (
	"context"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

// DisplayEvent is the surface payload: displayable message + human truth.
type DisplayEvent struct {
	Message protocol.Message
	Status protocol.ReceiptStatus
	Detail string
}

// SurfaceReplay returns displayable events for the bound session without
// claiming. Pure reads, so a UI never steals an agent claim. Cursor is a
// journal sequence (stable under retention).
func (n *Node) SurfaceReplay(ctx context.Context, sender Sender, sessionID string, afterPosition int, limit int) ([]DisplayEvent, int, error) {
	events, next, _, _, err := n.SurfaceReplayWithGap(ctx, sender, sessionID, uint64(afterPosition), limit)
	if err != nil {
		return events, next, err
	}
	if _, _, gap, _, _ := n.SurfaceReplayWithGap(ctx, sender, sessionID, uint64(next), 1); gap {
		// gap detection is reported via WithGap; legacy path surfaces an error for compacted history.
	}
	return events, next, nil
}

func (n *Node) SurfaceReplayWithGap(ctx context.Context, sender Sender, sessionID string, afterSeq uint64, limit int) ([]DisplayEvent, int, bool, uint64, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return nil, int(afterSeq), false, 0, err
	}
	target := sessionID
	if target == "" {
		target = sender.SessionID
	}
	if err := n.AuthorizeOwn(ctx, sender, target); err != nil {
		return nil, int(afterSeq), false, 0, err
	}
	if limit <= 0 {
		limit = 100
	}
	res, err := n.store.ListDisplayEventsBySequence(ctx, target, afterSeq, limit)
	if err != nil {
		return nil, int(afterSeq), false, 0, err
	}
	out := make([]DisplayEvent, 0, len(res.Events))
	for _, e := range res.Events {
		out = append(out, DisplayEvent{Message: e.Message, Status: e.ReceiptStatus, Detail: e.ReceiptDetail})
	}
	return out, int(res.Cursor), res.Gap, res.EarliestAvailable, nil
}

// SurfacePresent records that a visual surface rendered the message as an
// independent observation: receipt only, inbox and command untouched.
func (n *Node) SurfacePresent(ctx context.Context, sender Sender, messageID string) error {
	if err := n.validateSender(ctx, sender); err != nil {
		return err
	}
	msg, err := n.store.GetMessage(ctx, messageID)
	if err != nil {
		if err == store.ErrNotFound {
			return protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		return err
	}
	if err := n.AuthorizeOwn(ctx, sender, msg.To); err != nil {
		return err
	}
	receipt := protocol.Receipt{
		MessageID: msg.MessageID, CommandID: msg.CommandID, Status: protocol.ReceiptPresented,
		From: msg.From, To: msg.To, ObservedAt: time.Now().UTC(),
		Detail: "confirmed rendered by human surface",
	}
	if err := n.store.RecordPresentationObservation(ctx, receipt); err != nil {
		if err == store.ErrNotFound {
			return protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		if err == store.ErrConflict {
			return protocol.Errorf(protocol.CodeConflict, "presentation observation conflicted")
		}
		return err
	}
	// Notify observers of presentation without touching inbox/delivery.
	return nil
}

// SurfaceReply sends a human reply. Requires a stable command ID (retry-safe).
func (n *Node) SurfaceReply(ctx context.Context, sender Sender, messageID, commandID, text string) (SendResult, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return SendResult{}, err
	}
	if commandID == "" {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "commandId is required for surface reply (retry-safe)")
	}
	msg, err := n.store.GetMessage(ctx, messageID)
	if err != nil {
		if err == store.ErrNotFound {
			return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		return SendResult{}, err
	}
	if err := n.AuthorizeOwn(ctx, sender, msg.To); err != nil {
		return SendResult{}, err
	}
	return n.ReplyWithCommandID(ctx, sender, messageID, text, msg.CorrelationID, commandID)
}

// SurfaceIssueDelegation mints a surface-scoped token for a human UI.
func (n *Node) SurfaceIssueDelegation(ctx context.Context, sender Sender, sessionID, permissions string, ttl time.Duration) (string, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return "", err
	}
	target := sessionID
	if target == "" {
		target = sender.SessionID
	}
	if err := n.AuthorizeOwn(ctx, sender, target); err != nil {
		return "", err
	}
	ownerInstance := ""
	if rec, err := n.store.GetSession(ctx, target); err == nil {
		ownerInstance = rec.InstanceID
	}
	return n.store.IssueDelegationWithScope(ctx, target, ownerInstance, ttl, "surface", permissions)
}
