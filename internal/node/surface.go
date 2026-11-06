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
// claiming. Pure reads, so a UI never steals an agent claim.
func (n *Node) SurfaceReplay(ctx context.Context, sender Sender, sessionID string, afterPosition int, limit int) ([]DisplayEvent, int, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return nil, afterPosition, err
	}
	target := sessionID
	if target == "" {
		target = sender.SessionID
	}
	if err := n.AuthorizeOwn(ctx, sender, target); err != nil {
		return nil, afterPosition, err
	}
	if limit <= 0 {
		limit = 100
	}
	events, next, err := n.store.ListDisplayEvents(ctx, target, afterPosition, limit)
	if err != nil {
		return nil, afterPosition, err
	}
	out := make([]DisplayEvent, 0, len(events))
	for _, e := range events {
		out = append(out, DisplayEvent{Message: e.Message, Status: e.ReceiptStatus, Detail: e.ReceiptDetail})
	}
	return out, next, nil
}

// SurfacePresent records that a visual surface rendered the message. No
// claim fencing is required: presentation is observation, not inbox work.
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
	return n.recordReceipt(ctx, msg, protocol.ReceiptPresented, "confirmed rendered by human surface")
}

// SurfaceReply sends a human reply to the original sender, preserving
// correlation. It does not grant approval authority.
func (n *Node) SurfaceReply(ctx context.Context, sender Sender, messageID, commandID, text string) (SendResult, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return SendResult{}, err
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
	if commandID == "" {
		commandID = mustCommandID()
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

var _ = time.Now
