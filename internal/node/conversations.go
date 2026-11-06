package node

import (
	"context"
	"strings"

	"github.com/B-A-M-N/intercom/internal/ids"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

// CreateConversation creates a one-to-one (or small explicit group)
// conversation. Participants must be visible to the creator; the creator
// is always a participant. No public broadcast bus.
func (n *Node) CreateConversation(ctx context.Context, creator Sender, id string, participants []string, title string) (store.Conversation, error) {
	if err := n.validateSender(ctx, creator); err != nil {
		return store.Conversation{}, err
	}
	// Creator must be a participant.
	found := false
	for _, p := range participants {
		if p == creator.SessionID {
			found = true
		}
	}
	if !found {
		participants = append([]string{creator.SessionID}, participants...)
	}
	// Every participant must be visible (same principal or grant).
	for _, p := range participants {
		if err := n.AuthorizeObserve(ctx, creator, p); err != nil {
			return store.Conversation{}, err
		}
	}
	if id == "" {
		raw, err := ids.NewSessionID()
		if err != nil {
			return store.Conversation{}, err
		}
		id = "cnv_" + strings.TrimPrefix(raw, "ses_")
	}
	return n.store.CreateConversation(ctx, id, title, participants)
}

// CloseConversation explicitly closes a conversation (participant only).
func (n *Node) CloseConversation(ctx context.Context, actor Sender, id string) error {
	if err := n.validateSender(ctx, actor); err != nil {
		return err
	}
	ok, err := n.store.IsParticipant(ctx, id, actor.SessionID)
	if err != nil {
		return err
	}
	if !ok {
		// Same-principal participants may close? Keep strict: participant only.
		return protocol.Errorf(protocol.CodeForbidden, "only conversation participants may close it")
	}
	return n.store.CloseConversation(ctx, id)
}

// ConversationHistory returns readable history under access policy:
// participants only (same-principal non-participants are denied).
func (n *Node) ConversationHistory(ctx context.Context, actor Sender, id string, limit int) ([]protocol.Message, error) {
	return n.ConversationHistoryPage(ctx, actor, id, limit, "")
}

// ConversationHistoryPage returns history with cursor pagination.
func (n *Node) ConversationHistoryPage(ctx context.Context, actor Sender, id string, limit int, cursor string) ([]protocol.Message, error) {
	if err := n.validateSender(ctx, actor); err != nil {
		return nil, err
	}
	ok, err := n.store.IsParticipant(ctx, id, actor.SessionID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocol.Errorf(protocol.CodeForbidden, "only conversation participants may read its history")
	}
	// Closed conversations: history remains readable (resumption), new
	// sends are rejected in Send.
	return n.store.ConversationMessagesAfter(ctx, id, limit, cursor)
}

// authorizeConversationSend checks membership for a send carrying a
// conversation id. Empty conversation id means no conversation check.
func (n *Node) authorizeConversationSend(ctx context.Context, from Sender, conversationID, target string) error {
	if conversationID == "" {
		return nil
	}
	conv, err := n.store.GetConversation(ctx, conversationID)
	if err != nil {
		if err == store.ErrNotFound {
			return protocol.Errorf(protocol.CodeInvalidParams, "unknown conversation %q", conversationID)
		}
		return err
	}
	if conv.Status == "closed" {
		return protocol.Errorf(protocol.CodeForbidden, "conversation %q is closed", conversationID)
	}
	for _, want := range []string{from.SessionID, target} {
		ok, err := n.store.IsParticipant(ctx, conversationID, want)
		if err != nil {
			return err
		}
		if !ok {
			return protocol.Errorf(protocol.CodeForbidden, "session %q is not a participant of conversation %q", want, conversationID)
		}
	}
	return nil
}
