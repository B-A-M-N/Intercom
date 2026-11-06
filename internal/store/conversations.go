package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
)

// Conversation is the minimum Phase 4 model: explicit participants,
// open/closed status, and history by membership. No broadcast bus.
type Conversation struct {
	ID         string
	Title      string
	Status     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Participants []string
}

// CreateConversation creates a conversation with explicit participants.
func (s *Store) CreateConversation(ctx context.Context, id, title string, participants []string) (Conversation, error) {
	if len(participants) < 2 {
		return Conversation{}, fmt.Errorf("store: conversation requires at least 2 participants")
	}
	now := time.Now().UTC()
	conv := Conversation{ID: id, Title: title, Status: "open", CreatedAt: now, UpdatedAt: now, Participants: participants}
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO conversations (conversation_id, title, status, created_at, updated_at) VALUES (?, ?, 'open', ?, ?)`, id, title, now.Format(TimestampLayout), now.Format(TimestampLayout)); err != nil {
			return fmt.Errorf("store: create conversation: %w", err)
		}
		for _, p := range participants {
			if _, err := tx.ExecContext(ctx, `INSERT INTO conversation_participants (conversation_id, session_id) VALUES (?, ?)`, id, p); err != nil {
				return fmt.Errorf("store: add participant: %w", err)
			}
		}
		return nil
	})
	return conv, err
}

// GetConversation loads a conversation with participants.
func (s *Store) GetConversation(ctx context.Context, id string) (Conversation, error) {
	var conv Conversation
	var created, updated string
	err := s.db.QueryRowContext(ctx, `SELECT conversation_id, title, status, created_at, updated_at FROM conversations WHERE conversation_id = ?`, id).Scan(&conv.ID, &conv.Title, &conv.Status, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return Conversation{}, ErrNotFound
	}
	if err != nil {
		return Conversation{}, fmt.Errorf("store: get conversation: %w", err)
	}
	if t, err := parseTime(created); err == nil {
		conv.CreatedAt = t
	}
	if t, err := parseTime(updated); err == nil {
		conv.UpdatedAt = t
	}
	conv.Participants, err = s.ConversationParticipants(ctx, id)
	return conv, err
}

// ConversationParticipants lists participant session ids.
func (s *Store) ConversationParticipants(ctx context.Context, id string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT session_id FROM conversation_participants WHERE conversation_id = ? ORDER BY session_id ASC`, id)
	if err != nil {
		return nil, fmt.Errorf("store: participants: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return nil, fmt.Errorf("store: scan participant: %w", err)
		}
		out = append(out, sid)
	}
	return out, rows.Err()
}

// IsParticipant reports membership.
func (s *Store) IsParticipant(ctx context.Context, conversationID, sessionID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM conversation_participants WHERE conversation_id = ? AND session_id = ?`, conversationID, sessionID).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: is participant: %w", err)
	}
	return true, nil
}

// CloseConversation marks a conversation closed (explicit close/cancel).
func (s *Store) CloseConversation(ctx context.Context, id string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE conversations SET status = 'closed', updated_at = ? WHERE conversation_id = ?`, time.Now().UTC().Format(TimestampLayout), id)
		if err != nil {
			return fmt.Errorf("store: close conversation: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// ConversationMessages returns messages in a conversation, oldest first.
// Deduplication is by message id (primary key); replay is idempotent.
func (s *Store) ConversationMessages(ctx context.Context, conversationID string, limit int) ([]protocol.Message, error) {
	return s.ConversationMessagesAfter(ctx, conversationID, limit, "")
}

// ConversationMessagesAfter returns messages with cursor pagination.
// After is the message_id of the last returned message (empty = start).
func (s *Store) ConversationMessagesAfter(ctx context.Context, conversationID string, limit int, after string) ([]protocol.Message, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT message_id FROM messages WHERE conversation_id = ?`
	args := []any{conversationID}
	if after != "" {
		// Resolve the cursor position (created_at, message_id).
		var cCreated string
		if err := s.db.QueryRowContext(ctx, `SELECT created_at FROM messages WHERE message_id = ?`, after).Scan(&cCreated); err == nil {
			query += ` AND (created_at > ? OR (created_at = ? AND message_id > ?))`
			args = append(args, cCreated, cCreated, after)
		}
	}
	query += ` ORDER BY created_at ASC, message_id ASC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: conversation messages: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan conversation message: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]protocol.Message, 0, len(ids))
	for _, id := range ids {
		m, err := s.GetMessage(ctx, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// SetMessageConversation links a message to a conversation at admission.
// Called inside AdmitMessage via direct UPDATE in the same tx? Admission
// inserts conversation_id directly; this helper is for completeness.
func (s *Store) SetMessageConversation(ctx context.Context, messageID, conversationID string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE messages SET conversation_id = ? WHERE message_id = ?`, conversationID, messageID)
		return err
	})
}
