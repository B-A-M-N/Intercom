package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bamn/intercom/internal/protocol"
)

// InboxState tracks whether a message has been durably accepted by the
// recipient's inbox (ICSP-001 section 5.4).
type InboxState string

const (
	// InboxPending means the message is stored but not yet in a recipient inbox.
	InboxPending InboxState = "pending"
	// InboxAccepted means the recipient inbox durably accepted the message.
	InboxAccepted InboxState = "accepted"
	// InboxDelivered means delivery to the recipient completed.
	InboxDelivered InboxState = "delivered"
	// InboxVoided means the message was voided without delivery.
	InboxVoided InboxState = "voided"
)

// StoreMessage durably stores a message and returns it.
//
// Storing the message and recording its first receipt happen in the same
// transaction, so a message can never exist without its acceptance receipt.
func (s *Store) StoreMessage(ctx context.Context, msg protocol.Message, principalID string) error {
	body, err := json.Marshal(msg.Body)
	if err != nil {
		return fmt.Errorf("store: marshal body: %w", err)
	}
	delivery, err := json.Marshal(msg.Delivery)
	if err != nil {
		return fmt.Errorf("store: marshal delivery: %w", err)
	}
	metadata, err := json.Marshal(msg.Metadata)
	if err != nil {
		return fmt.Errorf("store: marshal metadata: %w", err)
	}

	return s.inTx(ctx, func(tx *sql.Tx) error {
		expires := ""
		if msg.ExpiresAt != nil {
			expires = msg.ExpiresAt.UTC().Format(TimestampLayout)
		}
		_, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO messages (message_id, command_id, from_session, to_session,
			        from_principal, body, delivery, reply_to, correlation_id, created_at,
			        expires_at, metadata, inbox_state)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			msg.MessageID, msg.CommandID, msg.From, msg.To, principalID,
			string(body), string(delivery), msg.ReplyTo, msg.CorrelationID,
			msg.CreatedAt.UTC().Format(TimestampLayout), expires, string(metadata),
			string(InboxPending))
		if err != nil {
			return fmt.Errorf("store: insert message: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO receipts (message_id, command_id, status, from_session, to_session, detail, observed_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			msg.MessageID, msg.CommandID, protocol.ReceiptAccepted, msg.From, msg.To,
			"durably admitted", time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: insert acceptance receipt: %w", err)
		}
		return nil
	})
}

// GetMessage loads one stored message.
func (s *Store) GetMessage(ctx context.Context, messageID string) (protocol.Message, error) {
	var msg protocol.Message
	var body, delivery, metadata string
	var expires sql.NullString
	var createdAt scanTime
	row := s.db.QueryRowContext(ctx,
		`SELECT message_id, command_id, from_session, to_session, body, delivery,
		        reply_to, correlation_id, created_at, expires_at, metadata
		 FROM messages WHERE message_id = ?`, messageID)
	if err := row.Scan(&msg.MessageID, &msg.CommandID, &msg.From, &msg.To, &body, &delivery,
		&msg.ReplyTo, &msg.CorrelationID, &createdAt, &expires, &metadata); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return protocol.Message{}, ErrNotFound
		}
		return protocol.Message{}, fmt.Errorf("store: get message: %w", err)
	}
	if err := json.Unmarshal([]byte(body), &msg.Body); err != nil {
		return protocol.Message{}, fmt.Errorf("store: decode body: %w", err)
	}
	if err := json.Unmarshal([]byte(delivery), &msg.Delivery); err != nil {
		return protocol.Message{}, fmt.Errorf("store: decode delivery: %w", err)
	}
	if err := json.Unmarshal([]byte(metadata), &msg.Metadata); err != nil {
		return protocol.Message{}, fmt.Errorf("store: decode metadata: %w", err)
	}
	msg.CreatedAt = createdAt.Time()
	if expires.Valid && expires.String != "" {
		t, err := parseTime(expires.String)
		if err != nil {
			return protocol.Message{}, fmt.Errorf("store: decode expires_at: %w", err)
		}
		msg.ExpiresAt = &t
	}
	return msg, nil
}

// MessageExists reports whether a message id is already stored. Recipients
// deduplicate a previously delivered messageId (ICSP-001 section 5.5).
func (s *Store) MessageExists(ctx context.Context, messageID string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM messages WHERE message_id = ?`, messageID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: message exists: %w", err)
	}
	return true, nil
}

// SetInboxState records the durable inbox acceptance state for a message.
func (s *Store) SetInboxState(ctx context.Context, messageID string, state InboxState) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE messages SET inbox_state = ? WHERE message_id = ?`, state, messageID)
		if err != nil {
			return fmt.Errorf("store: set inbox state: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// RecordReceipt appends a delivery receipt (ICSP-001 section 5.4).
//
// Receipts are append-only facts. A later receipt never rewrites an earlier
// one: presented and injected are independent observations, not a sequence.
func (s *Store) RecordReceipt(ctx context.Context, r protocol.Receipt) error {
	if !r.Status.Valid() {
		return fmt.Errorf("store: invalid receipt status %q", r.Status)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO receipts (message_id, command_id, status, from_session, to_session, detail, observed_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			r.MessageID, r.CommandID, r.Status, r.From, r.To, r.Detail,
			r.ObservedAt.UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: insert receipt: %w", err)
		}
		return nil
	})
}

// ReceiptsFor returns every receipt recorded for a message, oldest first.
func (s *Store) ReceiptsFor(ctx context.Context, messageID string) ([]protocol.Receipt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id, command_id, status, from_session, to_session, detail, observed_at
		 FROM receipts WHERE message_id = ? ORDER BY receipt_id ASC`, messageID)
	if err != nil {
		return nil, fmt.Errorf("store: query receipts: %w", err)
	}
	defer rows.Close()

	var out []protocol.Receipt
	for rows.Next() {
		var r protocol.Receipt
		var observedAt scanTime
		if err := rows.Scan(&r.MessageID, &r.CommandID, &r.Status, &r.From, &r.To,
			&r.Detail, &observedAt); err != nil {
			return nil, fmt.Errorf("store: scan receipt: %w", err)
		}
		r.ObservedAt = observedAt.Time()
		out = append(out, r)
	}
	return out, rows.Err()
}

// ReceiptsForSession returns every receipt recorded for a session, oldest
// first.
//
// A subscriber uses this to start from a known point instead of an empty
// history (ICSP-001 section 7.2).
func (s *Store) ReceiptsForSession(ctx context.Context, sessionID string) ([]protocol.Receipt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id, command_id, status, from_session, to_session, detail, observed_at
		 FROM receipts WHERE to_session = ? OR from_session = ?
		 ORDER BY receipt_id ASC`, sessionID, sessionID)
	if err != nil {
		return nil, fmt.Errorf("store: receipts for session: %w", err)
	}
	defer rows.Close()

	var out []protocol.Receipt
	for rows.Next() {
		var r protocol.Receipt
		var observedAt scanTime
		if err := rows.Scan(&r.MessageID, &r.CommandID, &r.Status, &r.From, &r.To,
			&r.Detail, &observedAt); err != nil {
			return nil, fmt.Errorf("store: scan session receipt: %w", err)
		}
		r.ObservedAt = observedAt.Time()
		out = append(out, r)
	}
	return out, rows.Err()
}

// LatestReceipt returns the most recent receipt for a message.
func (s *Store) LatestReceipt(ctx context.Context, messageID string) (protocol.Receipt, error) {
	var r protocol.Receipt
	var observedAt scanTime
	row := s.db.QueryRowContext(ctx,
		`SELECT message_id, command_id, status, from_session, to_session, detail, observed_at
		 FROM receipts WHERE message_id = ? ORDER BY receipt_id DESC LIMIT 1`, messageID)
	err := row.Scan(&r.MessageID, &r.CommandID, &r.Status, &r.From, &r.To, &r.Detail, &observedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.Receipt{}, ErrNotFound
	}
	if err != nil {
		return protocol.Receipt{}, fmt.Errorf("store: latest receipt: %w", err)
	}
	r.ObservedAt = observedAt.Time()
	return r, nil
}

// PendingInbox returns the messages durably stored for a recipient session
// that have not yet been delivered.
func (s *Store) PendingInbox(ctx context.Context, sessionID string, limit int) ([]protocol.Message, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id FROM messages WHERE to_session = ? AND inbox_state IN (?, ?)
		 ORDER BY created_at ASC LIMIT ?`,
		sessionID, InboxPending, InboxAccepted, limit)
	if err != nil {
		return nil, fmt.Errorf("store: query pending inbox: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan pending id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]protocol.Message, 0, len(ids))
	for _, id := range ids {
		msg, err := s.GetMessage(ctx, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}

// CountPendingInbox counts undelivered messages for a recipient.
func (s *Store) CountPendingInbox(ctx context.Context, sessionID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE to_session = ? AND inbox_state IN (?, ?)`,
		sessionID, InboxPending, InboxAccepted).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: count pending inbox: %w", err)
	}
	return n, nil
}

// ExpiredMessages returns messages whose expiry deadline has passed.
func (s *Store) ExpiredMessages(ctx context.Context, now time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id FROM messages
		 WHERE expires_at != '' AND expires_at < ? AND inbox_state IN (?, ?)
		 ORDER BY expires_at ASC LIMIT ?`,
		now.UTC().Format(TimestampLayout), InboxPending, InboxAccepted, limit)
	if err != nil {
		return nil, fmt.Errorf("store: query expired: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan expired id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
