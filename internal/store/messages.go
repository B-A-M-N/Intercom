package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
)

// newClaimToken mints a fencing token for one inbox claim.
func newClaimToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// InboxState tracks whether a message has been durably accepted by the
// recipient's inbox (ICSP-001 section 5.4).
type InboxState string

const (
	// InboxPending means the message is stored but not yet claimed by a
	// recipient adapter.
	InboxPending InboxState = "pending"
	// InboxAccepted means the recipient inbox durably accepted the message.
	InboxAccepted InboxState = "accepted"
	// InboxClaimed means a recipient adapter holds an exclusive lease.
	InboxClaimed InboxState = "claimed"
	// InboxAcknowledged means the recipient reported presented/injected/
	// handled/delivered; it must not be presented again.
	InboxAcknowledged InboxState = "acknowledged"
	// InboxRejected means delivery was refused.
	InboxRejected InboxState = "rejected"
	// InboxDelivered means delivery to the recipient completed.
	InboxDelivered InboxState = "delivered"
	// InboxVoided means the message was voided without delivery.
	InboxVoided InboxState = "voided"
	// InboxDeadLetter means redelivery attempts were exhausted.
	InboxDeadLetter InboxState = "dead_letter"
)

// MaxDeliveryAttempts bounds redelivery; beyond it a message is dead-lettered
// rather than retried forever.
const MaxDeliveryAttempts = 10

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
		res, err := tx.ExecContext(ctx,
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
		n, _ := res.RowsAffected()
		if n == 0 {
			// Duplicate message id: verify it is the identical payload
			// (conflicting reuse fails) and do NOT append a fresh
			// acceptance receipt.
			var existingBody, existingTo, existingFrom string
			err := tx.QueryRowContext(ctx, `SELECT body, to_session, from_session FROM messages WHERE message_id = ?`, msg.MessageID).Scan(&existingBody, &existingTo, &existingFrom)
			if err != nil {
				return fmt.Errorf("store: load duplicate message: %w", err)
			}
			if existingBody != string(body) || existingTo != msg.To || existingFrom != msg.From {
				return ErrConflict
			}
			return nil
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
		        reply_to, correlation_id, created_at, expires_at, metadata, conversation_id
		 FROM messages WHERE message_id = ?`, messageID)
	if err := row.Scan(&msg.MessageID, &msg.CommandID, &msg.From, &msg.To, &body, &delivery,
		&msg.ReplyTo, &msg.CorrelationID, &createdAt, &expires, &metadata, &msg.ConversationID); err != nil {
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
	// Claim fencing fields (populated after ClaimInbox).
	var token string
	var gen int64
	if err := s.db.QueryRowContext(ctx, `SELECT claim_token, claim_generation FROM message_claims WHERE message_id = ?`, messageID).Scan(&token, &gen); err == nil {
		msg.ClaimToken = token
		msg.ClaimGeneration = gen
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

// MessageDelivered reports whether a message id already reached a
// recipient.
//
// A recipient MUST deduplicate a previously delivered message id
// (ICSP-001 section 5.5). A delivery receipt is the evidence that a
// message reached the recipient, so the presence of one is the dedup key.
func (s *Store) MessageDelivered(ctx context.Context, messageID string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM receipts WHERE message_id = ? AND status IN (?, ?, ?)`,
		messageID, protocol.ReceiptDelivered, protocol.ReceiptPresented, protocol.ReceiptInjected).
		Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: message delivered: %w", err)
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

// DefaultClaimLease bounds how long a claimed message stays exclusive.
const DefaultClaimLease = 30 * time.Second

// PendingInbox claims messages for a session (owner = session itself) with
// an exclusive lease. It is the adapter inbox poll: a claimed message is
// not re-presented until its lease expires or it is acknowledged, which
// prevents repeated presentation of notifications.
func (s *Store) PendingInbox(ctx context.Context, sessionID string, limit int) ([]protocol.Message, error) {
	return s.ClaimInbox(ctx, sessionID, sessionID, DefaultClaimLease, limit)
}

// ClaimInbox atomically claims up to limit claimable messages for owner,
// expiring leases, voiding expired messages, and dead-lettering exhausted
// attempts. Claimable means pending/accepted, or claimed with an expired
// lease. Already-claimed (unexpired) messages are not returned.
func (s *Store) ClaimInbox(ctx context.Context, sessionID, owner string, lease time.Duration, limit int) ([]protocol.Message, error) {
	if limit <= 0 {
		limit = 100
	}
	if lease <= 0 {
		lease = DefaultClaimLease
	}
	now := time.Now().UTC()
	nowStr := now.Format(TimestampLayout)
	leaseStr := now.Add(lease).Format(TimestampLayout)

	// Expire due messages for this session before claiming: an expired
	// message must never be injected.
	expired, err := s.ExpiredMessages(ctx, now, limit)
	if err != nil {
		return nil, err
	}
	for _, id := range expired {
		msg, err := s.GetMessage(ctx, id)
		if err != nil {
			continue
		}
		_ = s.RecordReceipt(ctx, protocol.Receipt{MessageID: msg.MessageID, CommandID: msg.CommandID, Status: protocol.ReceiptExpired, From: msg.From, To: msg.To, ObservedAt: now, Detail: "message expired before delivery completed"})
		_ = s.SetInboxState(ctx, msg.MessageID, InboxVoided)
	}

	var claimed []string
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx,
			`SELECT message_id, delivery_attempts, command_id, from_session FROM messages
			 WHERE to_session = ? AND (
			   inbox_state IN (?, ?)
			   OR (inbox_state = ? AND (claim_expires = '' OR claim_expires < ?))
			 )
			 ORDER BY created_at ASC LIMIT ?`,
			sessionID, InboxPending, InboxAccepted, InboxClaimed, nowStr, limit)
		if err != nil {
			return fmt.Errorf("store: query claimable: %w", err)
		}
		type cand struct {
			id       string
			attempts int
			command  string
			from     string
		}
		var cands []cand
		for rows.Next() {
			var c cand
			if err := rows.Scan(&c.id, &c.attempts, &c.command, &c.from); err != nil {
				rows.Close()
				return fmt.Errorf("store: scan claimable: %w", err)
			}
			cands = append(cands, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, c := range cands {
			if c.attempts+1 > MaxDeliveryAttempts {
				if _, err := tx.ExecContext(ctx, `UPDATE messages SET inbox_state = ?, claim_owner = '', claim_expires = '' WHERE message_id = ?`, string(InboxDeadLetter), c.id); err != nil {
					return fmt.Errorf("store: dead-letter: %w", err)
				}
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO receipts (message_id, command_id, status, from_session, to_session, detail, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
					c.id, c.command, protocol.ReceiptRejected, c.from, sessionID, "delivery attempts exhausted; dead-lettered", nowStr); err != nil {
					return fmt.Errorf("store: dead-letter receipt: %w", err)
				}
				continue
			}
			res, err := tx.ExecContext(ctx,
				`UPDATE messages SET inbox_state = ?, claim_owner = ?, claim_expires = ?, delivery_attempts = delivery_attempts + 1
				 WHERE message_id = ? AND (
				   inbox_state IN (?, ?)
				   OR (inbox_state = ? AND (claim_expires = '' OR claim_expires < ?))
				 )`,
				string(InboxClaimed), owner, leaseStr, c.id, InboxPending, InboxAccepted, InboxClaimed, nowStr)
			if err != nil {
				return fmt.Errorf("store: claim: %w", err)
			}
			if n, _ := res.RowsAffected(); n == 1 {
				token := newClaimToken()
				gen := int64(1)
				var cur sql.NullInt64
				_ = tx.QueryRowContext(ctx, `SELECT claim_generation FROM message_claims WHERE message_id = ?`, c.id).Scan(&cur)
				if cur.Valid {
					gen = cur.Int64 + 1
				}
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO message_claims (message_id, claim_token, claim_generation) VALUES (?, ?, ?)
					 ON CONFLICT(message_id) DO UPDATE SET claim_token = excluded.claim_token, claim_generation = excluded.claim_generation`,
					c.id, token, gen); err != nil {
					return fmt.Errorf("store: claim fencing: %w", err)
				}
				claimed = append(claimed, c.id)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]protocol.Message, 0, len(claimed))
	for _, id := range claimed {
		msg, err := s.GetMessage(ctx, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		// Expiry re-check at hand-off: never hand an expired message to an
		// adapter for injection.
		if msg.ExpiresAt != nil && time.Now().UTC().After(*msg.ExpiresAt) {
			_ = s.RecordReceipt(ctx, protocol.Receipt{MessageID: msg.MessageID, CommandID: msg.CommandID, Status: protocol.ReceiptExpired, From: msg.From, To: msg.To, ObservedAt: time.Now().UTC(), Detail: "message expired before delivery completed"})
			_ = s.SetInboxState(ctx, msg.MessageID, InboxVoided)
			continue
		}
		out = append(out, msg)
	}
	return out, nil
}

// MessagePrincipal returns the authenticated sender principal recorded at
// admission (from_principal), for delivery-time policy rechecks.
func (s *Store) MessagePrincipal(ctx context.Context, messageID string) (string, error) {
	var principal string
	err := s.db.QueryRowContext(ctx, `SELECT from_principal FROM messages WHERE message_id = ?`, messageID).Scan(&principal)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("store: message principal: %w", err)
	}
	return principal, nil
}

// ClaimOwner returns the current claim owner for a message, if claimed.
func (s *Store) ClaimOwner(ctx context.Context, messageID string) (owner string, state InboxState, expires time.Time, err error) {
	var ownerStr, expStr, stateStr string
	row := s.db.QueryRowContext(ctx, `SELECT claim_owner, inbox_state, claim_expires FROM messages WHERE message_id = ?`, messageID)
	if err := row.Scan(&ownerStr, &stateStr, &expStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", time.Time{}, ErrNotFound
		}
		return "", "", time.Time{}, fmt.Errorf("store: claim owner: %w", err)
	}
	if expStr != "" {
		if t, perr := parseTime(expStr); perr == nil {
			expires = t
		}
	}
	return ownerStr, InboxState(stateStr), expires, nil
}

// ClaimInfo returns claim owner plus fencing token/generation.
func (s *Store) ClaimInfo(ctx context.Context, messageID string) (owner string, token string, generation int64, state InboxState, expires time.Time, err error) {
	owner, state, expires, err = s.ClaimOwner(ctx, messageID)
	if err != nil {
		return "", "", 0, "", time.Time{}, err
	}
	_ = s.db.QueryRowContext(ctx, `SELECT claim_token, claim_generation FROM message_claims WHERE message_id = ?`, messageID).Scan(&token, &generation)
	return owner, token, generation, state, expires, nil
}

// RenewClaim extends a claim lease iff the caller still holds the current
// token/generation. It fails closed for superseded claimants.
// Both the current claim token and generation are required (fencing):
// an empty token or a zero generation is a conflict, not a renewal.
func (s *Store) RenewClaim(ctx context.Context, messageID, owner, token string, generation int64, lease time.Duration) error {
	if lease <= 0 {
		lease = DefaultClaimLease
	}
	if token == "" || generation <= 0 {
		return ErrConflict
	}
	now := time.Now().UTC()
	leaseStr := now.Add(lease).Format(TimestampLayout)
	return s.inTx(ctx, func(tx *sql.Tx) error {
		var curToken string
		var curGen int64
		if err := tx.QueryRowContext(ctx, `SELECT claim_token, claim_generation FROM message_claims WHERE message_id = ?`, messageID).Scan(&curToken, &curGen); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrConflict
			}
			return fmt.Errorf("store: renew claim lookup: %w", err)
		}
		if token == "" || curToken == "" || generation <= 0 || curGen <= 0 {
			return ErrConflict
		}
		if curToken != token || curGen != generation {
			return ErrConflict
		}
		res, err := tx.ExecContext(ctx, `UPDATE messages SET claim_expires = ? WHERE message_id = ? AND inbox_state = ? AND claim_owner = ?`, leaseStr, messageID, InboxClaimed, owner)
		if err != nil {
			return fmt.Errorf("store: renew claim: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrConflict
		}
		return nil
	})
}

// RecordDeliveryOutcome atomically records a receipt, transitions the
// inbox, settles the command (when the outcome resolves admission), and
// appends the journal event in one commit. Invalid/stale transitions are
// rejected rather than partially applied.
func (s *Store) RecordDeliveryOutcome(ctx context.Context, receipt protocol.Receipt, event protocol.EventEnvelope) error {
	if !receipt.Status.Valid() {
		return fmt.Errorf("store: invalid receipt status %q", receipt.Status)
	}
	// Map receipt to inbox transition (same mapping as the node layer).
	var target InboxState
	switch receipt.Status {
	case protocol.ReceiptDelivered:
		target = InboxDelivered
	case protocol.ReceiptPresented, protocol.ReceiptInjected, protocol.ReceiptHandled:
		target = InboxAcknowledged
	case protocol.ReceiptRejected:
		target = InboxRejected
	case protocol.ReceiptExpired:
		target = InboxVoided
	default:
		target = ""
	}
	// Only terminal receipts settle the command. Presented/injected are
	// non-terminal observations: they free admission capacity via the
	// pending-count rule in AdmitMessage (acknowledged messages no longer
	// count), without redefining presentation as handling.
	settle := receipt.Status.Terminal()
	return s.inTx(ctx, func(tx *sql.Tx) error {
		// Stale-transition check: a terminal inbox state must not be
		// moved backwards by a late receipt.
		var curState string
		if err := tx.QueryRowContext(ctx, `SELECT inbox_state FROM messages WHERE message_id = ?`, receipt.MessageID).Scan(&curState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("store: outcome lookup: %w", err)
		}
		// Legal transition matrix (monotonic forward only, duplicates
		// idempotent, regressions rejected):
		//   pending/accepted/claimed -> any terminal target (forward)
		//   acknowledged -> delivered (forward), acknowledged -> acknowledged (idempotent)
		//   delivered -> delivered (idempotent); delivered -> acknowledged is a
		//     regression and is rejected.
		//   rejected/voided/dead_letter -> self (idempotent only); any
		//     success target is rejected, and switching between terminal
		//     failure states is rejected.
		//   unknown receipts (target "") never change state.
		cur := InboxState(curState)
		rank := func(s InboxState) int {
			switch s {
			case InboxPending:
				return 0
			case InboxAccepted:
				return 1
			case InboxClaimed:
				return 2
			case InboxAcknowledged:
				return 3
			case InboxDelivered:
				return 4
			default:
				return -1
			}
		}
		isTerminalFailure := func(s InboxState) bool {
			return s == InboxRejected || s == InboxVoided || s == InboxDeadLetter
		}
		if target != "" {
			if isTerminalFailure(cur) {
				if target != cur {
					return ErrConflict
				}
			} else if isTerminalFailure(target) {
				// acknowledged/delivered are success-terminal: a failure
				// receipt arriving later is a regression/rewrite, reject it.
				if cur == InboxAcknowledged || cur == InboxDelivered {
					return ErrConflict
				}
			} else {
				// Both non-failure: enforce rank monotonicity.
				rCur, rTarget := rank(cur), rank(target)
				if rCur > rTarget {
					return ErrConflict
				}
			}
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO receipts (message_id, command_id, status, from_session, to_session, detail, observed_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			receipt.MessageID, receipt.CommandID, receipt.Status, receipt.From, receipt.To, receipt.Detail,
			receipt.ObservedAt.UTC().Format(TimestampLayout)); err != nil {
			return fmt.Errorf("store: insert receipt outcome: %w", err)
		}
		if target != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE messages SET inbox_state = ? WHERE message_id = ?`, target, receipt.MessageID); err != nil {
				return fmt.Errorf("store: outcome inbox transition: %w", err)
			}
		}
		if settle && receipt.CommandID != "" {
			if _, err := tx.ExecContext(ctx,
				`UPDATE commands SET state = ?, disposition = ?, updated_at = ? WHERE command_id = ?`,
				protocol.CommandSettled, string(receipt.Status), time.Now().UTC().Format(TimestampLayout), receipt.CommandID); err != nil {
				return fmt.Errorf("store: settle command outcome: %w", err)
			}
		}
		if event.EventID != "" {
			if _, err := s.AppendEventTx(ctx, tx, event, receipt.CommandID); err != nil {
				return err
			}
		}
		return nil
	})
}

// CountPendingInbox counts undelivered messages for a recipient.
func (s *Store) CountPendingInbox(ctx context.Context, sessionID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE to_session = ? AND inbox_state IN (?, ?, ?)`,
		sessionID, InboxPending, InboxAccepted, InboxClaimed).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: count pending inbox: %w", err)
	}
	return n, nil
}

// PurgeMessagesOlderThan deletes messages and their receipts older than the
// cutoff, returning the number of messages removed.
//
// Retention MUST preserve the idempotency obligations for the advertised
// deduplication horizon (ICSP-001 section 13), so the caller chooses a
// cutoff at or beyond that horizon.
func (s *Store) PurgeMessagesOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	mark := cutoff.UTC().Format(TimestampLayout)
	var removed int64
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		// Purge only terminal messages. Pending/claimed messages (incl. active
		// leases) and dedup obligations inside the horizon are preserved:
		// the caller guarantees cutoff is at/older than the horizon.
		terminal := "inbox_state IN ('delivered','acknowledged','rejected','voided','dead_letter')"
		res, err := tx.ExecContext(ctx,
			`DELETE FROM receipts WHERE message_id IN (
				SELECT message_id FROM messages WHERE created_at < ? AND `+terminal+`
			)`, mark)
		if err != nil {
			return fmt.Errorf("store: purge receipts: %w", err)
		}
		_ = res

		res, err = tx.ExecContext(ctx,
			`DELETE FROM messages WHERE created_at < ? AND `+terminal, mark)
		if err != nil {
			return fmt.Errorf("store: purge messages: %w", err)
		}
		removed, _ = res.RowsAffected()
		// Purge settled commands whose messages are gone and which are older
		// than the cutoff, bounding the ledger without breaking dedup inside
		// the horizon (cutoff already respects it).
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM commands WHERE created_at < ? AND state NOT IN ('accepted','executing')
			 AND command_id NOT IN (SELECT command_id FROM messages)`, mark); err != nil {
			return fmt.Errorf("store: purge commands: %w", err)
		}
		return nil
	})
	return removed, err
}

// ExpiredMessages returns messages whose expiry deadline has passed.
func (s *Store) ExpiredMessages(ctx context.Context, now time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id FROM messages
		 WHERE expires_at != '' AND expires_at < ? AND inbox_state IN (?, ?, ?)
		 ORDER BY expires_at ASC LIMIT ?`,
		now.UTC().Format(TimestampLayout), InboxPending, InboxAccepted, InboxClaimed, limit)
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
