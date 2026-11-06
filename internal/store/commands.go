package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
)

// CommandRecord is one durable command ledger entry (ICSP-001 section 6.1).
type CommandRecord struct {
	CommandID     string
	PrincipalID   string
	TargetSession string
	Operation     string
	Fingerprint   string
	State         protocol.CommandState
	MessageID     string
	EventSeq      uint64
	Disposition   string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Fingerprint computes the canonical request fingerprint for a command.
//
// The fingerprint covers every field that distinguishes one request from
// another for the same operation, so that a retry with an identical request
// can be recognised while a conflicting reuse is rejected.
func Fingerprint(operation string, params any) (string, error) {
	canonical, err := canonicalJSON(params)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(operation))
	h.Write([]byte{0})
	h.Write(canonical)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// canonicalJSON produces a deterministic byte representation for hashing.
// encoding/json already sorts map keys, so struct and map inputs both hash
// consistently.
func canonicalJSON(v any) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("store: canonical json: %w", err)
	}
	return b, nil
}

// AdmitCommand durably admits a command, or returns the prior outcome for an
// identical retry.
//
// Semantics required by ICSP-001 section 5.5 and 6.1:
//
//   - Reusing an identical commandId with an identical request returns the
//     previously established admission outcome.
//   - Reusing a commandId with a conflicting request is rejected.
//   - Durable acceptance happens before the caller is told "accepted".
//
// The returned record is the authoritative ledger state after the call. When
// replayed is true, the command had already been admitted and no second
// admission took place.
func (s *Store) AdmitCommand(ctx context.Context, rec CommandRecord, fingerprint string) (out CommandRecord, replayed bool, err error) {
	now := time.Now().UTC()
	rec.Fingerprint = fingerprint
	rec.UpdatedAt = now
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	if rec.State == "" {
		rec.State = protocol.CommandAccepted
	}

	err = s.inTx(ctx, func(tx *sql.Tx) error {
		var existing CommandRecord
		row := tx.QueryRowContext(ctx,
			`SELECT command_id, principal_id, target_session, operation, fingerprint,
			        state, message_id, event_seq, disposition, created_at, updated_at
			 FROM commands WHERE command_id = ?`, rec.CommandID)

		var createdAt, updatedAt scanTime
		switch err := row.Scan(&existing.CommandID, &existing.PrincipalID, &existing.TargetSession,
			&existing.Operation, &existing.Fingerprint, &existing.State, &existing.MessageID,
			&existing.EventSeq, &existing.Disposition, &createdAt, &updatedAt); {
		case err == nil:
			// The command already exists. Identical fingerprint means an
			// idempotent retry; a different fingerprint is a conflict.
			if existing.Fingerprint != fingerprint {
				return ErrConflict
			}
			existing.CreatedAt = createdAt.Time()
			existing.UpdatedAt = updatedAt.Time()
			out, replayed = existing, true
			return nil
		case errors.Is(err, sql.ErrNoRows):
			// Fall through to insert.
		default:
			return fmt.Errorf("store: query command: %w", err)
		}

		_, err := tx.ExecContext(ctx,
			`INSERT INTO commands (command_id, principal_id, target_session, operation,
			        fingerprint, state, message_id, event_seq, disposition, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			rec.CommandID, rec.PrincipalID, rec.TargetSession, rec.Operation,
			rec.Fingerprint, rec.State, rec.MessageID, rec.EventSeq, rec.Disposition,
			rec.CreatedAt.UTC().Format(TimestampLayout), rec.UpdatedAt.UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: insert command: %w", err)
		}
		out = rec
		replayed = false
		return nil
	})
	if err != nil {
		return CommandRecord{}, false, err
	}
	return out, replayed, nil
}

// SetCommandMessage links a command to the message it admitted.
//
// The link is written after the message is stored, so a replayed command
// resolves to the message id that was originally assigned.
func (s *Store) SetCommandMessage(ctx context.Context, commandID, messageID string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE commands SET message_id = ?, updated_at = ? WHERE command_id = ?`,
			messageID, time.Now().UTC().Format(TimestampLayout), commandID)
		if err != nil {
			return fmt.Errorf("store: set command message: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// GetCommand loads one command ledger entry.
func (s *Store) GetCommand(ctx context.Context, commandID string) (CommandRecord, error) {
	var rec CommandRecord
	row := s.db.QueryRowContext(ctx,
		`SELECT command_id, principal_id, target_session, operation, fingerprint,
		        state, message_id, event_seq, disposition, created_at, updated_at
		 FROM commands WHERE command_id = ?`, commandID)
	var createdAt, updatedAt scanTime
	err := row.Scan(&rec.CommandID, &rec.PrincipalID, &rec.TargetSession, &rec.Operation,
		&rec.Fingerprint, &rec.State, &rec.MessageID, &rec.EventSeq, &rec.Disposition,
		&createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return CommandRecord{}, ErrNotFound
	}
	if err != nil {
		return CommandRecord{}, fmt.Errorf("store: get command: %w", err)
	}
	rec.CreatedAt = createdAt.Time()
	rec.UpdatedAt = updatedAt.Time()
	return rec, nil
}

// SetCommandState records a non-terminal state transition for a command.
//
// The lifecycle is received -> accepted -> executing -> settled. A command
// may be rejected at admission or become abandoned during recovery.
func (s *Store) SetCommandState(ctx context.Context, commandID string, state protocol.CommandState) error {
	if !state.Valid() {
		return fmt.Errorf("store: invalid command state %q", state)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE commands SET state = ?, updated_at = ? WHERE command_id = ?`,
			state, time.Now().UTC().Format(TimestampLayout), commandID)
		if err != nil {
			return fmt.Errorf("store: set command state: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// SettleCommand records a terminal disposition for a command.
func (s *Store) SettleCommand(ctx context.Context, commandID string, state protocol.CommandState, disposition string) error {
	if !state.Valid() {
		return fmt.Errorf("store: invalid command state %q", state)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE commands SET state = ?, disposition = ?, updated_at = ?
			 WHERE command_id = ?`,
			state, disposition, time.Now().UTC().Format(TimestampLayout), commandID)
		if err != nil {
			return fmt.Errorf("store: settle command: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// UnsettledCommands returns admitted commands that never reached a terminal
// disposition. These are the candidates for crash recovery classification
// (ICSP-001 section 6.2).
func (s *Store) UnsettledCommands(ctx context.Context) ([]CommandRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT command_id, principal_id, target_session, operation, fingerprint,
		        state, message_id, event_seq, disposition, created_at, updated_at
		 FROM commands WHERE state IN (?, ?) ORDER BY created_at ASC`,
		protocol.CommandAccepted, protocol.CommandExecuting)
	if err != nil {
		return nil, fmt.Errorf("store: query unsettled: %w", err)
	}
	defer rows.Close()

	var out []CommandRecord
	for rows.Next() {
		var rec CommandRecord
		var createdAt, updatedAt scanTime
		if err := rows.Scan(&rec.CommandID, &rec.PrincipalID, &rec.TargetSession,
			&rec.Operation, &rec.Fingerprint, &rec.State, &rec.MessageID, &rec.EventSeq,
			&rec.Disposition, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("store: scan unsettled: %w", err)
		}
		rec.CreatedAt = createdAt.Time()
		rec.UpdatedAt = updatedAt.Time()
		out = append(out, rec)
	}
	return out, rows.Err()
}

// AbandonCommand marks an unresolved command as abandoned. An unresolved
// command MUST NOT be silently re-executed if doing so could duplicate
// effects, so recovery records the conservative disposition instead.
func (s *Store) AbandonCommand(ctx context.Context, commandID, reason string) error {
	return s.SettleCommand(ctx, commandID, protocol.CommandAbandoned, reason)
}

// AdmissionLimits bounds enforced at atomic admission time.
type AdmissionLimits struct {
	MaxPendingCommands int
	MaxInboxMessages   int
	MaxDatabaseBytes   int64
}

// AdmitMessage atomically admits a command, its message, the command-message
// link, the acceptance receipt, and the acceptance event in ONE transaction.
// Identical retries return the original message id; conflicting reuse fails.
// Accepted work therefore always has a recoverable message record.
func (s *Store) AdmitMessage(ctx context.Context, rec CommandRecord, fingerprint string, msg protocol.Message, principalID string, ev protocol.EventEnvelope, limits AdmissionLimits) (out CommandRecord, replayed bool, err error) {
	now := time.Now().UTC()
	rec.Fingerprint = fingerprint
	rec.UpdatedAt = now
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	if rec.State == "" {
		rec.State = protocol.CommandAccepted
	}
	rec.MessageID = msg.MessageID

	body, err := json.Marshal(msg.Body)
	if err != nil {
		return CommandRecord{}, false, fmt.Errorf("store: marshal body: %w", err)
	}
	delivery, err := json.Marshal(msg.Delivery)
	if err != nil {
		return CommandRecord{}, false, fmt.Errorf("store: marshal delivery: %w", err)
	}
	metadata, err := json.Marshal(msg.Metadata)
	if err != nil {
		return CommandRecord{}, false, fmt.Errorf("store: marshal metadata: %w", err)
	}

	err = s.inTx(ctx, func(tx *sql.Tx) error {
		// Existing command? Idempotent retry or conflict.
		var existing CommandRecord
		row := tx.QueryRowContext(ctx,
			`SELECT command_id, principal_id, target_session, operation, fingerprint,
			        state, message_id, event_seq, disposition, created_at, updated_at
			 FROM commands WHERE command_id = ?`, rec.CommandID)
		var cAt, uAt scanTime
		switch scanErr := row.Scan(&existing.CommandID, &existing.PrincipalID, &existing.TargetSession,
			&existing.Operation, &existing.Fingerprint, &existing.State, &existing.MessageID,
			&existing.EventSeq, &existing.Disposition, &cAt, &uAt); {
		case scanErr == nil:
			if existing.Fingerprint != fingerprint {
				return ErrConflict
			}
			existing.CreatedAt = cAt.Time()
			existing.UpdatedAt = uAt.Time()
			out, replayed = existing, true
			return nil
		case errors.Is(scanErr, sql.ErrNoRows):
		default:
			return fmt.Errorf("store: query command: %w", scanErr)
		}

		// Enforce pending-command and storage bounds inside the tx so a
		// concurrent admission cannot slip past them. Presented/injected
		// (inbox acknowledged) do not consume capacity: admission is
		// resolved once the recipient has observed the message, even
		// though handling remains a separate lifecycle step.
		if limits.MaxPendingCommands > 0 {
			var n int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM commands c LEFT JOIN messages m ON c.message_id = m.message_id WHERE c.state IN (?, ?) AND (m.message_id IS NULL OR m.inbox_state IN (?, ?, ?))`, protocol.CommandAccepted, protocol.CommandExecuting, InboxPending, InboxAccepted, InboxClaimed).Scan(&n); err != nil {
				return fmt.Errorf("store: count pending commands: %w", err)
			}
			if n >= limits.MaxPendingCommands {
				return fmt.Errorf("store: pending commands at limit %d: %w", limits.MaxPendingCommands, ErrLimit)
			}
		}
		if limits.MaxInboxMessages > 0 {
			var n int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE to_session = ? AND inbox_state IN (?, ?, ?)`, msg.To, InboxPending, InboxAccepted, InboxClaimed).Scan(&n); err != nil {
				return fmt.Errorf("store: count inbox: %w", err)
			}
			if n >= limits.MaxInboxMessages {
				return fmt.Errorf("store: recipient inbox at limit %d: %w", limits.MaxInboxMessages, ErrLimit)
			}
		}
		if limits.MaxDatabaseBytes > 0 {
			var pageCount, pageSize int64
			_ = tx.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount)
			_ = tx.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize)
			if pageCount*pageSize >= limits.MaxDatabaseBytes {
				return fmt.Errorf("store: database at limit %d bytes: %w", limits.MaxDatabaseBytes, ErrLimit)
			}
		}

		// Command (accepted -> executing happens in the same tx; the
		// executing transition is part of admission, delivery follows).
		_, err := tx.ExecContext(ctx,
			`INSERT INTO commands (command_id, principal_id, target_session, operation,
			        fingerprint, state, message_id, event_seq, disposition, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			rec.CommandID, rec.PrincipalID, rec.TargetSession, rec.Operation,
			rec.Fingerprint, protocol.CommandExecuting, rec.MessageID, rec.EventSeq, rec.Disposition,
			rec.CreatedAt.UTC().Format(TimestampLayout), rec.UpdatedAt.UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: insert command: %w", err)
		}

		expires := ""
		if msg.ExpiresAt != nil {
			expires = msg.ExpiresAt.UTC().Format(TimestampLayout)
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO messages (message_id, command_id, from_session, to_session,
			        from_principal, body, delivery, reply_to, correlation_id, created_at,
			        expires_at, metadata, inbox_state, conversation_id)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			msg.MessageID, msg.CommandID, msg.From, msg.To, principalID,
			string(body), string(delivery), msg.ReplyTo, msg.CorrelationID,
			msg.CreatedAt.UTC().Format(TimestampLayout), expires, string(metadata), string(InboxPending), msg.ConversationID)
		if err != nil {
			return fmt.Errorf("store: insert message: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO receipts (message_id, command_id, status, from_session, to_session, detail, observed_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			msg.MessageID, msg.CommandID, protocol.ReceiptAccepted, msg.From, msg.To,
			"durably admitted", now.Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: insert acceptance receipt: %w", err)
		}

		if _, err := s.AppendEventTx(ctx, tx, ev, rec.CommandID); err != nil {
			return err
		}
		out = rec
		replayed = false
		return nil
	})
	if err != nil {
		return CommandRecord{}, false, err
	}
	return out, replayed, nil
}
