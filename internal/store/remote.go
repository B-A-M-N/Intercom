package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RemoteHost is an enrolled peer host. Fingerprint is the pinned TLS identity.
type RemoteHost struct {
	HostID      string
	Endpoint    string
	TrustStatus string
	Fingerprint string
	CreatedAt   time.Time
}

// RemoteTransfer tracks durable outbound state with the full envelope.
type RemoteTransfer struct {
	MessageID     string
	RemoteHost    string
	TargetSession string
	SenderSession string
	Envelope      string
	Status        string
	Attempts      int
	LastError     string
	NextRetryAt   time.Time
}

func (s *Store) EnrollHost(ctx context.Context, hostID, endpoint, fingerprint string) error {
	now := time.Now().UTC().Format(TimestampLayout)
	_, err := s.db.ExecContext(ctx, `INSERT INTO remote_hosts (host_id, endpoint, trust_status, credential_hash, fingerprint, created_at)
		VALUES (?, ?, 'trusted', '', ?, ?)
		ON CONFLICT(host_id) DO UPDATE SET endpoint = excluded.endpoint, credential_hash = '', fingerprint = excluded.fingerprint, trust_status = 'trusted'`,
		hostID, endpoint, fingerprint, now)
	if err != nil {
		return fmt.Errorf("store: enroll remote host: %w", err)
	}
	return nil
}

func (s *Store) RevokeHost(ctx context.Context, hostID string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE remote_hosts SET trust_status = 'revoked' WHERE host_id = ?`, hostID)
	if err != nil {
		return fmt.Errorf("store: revoke remote host: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RotateHost(ctx context.Context, hostID, endpoint, fingerprint string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE remote_hosts SET endpoint = ?, fingerprint = ?, trust_status = 'trusted' WHERE host_id = ?`, endpoint, fingerprint, hostID)
	if err != nil {
		return fmt.Errorf("store: rotate remote host: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return s.EnrollHost(ctx, hostID, endpoint, fingerprint)
	}
	return nil
}

func (s *Store) ListRemoteHosts(ctx context.Context) ([]RemoteHost, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT host_id, endpoint, trust_status, fingerprint, created_at FROM remote_hosts ORDER BY host_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("store: list remote hosts: %w", err)
	}
	defer rows.Close()
	var out []RemoteHost
	for rows.Next() {
		var h RemoteHost
		var created string
		if err := rows.Scan(&h.HostID, &h.Endpoint, &h.TrustStatus, &h.Fingerprint, &created); err != nil {
			return nil, fmt.Errorf("store: scan remote host: %w", err)
		}
		if t, err := parseTime(created); err == nil {
			h.CreatedAt = t
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *Store) GetRemoteHost(ctx context.Context, hostID string) (RemoteHost, string, error) {
	var h RemoteHost
	var created, hash string
	err := s.db.QueryRowContext(ctx, `SELECT host_id, endpoint, trust_status, fingerprint, created_at, credential_hash FROM remote_hosts WHERE host_id = ?`, hostID).Scan(&h.HostID, &h.Endpoint, &h.TrustStatus, &h.Fingerprint, &created, &hash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RemoteHost{}, "", ErrNotFound
		}
		return RemoteHost{}, "", fmt.Errorf("store: get remote host: %w", err)
	}
	if t, err := parseTime(created); err == nil {
		h.CreatedAt = t
	}
	return h, hash, nil
}

// RecordRemoteTransferFull records a durable outbound transfer.
// Idempotency is content-checked transactionally: recording an identical
// transfer is a no-op success, but reusing a message ID with different
// content (host, target, sender, or envelope) is ErrConflict, never
// a silent overwrite or a silent drop.
func (s *Store) RecordRemoteTransferFull(ctx context.Context, messageID, remoteHost, targetSession, senderSession, envelope string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_transfers (message_id, remote_host, target_session, sender_session, envelope, status, attempts, last_error, next_retry_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', 0, '', '', ?, ?)`, messageID, remoteHost, targetSession, senderSession, envelope, time.Now().UTC().Format(TimestampLayout), time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: record remote transfer: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			return nil
		}
		var existing struct {
			RemoteHost, TargetSession, SenderSession, Envelope string
		}
		if err := tx.QueryRowContext(ctx, `SELECT remote_host, target_session, sender_session, envelope FROM remote_transfers WHERE message_id = ?`, messageID).
			Scan(&existing.RemoteHost, &existing.TargetSession, &existing.SenderSession, &existing.Envelope); err != nil {
			return fmt.Errorf("store: read remote transfer: %w", err)
		}
		if existing.RemoteHost == remoteHost && existing.TargetSession == targetSession &&
			existing.SenderSession == senderSession && existing.Envelope == envelope {
			return nil // identical retry
		}
		return ErrConflict
	})
}

func (s *Store) RecordRemoteTransfer(ctx context.Context, messageID, remoteHost, targetSession string) error {
	return s.RecordRemoteTransferFull(ctx, messageID, remoteHost, targetSession, "", "")
}

func (s *Store) SetRemoteTransfer(ctx context.Context, messageID, status, lastError string, attempts int) error {
	now := time.Now().UTC().Format(TimestampLayout)
	_, err := s.db.ExecContext(ctx, `UPDATE remote_transfers SET status = ?, last_error = ?, attempts = ?, updated_at = ? WHERE message_id = ?`, status, lastError, attempts, now, messageID)
	if err != nil {
		return fmt.Errorf("store: set remote transfer: %w", err)
	}
	return nil
}

func (s *Store) SetRemoteTransferRetry(ctx context.Context, messageID, status, lastError string, attempts int, nextRetry time.Time) error {
	now := time.Now().UTC().Format(TimestampLayout)
	next := ""
	if !nextRetry.IsZero() {
		next = nextRetry.UTC().Format(TimestampLayout)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE remote_transfers SET status = ?, last_error = ?, attempts = ?, next_retry_at = ?, updated_at = ? WHERE message_id = ?`, status, lastError, attempts, next, now, messageID)
	if err != nil {
		return fmt.Errorf("store: set remote transfer: %w", err)
	}
	return nil
}

func (s *Store) GetRemoteTransfer(ctx context.Context, messageID string) (RemoteTransfer, error) {
	var t RemoteTransfer
	var next string
	err := s.db.QueryRowContext(ctx, `SELECT message_id, remote_host, target_session, sender_session, envelope, status, attempts, last_error, next_retry_at FROM remote_transfers WHERE message_id = ?`, messageID).Scan(&t.MessageID, &t.RemoteHost, &t.TargetSession, &t.SenderSession, &t.Envelope, &t.Status, &t.Attempts, &t.LastError, &next)
	if err != nil {
		return RemoteTransfer{}, ErrNotFound
	}
	if next != "" {
		if tt, err := parseTime(next); err == nil {
			t.NextRetryAt = tt
		}
	}
	return t, nil
}

func (s *Store) PendingRemoteTransfers(ctx context.Context) ([]RemoteTransfer, error) {
	nowStr := time.Now().UTC().Format(TimestampLayout)
	rows, err := s.db.QueryContext(ctx, `SELECT message_id, remote_host, target_session, sender_session, envelope, status, attempts, last_error, next_retry_at FROM remote_transfers WHERE status IN ('pending','failed') AND (next_retry_at = '' OR next_retry_at <= ?) ORDER BY created_at ASC`, nowStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RemoteTransfer
	for rows.Next() {
		var t RemoteTransfer
		var next string
		if err := rows.Scan(&t.MessageID, &t.RemoteHost, &t.TargetSession, &t.SenderSession, &t.Envelope, &t.Status, &t.Attempts, &t.LastError, &next); err != nil {
			return nil, err
		}
		if next != "" {
			if tt, err := parseTime(next); err == nil {
				t.NextRetryAt = tt
			}
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
