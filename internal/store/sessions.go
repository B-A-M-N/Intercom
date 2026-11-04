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

// RegisterSession records or refreshes a session in the peer registry.
//
// Sessions are keyed by session id, so an agent restart that preserves a
// recoverable session keeps its identity while a new process incarnation
// gets a new instance id (ICSP-001 section 3).
func (s *Store) RegisterSession(ctx context.Context, peer protocol.PeerInfo) error {
	caps, err := json.Marshal(peer.Capabilities)
	if err != nil {
		return fmt.Errorf("store: marshal capabilities: %w", err)
	}
	modes, err := json.Marshal(peer.Delivery)
	if err != nil {
		return fmt.Errorf("store: marshal delivery modes: %w", err)
	}
	now := time.Now().UTC().Format(TimestampLayout)

	return s.inTx(ctx, func(tx *sql.Tx) error {
		var exists int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM sessions WHERE session_id = ?`, peer.SessionID).Scan(&exists)
		switch {
		case err == nil:
			_, err = tx.ExecContext(ctx,
				`UPDATE sessions SET instance_id = ?, host_id = ?, principal_id = ?, alias = ?,
				        harness = ?, harness_version = ?, presence = ?, capabilities = ?, delivery_modes = ?,
				        updated_at = ?, last_seen = ?
				 WHERE session_id = ?`,
				peer.InstanceID, peer.HostID, peer.PrincipalID, peer.Alias, peer.Harness,
				peer.HarnessVersion, peer.Presence, string(caps), string(modes),
				now, now, peer.SessionID)
			if err != nil {
				return fmt.Errorf("store: update session: %w", err)
			}
		case errors.Is(err, sql.ErrNoRows):
			_, err = tx.ExecContext(ctx,
				`INSERT INTO sessions (session_id, instance_id, host_id, principal_id, alias, harness,
				        harness_version, state, presence, capabilities, delivery_modes, created_at, updated_at, last_seen)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				peer.SessionID, peer.InstanceID, peer.HostID, peer.PrincipalID, peer.Alias,
				peer.Harness, peer.HarnessVersion, protocol.SessionIdle, peer.Presence,
				string(caps), string(modes), now, now, now)
			if err != nil {
				return fmt.Errorf("store: insert session: %w", err)
			}
		default:
			return fmt.Errorf("store: lookup session: %w", err)
		}
		return nil
	})
}

// PeerRecord is one registered session with its resolved metadata.
type PeerRecord struct {
	protocol.PeerInfo
	HarnessVersion string
}

// GetSession loads one session's peer record.
func (s *Store) GetSession(ctx context.Context, sessionID string) (PeerRecord, error) {
	var rec PeerRecord
	var caps, modes, state string
	var created, updated, lastSeen string

	row := s.db.QueryRowContext(ctx,
		`SELECT session_id, instance_id, host_id, principal_id, alias, harness, harness_version,
		        state, presence, capabilities, delivery_modes, created_at, updated_at, last_seen
		 FROM sessions WHERE session_id = ?`, sessionID)
	err := row.Scan(&rec.SessionID, &rec.InstanceID, &rec.HostID, &rec.PrincipalID, &rec.Alias,
		&rec.Harness, &rec.HarnessVersion, &state, &rec.Presence, &caps, &modes,
		&created, &updated, &lastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return PeerRecord{}, ErrNotFound
	}
	if err != nil {
		return PeerRecord{}, fmt.Errorf("store: get session: %w", err)
	}
	if err := json.Unmarshal([]byte(caps), &rec.Capabilities); err != nil {
		return PeerRecord{}, fmt.Errorf("store: decode capabilities: %w", err)
	}
	if err := json.Unmarshal([]byte(modes), &rec.Delivery); err != nil {
		return PeerRecord{}, fmt.Errorf("store: decode delivery modes: %w", err)
	}
	rec.State = protocol.SessionState(state)
	return rec, nil
}

// ListPeers returns the sessions visible to a principal.
//
// Discovery MUST NOT reveal sessions outside the authenticated principal's
// discovery permissions (ICSP-001 section 4), so the filter is applied in the
// query rather than in the caller.
func (s *Store) ListPeers(ctx context.Context, principalID string, limit int) ([]PeerRecord, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT session_id, instance_id, host_id, principal_id, alias, harness, harness_version,
		        state, presence, capabilities, delivery_modes, created_at, updated_at, last_seen
		 FROM sessions WHERE principal_id = ? ORDER BY session_id ASC LIMIT ?`, principalID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list peers: %w", err)
	}
	defer rows.Close()

	var out []PeerRecord
	for rows.Next() {
		var rec PeerRecord
		var caps, modes, state string
		var created, updated, lastSeen string
		if err := rows.Scan(&rec.SessionID, &rec.InstanceID, &rec.HostID, &rec.PrincipalID,
			&rec.Alias, &rec.Harness, &rec.HarnessVersion, &state, &rec.Presence,
			&caps, &modes, &created, &updated, &lastSeen); err != nil {
			return nil, fmt.Errorf("store: scan peer: %w", err)
		}
		if err := json.Unmarshal([]byte(caps), &rec.Capabilities); err != nil {
			return nil, fmt.Errorf("store: decode capabilities: %w", err)
		}
		if err := json.Unmarshal([]byte(modes), &rec.Delivery); err != nil {
			return nil, fmt.Errorf("store: decode delivery modes: %w", err)
		}
		rec.State = protocol.SessionState(state)
		out = append(out, rec)
	}
	return out, rows.Err()
}

// ResolveSession maps an alias or session identifier to a canonical session id.
//
// Aliases resolve to stable canonical identities and never serve as
// authentication credentials (ICSP-001 section 3).
func (s *Store) ResolveSession(ctx context.Context, aliasOrID string) (PeerRecord, error) {
	rec, err := s.GetSession(ctx, aliasOrID)
	if err == nil {
		return rec, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return PeerRecord{}, err
	}

	var sessionID string
	row := s.db.QueryRowContext(ctx,
		`SELECT session_id FROM sessions WHERE alias = ? ORDER BY created_at ASC LIMIT 1`, aliasOrID)
	if err := row.Scan(&sessionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PeerRecord{}, ErrNotFound
		}
		return PeerRecord{}, fmt.Errorf("store: resolve alias: %w", err)
	}
	return s.GetSession(ctx, sessionID)
}

// SetSessionState updates a session's normalized state.
func (s *Store) SetSessionState(ctx context.Context, sessionID string, state protocol.SessionState) error {
	if !state.Valid() {
		return fmt.Errorf("store: invalid session state %q", state)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE sessions SET state = ?, updated_at = ? WHERE session_id = ?`,
			state, time.Now().UTC().Format(TimestampLayout), sessionID)
		if err != nil {
			return fmt.Errorf("store: set session state: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// SetPresence updates a session's presence state.
func (s *Store) SetPresence(ctx context.Context, sessionID string, presence protocol.PresenceState) error {
	if !presence.Valid() {
		return fmt.Errorf("store: invalid presence state %q", presence)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC().Format(TimestampLayout)
		res, err := tx.ExecContext(ctx,
			`UPDATE sessions SET presence = ?, last_seen = ?, updated_at = ? WHERE session_id = ?`,
			presence, now, now, sessionID)
		if err != nil {
			return fmt.Errorf("store: set presence: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// MarkAllDisconnected marks every session of a host as disconnected.
//
// A host disconnect alone SHALL NOT automatically terminate the logical
// session (ICSP-001 section 7.3); only presence changes.
func (s *Store) MarkAllDisconnected(ctx context.Context, hostID string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET presence = ?, updated_at = ? WHERE host_id = ?`,
		protocol.PresenceDisconnected, time.Now().UTC().Format(TimestampLayout), hostID)
	if err != nil {
		return 0, fmt.Errorf("store: mark disconnected: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// MarkStaleDisconnected marks sessions that have not reported liveness
// since the cutoff as disconnected.
//
// Only presence changes. The logical session survives, because a host
// disconnect alone does not terminate a session (ICSP-001 section 7.3).
func (s *Store) MarkStaleDisconnected(ctx context.Context, cutoff time.Time) (int64, error) {
	mark := cutoff.UTC().Format(TimestampLayout)
	res, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET presence = ?, updated_at = ?
		 WHERE presence != ? AND (last_seen = '' OR last_seen < ?)`,
		protocol.PresenceDisconnected, time.Now().UTC().Format(TimestampLayout),
		protocol.PresenceDisconnected, mark)
	if err != nil {
		return 0, fmt.Errorf("store: mark stale disconnected: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// RecordInstance registers a new agent process incarnation.
func (s *Store) RecordInstance(ctx context.Context, instanceID, hostID, sessionID, harness, version string, pid int) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO instances (instance_id, host_id, session_id, harness, harness_version, pid, started_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			instanceID, hostID, sessionID, harness, version, pid,
			time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: insert instance: %w", err)
		}
		return nil
	})
}

// EnsurePrincipal records a principal if it is not already known.
func (s *Store) EnsurePrincipal(ctx context.Context, principalID, displayName string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO principals (principal_id, display_name, created_at) VALUES (?, ?, ?)`,
			principalID, displayName, time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: ensure principal: %w", err)
		}
		return nil
	})
}

// EnsureHost records a host if it is not already known.
func (s *Store) EnsureHost(ctx context.Context, hostID, principalID, displayName string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO hosts (host_id, principal_id, display_name, created_at) VALUES (?, ?, ?, ?)`,
			hostID, principalID, displayName, time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: ensure host: %w", err)
		}
		return nil
	})
}

// Audit appends an audit record for a security-relevant transition.
func (s *Store) Audit(ctx context.Context, principalID, subject, action, detail string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO audit_log (principal_id, subject, action, detail, recorded_at) VALUES (?, ?, ?, ?, ?)`,
			principalID, subject, action, detail, time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: audit: %w", err)
		}
		return nil
	})
}
