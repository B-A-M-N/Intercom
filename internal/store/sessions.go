package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
)

// hashSessionSecret hashes a client-held resume secret for storage.
// Plaintext secrets are never persisted.
func hashSessionSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// generateSessionSecret mints a 256-bit resume secret.
func generateSessionSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is fatal for credential generation;
		// fall back to time-based entropy rather than an empty secret.
		return hashSessionSecret(fmt.Sprintf("%d", time.Now().UnixNano()))
	}
	return hex.EncodeToString(b)
}

// GenerateSessionSecretForNode mints a new resume secret for the node
// layer (in-process enrollment and ACP first-registration issuance).
func GenerateSessionSecretForNode() string { return generateSessionSecret() }

// RegisterSession records or refreshes a session in the peer registry.
//
// Sessions are keyed by session id, so an agent restart that preserves a
// recoverable session keeps its identity while a new process incarnation
// gets a new instance id (ICSP-001 section 3).
//
// Resume of an existing session requires the client-held session secret
// (peer.SessionSecret, or a freshly generated one for a new session).
// A shared OS UID or a client-supplied sessionId alone is not proof of
// ownership: without the secret, resume is forbidden. The plaintext
// secret is never stored; only its SHA-256 hash is persisted.
func (s *Store) RegisterSession(ctx context.Context, peer protocol.PeerInfo) error {
	secret := peer.SessionSecret
	if secret == "" {
		secret = generateSessionSecret()
		peer.SessionSecret = secret
	}
	_, err := s.RegisterSessionWithSecret(ctx, peer, secret)
	return err
}

// RegisterSessionWithSecret registers or resumes a session with an
// explicit resume secret, returning true when the session was newly
// created (secret freshly enrolled) and false on authorized resume.
func (s *Store) RegisterSessionWithSecret(ctx context.Context, peer protocol.PeerInfo, secret string) (bool, error) {
	if secret == "" {
		secret = generateSessionSecret()
	}
	peer.SessionSecret = secret
	caps, err := json.Marshal(peer.Capabilities)
	if err != nil {
		return false, fmt.Errorf("store: marshal capabilities: %w", err)
	}
	modes, err := json.Marshal(peer.Delivery)
	if err != nil {
		return false, fmt.Errorf("store: marshal delivery modes: %w", err)
	}
	tags, err := json.Marshal(peer.Tags)
	if err != nil {
		return false, fmt.Errorf("store: marshal tags: %w", err)
	}
	if peer.TrustDomain == "" {
		peer.TrustDomain = "local"
	}
	now := time.Now().UTC().Format(TimestampLayout)
	created := false
	secretHash := hashSessionSecret(secret)

	err = s.inTx(ctx, func(tx *sql.Tx) error {
		// Alias collisions are explicit failures, not silent first-match.
		// An alias must identify at most one session at a time.
		if peer.Alias != "" {
			var other string
			err := tx.QueryRowContext(ctx,
				`SELECT session_id FROM sessions WHERE alias = ? AND session_id != ? LIMIT 1`,
				peer.Alias, peer.SessionID).Scan(&other)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("store: alias lookup: %w", err)
			}
			if other != "" {
				return fmt.Errorf("store: alias %q already identifies session %q: %w", peer.Alias, other, ErrConflict)
			}
		}
		var existingPrincipal, existingHost string
		err := tx.QueryRowContext(ctx, `SELECT principal_id, host_id FROM sessions WHERE session_id = ?`, peer.SessionID).Scan(&existingPrincipal, &existingHost)
		switch {
		case err == nil:
			// Ownership transfer by supplying an identifier is forbidden.
			// A session may be resumed only by the principal that created
			// it AND with the client-held session secret. A different
			// non-empty host with a different principal is a takeover.
			if existingPrincipal != "" && peer.PrincipalID != "" && existingPrincipal != peer.PrincipalID {
				return fmt.Errorf("store: session %q is owned by a different principal: %w", peer.SessionID, ErrForbidden)
			}
			var storedHash string
			herr := tx.QueryRowContext(ctx, `SELECT secret_hash FROM session_secrets WHERE session_id = ?`, peer.SessionID).Scan(&storedHash)
			if herr != nil || storedHash == "" || storedHash != secretHash {
				return fmt.Errorf("store: session %q resume requires the enrolled session secret: %w", peer.SessionID, ErrForbidden)
			}
			// Preserve the owning principal/host on resume; never let a
			// registration overwrite them with caller-supplied values.
			if existingPrincipal != "" {
				peer.PrincipalID = existingPrincipal
			}
			if existingHost != "" && peer.HostID == "" {
				peer.HostID = existingHost
			}
			_, err = tx.ExecContext(ctx,
				`UPDATE sessions SET instance_id = ?, host_id = ?, principal_id = ?, alias = ?,
				        harness = ?, harness_version = ?, presence = ?, capabilities = ?, delivery_modes = ?,
				        trust_domain = ?, tags = ?,
				        updated_at = ?, last_seen = ?
				 WHERE session_id = ?`,
				peer.InstanceID, peer.HostID, peer.PrincipalID, peer.Alias, peer.Harness,
				peer.HarnessVersion, peer.Presence, string(caps), string(modes),
				peer.TrustDomain, string(tags), now, now, peer.SessionID)

			if err != nil {
				return fmt.Errorf("store: update session: %w", err)
			}
			created = false
		case errors.Is(err, sql.ErrNoRows):
			_, err = tx.ExecContext(ctx,
				`INSERT INTO sessions (session_id, instance_id, host_id, principal_id, alias, harness,
				        harness_version, state, state_source, presence, capabilities, delivery_modes,
				        created_at, updated_at, last_seen, trust_domain, tags)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				peer.SessionID, peer.InstanceID, peer.HostID, peer.PrincipalID, peer.Alias,
				peer.Harness, peer.HarnessVersion, protocol.SessionIdle, string(StateSourceInferred),
				peer.Presence, string(caps), string(modes), now, now, now, peer.TrustDomain, string(tags))
			if err != nil {
				return fmt.Errorf("store: insert session: %w", err)
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO session_secrets (session_id, secret_hash, created_at, updated_at) VALUES (?, ?, ?, ?)`,
				peer.SessionID, secretHash, now, now); err != nil {
				return fmt.Errorf("store: enroll session secret: %w", err)
			}
			created = true
		default:
			return fmt.Errorf("store: lookup session: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return created, nil
}

// VerifySessionSecret reports whether secret matches the enrolled hash.
func (s *Store) VerifySessionSecret(ctx context.Context, sessionID, secret string) (bool, error) {
	if secret == "" {
		return false, nil
	}
	var stored string
	err := s.db.QueryRowContext(ctx, `SELECT secret_hash FROM session_secrets WHERE session_id = ?`, sessionID).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: verify session secret: %w", err)
	}
	return stored != "" && stored == hashSessionSecret(secret), nil
}

// StateSource records where a session's state came from.
//
// Section 7.3 requires adapters to distinguish native session state from
// observations inferred by the adapter, so the source is stored alongside
// the state rather than being lost.
type StateSource string

const (
	// StateSourceNative means the harness reported the state itself.
	StateSourceNative StateSource = "native"
	// StateSourceInferred means the adapter inferred the state.
	StateSourceInferred StateSource = "inferred"
	// StateSourceUnknown means the origin of the state is not established.
	StateSourceUnknown StateSource = "unknown"
)

// PeerRecord is one registered session with its resolved metadata.
type PeerRecord struct {
	protocol.PeerInfo
	HarnessVersion string
	// StateSource records whether the state was reported by the harness or
	// inferred by the adapter.
	StateSource StateSource
}

func decodePeerExtras(rec *PeerRecord, caps, modes, trust, tags, lastSeen string) error {
	if err := json.Unmarshal([]byte(caps), &rec.Capabilities); err != nil {
		return fmt.Errorf("store: decode capabilities: %w", err)
	}
	if err := json.Unmarshal([]byte(modes), &rec.Delivery); err != nil {
		return fmt.Errorf("store: decode delivery modes: %w", err)
	}
	rec.TrustDomain = trust
	if rec.TrustDomain == "" {
		rec.TrustDomain = "local"
	}
	if tags != "" {
		_ = json.Unmarshal([]byte(tags), &rec.Tags)
	}
	if lastSeen != "" {
		if t, err := parseTime(lastSeen); err == nil {
			rec.LastSeen = t
		}
	}
	return nil
}

// GetSession loads one session's peer record.
func (s *Store) GetSession(ctx context.Context, sessionID string) (PeerRecord, error) {
	var rec PeerRecord
	var caps, modes, state string
	var created, updated, lastSeen string

	var trust, tags string
	row := s.db.QueryRowContext(ctx,
		`SELECT session_id, instance_id, host_id, principal_id, alias, harness, harness_version,
		        state, state_source, presence, capabilities, delivery_modes, created_at, updated_at, last_seen, trust_domain, tags
		 FROM sessions WHERE session_id = ?`, sessionID)
	var stateSource string
	err := row.Scan(&rec.SessionID, &rec.InstanceID, &rec.HostID, &rec.PrincipalID, &rec.Alias,
		&rec.Harness, &rec.HarnessVersion, &state, &stateSource, &rec.Presence, &caps, &modes,
		&created, &updated, &lastSeen, &trust, &tags)
	if errors.Is(err, sql.ErrNoRows) {
		return PeerRecord{}, ErrNotFound
	}
	if err != nil {
		return PeerRecord{}, fmt.Errorf("store: get session: %w", err)
	}
	if err := decodePeerExtras(&rec, caps, modes, trust, tags, lastSeen); err != nil {
		return PeerRecord{}, err
	}
	rec.State = protocol.SessionState(state)
	rec.StateSource = StateSource(stateSource)
	if rec.StateSource == "" {
		rec.StateSource = StateSourceUnknown
	}
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
		        state, state_source, presence, capabilities, delivery_modes, created_at, updated_at, last_seen, trust_domain, tags
		 FROM sessions WHERE principal_id = ? ORDER BY session_id ASC LIMIT ?`, principalID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list peers: %w", err)
	}
	defer rows.Close()

	var out []PeerRecord
	for rows.Next() {
		var rec PeerRecord
		var caps, modes, state, stateSource, trust, tags string
		var created, updated, lastSeen string
		if err := rows.Scan(&rec.SessionID, &rec.InstanceID, &rec.HostID, &rec.PrincipalID,
			&rec.Alias, &rec.Harness, &rec.HarnessVersion, &state, &stateSource, &rec.Presence,
			&caps, &modes, &created, &updated, &lastSeen, &trust, &tags); err != nil {
			return nil, fmt.Errorf("store: scan peer: %w", err)
		}
		rec.StateSource = StateSource(stateSource)
		if rec.StateSource == "" {
			rec.StateSource = StateSourceUnknown
		}
		if err := decodePeerExtras(&rec, caps, modes, trust, tags, lastSeen); err != nil {
			return nil, err
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
//
// The source records whether the harness reported the state or the adapter
// inferred it (ICSP-001 section 7.3).
func (s *Store) SetSessionState(ctx context.Context, sessionID string, state protocol.SessionState, source StateSource) error {
	if !state.Valid() {
		return fmt.Errorf("store: invalid session state %q", state)
	}
	if source == "" {
		source = StateSourceUnknown
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE sessions SET state = ?, state_source = ?, updated_at = ? WHERE session_id = ?`,
			state, source, time.Now().UTC().Format(TimestampLayout), sessionID)
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

// ExistingIdentity returns the host/principal identity already persisted
// in this store, if any. Host and principal identity must be stable across
// daemon restarts (ICSP-001 section 3), so a restarted daemon reuses them
// instead of minting new ones.
func (s *Store) ExistingIdentity(ctx context.Context) (hostID, principalID string, found bool, err error) {
	row := s.db.QueryRowContext(ctx, `SELECT host_id, principal_id FROM hosts ORDER BY created_at ASC LIMIT 1`)
	err = row.Scan(&hostID, &principalID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("store: existing identity: %w", err)
	}
	return hostID, principalID, true, nil
}

// ListAllPeers returns every registered session (for visibility-filtered
// discovery in the node layer; callers must not expose it unfiltered).
func (s *Store) ListAllPeers(ctx context.Context, limit int) ([]PeerRecord, error) {
	return s.ListAllPeersAfter(ctx, limit, "")
}

// ListAllPeersAfter returns registered sessions with session_id > after
// (deterministic cursor pagination), ordered by session_id.
func (s *Store) ListAllPeersAfter(ctx context.Context, limit int, after string) ([]PeerRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	query := `SELECT session_id, instance_id, host_id, principal_id, alias, harness, harness_version,
		        state, state_source, presence, capabilities, delivery_modes, created_at, updated_at, last_seen, trust_domain, tags
		        FROM sessions`
	args := []any{}
	if after != "" {
		query += ` WHERE session_id > ?`
		args = append(args, after)
	}
	query += ` ORDER BY session_id ASC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list all peers: %w", err)
	}
	defer rows.Close()
	var out []PeerRecord
	for rows.Next() {
		var rec PeerRecord
		var caps, modes, state, stateSource, trust, tags string
		var created, updated, lastSeen string
		if err := rows.Scan(&rec.SessionID, &rec.InstanceID, &rec.HostID, &rec.PrincipalID,
			&rec.Alias, &rec.Harness, &rec.HarnessVersion, &state, &stateSource, &rec.Presence,
			&caps, &modes, &created, &updated, &lastSeen, &trust, &tags); err != nil {
			return nil, fmt.Errorf("store: scan peer: %w", err)
		}
		rec.StateSource = StateSource(stateSource)
		if rec.StateSource == "" {
			rec.StateSource = StateSourceUnknown
		}
		if err := decodePeerExtras(&rec, caps, modes, trust, tags, lastSeen); err != nil {
			return nil, err
		}
		rec.State = protocol.SessionState(state)
		out = append(out, rec)
	}
	return out, rows.Err()
}

// ListByAlias returns every session carrying an alias. More than one
// result means the alias is ambiguous and must fail explicitly.
func (s *Store) ListByAlias(ctx context.Context, alias string) ([]PeerRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT session_id FROM sessions WHERE alias = ? ORDER BY session_id ASC`, alias)
	if err != nil {
		return nil, fmt.Errorf("store: list by alias: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan alias: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]PeerRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := s.GetSession(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
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
