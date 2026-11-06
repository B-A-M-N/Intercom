package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// DelegationInfo describes a verified scoped delegation.
type DelegationInfo struct {
	SessionID string
	Scope string
	Permissions []string
	OwnerInstance string
	ExpiresAt time.Time
}

func parsePermissions(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func (d DelegationInfo) Has(perm string) bool {
	if d.Scope == "mcp" {
		return true
	}
	for _, p := range d.Permissions {
		if p == perm {
			return true
		}
	}
	return false
}

// IssueDelegation mints a scoped, short-lived delegation token for an
// attached client (e.g. an embedded intercom-mcp). The caller must already
// be the session owner (resume secret verified by the node layer).
func (s *Store) IssueDelegation(ctx context.Context, sessionID, ownerInstance string, ttl time.Duration) (string, error) {
	return s.IssueDelegationWithScope(ctx, sessionID, ownerInstance, ttl, "mcp", "")
}

// IssueDelegationWithScope mints a scoped token. scope must be "mcp" or
// "surface". permissions is a comma-separated list for surface tokens
// (read, present, reply). MCP tokens carry no permissions and use the MCP
// allowlist.
func (s *Store) IssueDelegationWithScope(ctx context.Context, sessionID, ownerInstance string, ttl time.Duration, scope, permissions string) (string, error) {
	if scope == "" {
		scope = "mcp"
	}
	if scope != "mcp" && scope != "surface" {
		return "", fmt.Errorf("store: invalid delegation scope %q", scope)
	}
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	if ttl > 24*time.Hour {
		ttl = 24 * time.Hour
	}
	// ttl < 0 means already-expired: only for tests exercising expiry.
	token := generateSessionSecret()
	hash := hashSessionSecret(token)
	now := time.Now().UTC().Format(TimestampLayout)
	expires := time.Now().UTC().Add(ttl).Format(TimestampLayout)
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO session_delegations (token_hash, session_id, scope, permissions, owner_instance, created_at, expires_at, revoked) VALUES (?, ?, ?, ?, ?, ?, ?, 0)`,
		hash, sessionID, scope, permissions, ownerInstance, now, expires); err != nil {
		return "", fmt.Errorf("store: issue delegation: %w", err)
	}
	return token, nil
}

// VerifyDelegationDetailed checks session, scope, revocation, expiry (fail
// closed on malformed expiry) and current owner instance. It never modifies
// the session row.
func (s *Store) VerifyDelegationDetailed(ctx context.Context, sessionID, token string) (DelegationInfo, error) {
	if token == "" || sessionID == "" {
		return DelegationInfo{}, ErrForbidden
	}
	hash := hashSessionSecret(token)
	var sid, scope, perms, ownerInstance, expires string
	var revoked int
	err := s.db.QueryRowContext(ctx,
		`SELECT session_id, scope, COALESCE(permissions,''), owner_instance, expires_at, revoked FROM session_delegations WHERE token_hash = ?`, hash).
		Scan(&sid, &scope, &perms, &ownerInstance, &expires, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return DelegationInfo{}, ErrForbidden
	}
	if err != nil {
		return DelegationInfo{}, fmt.Errorf("store: verify delegation: %w", err)
	}
	if sid != sessionID || revoked != 0 {
		return DelegationInfo{}, ErrForbidden
	}
	if scope != "mcp" && scope != "surface" {
		return DelegationInfo{}, ErrForbidden
	}
	if ownerInstance == "" {
		return DelegationInfo{}, ErrForbidden
	}
	var currentInstance string
	if err := s.db.QueryRowContext(ctx, `SELECT instance_id FROM sessions WHERE session_id = ?`, sessionID).Scan(&currentInstance); err != nil {
		return DelegationInfo{}, ErrForbidden
	}
	if currentInstance == "" || currentInstance != ownerInstance {
		return DelegationInfo{}, ErrForbidden
	}
	expiry, perr := parseTime(expires)
	if perr != nil || expires == "" {
		return DelegationInfo{}, ErrForbidden
	}
	if time.Now().UTC().After(expiry) {
		return DelegationInfo{}, ErrForbidden
	}
	return DelegationInfo{SessionID: sid, Scope: scope, Permissions: parsePermissions(perms), OwnerInstance: ownerInstance, ExpiresAt: expiry}, nil
}

// VerifyDelegation reports whether token is a live, unexpired delegation
// for sessionID. It never modifies the session row.
func (s *Store) VerifyDelegation(ctx context.Context, sessionID, token string) error {
	_, err := s.VerifyDelegationDetailed(ctx, sessionID, token)
	return err
}

// RevokeDelegation marks a delegation token revoked. Subsequent verifies
// (including on already-established connections) fail.
func (s *Store) RevokeDelegation(ctx context.Context, token string) error {
	if token == "" {
		return ErrForbidden
	}
	hash := hashSessionSecret(token)
	res, err := s.db.ExecContext(ctx, `UPDATE session_delegations SET revoked = 1 WHERE token_hash = ?`, hash)
	if err != nil {
		return fmt.Errorf("store: revoke delegation: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrForbidden
	}
	return nil
}
