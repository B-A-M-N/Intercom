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

// Role classifies the authority a peer holds over a session
// (ICSP-001 section 11.2).
type Role string

const (
	// RoleOwner can manage a session and, subject to policy, approve
	// protected actions.
	RoleOwner Role = "owner"
	// RolePeer can submit permitted communications and retrieve explicitly
	// authorized status information.
	RolePeer Role = "peer"
)

// Valid reports whether r is a defined role.
func (r Role) Valid() bool {
	switch r {
	case RoleOwner, RolePeer:
		return true
	}
	return false
}

// GrantPermission grants a peer access to an owner's session.
//
// Revocation is a timestamp rather than a delete, so the audit trail keeps
// the fact that access existed and when it ended.
func (s *Store) GrantPermission(ctx context.Context, peerSession, ownerSession string, role Role) error {
	if !role.Valid() {
		return fmt.Errorf("store: invalid role %q", role)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		// A revoked grant is replaced rather than duplicated.
		_, err := tx.ExecContext(ctx,
			`INSERT INTO peer_permissions (peer_session, owner_session, role, granted_at, revoked_at)
			 VALUES (?, ?, ?, ?, '')
			 ON CONFLICT(peer_session, owner_session)
			 DO UPDATE SET role = excluded.role, granted_at = excluded.granted_at, revoked_at = ''`,
			peerSession, ownerSession, role, time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: grant permission: %w", err)
		}
		return nil
	})
}

// RevokePermission revokes a peer's access to an owner's session.
func (s *Store) RevokePermission(ctx context.Context, peerSession, ownerSession string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE peer_permissions SET revoked_at = ? WHERE peer_session = ? AND owner_session = ? AND revoked_at = ''`,
			time.Now().UTC().Format(TimestampLayout), peerSession, ownerSession)
		if err != nil {
			return fmt.Errorf("store: revoke permission: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// Permission is one resolved peer-to-owner authorization.
type Permission struct {
	PeerSession  string
	OwnerSession string
	Role         Role
	GrantedAt    time.Time
	RevokedAt    time.Time
}

// Revoked reports whether the grant has ended.
func (p Permission) Revoked() bool { return !p.RevokedAt.IsZero() }

// LookupPermission resolves the active grant a peer holds over an owner's
// session.
//
// A missing grant means no authority: absence is never read as permission.
func (s *Store) LookupPermission(ctx context.Context, peerSession, ownerSession string) (Permission, error) {
	var p Permission
	var role, granted, revoked string
	row := s.db.QueryRowContext(ctx,
		`SELECT peer_session, owner_session, role, granted_at, revoked_at
		 FROM peer_permissions WHERE peer_session = ? AND owner_session = ?`, peerSession, ownerSession)
	err := row.Scan(&p.PeerSession, &p.OwnerSession, &role, &granted, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return Permission{}, ErrNotFound
	}
	if err != nil {
		return Permission{}, fmt.Errorf("store: lookup permission: %w", err)
	}
	p.Role = Role(role)
	if t, err := parseTime(granted); err == nil {
		p.GrantedAt = t
	}
	if revoked != "" {
		if t, err := parseTime(revoked); err == nil {
			p.RevokedAt = t
		}
	}
	return p, nil
}

// ListPermissions returns the active grants held by a peer.
func (s *Store) ListPermissions(ctx context.Context, peerSession string) ([]Permission, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT peer_session, owner_session, role, granted_at, revoked_at
		 FROM peer_permissions WHERE peer_session = ? AND revoked_at = '' ORDER BY owner_session ASC`,
		peerSession)
	if err != nil {
		return nil, fmt.Errorf("store: list permissions: %w", err)
	}
	defer rows.Close()

	var out []Permission
	for rows.Next() {
		var p Permission
		var role, granted, revoked string
		if err := rows.Scan(&p.PeerSession, &p.OwnerSession, &role, &granted, &revoked); err != nil {
			return nil, fmt.Errorf("store: scan permission: %w", err)
		}
		p.Role = Role(role)
		if t, err := parseTime(granted); err == nil {
			p.GrantedAt = t
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SetDeliveryPolicy sets the delivery modes a recipient accepts from a
// peer.
//
// The receiving session enforces its own policy independently of the
// sender's authority (ICSP-001 section 11.3). A grant permits a submission;
// this policy decides what happens to it.
func (s *Store) SetDeliveryPolicy(ctx context.Context, ownerSession, peerSession string, modes []protocol.DeliveryMode) error {
	encoded, err := json.Marshal(modes)
	if err != nil {
		return fmt.Errorf("store: marshal delivery policy: %w", err)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO delivery_policies (owner_session, peer_session, allowed_modes, updated_at)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(owner_session, peer_session)
			 DO UPDATE SET allowed_modes = excluded.allowed_modes, updated_at = excluded.updated_at`,
			ownerSession, peerSession, string(encoded), time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: set delivery policy: %w", err)
		}
		return nil
	})
}

// DeliveryPolicy returns the delivery modes a recipient accepts from a
// peer.
//
// A peer with no explicit policy accepts every mode its grant allows. The
// policy narrows what the grant permits; it never widens it.
func (s *Store) DeliveryPolicy(ctx context.Context, ownerSession, peerSession string) ([]protocol.DeliveryMode, bool, error) {
	var encoded string
	err := s.db.QueryRowContext(ctx,
		`SELECT allowed_modes FROM delivery_policies WHERE owner_session = ? AND peer_session = ?`,
		ownerSession, peerSession).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: delivery policy: %w", err)
	}
	var modes []protocol.DeliveryMode
	if err := json.Unmarshal([]byte(encoded), &modes); err != nil {
		return nil, false, fmt.Errorf("store: decode delivery policy: %w", err)
	}
	return modes, true, nil
}

// CanSend reports whether a peer may submit messages to an owner's session.
//
// Peer authority permits communication only. It never confers approval
// authority, so there is deliberately no path from a peer grant to an
// approval decision (ICSP-001 section 11.2).
func (s *Store) CanSend(ctx context.Context, peerSession, ownerSession string) (bool, error) {
	p, err := s.LookupPermission(ctx, peerSession, ownerSession)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !p.Revoked(), nil
}

// CanApprove reports whether the acting principal holds owner authority for a
// session.
//
// Only owner authority can approve protected actions; a peer MUST NOT be
// able to use messaging to approve tool execution.
func (s *Store) CanApprove(ctx context.Context, peerSession, ownerSession string) (bool, error) {
	p, err := s.LookupPermission(ctx, peerSession, ownerSession)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if p.Revoked() {
		return false, nil
	}
	return p.Role == RoleOwner, nil
}
