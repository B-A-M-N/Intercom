package node

import (
	"context"
	"errors"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

// PermissionRecord is one resolved peer-to-owner authorization.
type PermissionRecord struct {
	PeerSession  string `json:"peerSession"`
	OwnerSession string `json:"ownerSession"`
	Role         string `json:"role"`
	Revoked      bool   `json:"revoked"`
}

// GrantPermission grants a peer access to an owner's session.
//
// Only the owner may change permissions. A peer MUST NOT be able to modify
// another session's permission policy (ICSP-001 section 11.2), so the
// caller must hold owner authority over the target session.
//
// The role is explicit because owner authority and peer authority are
// different things. A peer grant permits communication only; it never
// confers approval authority.
func (n *Node) GrantPermission(ctx context.Context, actor Sender, ownerSession, peerSession string, role store.Role) error {
	if role != store.RolePeer && role != store.RoleOwner {
		return protocol.Errorf(protocol.CodeInvalidParams, "invalid role %q", role)
	}
	if ownerSession == "" || peerSession == "" {
		return protocol.Errorf(protocol.CodeInvalidParams, "ownerSession and peerSession are required")
	}

	// The actor must hold owner authority over the target session. Granting
	// owner authority requires already holding it.
	allowed, err := n.holdsOwnerAuthority(ctx, actor, ownerSession)
	if err != nil {
		return err
	}
	if !allowed {
		return protocol.Errorf(protocol.CodeForbidden,
			"session %q does not hold owner authority over %q", actor.SessionID, ownerSession)
	}

	return n.GrantPermissionWithExpiry(ctx, actor, ownerSession, peerSession, role, time.Time{})
}

// GrantPermissionWithExpiry grants access until expires (zero = no expiry).
func (n *Node) GrantPermissionWithExpiry(ctx context.Context, actor Sender, ownerSession, peerSession string, role store.Role, expires time.Time) error {
	if role != store.RolePeer && role != store.RoleOwner {
		return protocol.Errorf(protocol.CodeInvalidParams, "invalid role %q", role)
	}
	if ownerSession == "" || peerSession == "" {
		return protocol.Errorf(protocol.CodeInvalidParams, "ownerSession and peerSession are required")
	}
	allowed, err := n.holdsOwnerAuthority(ctx, actor, ownerSession)
	if err != nil {
		return err
	}
	if !allowed {
		return protocol.Errorf(protocol.CodeForbidden,
			"session %q does not hold owner authority over %q", actor.SessionID, ownerSession)
	}
	if err := n.store.GrantPermissionWithExpiry(ctx, peerSession, ownerSession, role, expires); err != nil {
		return err
	}
	detail := string(role) + " over " + ownerSession
	if !expires.IsZero() {
		detail += " until " + expires.UTC().Format(store.TimestampLayout)
	}
	return n.store.Audit(ctx, actor.PrincipalID, peerSession, "permission.granted", detail)
}

// RevokePermission revokes a peer's access to an owner's session.
//
// Only the owner may revoke. Revocation is recorded rather than deleted, so
// the audit trail keeps the fact that access existed.
func (n *Node) RevokePermission(ctx context.Context, actor Sender, ownerSession, peerSession string) error {
	if ownerSession == "" || peerSession == "" {
		return protocol.Errorf(protocol.CodeInvalidParams, "ownerSession and peerSession are required")
	}

	allowed, err := n.holdsOwnerAuthority(ctx, actor, ownerSession)
	if err != nil {
		return err
	}
	if !allowed {
		return protocol.Errorf(protocol.CodeForbidden,
			"session %q does not hold owner authority over %q", actor.SessionID, ownerSession)
	}

	if err := n.store.RevokePermission(ctx, peerSession, ownerSession); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Errorf(protocol.CodeInvalidParams, "no active grant to revoke")
		}
		return err
	}
	return n.store.Audit(ctx, actor.PrincipalID, peerSession, "permission.revoked", ownerSession)
}

// SetDeliveryPolicy sets the delivery modes a recipient accepts from a peer.
//
// Only the owner may set the policy. A peer MUST NOT be able to modify
// another session's permission policy (ICSP-001 section 11.2).
func (n *Node) SetDeliveryPolicy(ctx context.Context, actor Sender, ownerSession, peerSession string, modes []protocol.DeliveryMode) error {
	if ownerSession == "" || peerSession == "" {
		return protocol.Errorf(protocol.CodeInvalidParams, "ownerSession and peerSession are required")
	}
	for _, m := range modes {
		if !m.Valid() {
			return protocol.Errorf(protocol.CodeInvalidParams, "invalid delivery mode %q", m)
		}
	}
	allowed, err := n.holdsOwnerAuthority(ctx, actor, ownerSession)
	if err != nil {
		return err
	}
	if !allowed {
		return protocol.Errorf(protocol.CodeForbidden,
			"session %q does not hold owner authority over %q", actor.SessionID, ownerSession)
	}
	if err := n.store.SetDeliveryPolicy(ctx, ownerSession, peerSession, modes); err != nil {
		return err
	}
	return n.store.Audit(ctx, actor.PrincipalID, peerSession, "policy.set", ownerSession)
}

// ListPermissions returns the active grants held by a peer.
func (n *Node) ListPermissions(ctx context.Context, peerSession string) ([]PermissionRecord, error) {
	perms, err := n.store.ListPermissions(ctx, peerSession)
	if err != nil {
		return nil, err
	}
	out := make([]PermissionRecord, 0, len(perms))
	for _, p := range perms {
		out = append(out, PermissionRecord{
			PeerSession:  p.PeerSession,
			OwnerSession: p.OwnerSession,
			Role:         string(p.Role),
			Revoked:      p.Revoked(),
		})
	}
	return out, nil
}

// holdsOwnerAuthority reports whether the actor holds owner authority over a
// session.
//
// Authority over a session is per session, not per principal. A session
// always owns itself. Any authority over a different session comes from an
// explicit grant, and only an owner-role grant carries owner authority.
//
// This is what stops a peer from managing permissions. Two sessions that
// share a principal are peers for communication purposes, but one of them
// still cannot reconfigure the other's access: peer communication MUST NOT
// grant owner authority, and a peer MUST NOT be able to modify another
// session's permission policy (ICSP-001 section 11.2).
func (n *Node) holdsOwnerAuthority(ctx context.Context, actor Sender, ownerSession string) (bool, error) {
	if actor.PrincipalID == "" || actor.SessionID == "" {
		return false, nil
	}
	// A session always owns itself.
	if actor.SessionID == ownerSession {
		return true, nil
	}
	return n.store.CanApprove(ctx, actor.SessionID, ownerSession)
}

// ApproveFor reports whether an actor may approve a protected action for a
// session.
//
// This is the owner-authority check that peer messages can never satisfy
// (ICSP-001 section 11.2). A peer may not approve tool execution, modify
// another session's permission policy, or forge human consent.
func (n *Node) ApproveFor(ctx context.Context, actor Sender, ownerSession string) (bool, error) {
	return n.holdsOwnerAuthority(ctx, actor, ownerSession)
}
