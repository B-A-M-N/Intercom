// Package transport implements Intercom's physical transports.
//
// Intercom separates messaging semantics from physical transport
// (ICSP-001 section 10). The transport moves opaque frames; it does not
// interpret them.
package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

// ErrClosed is returned when the transport is shut down.
var ErrClosed = errors.New("transport: closed")

// Frame is one opaque protocol frame.
type Frame []byte

// Conn is one authenticated transport connection.
//
// The connection exposes the authenticated peer identity so the server can
// bind message senders to it. A sender identity MUST be established by the
// authenticated transport or trusted local adapter, never solely by a
// sender-controlled JSON field (ICSP-001 section 3).
type Conn interface {
	io.ReadWriteCloser
	// PeerIdentity returns the authenticated identity of the remote party.
	PeerIdentity() PeerIdentity
	// RemoteAddr returns the remote address description.
	RemoteAddr() net.Addr
	// SetDeadline sets the read/write deadline.
	SetDeadline(t time.Time) error
}

// PeerIdentity is the authenticated identity of a transport peer.
type PeerIdentity struct {
	// PrincipalID is the authenticated principal, when the transport can
	// establish one.
	PrincipalID string
	// PID is the operating system process id of the peer, when available.
	PID int
	// UID is the operating system user id of the peer, when available.
	UID uint32
	// GID is the operating system group id of the peer, when available.
	GID uint32
	// Authenticated reports whether the transport established a verified
	// identity. An unauthenticated connection MUST NOT be trusted to
	// establish a sender identity.
	Authenticated bool
	// Method names how the identity was established, for audit records.
	Method string
}

// Listener accepts inbound transport connections.
type Listener interface {
	// Accept waits for and returns the next connection.
	Accept(ctx context.Context) (Conn, error)
	// Addr returns the listener's address.
	Addr() net.Addr
	// Close shuts the listener down.
	Close() error
}
