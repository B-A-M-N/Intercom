// Package ids implements the Intercom identity model (ICSP-001 section 3).
//
// Four identifiers are distinguished, each with a distinct lifetime:
//
//	principalId - authenticated user/service identity; credential lifetime
//	hostId      - authenticated Intercom node; host registration lifetime
//	instanceId  - particular agent process incarnation; process lifetime
//	sessionId   - persistent agent conversation identity; session lifetime
//
// A process restart MUST create a new instanceId while preserving sessionId
// only when the underlying session is genuinely recoverable.
package ids

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Prefix constants make identifiers self-describing on the wire and in logs.
const (
	PrincipalPrefix = "prn"
	HostPrefix      = "hst"
	InstancePrefix  = "ins"
	SessionPrefix   = "ses"
	MessagePrefix   = "msg"
	EventPrefix     = "evt"
)

// ErrInvalid is returned when an identifier fails structural validation.
var ErrInvalid = errors.New("invalid identifier")

// Principal is an authenticated user or service identity.
type Principal struct {
	ID string `json:"principalId"`
}

// Host is an authenticated Intercom node.
type Host struct {
	ID string `json:"hostId"`
}

// Instance identifies one agent process incarnation.
type Instance struct {
	ID string `json:"instanceId"`
}

// Session identifies a persistent agent conversation.
type Session struct {
	ID string `json:"sessionId"`
}

// NewPrincipalID mints a fresh principal identifier.
func NewPrincipalID() (string, error) { return mint(PrincipalPrefix) }

// NewHostID mints a fresh host identifier.
func NewHostID() (string, error) { return mint(HostPrefix) }

// NewInstanceID mints a fresh instance identifier. A new instance identifier is
// minted for every process start; it is never derived from a session identifier.
func NewInstanceID() (string, error) { return mint(InstancePrefix) }

// NewSessionID mints a fresh session identifier.
func NewSessionID() (string, error) { return mint(SessionPrefix) }

// mint produces a prefixed UUIDv7. UUIDv7 is time-ordered, which keeps
// identifier allocation monotonic without a coordination round trip.
func mint(prefix string) (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("mint %s id: %w", prefix, err)
	}
	return prefix + "_" + u.String(), nil
}

// Validate checks that id is a well-formed identifier of the given kind.
func Validate(id, prefix string) error {
	if id == "" {
		return fmt.Errorf("%w: empty", ErrInvalid)
	}
	if !strings.HasPrefix(id, prefix+"_") {
		return fmt.Errorf("%w: %q does not carry %q prefix", ErrInvalid, id, prefix+"_")
	}
	raw := strings.TrimPrefix(id, prefix+"_")
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: %q is not a UUID", ErrInvalid, id)
	}
	if parsed.Version() != 7 {
		return fmt.Errorf("%w: %q is not UUIDv7", ErrInvalid, id)
	}
	return nil
}

// NewMessageID mints a fresh message identifier.
func NewMessageID() (string, error) { return mint("msg") }

// NewEventID mints a fresh event identifier.
func NewEventID() (string, error) { return mint("evt") }

// ValidatePrincipal checks a principal identifier.
func ValidatePrincipal(id string) error { return Validate(id, PrincipalPrefix) }

// ValidateHost checks a host identifier.
func ValidateHost(id string) error { return Validate(id, HostPrefix) }

// ValidateInstance checks an instance identifier.
func ValidateInstance(id string) error { return Validate(id, InstancePrefix) }

// ValidateSession checks a session identifier.
func ValidateSession(id string) error { return Validate(id, SessionPrefix) }

// ValidateMessage checks a message identifier.
func ValidateMessage(id string) error { return Validate(id, MessagePrefix) }

// ValidateEvent checks an event identifier.
func ValidateEvent(id string) error { return Validate(id, EventPrefix) }

// NewCommandID mints a fresh command identifier.
func NewCommandID() (string, error) { return mint("cmd") }
