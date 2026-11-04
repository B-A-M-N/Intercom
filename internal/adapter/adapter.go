// Package adapter defines the contract between Intercom and agent harnesses.
//
// Every adapter exposes harness identity, supported delivery modes, native
// versus emulated capability classification, and known limitations
// (ICSP-001 section 12). An adapter MUST NOT advertise functionality it
// cannot implement.
package adapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/bamn/intercom/internal/protocol"
)

// ErrUnsupported is returned when a harness genuinely cannot perform a
// requested operation. Unsupported behavior MUST NOT be silently simulated
// with a semantically different operation.
var ErrUnsupported = errors.New("adapter: unsupported")

// ErrNotReady is returned when the harness cannot accept work right now.
var ErrNotReady = errors.New("adapter: not ready")

// SessionState reports the harness's own view of a session.
type SessionState struct {
	// State is the normalized Intercom session state.
	State protocol.SessionState
	// TurnID identifies the active turn, when one is running.
	TurnID string
	// SafePoint indicates whether the harness is currently at a boundary
	// where a steer can be applied safely.
	SafePoint bool
	// Detail carries a human-readable explanation of the reported state.
	Detail string
}

// DeliveryResult is the outcome of an adapter delivery attempt.
//
// The values distinguish the observable facts required by ICSP-001
// section 5.4. In particular, Presented and Injected are separate: a
// notification surface may present a message that is never injected into the
// model's context.
type DeliveryResult struct {
	// Status is the observed receipt status.
	Status protocol.ReceiptStatus
	// Detail explains the outcome.
	Detail string
}

// Adapter is one agent harness integration.
type Adapter interface {
	// Identity names the harness and version.
	Identity() (harness string, version string)

	// Capabilities returns the effective capability set with honest support
	// levels and limitations.
	Capabilities() []protocol.Capability

	// SupportedDeliveryModes reports which delivery modes the harness
	// genuinely supports.
	SupportedDeliveryModes() []protocol.DeliveryMode

	// State reports the harness's current view of a session.
	State(ctx context.Context, sessionID string) (SessionState, error)

	// Deliver attempts to deliver a message to a session.
	//
	// The adapter must report what actually happened. An adapter that cannot
	// accept a message during an active turn MUST NOT report a newly
	// launched prompt as successful active-turn steering.
	Deliver(ctx context.Context, sessionID string, msg protocol.Message) (DeliveryResult, error)

	// Notify presents or records a message without injecting its contents
	// into the model's context.
	Notify(ctx context.Context, sessionID string, msg protocol.Message) (DeliveryResult, error)

	// Wake initiates or wakes the recipient's execution so that it acts on a
	// message that has already been delivered.
	//
	// Wake is separate from delivery: the delivery mode decides what
	// happens to the message, and the wake policy decides whether the
	// recipient is started or woken (ICSP-001 section 5.3). An adapter that
	// cannot wake its harness must return ErrUnsupported rather than
	// pretending that a delivery was a wake.
	Wake(ctx context.Context, sessionID string, msg protocol.Message) (DeliveryResult, error)
}

// Delivery modes a harness can claim. These map directly onto the support
// levels required by ICSP-001 section 12.
const (
	// ModeNative means the harness directly exposes the behavior.
	ModeNative = "native"
	// ModeEmulated means the adapter implements an equivalent documented
	// behavior without misrepresenting its limitations.
	ModeEmulated = "emulated"
	// ModeUnsupported means no safe implementation is available.
	ModeUnsupported = "unsupported"
)

// CapabilityMatrix records, per delivery mode, how a harness supports it.
//
// This is the mechanism that keeps an adapter honest: a mode that is not
// listed, or listed as unsupported, must be refused or downgraded rather
// than simulated.
type CapabilityMatrix struct {
	// Modes maps a delivery mode to its support classification.
	Modes map[protocol.DeliveryMode]string
	// Limitations explains the boundaries of the emulated modes.
	Limitations []string
}

// Supports reports whether a delivery mode is available and how.
func (m CapabilityMatrix) Supports(mode protocol.DeliveryMode) (string, bool) {
	level, ok := m.Modes[mode]
	if !ok || level == ModeUnsupported {
		return ModeUnsupported, false
	}
	return level, true
}

// Native reports whether a delivery mode is natively supported.
func (m CapabilityMatrix) Native(mode protocol.DeliveryMode) bool {
	level, ok := m.Modes[mode]
	return ok && level == ModeNative
}

// Emulated reports whether a delivery mode is emulated.
func (m CapabilityMatrix) Emulated(mode protocol.DeliveryMode) bool {
	level, ok := m.Modes[mode]
	return ok && level == ModeEmulated
}

// DowngradeTo returns the best available substitute for a requested mode.
//
// The substitute is chosen by fidelity: steering degrades to queueing, and
// queueing degrades to notification. Notification is the floor because it
// never claims model attention.
func (m CapabilityMatrix) DowngradeTo(requested protocol.DeliveryMode) (protocol.DeliveryMode, bool) {
	if _, ok := m.Supports(requested); ok {
		return requested, true
	}
	switch requested {
	case protocol.DeliverySteer:
		if _, ok := m.Supports(protocol.DeliveryQueue); ok {
			return protocol.DeliveryQueue, true
		}
		if _, ok := m.Supports(protocol.DeliveryNotify); ok {
			return protocol.DeliveryNotify, true
		}
	case protocol.DeliveryQueue:
		if _, ok := m.Supports(protocol.DeliveryNotify); ok {
			return protocol.DeliveryNotify, true
		}
	}
	return requested, false
}

// FormatDowngrade describes a downgrade for a receipt detail.
func FormatDowngrade(requested, actual protocol.DeliveryMode) string {
	return fmt.Sprintf("delivery mode %q unsupported by recipient harness; downgraded to %q", requested, actual)
}
