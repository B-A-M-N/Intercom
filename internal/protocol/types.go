// Package protocol defines the Intercom wire contract (ICSP-001).
//
// The types here are the canonical envelopes for messages, commands, delivery
// receipts, and session events. They are transport-neutral: the local Unix
// socket transport and the ACP extension transport carry the same structures.
package protocol

import (
	"encoding/json"
	"time"
)

// Version constants for the Intercom protocol itself.
const (
	// SpecVersion identifies the ICSP revision implemented by this package.
	SpecVersion = "ICSP-001"
	// SpecRevision is the version of the ICSP specification text.
	SpecRevision = "0.1.0"
)

// DeliveryMode selects how a message should be delivered (ICSP-001 section 5.2).
type DeliveryMode string

const (
	// DeliverySteer delivers the message into the recipient's active turn at a
	// supported safe execution boundary.
	DeliverySteer DeliveryMode = "steer"
	// DeliveryQueue persists the message for the recipient's next eligible turn.
	DeliveryQueue DeliveryMode = "queue"
	// DeliveryNotify presents or records the message without injecting it into
	// the model's context.
	DeliveryNotify DeliveryMode = "notify"
)

// Valid reports whether m is one of the three defined delivery modes.
func (m DeliveryMode) Valid() bool {
	switch m {
	case DeliverySteer, DeliveryQueue, DeliveryNotify:
		return true
	}
	return false
}

// WakePolicy requests whether the recipient should be woken (ICSP-001 section 5.3).
type WakePolicy string

const (
	// WakeNone does not initiate or wake execution.
	WakeNone WakePolicy = "none"
	// WakeIfIdle attempts delivery when the recipient is idle.
	WakeIfIdle WakePolicy = "if_idle"
	// WakeNextSafePoint requests an eligible safe-point wake or steer.
	WakeNextSafePoint WakePolicy = "next_safe_point"
)

// Valid reports whether w is one of the three defined wake policies.
func (w WakePolicy) Valid() bool {
	switch w {
	case WakeNone, WakeIfIdle, WakeNextSafePoint:
		return true
	}
	return false
}

// UnsupportedPolicy decides what happens when a requested delivery feature is
// unavailable on the recipient (ICSP-001 section 5.3).
type UnsupportedPolicy string

const (
	// UnsupportedReject refuses the command rather than downgrading it.
	UnsupportedReject UnsupportedPolicy = "reject"
	// UnsupportedDowngrade accepts the command at the best available
	// delivery semantics and reports the downgrade on the receipt.
	UnsupportedDowngrade UnsupportedPolicy = "downgrade"
	// UnsupportedNotify falls back to notification-only delivery.
	UnsupportedNotify UnsupportedPolicy = "notify"
)

// Valid reports whether p is a defined unsupported-feature policy.
func (p UnsupportedPolicy) Valid() bool {
	switch p {
	case UnsupportedReject, UnsupportedDowngrade, UnsupportedNotify:
		return true
	}
	return false
}

// ReceiptStatus enumerates observable delivery outcomes (ICSP-001 section 5.4).
//
// These are observable facts, not interchangeable success states. A sender
// MUST NOT infer delivery from submission acceptance.
type ReceiptStatus string

const (
	// ReceiptAccepted means the submission was durably admitted.
	ReceiptAccepted ReceiptStatus = "accepted"
	// ReceiptDelivered means the recipient inbox durably accepted the message.
	ReceiptDelivered ReceiptStatus = "delivered"
	// ReceiptPresented means a recipient UI or notification handler presented it.
	ReceiptPresented ReceiptStatus = "presented"
	// ReceiptInjected means the adapter confirmed introduction into the
	// recipient model's input context.
	ReceiptInjected ReceiptStatus = "injected"
	// ReceiptHandled means the recipient explicitly reported handling the
	// message. It does not imply the requested task was completed correctly.
	ReceiptHandled ReceiptStatus = "handled"
	// ReceiptRejected means delivery or admission was refused.
	ReceiptRejected ReceiptStatus = "rejected"
	// ReceiptExpired means the message expired without completing delivery.
	ReceiptExpired ReceiptStatus = "expired"
	// ReceiptUnknown means the outcome cannot be established.
	ReceiptUnknown ReceiptStatus = "unknown"
)

// Valid reports whether s is a defined receipt status.
func (s ReceiptStatus) Valid() bool {
	switch s {
	case ReceiptAccepted, ReceiptDelivered, ReceiptPresented, ReceiptInjected,
		ReceiptHandled, ReceiptRejected, ReceiptExpired, ReceiptUnknown:
		return true
	}
	return false
}

// Terminal reports whether the status is a final disposition for the message.
// presented and injected are explicitly non-terminal: a message may be
// presented without ever being injected.
func (s ReceiptStatus) Terminal() bool {
	switch s {
	case ReceiptDelivered, ReceiptHandled, ReceiptRejected, ReceiptExpired:
		return true
	}
	return false
}

// SessionState enumerates normalized session states (ICSP-001 section 7.3).
type SessionState string

const (
	SessionCreated         SessionState = "created"
	SessionIdle            SessionState = "idle"
	SessionRunning         SessionState = "running"
	SessionWaitingApproval SessionState = "waiting_approval"
	SessionInterrupted     SessionState = "interrupted"
	SessionEnded           SessionState = "ended"
	SessionUnavailable     SessionState = "unavailable"
	SessionUnknown         SessionState = "unknown"
)

// Valid reports whether s is a defined session state.
func (s SessionState) Valid() bool {
	switch s {
	case SessionCreated, SessionIdle, SessionRunning, SessionWaitingApproval,
		SessionInterrupted, SessionEnded, SessionUnavailable, SessionUnknown:
		return true
	}
	return false
}

// PresenceState enumerates peer presence states (ICSP-001 section 4).
//
// "disconnected" describes loss of transport or heartbeat, not necessarily
// agent termination.
type PresenceState string

const (
	PresenceOnline       PresenceState = "online"
	PresenceBusy         PresenceState = "busy"
	PresenceIdle         PresenceState = "idle"
	PresenceDisconnected PresenceState = "disconnected"
	PresenceUnknown      PresenceState = "unknown"
)

// Valid reports whether p is a defined presence state.
func (p PresenceState) Valid() bool {
	switch p {
	case PresenceOnline, PresenceBusy, PresenceIdle, PresenceDisconnected, PresenceUnknown:
		return true
	}
	return false
}

// CommandState enumerates the command lifecycle (ICSP-001 section 6.1).
type CommandState string

const (
	// CommandReceived is the initial intake state.
	CommandReceived CommandState = "received"
	// CommandAccepted means the command was durably admitted.
	CommandAccepted CommandState = "accepted"
	// CommandExecuting means the command has begun executing.
	CommandExecuting CommandState = "executing"
	// CommandSettled means the command reached a terminal disposition.
	CommandSettled CommandState = "settled"
	// CommandRejected means the command was refused at admission.
	CommandRejected CommandState = "rejected"
	// CommandAbandoned means the command's outcome could not be established
	// during recovery. It is deliberately not reported as success.
	CommandAbandoned CommandState = "abandoned"
)

// Valid reports whether s is a defined command state.
func (s CommandState) Valid() bool {
	switch s {
	case CommandReceived, CommandAccepted, CommandExecuting, CommandSettled,
		CommandRejected, CommandAbandoned:
		return true
	}
	return false
}

// CapabilityID names an Intercom capability (ICSP-001 section 8.3).
type CapabilityID string

const (
	CapPeerDiscovery  CapabilityID = "peer.discovery"
	CapPeerSend       CapabilityID = "peer.send"
	CapPeerReply      CapabilityID = "peer.reply"
	CapDeliveryNotify CapabilityID = "delivery.notify"
	CapDeliveryQueue  CapabilityID = "delivery.queue"
	CapDeliverySteer  CapabilityID = "delivery.steer"
	CapDeliveryWake   CapabilityID = "delivery.wake"
	CapSessionObserve CapabilityID = "session.observe"
	CapSessionReplay  CapabilityID = "session.replay"
	CapCommandDurable CapabilityID = "command.durable"
	CapApprovalOwner  CapabilityID = "approval.owner_control"
)

// AllCapabilities lists every capability identifier defined by the spec.
func AllCapabilities() []CapabilityID {
	return []CapabilityID{
		CapPeerDiscovery, CapPeerSend, CapPeerReply,
		CapDeliveryNotify, CapDeliveryQueue, CapDeliverySteer, CapDeliveryWake,
		CapSessionObserve, CapSessionReplay, CapCommandDurable, CapApprovalOwner,
	}
}

// SupportLevel classifies how completely a capability is implemented.
//
// An adapter MUST NOT advertise functionality it cannot implement, and
// unsupported behavior MUST NOT be silently simulated with a semantically
// different operation (ICSP-001 sections 1.1 and 12).
type SupportLevel string

const (
	// SupportNative means the underlying harness directly exposes the capability.
	SupportNative SupportLevel = "native"
	// SupportEmulated means an adapter implements an equivalent documented
	// behavior without misrepresenting its limitations.
	SupportEmulated SupportLevel = "emulated"
	// SupportUnsupported means no safe implementation is available.
	SupportUnsupported SupportLevel = "unsupported"
)

// Valid reports whether l is a defined support level.
func (l SupportLevel) Valid() bool {
	switch l {
	case SupportNative, SupportEmulated, SupportUnsupported:
		return true
	}
	return false
}

// Capability describes one advertised capability and its limitations.
type Capability struct {
	ID          CapabilityID `json:"id"`
	Support     SupportLevel `json:"support"`
	Limitations []string     `json:"limitations,omitempty"`
}

// MessageBody is the typed content of a message (ICSP-001 section 5.1).
//
// Message payloads SHALL be treated as untrusted peer-supplied data, never
// privileged system or user instructions.
type MessageBody struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`

	// Data carries structured payloads for non-text body types.
	Data json.RawMessage `json:"data,omitempty"`
}

// Delivery describes the requested delivery semantics of a message.
type Delivery struct {
	Mode          DeliveryMode      `json:"mode"`
	Wake          WakePolicy        `json:"wake,omitempty"`
	OnUnsupported UnsupportedPolicy `json:"onUnsupported,omitempty"`
}

// Message is the canonical Intercom message envelope (ICSP-001 section 5.1).
type Message struct {
	MessageID     string         `json:"messageId"`
	CommandID     string         `json:"commandId"`
	From          string         `json:"from"`
	To            string         `json:"to"`
	Body          MessageBody    `json:"body"`
	Delivery      Delivery       `json:"delivery"`
	ReplyTo       string         `json:"replyTo,omitempty"`
	CorrelationID string         `json:"correlationId,omitempty"`
	CreatedAt     time.Time      `json:"createdAt"`
	ExpiresAt     *time.Time     `json:"expiresAt,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

// Receipt is a delivery receipt (ICSP-001 section 5.4).
type Receipt struct {
	MessageID  string        `json:"messageId"`
	CommandID  string        `json:"commandId"`
	Status     ReceiptStatus `json:"status"`
	From       string        `json:"from"`
	To         string        `json:"to"`
	ObservedAt time.Time     `json:"observedAt"`
	Detail     string        `json:"detail,omitempty"`
}

// EventEnvelope is one entry in a session event journal (ICSP-001 section 7.1).
type EventEnvelope struct {
	EventID     string          `json:"eventId"`
	SessionID   string          `json:"sessionId"`
	Sequence    uint64          `json:"sequence"`
	EventType   string          `json:"eventType"`
	Timestamp   time.Time       `json:"timestamp"`
	CausationID string          `json:"causationId,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

// Event types emitted by the reference implementation.
const (
	EventSessionCreated  = "session.created"
	EventSessionState    = "session.state"
	EventMessageAccepted = "message.accepted"
	EventMessageReceipt  = "message.receipt"
	EventCommandAccepted = "command.accepted"
	EventCommandSettled  = "command.settled"
)

// PeerInfo describes one visible peer (ICSP-001 section 4).
type PeerInfo struct {
	SessionID      string         `json:"sessionId"`
	Alias          string         `json:"alias,omitempty"`
	Harness        string         `json:"harness"`
	HarnessVersion string         `json:"harnessVersion,omitempty"`
	HostID         string         `json:"hostId"`
	InstanceID     string         `json:"instanceId"`
	PrincipalID    string         `json:"principalId"`
	Delivery       []DeliveryMode `json:"deliveryModes"`
	Capabilities   []Capability   `json:"capabilities"`
	Presence       PresenceState  `json:"presence"`
	State          SessionState   `json:"state,omitempty"`
	LastSeen       time.Time      `json:"lastSeen"`
}

// Snapshot is a coherent state projection of a session (ICSP-001 section 7.2).
type Snapshot struct {
	SessionID string       `json:"sessionId"`
	State     SessionState `json:"state"`
	Sequence  uint64       `json:"sequence"`
	UpdatedAt time.Time    `json:"updatedAt"`
	Pending   int          `json:"pendingMessages"`
	Recovery  string       `json:"recovery"`
}

// RecoveryClassification describes how a session's state was reconstructed
// after an interruption (ICSP-001 section 7.2).
type RecoveryClassification string

const (
	// RecoveryClean means the session state was reconstructed without incident.
	RecoveryClean RecoveryClassification = "clean"
	// RecoveryReplayed means state was rebuilt from a durable event journal.
	RecoveryReplayed RecoveryClassification = "replayed"
	// RecoveryInterrupted means the session was found mid-flight and is not
	// safely resumable.
	RecoveryInterrupted RecoveryClassification = "interrupted"
	// RecoveryUnknown means the session state could not be established.
	RecoveryUnknown RecoveryClassification = "unknown"
)
