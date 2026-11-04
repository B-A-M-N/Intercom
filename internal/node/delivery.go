package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bamn/intercom/internal/adapter"
	"github.com/bamn/intercom/internal/ids"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
)

// Sender is the authenticated identity of a message sender.
//
// The sender identity MUST be established by the authenticated transport or
// a trusted local adapter, never solely by a sender-controlled JSON field
// (ICSP-001 section 3). Both fields are therefore supplied by the caller of
// the transport, not by the request payload.
type Sender struct {
	// PrincipalID is the authenticated principal.
	PrincipalID string
	// SessionID is the sender's own session identity.
	SessionID string
}

// SendRequest is the parameters of a peer message submission
// (ICSP-001 section 5.1).
type SendRequest struct {
	// CommandID is the idempotency identity of the submission.
	CommandID string
	// To is the canonical recipient session identity.
	To string
	// Body is the typed message content.
	Body protocol.MessageBody
	// Delivery requests the delivery semantics.
	Delivery protocol.Delivery
	// ReplyTo is an optional parent message identity.
	ReplyTo string
	// CorrelationID is an optional conversation or task correlation identity.
	CorrelationID string
	// TTLSeconds bounds how long the message remains deliverable.
	TTLSeconds int
	// Metadata is namespaced optional metadata.
	Metadata map[string]any
}

// SendResult reports the admission outcome of a message submission.
type SendResult struct {
	// CommandID echoes the submission's idempotency identity.
	CommandID string
	// MessageID is the assigned message identity.
	MessageID string
	// Status is the admission status.
	Status protocol.ReceiptStatus
	// Replayed is true when the command had already been admitted and the
	// previously established outcome was returned.
	Replayed bool
	// Detail explains the outcome.
	Detail string
}

// Send submits a peer message.
//
// The ordering of the guarantees matters and is enforced here:
//
//  1. The command is durably admitted before any delivery is attempted.
//  2. An identical retry returns the previously established outcome.
//  3. A conflicting reuse of the command id is rejected.
//  4. The acknowledgement reports admission, not delivery completion.
//
// The acknowledgement returned by Send therefore reports "accepted" even
// when the subsequent delivery attempt fails; the failure is reported
// through the receipt trail instead.
func (n *Node) Send(ctx context.Context, from Sender, req SendRequest) (SendResult, error) {
	if req.CommandID == "" {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "commandId is required")
	}
	if req.To == "" {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "to is required")
	}
	if !req.Delivery.Mode.Valid() {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "unsupported delivery mode %q", req.Delivery.Mode)
	}
	if req.Delivery.Wake == "" {
		req.Delivery.Wake = protocol.WakeNone
	}
	if !req.Delivery.Wake.Valid() {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "unsupported wake policy %q", req.Delivery.Wake)
	}
	if req.Delivery.OnUnsupported == "" {
		req.Delivery.OnUnsupported = protocol.UnsupportedReject
	}
	if !req.Delivery.OnUnsupported.Valid() {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "unsupported onUnsupported policy %q", req.Delivery.OnUnsupported)
	}

	fromPrincipal := from.PrincipalID
	if fromPrincipal == "" {
		return SendResult{}, protocol.Errorf(protocol.CodeUnauthorized,
			"the sender identity could not be established by the transport")
	}
	if from.SessionID == "" {
		return SendResult{}, protocol.Errorf(protocol.CodeUnauthorized,
			"the sender session identity could not be established by the transport")
	}

	// Enforce the message size bound before admitting anything.
	size := estimateSize(req.Body)
	if size > n.cfg.Limits.MaxMessageBytes {
		return SendResult{}, protocol.Errorf(protocol.CodeLimitExceeded,
			"message of %d bytes exceeds the %d byte limit", size, n.cfg.Limits.MaxMessageBytes)
	}

	// Rate limit per peer before touching durable state.
	if err := n.checkRate(ctx, fromPrincipal); err != nil {
		return SendResult{}, err
	}

	// Resolve the recipient. Resolution failures are authorization failures:
	// an unauthorized peer must not be able to learn whether a session exists.
	target, err := n.store.ResolveSession(ctx, req.To)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return SendResult{}, protocol.Errorf(protocol.CodeForbidden, "recipient is not reachable from this principal")
		}
		return SendResult{}, err
	}

	// Authorize the submission. Peer authority permits communication only.
	if err := n.authorizeSend(ctx, from, target.SessionID); err != nil {
		return SendResult{}, err
	}

	// Enforce the recipient inbox bound.
	pending, err := n.store.CountPendingInbox(ctx, target.SessionID)
	if err != nil {
		return SendResult{}, err
	}
	if pending >= n.cfg.Limits.MaxInboxMessages {
		return SendResult{}, protocol.Errorf(protocol.CodeLimitExceeded,
			"recipient inbox holds %d undelivered messages, at the configured limit", pending)
	}

	// Compute the fingerprint before admitting, so an identical retry is
	// recognised and a conflicting one is rejected.
	fingerprint, err := store.Fingerprint("message/send", req)
	if err != nil {
		return SendResult{}, err
	}

	messageID, err := ids.NewMessageID()
	if err != nil {
		return SendResult{}, err
	}

	created := time.Now().UTC()
	msg := protocol.Message{
		MessageID:     messageID,
		CommandID:     req.CommandID,
		From:          from.SessionID,
		To:            target.SessionID,
		Body:          req.Body,
		Delivery:      req.Delivery,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		CreatedAt:     created,
		Metadata:      req.Metadata,
	}
	// A non-positive TTL means the message is already expired and must never
	// be injected into agent context.
	if req.TTLSeconds != 0 {
		exp := created.Add(time.Duration(req.TTLSeconds) * time.Second)
		msg.ExpiresAt = &exp
	}

	// Durable admission. A replayed command keeps its original message id.
	cmdRec := store.CommandRecord{
		CommandID:     req.CommandID,
		PrincipalID:   fromPrincipal,
		TargetSession: target.SessionID,
		Operation:     "message/send",
		State:         protocol.CommandAccepted,
	}
	admitted, replayed, err := n.store.AdmitCommand(ctx, cmdRec, fingerprint)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return SendResult{}, protocol.Errorf(protocol.CodeConflict,
				"commandId %q was already used with a different request", req.CommandID)
		}
		return SendResult{}, err
	}

	if replayed {
		// Return the previously established admission outcome without
		// re-admitting or re-delivering.
		existing, err := n.store.GetMessage(ctx, admitted.MessageID)
		if err == nil {
			return SendResult{
				CommandID: admitted.CommandID,
				MessageID: admitted.MessageID,
				Status:    protocol.ReceiptAccepted,
				Replayed:  true,
				Detail:    "command already admitted; original outcome returned",
			}, nil
		}
		_ = existing
		return SendResult{
			CommandID: admitted.CommandID,
			MessageID: admitted.MessageID,
			Status:    protocol.ReceiptAccepted,
			Replayed:  true,
			Detail:    "command already admitted; original outcome returned",
		}, nil
	}

	// The message already carries the transport-authenticated sender; store
	// it atomically with its acceptance receipt.
	stored := msg
	if err := n.store.StoreMessage(ctx, stored, fromPrincipal); err != nil {
		return SendResult{}, err
	}
	// Link the command to the message it admitted, so an idempotent retry
	// resolves to the message id that was originally assigned.
	if err := n.store.SetCommandMessage(ctx, req.CommandID, stored.MessageID); err != nil {
		return SendResult{}, err
	}
	if err := n.store.SettleCommand(ctx, req.CommandID, protocol.CommandSettled, "accepted"); err != nil {
		return SendResult{}, err
	}

	// Journal the acceptance so the recipient's session history records it.
	if _, err := n.store.AppendEvent(ctx, protocol.EventEnvelope{
		EventID:   mustID("evt"),
		SessionID: target.SessionID,
		EventType: protocol.EventMessageAccepted,
		Timestamp: created,
		Payload:   mustJSON(map[string]any{"messageId": stored.MessageID, "commandId": req.CommandID}),
	}, req.CommandID); err != nil {
		return SendResult{}, err
	}

	// Delivery is attempted after durable admission. A delivery failure does
	// not undo the admission; it is reported through the receipt trail.
	go n.deliver(ctx, stored, req.Delivery)

	return SendResult{
		CommandID: req.CommandID,
		MessageID: stored.MessageID,
		Status:    protocol.ReceiptAccepted,
		Detail:    "durably admitted",
	}, nil
}

// deliver attempts delivery and records the resulting receipts.
func (n *Node) deliver(ctx context.Context, msg protocol.Message, requested protocol.Delivery) {
	// An expired message MUST NOT be newly injected into agent context.
	if msg.ExpiresAt != nil && time.Now().UTC().After(*msg.ExpiresAt) {
		n.recordReceipt(ctx, msg, protocol.ReceiptExpired, "message expired before delivery was attempted")
		_ = n.store.SetInboxState(ctx, msg.MessageID, store.InboxVoided)
		return
	}

	ad, ok := n.Adapter(msg.To)
	if !ok {
		// The recipient has no live adapter. The durable inbox still holds
		// the message, and the outcome is unknown rather than delivered.
		n.recordReceipt(ctx, msg, protocol.ReceiptUnknown, "recipient adapter is not connected; message remains in the durable inbox")
		return
	}

	// Resolve the effective mode against the recipient's honest capability
	// matrix.
	effective := requested.Mode
	matrix := capabilityMatrix(ad)
	if level, supported := matrix.Supports(requested.Mode); !supported {
		switch requested.OnUnsupported {
		case protocol.UnsupportedDowngrade, protocol.UnsupportedNotify:
			downgraded, ok := matrix.DowngradeTo(requested.Mode)
			if !ok {
				n.recordReceipt(ctx, msg, protocol.ReceiptRejected,
					fmt.Sprintf("delivery mode %q is unsupported by the recipient harness", requested.Mode))
				return
			}
			effective = downgraded
			_ = level
			n.recordReceipt(ctx, msg, protocol.ReceiptAccepted,
				adapter.FormatDowngrade(requested.Mode, downgraded))
		default:
			n.recordReceipt(ctx, msg, protocol.ReceiptRejected,
				fmt.Sprintf("delivery mode %q is unsupported by the recipient harness", requested.Mode))
			return
		}
	}

	switch effective {
	case protocol.DeliveryNotify:
		res, err := ad.Notify(ctx, msg.To, msg)
		if err != nil {
			n.recordReceipt(ctx, msg, protocol.ReceiptUnknown, err.Error())
			return
		}
		n.recordReceipt(ctx, msg, res.Status, res.Detail)
	case protocol.DeliveryQueue, protocol.DeliverySteer:
		res, err := ad.Deliver(ctx, msg.To, msg)
		if err != nil {
			if errors.Is(err, adapter.ErrUnsupported) {
				n.recordReceipt(ctx, msg, protocol.ReceiptRejected, err.Error())
				return
			}
			n.recordReceipt(ctx, msg, protocol.ReceiptUnknown, err.Error())
			return
		}
		n.recordReceipt(ctx, msg, res.Status, res.Detail)
	default:
		n.recordReceipt(ctx, msg, protocol.ReceiptRejected, "no delivery path selected")
	}
}

// recordReceipt persists a receipt and notifies observers.
func (n *Node) recordReceipt(ctx context.Context, msg protocol.Message, status protocol.ReceiptStatus, detail string) {
	receipt := protocol.Receipt{
		MessageID:  msg.MessageID,
		CommandID:  msg.CommandID,
		Status:     status,
		From:       msg.From,
		To:         msg.To,
		ObservedAt: time.Now().UTC(),
		Detail:     detail,
	}
	if err := n.store.RecordReceipt(ctx, receipt); err != nil {
		return
	}
	if status == protocol.ReceiptDelivered {
		_ = n.store.SetInboxState(ctx, msg.MessageID, store.InboxDelivered)
	}
	if _, err := n.store.AppendEvent(ctx, protocol.EventEnvelope{
		EventID:   mustID("evt"),
		SessionID: msg.To,
		EventType: protocol.EventMessageReceipt,
		Timestamp: receipt.ObservedAt,
		Payload:   mustJSON(receipt),
	}, msg.CommandID); err != nil {
		return
	}
	n.notifyObservers(msg.To, receipt)
}

// capabilityMatrix derives a delivery capability matrix from an adapter.
func capabilityMatrix(ad adapter.Adapter) adapter.CapabilityMatrix {
	matrix := adapter.CapabilityMatrix{Modes: map[protocol.DeliveryMode]string{}}
	for _, mode := range ad.SupportedDeliveryModes() {
		matrix.Modes[mode] = adapter.ModeNative
	}
	return matrix
}

// checkRate enforces the per-peer send rate limit.
func (n *Node) checkRate(ctx context.Context, peer string) error {
	if peer == "" {
		return nil
	}
	limit := n.cfg.Limits.PerPeerRatePerMinute
	if limit <= 0 {
		return nil
	}
	now := time.Now()

	n.rateMu.Lock()
	defer n.rateMu.Unlock()

	cutoff := now.Add(-time.Minute)
	kept := n.rateSeen[peer][:0]
	for _, t := range n.rateSeen[peer] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= limit {
		n.rateSeen[peer] = kept
		return protocol.Errorf(protocol.CodeLimitExceeded,
			"peer exceeded the configured rate of %d messages per minute", limit)
	}
	n.rateSeen[peer] = append(kept, now)
	return nil
}

// authorizeSend checks that a sender may send to a session.
//
// Grants are keyed by the sending session, because the grant records which
// peer was given access. The principal is checked as well: a session that
// belongs to this host's own principal acts as the host owner.
//
// Peer authority permits communication only. It never confers approval
// authority, so there is no path from a send grant to an approval decision.
func (n *Node) authorizeSend(ctx context.Context, from Sender, targetSession string) error {
	if from.PrincipalID == n.principalID {
		return nil
	}
	allowed, err := n.store.CanSend(ctx, from.SessionID, targetSession)
	if err != nil {
		return err
	}
	if !allowed {
		return protocol.Errorf(protocol.CodeForbidden,
			"session %q is not authorized to send to session %q", from.SessionID, targetSession)
	}
	return nil
}

// Receipts returns every receipt recorded for a message.
func (n *Node) Receipts(ctx context.Context, messageID string) ([]protocol.Receipt, error) {
	return n.store.ReceiptsFor(ctx, messageID)
}

// MessageStatus returns the latest receipt for a message.
func (n *Node) MessageStatus(ctx context.Context, messageID string) (protocol.Receipt, error) {
	r, err := n.store.LatestReceipt(ctx, messageID)
	if errors.Is(err, store.ErrNotFound) {
		return protocol.Receipt{}, protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
	}
	return r, err
}

// Subscribe returns a channel of receipts for a session.
func (n *Node) Subscribe(sessionID string) <-chan protocol.Receipt {
	ch := make(chan protocol.Receipt, 64)
	n.obsMu.Lock()
	n.observers[sessionID] = append(n.observers[sessionID], ch)
	n.obsMu.Unlock()
	return ch
}

// Unsubscribe removes a subscription channel.
func (n *Node) Unsubscribe(sessionID string, ch <-chan protocol.Receipt) {
	n.obsMu.Lock()
	defer n.obsMu.Unlock()
	chans := n.observers[sessionID]
	for i, c := range chans {
		if (<-chan protocol.Receipt)(c) == ch {
			n.observers[sessionID] = append(chans[:i], chans[i+1:]...)
			close(c)
			return
		}
	}
}

// notifyObservers fans a receipt out to the session's subscribers.
func (n *Node) notifyObservers(sessionID string, r protocol.Receipt) {
	n.obsMu.RLock()
	chans := append([]chan protocol.Receipt(nil), n.observers[sessionID]...)
	n.obsMu.RUnlock()

	for _, ch := range chans {
		select {
		case ch <- r:
		default:
			// A slow subscriber must not stall delivery.
		}
	}
}

// mustID mints an identifier for an internal record.
func mustID(prefix string) string {
	switch prefix {
	case "evt":
		id, err := ids.NewEventID()
		if err != nil {
			return "evt_unknown"
		}
		return id
	default:
		id, err := ids.NewSessionID()
		if err != nil {
			return "unknown"
		}
		return id
	}
}

// mustJSON marshals a value, falling back to an empty object.
func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}

// estimateSize approximates the serialized size of a message body.
func estimateSize(body protocol.MessageBody) int {
	b, err := json.Marshal(body)
	if err != nil {
		return 0
	}
	return len(b)
}
