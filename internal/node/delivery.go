package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/B-A-M-N/intercom/internal/adapter"
	"github.com/B-A-M-N/intercom/internal/ids"
	"github.com/B-A-M-N/intercom/internal/policy"
	"github.com/B-A-M-N/intercom/internal/routing"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
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
	// ConversationID links the message to an explicit conversation.
	ConversationID string
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
	if err := n.validateSender(ctx, from); err != nil {
		return SendResult{}, err
	}

	// TTL bounds: negative and overflowing values are rejected; the
	// maximum deliverable window is 24h (matches remote validation).
	if req.TTLSeconds < 0 || req.TTLSeconds > 24*3600 {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "ttlSeconds must be between 0 and 86400")
	}
	// Metadata bounds: each value and the whole map are size-bounded.
	if len(req.Metadata) > 32 {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "metadata has too many entries")
	}
	for k, v := range req.Metadata {
		if len(k) > 128 {
			return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "metadata key %q too long", k)
		}
		if b, err := json.Marshal(v); err == nil && len(b) > 4096 {
			return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "metadata value for %q exceeds 4096 bytes", k)
		}
	}

	// Enforce the size bound on the complete encoded request (body +
	// metadata + envelope), not just the body, before admitting anything.
	size := estimateSize(req.Body)
	if meta, err := json.Marshal(req.Metadata); err == nil {
		size += len(meta)
	}
	size += len(req.CommandID) + len(req.To) + len(req.ReplyTo) + len(req.CorrelationID) + len(req.ConversationID) + 256
	if size > n.cfg.Limits.MaxMessageBytes {
		return SendResult{}, protocol.Errorf(protocol.CodeLimitExceeded,
			"message of %d bytes exceeds the %d byte limit", size, n.cfg.Limits.MaxMessageBytes)
	}

	// Rate limit: per-session bucket first (independent sessions under one
	// principal do not contend), with the principal aggregate as ceiling.
	if err := n.checkRate(ctx, "session:"+from.SessionID); err != nil {
		return SendResult{}, err
	}
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

	// Authorize via the central policy decision (deny-overrides,
	// explainable). Peer authority permits communication only.
	if err := n.authorizeSend(ctx, from, target, req.Delivery.Mode); err != nil {
		return SendResult{}, err
	}
	if err := n.authorizeConversationSend(ctx, from, req.ConversationID, target.SessionID); err != nil {
		return SendResult{}, err
	}
	// Structured payload validation: text needs text, json needs data.
	if req.Body.Type == "json" && len(req.Body.Data) == 0 {
		return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "json body requires data")
	}
	if req.Body.Type == "text" || req.Body.Type == "" {
		// text body may be empty only if data present; keep permissive.
	}

	// Relay hop limit / loop prevention: metadata "hops" counts forwards.
	// Malformed hop values (wrong type, fractional, negative, overflow)
	// are rejected, never silently treated as zero.
	if req.Metadata != nil {
		if h, ok := req.Metadata["hops"]; ok {
			hops, herr := parseHops(h)
			if herr != nil {
				return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "invalid hops metadata: %v", herr)
			}
			if err := routing.CheckHops(hops); err != nil {
				return SendResult{}, protocol.Errorf(protocol.CodeForbidden, "%v", err)
			}
		}
	}

	// Fingerprint covers sender identity as well as the request, so two
	// different senders reusing a commandId cannot be mistaken for a retry.
	fingerprint, err := store.Fingerprint("message/send", map[string]any{
		"fromPrincipal": fromPrincipal, "fromSession": from.SessionID, "request": req,
	})
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
		ReplyTo:        req.ReplyTo,
		CorrelationID:  req.CorrelationID,
		ConversationID: req.ConversationID,
		CreatedAt:      created,
		Metadata:       req.Metadata,
	}
	if req.TTLSeconds != 0 {
		exp := created.Add(time.Duration(req.TTLSeconds) * time.Second)
		msg.ExpiresAt = &exp
	}

	// Atomic admission: command + message + link + acceptance receipt +
	// acceptance event commit together. Accepted work always has a
	// recoverable message record; retries return the original message id.
	cmdRec := store.CommandRecord{
		CommandID:     req.CommandID,
		PrincipalID:   fromPrincipal,
		TargetSession: target.SessionID,
		Operation:     "message/send",
		State:         protocol.CommandAccepted,
	}
	ev := protocol.EventEnvelope{
		EventID:   mustID("evt"),
		SessionID: target.SessionID,
		EventType: protocol.EventMessageAccepted,
		Timestamp: created,
		Payload:   mustJSON(map[string]any{"messageId": messageID, "commandId": req.CommandID}),
	}
	admitted, replayed, err := n.store.AdmitMessage(ctx, cmdRec, fingerprint, msg, fromPrincipal, ev, store.AdmissionLimits{
		MaxPendingCommands: n.cfg.Limits.MaxPendingCommands,
		MaxInboxMessages:   n.cfg.Limits.MaxInboxMessages,
		MaxDatabaseBytes:   n.cfg.Limits.MaxDatabaseBytes,
	})
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return SendResult{}, protocol.Errorf(protocol.CodeConflict,
				"commandId %q was already used with a different request", req.CommandID)
		}
		if errors.Is(err, store.ErrLimit) {
			return SendResult{}, protocol.Errorf(protocol.CodeLimitExceeded, "%v", err)
		}
		return SendResult{}, err
	}
	if replayed {
		return SendResult{
			CommandID: admitted.CommandID,
			MessageID: admitted.MessageID,
			Status:    protocol.ReceiptAccepted,
			Replayed:  true,
			Detail:    "command already admitted; original outcome returned",
		}, nil
	}
	stored := msg

	// Delivery is attempted after durable admission on the daemon-owned
	// dispatcher. A delivery failure does not undo admission; it is
	// reported through the receipt trail and the message remains claimable
	// in the durable inbox.
	stream := streamKey{from: from.SessionID, to: target.SessionID}
	n.dispatch(stream, stored, req.Delivery)

	return SendResult{
		CommandID: req.CommandID,
		MessageID: stored.MessageID,
		Status:    protocol.ReceiptAccepted,
		Detail:    "durably admitted",
	}, nil
}

// streamKey identifies an ordered sender-recipient stream.
type streamKey struct {
	from string
	to   string
}

// Dispatcher bounds.
const (
	maxStreams             = 256
	streamQueue            = 128
	maxSessionSubscribers  = 32
	// streamIdleAfter retires an empty dispatcher stream worker.
	streamIdleAfter = 60 * time.Second
)

// dispatch enqueues a message for ordered delivery on the daemon-owned
// lifecycle context. It never blocks the RPC caller: if the stream does
// not exist and the stream bound is hit, or the queue is full, the message
// stays in the durable inbox (unknown receipt) for claim-based delivery.
// Idle empty streams retire themselves (runStream), so the bound cannot
// be permanently exhausted by short-lived sender-recipient pairs.
func (n *Node) dispatch(stream streamKey, msg protocol.Message, requested protocol.Delivery) {
	n.streamMu.Lock()
	if n.streams == nil {
		n.streams = make(map[streamKey]chan protocol.Message)
	}
	ch, ok := n.streams[stream]
	if !ok {
		if len(n.streams) >= maxStreams {
			// Evict an idle empty stream to make room; otherwise degrade
			// to the durable inbox path (no data loss, ordering falls back
			// to claim order for this message).
			evicted := false
			for k, v := range n.streams {
				if len(v) == 0 {
					close(v)
					delete(n.streams, k)
					evicted = true
					break
				}
			}
			if !evicted {
				n.streamMu.Unlock()
				_ = n.recordReceipt(n.lifecycleCtx, msg, protocol.ReceiptUnknown, "dispatcher stream bound reached; message remains in durable inbox for claim delivery")
				return
			}
		}
		ch = make(chan protocol.Message, streamQueue)
		n.streams[stream] = ch
		go n.runStream(n.lifecycleCtx, stream, ch)
	}
	n.streamMu.Unlock()

	select {
	case ch <- msg:
	default:
		// Bounded backpressure: do not block admission acknowledgement.
		// The durable inbox claim path will deliver it.
		_ = n.recordReceipt(n.lifecycleCtx, msg, protocol.ReceiptUnknown, "dispatcher queue full; message remains in durable inbox for claim delivery")
	}
}

// runStream delivers the messages of one stream in order. It exits when
// the daemon lifecycle ends or after streamIdleAfter with an empty queue;
// undelivered messages remain durable. On idle exit it removes itself
// from the stream map so a later message recreates the worker.
func (n *Node) runStream(ctx context.Context, stream streamKey, ch <-chan protocol.Message) {
	timer := time.NewTimer(streamIdleAfter)
	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			if len(ch) == 0 {
				n.streamMu.Lock()
				if cur, ok := n.streams[stream]; ok && cur == ch {
					delete(n.streams, stream)
					n.streamMu.Unlock()
					return
				}
				n.streamMu.Unlock()
			}
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(streamIdleAfter)
		case msg, ok := <-ch:
			if !ok {
				// Evicted while bound was full.
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(streamIdleAfter)
			n.deliver(ctx, msg, msg.Delivery)
		}
	}
}

// deliver attempts delivery and records the resulting receipts.
func (n *Node) deliver(ctx context.Context, msg protocol.Message, requested protocol.Delivery) {
	// An expired message MUST NOT be newly injected into agent context.
	if msg.ExpiresAt != nil && time.Now().UTC().After(*msg.ExpiresAt) {
		n.recordReceipt(ctx, msg, protocol.ReceiptExpired, "message expired before delivery was attempted")
		_ = n.store.SetInboxState(ctx, msg.MessageID, store.InboxVoided)
		return
	}

	// A recipient MUST deduplicate a previously delivered message id
	// (ICSP-001 section 5.5). A message that already reached the adapter
	// is not delivered again.
	alreadyDelivered, err := n.store.MessageDelivered(ctx, msg.MessageID)
	if err != nil {
		return
	}
	if alreadyDelivered {
		n.recordReceipt(ctx, msg, protocol.ReceiptDelivered,
			"the recipient already delivered this message id; delivery was not repeated")
		return
	}

	// Revocation/expiry recheck at delivery time: previously queued work
	// must not be delivered after the sender's grant ends.
	if err := n.recheckDelivery(ctx, msg, requested.Mode); err != nil {
		n.recordReceipt(ctx, msg, protocol.ReceiptRejected, err.Error())
		_ = n.store.SetInboxState(ctx, msg.MessageID, store.InboxRejected)
		return
	}

	ad, ok := n.Adapter(msg.To)
	if !ok {
		// The recipient has no live adapter. The durable inbox still holds
		// the message, and the outcome is unknown rather than delivered.
		n.recordReceipt(ctx, msg, protocol.ReceiptUnknown, "recipient adapter is not connected; message remains in the durable inbox")
		return
	}

	// The delivery mode decides what happens to the message. The wake
	// policy is independent of it: it governs whether the recipient is
	// initiated or woken to act on the message (ICSP-001 section 5.3).

	// Resolve the effective mode against the recipient's honest capability
	// matrix. A downgrade is a new delivery operation: the recipient's
	// mode policy is re-evaluated for the effective mode, and a downgrade
	// the recipient excludes is rejected rather than delivered.
	effective := requested.Mode
	matrix := capabilityMatrix(ad)
	if level, supported := matrix.Supports(requested.Mode); !supported {
		switch requested.OnUnsupported {
		case protocol.UnsupportedDowngrade:
			downgraded, ok := matrix.DowngradeTo(requested.Mode)
			if !ok {
				_ = n.recordReceipt(ctx, msg, protocol.ReceiptRejected,
					fmt.Sprintf("delivery mode %q is unsupported by the recipient harness", requested.Mode))
				return
			}
			effective = downgraded
			_ = level
			_ = n.recordReceipt(ctx, msg, protocol.ReceiptAccepted,
				adapter.FormatDowngrade(requested.Mode, downgraded))
		case protocol.UnsupportedNotify:
			// The notify fallback is notification-only, not a general
			// downgrade. Queueing would still make the message available
			// for the next turn, which is a different operation from the
			// notification the sender asked to fall back to.
			if _, ok := matrix.Supports(protocol.DeliveryNotify); !ok {
				_ = n.recordReceipt(ctx, msg, protocol.ReceiptRejected,
					fmt.Sprintf("delivery mode %q is unsupported by the recipient harness and notification is unavailable", requested.Mode))
				return
			}
			effective = protocol.DeliveryNotify
			_ = level
			_ = n.recordReceipt(ctx, msg, protocol.ReceiptAccepted,
				adapter.FormatDowngrade(requested.Mode, protocol.DeliveryNotify))
		default:
			_ = n.recordReceipt(ctx, msg, protocol.ReceiptRejected,
				fmt.Sprintf("delivery mode %q is unsupported by the recipient harness", requested.Mode))
			return
		}
		if effective != requested.Mode {
			if err := n.recheckDelivery(ctx, msg, effective); err != nil {
				_ = n.recordReceipt(ctx, msg, protocol.ReceiptRejected,
					fmt.Sprintf("downgrade from %q to %q denied by recipient policy: %v", requested.Mode, effective, err))
				return
			}
		}
	}

	// A message that may enter the model's context carries its provenance,
	// so the recipient can tell peer-supplied content from the user's own
	// instructions (ICSP-001 section 11.3).
	provenance := protocol.PeerProvenance(msg, n.principalIDFor(ctx, msg.From))

	switch effective {
	case protocol.DeliveryNotify:
		res, err := ad.Notify(ctx, msg.To, msg)
		if err != nil {
			n.recordReceipt(ctx, msg, protocol.ReceiptUnknown, err.Error())
			return
		}
		n.recordReceipt(ctx, msg, res.Status, res.Detail)
	case protocol.DeliveryQueue, protocol.DeliverySteer:
		// The injected content is rendered with its provenance attached.
		injected := msg
		injected.Body = provenance.RenderAsPeerContent(msg.Body)
		res, err := ad.Deliver(ctx, msg.To, injected)
		if err != nil {
			if errors.Is(err, adapter.ErrUnsupported) {
				n.recordReceipt(ctx, msg, protocol.ReceiptRejected, err.Error())
				return
			}
			n.recordReceipt(ctx, msg, protocol.ReceiptUnknown, err.Error())
			return
		}
		n.recordReceipt(ctx, msg, res.Status, res.Detail)

		// A queued message does not start a turn by itself. The wake policy
		// decides whether the recipient is initiated.
		if effective == protocol.DeliveryQueue {
			n.applyWake(ctx, ad, msg, requested.Wake)
		}
	default:
		n.recordReceipt(ctx, msg, protocol.ReceiptRejected, "no delivery path selected")
	}
}

// recordReceipt persists a receipt atomically (receipt + inbox transition
// + command settlement + journal event in one commit) and notifies observers.
//
// Presented/injected settle the command for capacity purposes: admission
// is resolved once the recipient has observed the message, while
// handled/delivered remain separately tracked in the receipt trail and
// inbox state. Presentation is never redefined as handling.
func (n *Node) recordReceipt(ctx context.Context, msg protocol.Message, status protocol.ReceiptStatus, detail string) error {
	receipt := protocol.Receipt{
		MessageID:  msg.MessageID,
		CommandID:  msg.CommandID,
		Status:     status,
		From:       msg.From,
		To:         msg.To,
		ObservedAt: time.Now().UTC(),
		Detail:     detail,
	}
	event := protocol.EventEnvelope{
		EventID:   mustID("evt"),
		SessionID: msg.To,
		EventType: protocol.EventMessageReceipt,
		Timestamp: receipt.ObservedAt,
		Payload:   mustJSON(receipt),
	}
	if err := n.store.RecordDeliveryOutcome(ctx, receipt, event); err != nil {
		return err
	}
	n.notifyObservers(msg.To, receipt)
	return nil
}

// applyWake initiates or wakes the recipient's execution, if the requested
// wake policy calls for it.
//
// The wake policy is independent of the delivery mode (ICSP-001 section
// 5.3). It governs initiating execution, not whether the message is
// delivered:
//
//   - none: do not initiate or wake execution. The message stays available
//     and the recipient picks it up itself.
//   - if_idle: wake the recipient when it is idle.
//   - next_safe_point: wake the recipient at an eligible safe point.
//
// A wake that is not satisfied is not an error. The message is already
// delivered, so the receipt trail records the delivery and the wake is
// simply not applicable right now.
func (n *Node) applyWake(ctx context.Context, ad adapter.Adapter, msg protocol.Message, wake protocol.WakePolicy) {
	switch wake {
	case protocol.WakeIfIdle:
		state, err := ad.State(ctx, msg.To)
		if err != nil {
			return
		}
		if state.State != protocol.SessionIdle && state.State != protocol.SessionCreated {
			return
		}
	case protocol.WakeNextSafePoint:
		state, err := ad.State(ctx, msg.To)
		if err != nil {
			return
		}
		if !state.SafePoint {
			return
		}
	case "", protocol.WakeNone:
		// The sender explicitly asked for no wake.
		return
	default:
		return
	}

	res, err := ad.Wake(ctx, msg.To, msg)
	if err != nil {
		// A failed wake does not undo the delivery that already happened.
		return
	}
	if res.Status == protocol.ReceiptRejected || res.Status == protocol.ReceiptUnknown {
		return
	}
	n.recordReceipt(ctx, msg, res.Status, res.Detail)
}

// Redeiver attempts delivery of an already-stored message again.
//
// It exists so that the deduplication guarantee can be exercised: a
// message that already reached the recipient must not be delivered twice.
func (n *Node) Redeiver(ctx context.Context, msg protocol.Message) {
	n.deliver(ctx, msg, msg.Delivery)
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

// recipientPolicyFacts builds policy facts for a send/delivery decision.
func (n *Node) recipientPolicyFacts(ctx context.Context, from Sender, target store.PeerRecord, mode protocol.DeliveryMode) (policy.Facts, error) {
	modes, hasPolicy, err := n.store.DeliveryPolicy(ctx, target.SessionID, from.SessionID)
	if err != nil {
		return policy.Facts{}, err
	}
	deliveryAllowed := false
	if !hasPolicy {
		// No explicit row: notify/queue allowed by default for authorized
		// senders; steer still needs native capability (checked below).
		deliveryAllowed = mode != protocol.DeliverySteer
	} else {
		for _, m := range modes {
			if m == mode {
				deliveryAllowed = true
			}
		}
	}
	grantActive := false
	grantRevoked := false
	if from.SessionID != target.SessionID {
		perm, lerr := n.store.LookupPermission(ctx, from.SessionID, target.SessionID)
		if lerr == nil {
			grantActive = !perm.Revoked()
			grantRevoked = perm.Revoked()
		}
	}
	capNative := false
	if mode == protocol.DeliverySteer {
		if ad, ok := n.Adapter(target.SessionID); ok {
			for _, m := range ad.SupportedDeliveryModes() {
				if m == protocol.DeliverySteer {
					capNative = true
				}
			}
		} else {
			// No live adapter: capability unknown; deliver() will record
			// unknown. For policy, treat as not native so cross-principal
			// steer is denied, same-principal steer defers to delivery.
			capNative = target.PrincipalID == from.PrincipalID
		}
	}
	// Host-qualified remote senders are never same-principal locally.
	isRemote := strings.Contains(from.SessionID, "/")
	return policy.Facts{
		SenderPrincipal: from.PrincipalID, SenderSession: from.SessionID,
		TargetPrincipal: target.PrincipalID, TargetSession: target.SessionID,
		SamePrincipal: !isRemote && from.PrincipalID != "" && from.PrincipalID == target.PrincipalID,
		Self: from.SessionID == target.SessionID,
		GrantActive: grantActive, GrantRevoked: grantRevoked,
		DeliveryAllowed: deliveryAllowed, DeliveryPolicySet: hasPolicy,
		CapabilityNative: capNative,
	}, nil
}

// authorizeSend evaluates central policy for a send. Denials are audited
// with the explainable rule.
func (n *Node) authorizeSend(ctx context.Context, from Sender, target store.PeerRecord, mode protocol.DeliveryMode) error {
	facts, err := n.recipientPolicyFacts(ctx, from, target, mode)
	if err != nil {
		return err
	}
	dec := policy.Evaluate(policy.OperationForMode(mode), facts)
	if !dec.Allowed {
		_ = n.store.Audit(ctx, from.PrincipalID, target.SessionID, "policy.denied", dec.Rule+": "+dec.Reason)
		return protocol.Errorf(protocol.CodeForbidden, "policy denied (%s): %s", dec.Rule, dec.Reason)
	}
	return nil
}

// recheckDelivery re-evaluates authorization at delivery time so revocation
// prevents future delivery of previously queued work.
func (n *Node) recheckDelivery(ctx context.Context, msg protocol.Message, mode protocol.DeliveryMode) error {
	fromPrincipal, err := n.store.MessagePrincipal(ctx, msg.MessageID)
	if err != nil || fromPrincipal == "" {
		fromPrincipal = n.principalIDFor(ctx, msg.From)
	}
	if fromPrincipal == "" {
		fromPrincipal = n.principalID
	}
	target, err := n.store.GetSession(ctx, msg.To)
	if err != nil {
		return err
	}
	from := Sender{PrincipalID: fromPrincipal, SessionID: msg.From}
	facts, err := n.recipientPolicyFacts(ctx, from, target, mode)
	if err != nil {
		return err
	}
	dec := policy.Evaluate(policy.OperationForMode(mode), facts)
	if !dec.Allowed {
		return protocol.Errorf(protocol.CodeForbidden, "policy denied at delivery (%s): %s", dec.Rule, dec.Reason)
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
//
// A subscriber receives every receipt recorded for that session, so a
// client that subscribes sees the session's delivery history from the
// moment it subscribed (ICSP-001 section 7.2).
func (n *Node) Subscribe(ctx context.Context, sessionID string) (<-chan protocol.Receipt, error) {
	if _, err := n.store.GetSession(ctx, sessionID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", sessionID)
		}
		return nil, err
	}

	ch := make(chan protocol.Receipt, 256)

	// Replay the receipts already recorded, so the subscriber starts from a
	// known point rather than from an empty history.
	existing, err := n.store.ReceiptsForSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	n.obsMu.Lock()
	if len(n.observers[sessionID]) >= maxSessionSubscribers {
		n.obsMu.Unlock()
		return nil, protocol.Errorf(protocol.CodeLimitExceeded, "subscriber limit for session %q reached", sessionID)
	}
	n.observers[sessionID] = append(n.observers[sessionID], ch)
	n.obsMu.Unlock()

	for _, r := range existing {
		select {
		case ch <- r:
		default:
			// The backlog exceeded the buffer. The client can still recover
			// the history through session/replay.
		}
	}

	return ch, nil
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
// The read lock is held during sends (sends are non-blocking), so an
// unsubscribe cannot close a channel mid-fan-out.
func (n *Node) notifyObservers(sessionID string, r protocol.Receipt) {
	n.obsMu.RLock()
	defer n.obsMu.RUnlock()
	for _, ch := range n.observers[sessionID] {
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

// parseHops strictly parses a hops metadata value.
func parseHops(v any) (int, error) {
	switch t := v.(type) {
	case int:
		if t < 0 {
			return 0, fmt.Errorf("hops must not be negative")
		}
		if t > 100 {
			return 0, fmt.Errorf("hops exceeds maximum")
		}
		return t, nil
	case int64:
		if t < 0 || t > 100 {
			return 0, fmt.Errorf("hops out of range")
		}
		return int(t), nil
	case float64:
		if t < 0 || t > 100 || t != float64(int(t)) {
			return 0, fmt.Errorf("hops must be a non-negative integer")
		}
		return int(t), nil
	case json.Number:
		if t.String() == "" {
			return 0, fmt.Errorf("hops empty")
		}
		for _, r := range t.String() {
			if r < '0' || r > '9' {
				return 0, fmt.Errorf("hops must be a non-negative integer")
			}
		}
		var n int
		if _, err := fmt.Sscan(t.String(), &n); err != nil {
			return 0, err
		}
		if n > 100 {
			return 0, fmt.Errorf("hops exceeds maximum")
		}
		return n, nil
	default:
		return 0, fmt.Errorf("hops must be an integer, got %T", v)
	}
}
