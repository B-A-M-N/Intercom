package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/B-A-M-N/intercom/internal/ids"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

// SessionState returns the normalized state and recovery classification for a
// session (ICSP-001 sections 7.2 and 7.3).
func (n *Node) SessionState(ctx context.Context, sessionID string) (protocol.Snapshot, error) {
	rec, err := n.store.GetSession(ctx, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Snapshot{}, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", sessionID)
		}
		return protocol.Snapshot{}, err
	}

	// A live adapter is the authority for the session's state, so the
	// recorded state is refreshed from the harness before it is reported.
	// The refresh marks the state as native, which distinguishes it from a
	// state the adapter inferred (ICSP-001 section 7.3).
	if ad, ok := n.Adapter(sessionID); ok {
		state, err := ad.State(ctx, sessionID)
		if err == nil && state.State != "" && state.State.Valid() {
			if err := n.store.SetSessionState(ctx, sessionID, state.State, store.StateSourceNative); err != nil {
				return protocol.Snapshot{}, err
			}
			rec.State = state.State
			rec.StateSource = store.StateSourceNative
		}
	}

	seq, err := n.store.LatestSequence(ctx, sessionID)
	if err != nil {
		return protocol.Snapshot{}, err
	}
	pending, err := n.store.CountPendingInbox(ctx, sessionID)
	if err != nil {
		return protocol.Snapshot{}, err
	}

	recovery, err := n.classifyRecovery(ctx, sessionID)
	if err != nil {
		return protocol.Snapshot{}, err
	}

	return protocol.Snapshot{
		SessionID:   sessionID,
		State:       rec.State,
		StateSource: string(rec.StateSource),
		Sequence:    seq,
		UpdatedAt:   rec.LastSeen,
		Pending:     pending,
		Recovery:    string(recovery),
	}, nil
}

// classifyRecovery determines how a session's state was reconstructed.
//
// The classification distinguishes native session state from observations
// inferred by the adapter (ICSP-001 section 7.3), so a caller can tell a
// harness-reported state from a reconstructed one.
func (n *Node) classifyRecovery(ctx context.Context, sessionID string) (protocol.RecoveryClassification, error) {
	rec, err := n.store.GetSession(ctx, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.RecoveryUnknown, nil
		}
		return protocol.RecoveryUnknown, err
	}

	// A live adapter reporting a state means the native harness is the
	// authority for that state.
	if _, ok := n.Adapter(sessionID); ok {
		if rec.StateSource == store.StateSourceNative {
			return protocol.RecoveryClean, nil
		}
	}

	switch rec.State {
	case protocol.SessionCreated, protocol.SessionIdle, protocol.SessionRunning,
		protocol.SessionWaitingApproval, protocol.SessionEnded:
		return protocol.RecoveryClean, nil
	case protocol.SessionInterrupted:
		return protocol.RecoveryInterrupted, nil
	case protocol.SessionUnavailable:
		return protocol.RecoveryUnknown, nil
	default:
		return protocol.RecoveryUnknown, nil
	}
}

// Snapshot returns a coherent state projection of a session.
func (n *Node) Snapshot(ctx context.Context, sessionID string) (protocol.Snapshot, error) {
	return n.SessionState(ctx, sessionID)
}

// Replay returns journal events after a cursor.
//
// When the requested history has been compacted, the response carries an
// explicit cursor-gap indication and a snapshot-recovery path. It MUST NOT
// silently omit missing history while claiming continuous replay.
func (n *Node) Replay(ctx context.Context, sessionID string, afterCursor uint64, limit int) (store.ReplayResult, error) {
	if _, err := n.store.GetSession(ctx, sessionID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.ReplayResult{}, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", sessionID)
		}
		return store.ReplayResult{}, err
	}
	if limit <= 0 || limit > n.cfg.Limits.MaxReplayBatch {
		limit = n.cfg.Limits.MaxReplayBatch
	}
	return n.store.Replay(ctx, sessionID, afterCursor, limit)
}

// SetSessionState updates a session's state and journals the change.
//
// The source records whether the harness reported the state or the adapter
// inferred it, so a reader can tell a native state from an inferred one
// (ICSP-001 section 7.3).
func (n *Node) SetSessionState(ctx context.Context, sessionID string, state protocol.SessionState, source store.StateSource) error {
	if !state.Valid() {
		return protocol.Errorf(protocol.CodeInvalidParams, "invalid session state %q", state)
	}
	if err := n.store.SetSessionState(ctx, sessionID, state, source); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", sessionID)
		}
		return err
	}
	_, err := n.store.AppendEvent(ctx, protocol.EventEnvelope{
		EventID:   mustID("evt"),
		SessionID: sessionID,
		EventType: protocol.EventSessionState,
		Timestamp: time.Now().UTC(),
		Payload:   mustJSON(map[string]any{"state": state}),
	}, "")
	return err
}

// reconcileUnsettled classifies one unsettled command against its message
// and receipt trail. Returns nil after settling, preserving, or abandoning.
func (n *Node) reconcileUnsettled(ctx context.Context, cmd store.CommandRecord, msgID string) error {
	if _, err := n.store.GetMessage(ctx, msgID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return n.store.AbandonCommand(ctx, cmd.CommandID, "linked message missing at recovery; outcome not established")
		}
		return err
	}
	receipts, err := n.store.ReceiptsFor(ctx, msgID)
	if err != nil {
		return err
	}
	// Observed harness effects: delivered/presented/injected/handled means
	// the harness accepted the message; settle the command to that fact.
	for _, r := range receipts {
		switch r.Status {
		case protocol.ReceiptDelivered, protocol.ReceiptPresented, protocol.ReceiptInjected, protocol.ReceiptHandled:
			return n.store.SettleCommand(ctx, cmd.CommandID, protocol.CommandSettled, string(r.Status))
		case protocol.ReceiptRejected, protocol.ReceiptExpired:
			return n.store.SettleCommand(ctx, cmd.CommandID, protocol.CommandSettled, string(r.Status))
		}
	}
	// Safely pending durable work: message still waits in the inbox with no
	// harness observation. Preserve it (leave command unsettled) so a later
	// claim can deliver it. The command remains pending for capacity
	// accounting until delivery resolves it.
	owner, state, _, err := n.store.ClaimOwner(ctx, msgID)
	if err != nil {
		state = store.InboxPending
	}
	_ = owner
	switch state {
	case store.InboxPending, store.InboxAccepted:
		// Explicitly pending: preserve.
		return nil
	case store.InboxClaimed:
		// Claimed but never observed: execution is uncertain. Do not replay;
		// mark abandoned so the operator can decide, while the message
		// remains claimable for a safe re-claim (inbox lease expiry governs).
		return n.store.AbandonCommand(ctx, cmd.CommandID, "message claimed at crash; harness effects unknown, not replayed")
	default:
		// Terminal inbox states with no receipt evidence (e.g. voided by
		// expiry): settle to the inbox fact if it is terminal, else abandon.
		switch state {
		case store.InboxRejected:
			return n.store.SettleCommand(ctx, cmd.CommandID, protocol.CommandSettled, string(protocol.ReceiptRejected))
		case store.InboxVoided, store.InboxDeadLetter:
			return n.store.SettleCommand(ctx, cmd.CommandID, protocol.CommandSettled, string(protocol.ReceiptExpired))
		}
		return n.store.AbandonCommand(ctx, cmd.CommandID, "admitted without settlement; outcome not established")
	}
}

// Recover reconstructs command intake state after a restart
// (ICSP-001 section 6.2).
//
// Recovery reconciles message state, command state, and observed harness
// effects: safely pending durable work is preserved, known outcomes are
// settled, and only ambiguous execution is marked abandoned. It never
// automatically replays a potentially effectful harness command.
func (n *Node) Recover(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport

	unsettled, err := n.store.UnsettledCommands(ctx)
	if err != nil {
		return report, err
	}
	report.Unsettled = len(unsettled)

	for _, cmd := range unsettled {
		// Reconcile message state, command state, and observed harness
		// effects together. Preserve legitimately pending durable work;
		// settle commands whose outcome is known; abandon only ambiguous
		// execution. Never automatically replay a potentially effectful
		// harness command.
		msgID := cmd.MessageID
		if msgID == "" {
			// Link may be missing on very old rows; treat as uncertain.
			if err := n.store.AbandonCommand(ctx, cmd.CommandID, "admitted without settlement or linked message; outcome not established"); err != nil {
				return report, err
			}
			report.Abandoned = append(report.Abandoned, cmd.CommandID)
			continue
		}
		if err := n.reconcileUnsettled(ctx, cmd, msgID); err != nil {
			return report, err
		}
	}

	// Mark every session of this host as disconnected. A host disconnect
	// alone does not terminate the logical session.
	marked, err := n.store.MarkAllDisconnected(ctx, n.hostID)
	if err != nil {
		return report, err
	}
	report.MarkedDisconnected = marked

	// Expire messages whose deadline passed while the host was down.
	expired, err := n.store.ExpiredMessages(ctx, time.Now().UTC(), n.cfg.Limits.MaxInboxMessages)
	if err != nil {
		return report, err
	}
	for _, messageID := range expired {
		msg, err := n.store.GetMessage(ctx, messageID)
		if err != nil {
			continue
		}
		n.recordReceipt(ctx, msg, protocol.ReceiptExpired, "message expired before delivery completed")
		_ = n.store.SetInboxState(ctx, msg.MessageID, store.InboxVoided)
	}
	report.Expired = len(expired)

	return report, nil
}

// RegisterAdapterSession records an out-of-process adapter's session and its
// advertised capability set.
//
// The capabilities come from the adapter, and the Go core stores them
// verbatim so that every peer sees exactly what the harness claimed. The
// core does not upgrade an adapter's claims.
func (n *Node) RegisterAdapterSession(ctx context.Context, peer protocol.PeerInfo) error {
	_, err := n.RegisterAdapterSessionWithSecret(ctx, peer)
	return err
}

// RegisterAdapterSessionWithSecret registers or resumes an adapter session
// with an explicit resume secret. It returns the freshly issued secret
// when (and only when) this call created the session and the caller did
// not supply a secret; the caller must persist it for future resumes.
// Resume without the enrolled secret is forbidden, even for the same UID.
func (n *Node) RegisterAdapterSessionWithSecret(ctx context.Context, peer protocol.PeerInfo) (string, error) {
	if peer.SessionID == "" {
		return "", protocol.Errorf(protocol.CodeInvalidParams, "sessionId is required")
	}
	if peer.InstanceID == "" {
		return "", protocol.Errorf(protocol.CodeInvalidParams, "instanceId is required")
	}
	if peer.PrincipalID == "" {
		peer.PrincipalID = n.principalID
	}
	if peer.HostID == "" {
		peer.HostID = n.hostID
	}
	if peer.Presence == "" {
		peer.Presence = protocol.PresenceOnline
	}
	// Authoritative trust domain: assigned by the host, never client-supplied.
	peer.TrustDomain = "local:" + n.hostID
	supplied := peer.SessionSecret
	secret := supplied
	if secret == "" {
		secret = store.GenerateSessionSecretForNode()
		peer.SessionSecret = secret
	}
	created, err := n.store.RegisterSessionWithSecret(ctx, peer, secret)
	if err != nil {
		return "", err
	}
	if _, err := n.store.AppendEvent(ctx, protocol.EventEnvelope{
		EventID:   mustID("evt"),
		SessionID: peer.SessionID,
		EventType: protocol.EventSessionCreated,
		Timestamp: time.Now().UTC(),
		Payload:   mustJSON(peer),
	}, ""); err != nil {
		return "", err
	}
	if err := n.store.Audit(ctx, n.principalID, peer.SessionID, "adapter.registered", peer.Harness); err != nil {
		return "", err
	}
	if created && supplied == "" {
		return secret, nil
	}
	return "", nil
}

// PendingInbox returns the messages waiting for a session to claim.
//
// An out-of-process adapter uses this to claim its own messages. The
// messages stay in the durable inbox until an adapter reports an outcome.
func (n *Node) PendingInbox(ctx context.Context, sessionID string, limit int) ([]protocol.Message, error) {
	if _, err := n.store.GetSession(ctx, sessionID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", sessionID)
		}
		return nil, err
	}
	if limit <= 0 || limit > n.cfg.Limits.MaxReplayBatch {
		limit = n.cfg.Limits.MaxReplayBatch
	}
	msgs, err := n.store.PendingInbox(ctx, sessionID, limit)
	if err != nil {
		return nil, err
	}
	if msgs == nil {
		msgs = []protocol.Message{}
	}
	return msgs, nil
}

// ReportDelivery records an adapter's observed delivery outcome.
//
// The receipt is appended to the same trail as the core's own receipts, so
// the sender sees one ordered history of what happened to its message.
func (n *Node) ReportDelivery(ctx context.Context, messageID string, status protocol.ReceiptStatus, detail string) error {
	if !status.Valid() {
		return protocol.Errorf(protocol.CodeInvalidParams, "invalid receipt status %q", status)
	}
	msg, err := n.store.GetMessage(ctx, messageID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		return err
	}
	return n.recordReceipt(ctx, msg, status, detail)
}

// RecoveryReport summarizes a recovery pass.
type RecoveryReport struct {
	// Unsettled counts commands that were admitted without settlement.
	Unsettled int
	// Abandoned lists the command ids marked abandoned.
	Abandoned []string
	// MarkedDisconnected counts sessions marked disconnected.
	MarkedDisconnected int64
	// Expired counts messages expired during recovery.
	Expired int
}

// String renders the report.
func (r RecoveryReport) String() string {
	return fmt.Sprintf("recovered: %d unsettled (%d abandoned), %d sessions disconnected, %d messages expired",
		r.Unsettled, len(r.Abandoned), r.MarkedDisconnected, r.Expired)
}

// Reply sends a correlated reply to the sender of a message.
func (n *Node) Reply(ctx context.Context, from Sender, messageID, text string, correlationID string) (SendResult, error) {
	return n.ReplyWithCommandID(ctx, from, messageID, text, correlationID, mustCommandID())
}

// ReplyWithCommandID sends a correlated reply with a caller-supplied
// idempotency identity, preserved across retries.
func (n *Node) ReplyWithCommandID(ctx context.Context, from Sender, messageID, text string, correlationID string, commandID string) (SendResult, error) {
	if commandID == "" {
		commandID = mustCommandID()
	}
	if err := n.validateSender(ctx, from); err != nil {
		return SendResult{}, err
	}
	orig, err := n.store.GetMessage(ctx, messageID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return SendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", messageID)
		}
		return SendResult{}, err
	}
	// Only a party to the original message (or a same-principal peer of
	// the recipient) may reply to it. A stranger must not be able to
	// inject a correlated reply into someone else's conversation.
	if from.SessionID != orig.To && from.SessionID != orig.From {
		recipient, err := n.store.GetSession(ctx, orig.To)
		if err != nil || recipient.PrincipalID == "" || recipient.PrincipalID != from.PrincipalID {
			return SendResult{}, protocol.Errorf(protocol.CodeForbidden, "only a party to message %q may reply to it", messageID)
		}
	}
	if correlationID == "" {
		correlationID = orig.CorrelationID
	}
	return n.Send(ctx, from, SendRequest{
		CommandID:     commandID,
		To:            orig.From,
		Body:          protocol.MessageBody{Type: "text", Text: text},
		Delivery:      protocol.Delivery{Mode: protocol.DeliveryQueue, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		ReplyTo:       messageID,
		CorrelationID: correlationID,
	})
}

// mustCommandID mints a command identifier.
func mustCommandID() string {
	id, err := ids.NewCommandID()
	if err != nil {
		return "cmd_unknown"
	}
	return id
}

// IssueDelegationFor mints a scoped attached-client token. The caller must
// own the session (the connection is already bound to it via a successful
// session/register with the resume secret).
func (n *Node) IssueDelegationFor(ctx context.Context, sender Sender, sessionID string) (string, error) {
	if err := n.validateSender(ctx, sender); err != nil {
		return "", err
	}
	target := sessionID
	if target == "" {
		target = sender.SessionID
	}
	if err := n.AuthorizeOwn(ctx, sender, target); err != nil {
		return "", err
	}
	ownerInstance := ""
	if rec, err := n.store.GetSession(ctx, target); err == nil {
		ownerInstance = rec.InstanceID
	}
	return n.store.IssueDelegation(ctx, target, ownerInstance, 24*time.Hour)
}

// VerifyDelegationFor checks a delegation token without mutating the session.
func (n *Node) VerifyDelegationFor(ctx context.Context, sessionID, token string) error {
	return n.store.VerifyDelegation(ctx, sessionID, token)
}

// VerifyDelegationDetailed checks a delegation token and returns its detail.
// Attached connections must re-verify on every request so expiry and
// revocation take effect on already-established connections.
func (n *Node) VerifyDelegationDetailed(ctx context.Context, sessionID, token string) (store.DelegationInfo, error) {
	return n.store.VerifyDelegationDetailed(ctx, sessionID, token)
}

// RevokeDelegationFor revokes a delegation token (owner operation).
func (n *Node) RevokeDelegationFor(ctx context.Context, sender Sender, token string) error {
	if err := n.validateSender(ctx, sender); err != nil {
		return err
	}
	return n.store.RevokeDelegation(ctx, token)
}
