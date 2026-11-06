package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/remote"
	"github.com/B-A-M-N/intercom/internal/routing"
	"github.com/B-A-M-N/intercom/internal/store"
)

func (n *Node) RemoteIdentity() *remote.Identity { return n.remoteIdentity }

// SetRemoteIdentity sets this host's TLS identity.
func (n *Node) SetRemoteIdentity(id *remote.Identity) { n.remoteIdentity = id }

// AuthorizeRemoteAdmin requires the sender to be the local operator
// (principal == node principal). Used for enroll/list/revoke/rotate.
func (n *Node) AuthorizeRemoteAdmin(ctx context.Context, sender Sender) error {
	if err := n.validateSender(ctx, sender); err != nil {
		return err
	}
	if sender.PrincipalID != n.principalID {
		_ = n.store.Audit(ctx, sender.PrincipalID, sender.SessionID, "remote.admin.denied", "not local operator")
		return protocol.Errorf(protocol.CodeForbidden, "remote administration requires the local operator principal")
	}
	return nil
}

type RemoteSendRequest struct {
	RemoteHost    string
	TargetSession string
	CommandID     string
	Body          protocol.MessageBody
	Mode          protocol.DeliveryMode
	CorrelationID string
	TTLSeconds    int
}

type RemoteSendResult struct {
	MessageID      string
	RemoteHost     string
	RemoteAccepted bool
	Attempts       int
}

// MaxRemoteAttempts bounds outbound retries.
const MaxRemoteAttempts = 5

func backoff(attempt int) time.Duration {
	base := 10 * time.Millisecond
	d := base << uint(attempt)
	if d > 200*time.Millisecond {
		d = 200 * time.Millisecond
	}
	jitter := time.Duration(rand.Int63n(int64(5 * time.Millisecond)))
	return d + jitter
}

func (n *Node) SendRemote(ctx context.Context, from Sender, req RemoteSendRequest) (RemoteSendResult, error) {
	if err := n.validateSender(ctx, from); err != nil {
		return RemoteSendResult{}, err
	}
	if req.RemoteHost == "" || req.TargetSession == "" {
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "remoteHost and targetSession are required")
	}
	if strings.Contains(req.TargetSession, "/") {
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "targetSession must be a local session id on the remote host")
	}
	if len(req.Body.Text) > remote.MaxBodyBytes || len(req.Body.Data) > remote.MaxBodyBytes {
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "body too large")
	}
	host, _, err := n.store.GetRemoteHost(ctx, req.RemoteHost)
	if err != nil {
		if err == store.ErrNotFound {
			return RemoteSendResult{}, protocol.Errorf(protocol.CodeForbidden, "remote host %q is not enrolled", req.RemoteHost)
		}
		return RemoteSendResult{}, err
	}
	if host.TrustStatus != "trusted" {
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeForbidden, "remote host %q is revoked", req.RemoteHost)
	}
	if n.remoteIdentity == nil {
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeInternalError, "remote identity not configured")
	}
	if req.CommandID == "" {
		req.CommandID = "cmd_remote_" + fmt.Sprintf("%d", time.Now().UnixNano())
	}
	localID := "msg_" + from.SessionID + "_" + req.CommandID
	if req.Mode == "" {
		req.Mode = protocol.DeliveryNotify
	}
	// Idempotent retry: if this CommandID was already admitted, reuse the
	// single stored envelope (with its absolute expiry) instead of
	// recomputing ExpiresAt. A conflicting request fails even when the
	// original was acknowledged; an acknowledged identical retry returns
	// immediately without another network attempt.
	if existing, gerr := n.store.GetRemoteTransfer(ctx, localID); gerr == nil {
		if existing.Envelope != "" {
			var stored remote.Envelope
			if jerr := json.Unmarshal([]byte(existing.Envelope), &stored); jerr == nil {
				storedType := stored.Body.Type
				if storedType == "" {
					storedType = "text"
				}
				reqType := req.Body.Type
				if reqType == "" {
					reqType = "text"
				}
				if existing.RemoteHost != req.RemoteHost {
					return RemoteSendResult{}, store.ErrConflict
				}
				if stored.TargetSession != req.TargetSession || stored.FromSession != from.SessionID ||
					storedType != reqType || stored.Body.Text != req.Body.Text ||
					string(stored.Body.Data) != string(req.Body.Data) ||
					stored.Delivery.Mode != string(req.Mode) || stored.CorrelationID != req.CorrelationID {
					return RemoteSendResult{}, store.ErrConflict
				}
				if existing.Status == "acked" {
					return RemoteSendResult{MessageID: localID, RemoteHost: existing.RemoteHost, RemoteAccepted: true, Attempts: existing.Attempts}, nil
				}
				if !existing.NextRetryAt.IsZero() && time.Now().UTC().Before(existing.NextRetryAt) {
					return RemoteSendResult{MessageID: localID, RemoteHost: host.HostID, RemoteAccepted: false, Attempts: existing.Attempts}, protocol.Errorf(protocol.CodeLimitExceeded, "remote retry backoff in effect until %s", existing.NextRetryAt.UTC().Format(time.RFC3339))
				}
				return n.attemptRemote(ctx, from, localID, host, stored, existing.Attempts)
			}
		}
	}
	env := remote.Envelope{
		OriginHost: n.remoteIdentity.HostID, OriginFingerprint: n.remoteIdentity.Fingerprint,
		FromPrincipal: from.PrincipalID, FromSession: from.SessionID,
		OriginalMessageID: localID, OriginalCommandID: req.CommandID,
		TargetSession: req.TargetSession, CorrelationID: req.CorrelationID, Hops: 0,
	}
	env.Body.Type = req.Body.Type
	if env.Body.Type == "" {
		env.Body.Type = "text"
	}
	env.Body.Text = req.Body.Text
	env.Body.Data = json.RawMessage(req.Body.Data)
	env.Delivery.Mode = string(req.Mode)
	if req.TTLSeconds > 0 {
		env.ExpiresAt = time.Now().UTC().Add(time.Duration(req.TTLSeconds) * time.Second).Format(time.RFC3339)
	}
	envelopeJSON, _ := json.Marshal(env)
	// Durable outbound record BEFORE network (crash-safe).
	if err := n.store.RecordRemoteTransferFull(ctx, localID, req.RemoteHost, req.TargetSession, from.SessionID, string(envelopeJSON)); err != nil {
		return RemoteSendResult{}, err
	}
	return n.attemptRemote(ctx, from, localID, host, env, 0)
}

func (n *Node) attemptRemote(ctx context.Context, from Sender, localID string, host store.RemoteHost, env remote.Envelope, attemptsAlready int) (RemoteSendResult, error) {
	if attemptsAlready < 0 {
		attemptsAlready = 0
	}
	if attemptsAlready >= MaxRemoteAttempts {
		_ = n.store.SetRemoteTransfer(ctx, localID, "failed", "remote retry budget exhausted; manual retry required", attemptsAlready)
		return RemoteSendResult{MessageID: localID, RemoteHost: host.HostID, RemoteAccepted: false, Attempts: attemptsAlready}, fmt.Errorf("remote retry budget exhausted after %d attempts", attemptsAlready)
	}
	var lastAttempt = attemptsAlready
	var lastErr error
	for attempt := attemptsAlready + 1; attempt <= MaxRemoteAttempts; attempt++ {
		lastAttempt = attempt
		if env.ExpiresAt != "" {
			exp, _ := time.Parse(time.RFC3339, env.ExpiresAt)
			if !exp.IsZero() && exp.Before(time.Now().UTC()) {
				_ = n.store.SetRemoteTransfer(ctx, localID, "expired", "TTL expired before delivery", attempt)
				return RemoteSendResult{MessageID: localID, RemoteHost: host.HostID, RemoteAccepted: false, Attempts: attempt}, protocol.Errorf(protocol.CodeInvalidParams, "message expired before remote delivery")
			}
		}
		remoteID, err := remote.Client(ctx, host.Endpoint, n.remoteIdentity, host.HostID, host.Fingerprint, env)
		if err == nil {
			_ = n.store.SetRemoteTransfer(ctx, localID, "acked", remoteID, attempt)
			_ = n.store.Audit(ctx, from.PrincipalID, env.TargetSession, "remote.send.acked", host.HostID+" remoteMessageId="+remoteID)
			return RemoteSendResult{MessageID: localID, RemoteHost: host.HostID, RemoteAccepted: true, Attempts: attempt}, nil
		}
		lastErr = err
		_ = n.store.SetRemoteTransferRetry(ctx, localID, "failed", err.Error(), attempt, time.Now().Add(backoff(attempt)))
		if attempt < MaxRemoteAttempts {
			select {
			case <-ctx.Done():
				return RemoteSendResult{MessageID: localID, RemoteHost: host.HostID, RemoteAccepted: false, Attempts: attempt}, ctx.Err()
			case <-time.After(backoff(attempt)):
			}
		}
	}
	_ = n.store.SetRemoteTransfer(ctx, localID, "failed", lastErr.Error(), lastAttempt)
	return RemoteSendResult{MessageID: localID, RemoteHost: host.HostID, RemoteAccepted: false, Attempts: lastAttempt}, lastErr
}

// ResumeRemoteTransfers is the restart recovery worker: it
// re-attempts every pending/failed durable outbound transfer that
// still has a stored envelope. Expired or envelope-less transfers
// are terminally classified, never silently dropped and never
// resent with recreated content. Bounded by MaxRemoteAttempts per
// transfer (attemptRemote). Returns the number of transfers that
// reached a terminal outcome (acked, expired, or exhausted).
func (n *Node) ResumeRemoteTransfers(ctx context.Context) (int, error) {
	pending, err := n.store.PendingRemoteTransfers(ctx)
	if err != nil {
		return 0, err
	}
	if n.remoteIdentity == nil {
		return 0, fmt.Errorf("remote identity not configured")
	}
	terminal := 0
	for _, t := range pending {
		// Respect the durable retry budget and schedule: a restart must
		// not retry exhausted transfers or transfers whose next eligible
		// time is in the future (no retry storm).
		if t.Attempts >= MaxRemoteAttempts {
			_ = n.store.SetRemoteTransfer(ctx, t.MessageID, "failed", "remote retry budget exhausted; manual retry required", t.Attempts)
			terminal++
			continue
		}
		if !t.NextRetryAt.IsZero() && time.Now().UTC().Before(t.NextRetryAt) {
			continue
		}
		if t.Envelope == "" {
			_ = n.store.SetRemoteTransfer(ctx, t.MessageID, "failed", "no stored envelope at recovery; manual retry requires the stored envelope", t.Attempts)
			continue
		}
		var env remote.Envelope
		if err := json.Unmarshal([]byte(t.Envelope), &env); err != nil {
			_ = n.store.SetRemoteTransfer(ctx, t.MessageID, "expired", "stored envelope corrupt at recovery", t.Attempts)
			terminal++
			continue
		}
		if err := remote.ValidateEnvelope(env); err != nil {
			_ = n.store.SetRemoteTransfer(ctx, t.MessageID, "expired", err.Error(), t.Attempts)
			terminal++
			continue
		}
		host, _, herr := n.store.GetRemoteHost(ctx, t.RemoteHost)
		if herr != nil || host.TrustStatus != "trusted" {
			// Host revoked or gone while we were down: the
			// transfer can never deliver; classify failed.
			_ = n.store.SetRemoteTransfer(ctx, t.MessageID, "failed",
				"remote host not trusted at recovery", t.Attempts)
			continue
		}
		from := Sender{PrincipalID: env.FromPrincipal, SessionID: env.FromSession}
		if _, aerr := n.attemptRemote(ctx, from, t.MessageID, host, env, t.Attempts); aerr != nil {
			// attemptRemote records failed/exhausted itself;
			// the transfer stays available for manual retry.
			continue
		}
		terminal++
	}
	return terminal, nil
}

// RetryRemote re-sends the STORED envelope (no recreation).
func (n *Node) RetryRemote(ctx context.Context, from Sender, messageID string) (RemoteSendResult, error) {
	transfer, err := n.store.GetRemoteTransfer(ctx, messageID)
	if err != nil {
		return RemoteSendResult{}, err
	}
	// Ownership first: an acknowledged transfer must not leak its
	// delivery outcome to a caller who does not own it.
	if transfer.SenderSession != from.SessionID {
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeForbidden, "only the original sender may retry this transfer")
	}
	if transfer.Status == "acked" {
		return RemoteSendResult{MessageID: messageID, RemoteHost: transfer.RemoteHost, RemoteAccepted: true, Attempts: transfer.Attempts}, nil
	}
	if transfer.Envelope == "" {
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "transfer has no stored envelope")
	}
	var env remote.Envelope
	if err := json.Unmarshal([]byte(transfer.Envelope), &env); err != nil {
		return RemoteSendResult{}, err
	}
	if err := remote.ValidateEnvelope(env); err != nil {
		_ = n.store.SetRemoteTransfer(ctx, messageID, "expired", err.Error(), transfer.Attempts)
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeInvalidParams, "%v", err)
	}
	host, _, err := n.store.GetRemoteHost(ctx, transfer.RemoteHost)
	if err != nil {
		return RemoteSendResult{}, err
	}
	if host.TrustStatus != "trusted" {
		return RemoteSendResult{}, protocol.Errorf(protocol.CodeForbidden, "remote host revoked")
	}
	if !transfer.NextRetryAt.IsZero() && time.Now().UTC().Before(transfer.NextRetryAt) {
		return RemoteSendResult{MessageID: messageID, RemoteHost: transfer.RemoteHost, RemoteAccepted: false, Attempts: transfer.Attempts}, protocol.Errorf(protocol.CodeLimitExceeded, "remote retry backoff in effect until %s", transfer.NextRetryAt.UTC().Format(time.RFC3339))
	}
	return n.attemptRemote(ctx, from, messageID, host, env, transfer.Attempts)
}

// AdmitRemote is receiving-host admission. peerHost/peerFP are TLS-authenticated.
func (n *Node) AdmitRemote(ctx context.Context, env remote.Envelope, peerHost, peerFP string) (string, error) {
	if peerHost == "" || peerFP == "" {
		return "", protocol.Errorf(protocol.CodeForbidden, "unauthenticated remote origin")
	}
	// Mutual enrollment: origin host must be enrolled + trusted here, fingerprint pinned.
	origin, _, err := n.store.GetRemoteHost(ctx, peerHost)
	if err != nil {
		if err == store.ErrNotFound {
			return "", protocol.Errorf(protocol.CodeForbidden, "remote origin %q not enrolled on this host", peerHost)
		}
		return "", err
	}
	if origin.TrustStatus != "trusted" {
		return "", protocol.Errorf(protocol.CodeForbidden, "remote origin %q revoked", peerHost)
	}
	if origin.Fingerprint != peerFP {
		return "", protocol.Errorf(protocol.CodeForbidden, "remote origin fingerprint mismatch")
	}
	if err := remote.ValidateEnvelope(env); err != nil {
		return "", protocol.Errorf(protocol.CodeInvalidParams, "%v", err)
	}
	if err := routing.CheckHops(env.Hops); err != nil {
		return "", protocol.Errorf(protocol.CodeForbidden, "%v", err)
	}
	if _, err := n.store.GetSession(ctx, env.TargetSession); err != nil {
		if err == store.ErrNotFound {
			return "", protocol.Errorf(protocol.CodeInvalidParams, "remote target %q not found", env.TargetSession)
		}
		return "", err
	}
	// Host-qualified sender identity: grants are for "host/session".
	qualified := peerHost + "/" + env.FromSession
	can, err := n.store.CanSend(ctx, qualified, env.TargetSession)
	if err != nil {
		return "", err
	}
	if !can {
		_ = n.store.Audit(ctx, env.FromPrincipal, env.TargetSession, "remote.policy.denied", "no grant for "+qualified)
		return "", protocol.Errorf(protocol.CodeForbidden, "remote policy denied: no grant for %s", qualified)
	}
	modes, hasPolicy, err := n.store.DeliveryPolicy(ctx, env.TargetSession, qualified)
	if err != nil {
		return "", err
	}
	mode := protocol.DeliveryMode(env.Delivery.Mode)
	if mode == "" {
		mode = protocol.DeliveryNotify
	}
	if hasPolicy {
		allowed := false
		for _, m := range modes {
			if m == mode {
				allowed = true
			}
		}
		if !allowed {
			return "", protocol.Errorf(protocol.CodeForbidden, "remote delivery policy denied")
		}
	}
	created := time.Now().UTC()
	bodyType := env.Body.Type
	if bodyType == "" {
		bodyType = "text"
	}
	// Namespaced command id prevents cross-host collisions.
	remoteCommandID := "remote:" + peerHost + ":" + env.OriginalCommandID
	msgID := "msg_remote_" + peerHost + "_" + env.OriginalMessageID
	var expires *time.Time
	if env.ExpiresAt != "" {
		if t, err := time.Parse(time.RFC3339, env.ExpiresAt); err == nil {
			expires = &t
		}
	}
	msg := protocol.Message{
		MessageID: msgID, CommandID: remoteCommandID, From: qualified, To: env.TargetSession,
		Body: protocol.MessageBody{Type: bodyType, Text: env.Body.Text, Data: []byte(env.Body.Data)},
		Delivery: protocol.Delivery{Mode: mode, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
		CorrelationID: env.CorrelationID, CreatedAt: created, ExpiresAt: expires,
		Metadata: map[string]any{"remote": true, "originHost": peerHost, "originalFromSession": env.FromSession, "originalMessageId": env.OriginalMessageID, "routeHistory": []string{"remote:" + peerHost + ":" + env.OriginalMessageID}},
	}
	// Full canonical fingerprint.
	fpParams := map[string]any{
		"originHost": peerHost, "originFingerprint": peerFP, "fromSession": env.FromSession,
		"originalCommandId": env.OriginalCommandID, "originalMessageId": env.OriginalMessageID,
		"target": env.TargetSession, "bodyType": bodyType, "text": env.Body.Text,
		"data": string(env.Body.Data), "mode": string(mode), "correlationId": env.CorrelationID,
		"expiresAt": env.ExpiresAt, "hops": env.Hops,
	}
	fingerprint, err := store.Fingerprint("remote.receive", fpParams)
	if err != nil {
		return "", err
	}
	cmdRec := store.CommandRecord{CommandID: remoteCommandID, PrincipalID: env.FromPrincipal, TargetSession: env.TargetSession, Operation: "remote.receive", State: protocol.CommandAccepted}
	ev := protocol.EventEnvelope{EventID: mustID("evt"), SessionID: env.TargetSession, EventType: protocol.EventMessageAccepted, Timestamp: created, Payload: mustJSON(map[string]any{"messageId": msgID, "commandId": remoteCommandID})}
	admitted, replayed, err := n.store.AdmitMessage(ctx, cmdRec, fingerprint, msg, env.FromPrincipal, ev, store.AdmissionLimits{MaxPendingCommands: n.cfg.Limits.MaxPendingCommands, MaxInboxMessages: n.cfg.Limits.MaxInboxMessages, MaxDatabaseBytes: n.cfg.Limits.MaxDatabaseBytes})
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return "", protocol.Errorf(protocol.CodeConflict, "remote command reuse with conflicting payload")
		}
		if errors.Is(err, store.ErrLimit) {
			return "", protocol.Errorf(protocol.CodeLimitExceeded, "%v", err)
		}
		return "", err
	}
	if replayed {
		return admitted.MessageID, nil
	}
	_ = n.store.Audit(ctx, env.FromPrincipal, env.TargetSession, "remote.admitted", "origin="+peerHost+" originalMessageId="+env.OriginalMessageID)
	return msgID, nil
}
