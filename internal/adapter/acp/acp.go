// Package acp implements the Intercom ACP extension surface.
//
// The extension is implemented directly over JSON-RPC 2.0 rather than through
// an ACP SDK. The surface is deliberately small: the standard ACP
// initialize/session lifecycle plus the _intercom/* extension methods. This
// keeps the dependency surface at zero while remaining a conforming ACP
// extension (ICSP-001 section 8).
//
// Intercom MUST NOT redefine the semantics of existing ACP methods, and an
// ACP client without Intercom support MUST remain operable as an ordinary ACP
// client.
package acp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/bamn/intercom/internal/node"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
)

// ProtocolVersion is the ACP protocol version this extension targets.
// ACP v2 is treated as a separate future compatibility profile.
const ProtocolVersion = 1

// Server serves the Intercom ACP extension over a JSON-RPC connection.
type Server struct {
	n *node.Node
	// peerSession is the session identity established by the transport.
	peerSession string
	// peerPrincipal is the principal established by the transport.
	peerPrincipal string
	// emit sends a server-initiated notification to the connected client.
	// It is nil when the server is used without a transport.
	emit func(ctx context.Context, method string, params any)
}

// NewServer creates an ACP extension server bound to a node.
//
// The peer identity comes from the authenticated transport, never from the
// request payload.
func NewServer(n *node.Node, peerPrincipal, peerSession string) *Server {
	return &Server{n: n, peerPrincipal: peerPrincipal, peerSession: peerSession}
}

// SetEmitter installs the function used to send server-initiated
// notifications to the connected client.
//
// Without an emitter, session/subscribe still records the subscription and
// still replays the backlog into the subscriber channel, but the events are
// not pushed onto the wire.
func (s *Server) SetEmitter(emit func(ctx context.Context, method string, params any)) {
	s.emit = emit
}

// InitializeResult is the response to the ACP initialize method.
type InitializeResult struct {
	ProtocolVersion   int              `json:"protocolVersion"`
	AgentCapabilities map[string]any   `json:"agentCapabilities"`
	AgentInfo         map[string]any   `json:"agentInfo"`
	AuthMethods       []map[string]any `json:"authMethods"`
	Meta              map[string]any   `json:"_meta,omitempty"`
}

// HelloResult is the response to _intercom/hello.
type HelloResult struct {
	SpecVersion  string                `json:"specVersion"`
	SpecRevision string                `json:"specRevision"`
	Capabilities []protocol.Capability `json:"capabilities"`
	Profiles     []string              `json:"profiles"`
	Meta         map[string]any        `json:"_meta,omitempty"`
}

// Capability advertisement for the ACP agent capabilities block.
//
// The Intercom extension is advertised through _meta so that an ordinary ACP
// client ignores it entirely.
func (s *Server) initializeResult() InitializeResult {
	return InitializeResult{
		ProtocolVersion: ProtocolVersion,
		AgentCapabilities: map[string]any{
			"loadSession":         true,
			"promptCapabilities":  map[string]any{},
			"sessionCapabilities": map[string]any{},
		},
		AgentInfo: map[string]any{
			"name":    "intercom",
			"version": protocol.SpecRevision,
		},
		AuthMethods: []map[string]any{},
		Meta: map[string]any{
			"intercom": map[string]any{
				"specVersion":  protocol.SpecVersion,
				"specRevision": protocol.SpecRevision,
				"namespace":    protocol.IntercomMethodPrefix,
			},
		},
	}
}

// Handle dispatches one JSON-RPC request.
//
// The method is either a standard ACP method or an _intercom/* extension
// method. Standard ACP methods keep their own semantics; the extension only
// adds new methods.
func (s *Server) Handle(ctx context.Context, method string, params json.RawMessage) (any, *protocol.RPCError) {
	return s.handle(ctx, method, params)
}

// HandleValue dispatches a request whose parameters are supplied as a Go
// value. It marshals the parameters and delegates to Handle.
func (s *Server) HandleValue(ctx context.Context, method string, params any) (any, *protocol.RPCError) {
	if params == nil {
		return s.handle(ctx, method, nil)
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "invalid params: %v", err)
	}
	return s.handle(ctx, method, raw)
}

// handle performs the dispatch.
func (s *Server) handle(ctx context.Context, method string, params json.RawMessage) (any, *protocol.RPCError) {
	switch method {
	case "initialize":
		return s.initializeResult(), nil
	case protocol.MethodHello:
		return s.hello()
	case protocol.MethodPeerList:
		return s.peerList(ctx)
	case protocol.MethodPeerResolve:
		return s.peerResolve(ctx, params)
	case protocol.MethodPeerCapabilities:
		return s.peerCapabilities(ctx, params)
	case protocol.MethodPeerPresence:
		return s.peerPresence(ctx, params)
	case protocol.MethodMessageSend:
		return s.messageSend(ctx, params)
	case protocol.MethodMessageReply:
		return s.messageReply(ctx, params)
	case protocol.MethodMessageStatus:
		return s.messageStatus(ctx, params)
	case protocol.MethodSessionState:
		return s.sessionState(ctx, params)
	case protocol.MethodSessionSnapshot:
		return s.sessionSnapshot(ctx, params)
	case protocol.MethodSessionReplay:
		return s.sessionReplay(ctx, params)
	case protocol.MethodSessionSubscribe:
		return s.sessionSubscribe(ctx, params)
	case protocol.MethodSessionRegister:
		return s.sessionRegister(ctx, params)
	case protocol.MethodMessageListPending:
		return s.messageListPending(ctx, params)
	case protocol.MethodMessageReport:
		return s.messageReport(ctx, params)
	case protocol.MethodHeartbeat:
		return s.heartbeat(ctx, params)
	case protocol.MethodPermissionGrant:
		return s.permissionGrant(ctx, params)
	case protocol.MethodPermissionRevoke:
		return s.permissionRevoke(ctx, params)
	case protocol.MethodPermissionList:
		return s.permissionList(ctx, params)
	default:
		if protocol.IsIntercomMethod(method) {
			return nil, protocol.Errorf(protocol.CodeMethodNotFound, "unknown Intercom method %q", method)
		}
		return nil, protocol.Errorf(protocol.CodeMethodNotFound, "unknown method %q", method)
	}
}

// hello negotiates protocol features.
func (s *Server) hello() (HelloResult, *protocol.RPCError) {
	return HelloResult{
		SpecVersion:  protocol.SpecVersion,
		SpecRevision: protocol.SpecRevision,
		Capabilities: s.n.CapabilityAdvertisement(),
		Profiles:     s.n.SupportedProfiles(),
	}, nil
}

// PeerListParams are the parameters of _intercom/peer/list.
type PeerListParams struct {
	Limit int `json:"limit,omitempty"`
}

// PeerListResult is the response to _intercom/peer/list.
type PeerListResult struct {
	Peers []protocol.PeerInfo `json:"peers"`
}

// peerList discovers visible peers.
func (s *Server) peerList(ctx context.Context) (any, *protocol.RPCError) {
	peers, err := s.n.Discover(ctx)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "discovery failed: %v", err)
	}
	return PeerListResult{Peers: peers}, nil
}

// PeerResolveParams are the parameters of _intercom/peer/resolve.
type PeerResolveParams struct {
	Alias   string `json:"alias,omitempty"`
	Session string `json:"sessionId,omitempty"`
}

// PeerResolveResult is the response to _intercom/peer/resolve.
type PeerResolveResult struct {
	Peer protocol.PeerInfo `json:"peer"`
}

// peerResolve resolves an alias or session identifier.
func (s *Server) peerResolve(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PeerResolveParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	target := p.Alias
	if target == "" {
		target = p.Session
	}
	if target == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "alias or sessionId is required")
	}
	peer, err := s.n.Resolve(ctx, target)
	if err != nil {
		if errors.Is(err, node.ErrNotFound) {
			return nil, protocol.Errorf(protocol.CodeInvalidParams, "no session resolves from %q", target)
		}
		return nil, protocol.Errorf(protocol.CodeInternalError, "resolve failed: %v", err)
	}
	return PeerResolveResult{Peer: peer}, nil
}

// PeerCapabilitiesParams are the parameters of _intercom/peer/capabilities.
type PeerCapabilitiesParams struct {
	Session string `json:"sessionId"`
}

// PeerCapabilitiesResult is the response to _intercom/peer/capabilities.
type PeerCapabilitiesResult struct {
	SessionID    string                `json:"sessionId"`
	Capabilities []protocol.Capability `json:"capabilities"`
}

// peerCapabilities retrieves a peer's effective capabilities.
func (s *Server) peerCapabilities(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PeerCapabilitiesParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	caps, err := s.n.Capabilities(ctx, p.Session)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", p.Session)
	}
	return PeerCapabilitiesResult{SessionID: p.Session, Capabilities: caps}, nil
}

// PeerPresenceParams are the parameters of _intercom/peer/presence.
type PeerPresenceParams struct {
	Session string `json:"sessionId"`
}

// PeerPresenceResult is the response to _intercom/peer/presence.
type PeerPresenceResult struct {
	SessionID string                 `json:"sessionId"`
	Presence  protocol.PresenceState `json:"presence"`
}

// peerPresence queries a peer's last observed presence.
func (s *Server) peerPresence(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PeerPresenceParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	presence, err := s.n.Presence(ctx, p.Session)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", p.Session)
	}
	return PeerPresenceResult{SessionID: p.Session, Presence: presence}, nil
}

// MessageSendParams are the parameters of _intercom/message/send.
type MessageSendParams struct {
	CommandID     string               `json:"commandId"`
	To            json.RawMessage      `json:"to"`
	Body          protocol.MessageBody `json:"body"`
	Delivery      protocol.Delivery    `json:"delivery"`
	ReplyTo       string               `json:"replyTo,omitempty"`
	CorrelationID string               `json:"correlationId,omitempty"`
	TTLSeconds    int                  `json:"ttlSeconds,omitempty"`
	Metadata      map[string]any       `json:"metadata,omitempty"`
}

// MessageSendResult is the response to _intercom/message/send.
//
// It reports durable admission, not recipient processing.
type MessageSendResult struct {
	CommandID string                 `json:"commandId"`
	MessageID string                 `json:"messageId"`
	Status    protocol.ReceiptStatus `json:"status"`
}

// messageSend submits a peer message.
func (s *Server) messageSend(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p MessageSendParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}

	target, rpcErr := resolveTarget(p.To)
	if rpcErr != nil {
		return nil, rpcErr
	}

	res, err := s.n.Send(ctx, s.sender(), node.SendRequest{
		CommandID:     p.CommandID,
		To:            target,
		Body:          p.Body,
		Delivery:      p.Delivery,
		ReplyTo:       p.ReplyTo,
		CorrelationID: p.CorrelationID,
		TTLSeconds:    p.TTLSeconds,
		Metadata:      p.Metadata,
	})
	if err != nil {
		return nil, toRPCError(err)
	}
	return MessageSendResult{CommandID: res.CommandID, MessageID: res.MessageID, Status: res.Status}, nil
}

// MessageReplyParams are the parameters of _intercom/message/reply.
type MessageReplyParams struct {
	CommandID     string `json:"commandId"`
	MessageID     string `json:"messageId"`
	Text          string `json:"text"`
	CorrelationID string `json:"correlationId,omitempty"`
}

// MessageReplyResult is the response to _intercom/message/reply.
type MessageReplyResult struct {
	CommandID string                 `json:"commandId"`
	MessageID string                 `json:"messageId"`
	Status    protocol.ReceiptStatus `json:"status"`
}

// messageReply sends a correlated reply.
func (s *Server) messageReply(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p MessageReplyParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	res, err := s.n.Reply(ctx, s.sender(), p.MessageID, p.Text, p.CorrelationID)
	if err != nil {
		return nil, toRPCError(err)
	}
	return MessageReplyResult{CommandID: res.CommandID, MessageID: res.MessageID, Status: res.Status}, nil
}

// MessageStatusParams are the parameters of _intercom/message/status.
type MessageStatusParams struct {
	MessageID string `json:"messageId"`
}

// MessageStatusResult is the response to _intercom/message/status.
type MessageStatusResult struct {
	MessageID string                 `json:"messageId"`
	Status    protocol.ReceiptStatus `json:"status"`
	Receipts  []protocol.Receipt     `json:"receipts"`
}

// messageStatus reads delivery receipts.
func (s *Server) messageStatus(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p MessageStatusParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	receipts, err := s.n.Receipts(ctx, p.MessageID)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "receipt read failed: %v", err)
	}
	if len(receipts) == 0 {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown messageId %q", p.MessageID)
	}
	return MessageStatusResult{
		MessageID: p.MessageID,
		Status:    receipts[len(receipts)-1].Status,
		Receipts:  receipts,
	}, nil
}

// SessionStateParams are the parameters of _intercom/session/state.
type SessionStateParams struct {
	Session string `json:"sessionId"`
}

// SessionStateResult is the response to _intercom/session/state.
type SessionStateResult struct {
	SessionID string                `json:"sessionId"`
	State     protocol.SessionState `json:"state"`
	Recovery  string                `json:"recovery"`
	Sequence  uint64                `json:"sequence"`
}

// sessionState queries a session's state and recovery classification.
func (s *Server) sessionState(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p SessionStateParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	snap, err := s.n.SessionState(ctx, p.Session)
	if err != nil {
		return nil, toRPCError(err)
	}
	return SessionStateResult{
		SessionID: snap.SessionID,
		State:     snap.State,
		Recovery:  snap.Recovery,
		Sequence:  snap.Sequence,
	}, nil
}

// SessionSnapshotResult is the response to _intercom/session/snapshot.
type SessionSnapshotResult struct {
	Snapshot protocol.Snapshot `json:"snapshot"`
}

// sessionSnapshot obtains a coherent state projection.
func (s *Server) sessionSnapshot(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p SessionStateParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	snap, err := s.n.Snapshot(ctx, p.Session)
	if err != nil {
		return nil, toRPCError(err)
	}
	return SessionSnapshotResult{Snapshot: snap}, nil
}

// SessionReplayParams are the parameters of _intercom/session/replay.
type SessionReplayParams struct {
	Session string `json:"sessionId"`
	Cursor  uint64 `json:"cursor,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

// SessionReplayResult is the response to _intercom/session/replay.
//
// CursorGap is set when the requested history was compacted away, so the
// client knows to recover through a snapshot rather than assume continuity.
type SessionReplayResult struct {
	SessionID         string                   `json:"sessionId"`
	Events            []protocol.EventEnvelope `json:"events"`
	Cursor            uint64                   `json:"cursor"`
	CursorGap         bool                     `json:"cursorGap,omitempty"`
	EarliestAvailable uint64                   `json:"earliestAvailable,omitempty"`
	SnapshotPath      string                   `json:"snapshotPath,omitempty"`
}

// sessionReplay replays the event stream.
func (s *Server) sessionReplay(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p SessionReplayParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	res, err := s.n.Replay(ctx, p.Session, p.Cursor, p.Limit)
	if err != nil {
		return nil, toRPCError(err)
	}

	// A cursor gap is an explicit recovery response, not a silently short
	// history. The error carries the snapshot path and the earliest
	// available sequence so the client knows how to recover coherently
	// (ICSP-001 section 7.2).
	if res.Gap {
		return nil, protocol.Errorf(protocol.CodeCursorGap,
			"the requested history has been compacted; recover through %q",
			res.SnapshotPath).
			WithData(map[string]any{
				"sessionId":         p.Session,
				"earliestAvailable": res.EarliestAvailable,
				"requestedCursor":   p.Cursor,
				"snapshotPath":      res.SnapshotPath,
			})
	}

	return SessionReplayResult{
		SessionID:         p.Session,
		Events:            res.Events,
		Cursor:            res.Cursor,
		EarliestAvailable: res.EarliestAvailable,
	}, nil
}

// SessionSubscribeParams are the parameters of _intercom/session/subscribe.
type SessionSubscribeParams struct {
	Session string `json:"sessionId"`
}

// SessionSubscribeResult is the response to _intercom/session/subscribe.
type SessionSubscribeResult struct {
	SessionID  string `json:"sessionId"`
	Subscribed bool   `json:"subscribed"`
}

// sessionSubscribe subscribes to subsequent changes.
//
// The subscription is delivered over the same connection as the request, as
// _intercom/event notifications. The backlog already recorded for the
// session is sent first, so the client starts from a known point rather
// than from an empty history (ICSP-001 section 7.2).
func (s *Server) sessionSubscribe(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p SessionSubscribeParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if p.Session == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "sessionId is required")
	}

	ch, err := s.n.Subscribe(ctx, p.Session)
	if err != nil {
		return nil, toRPCError(err)
	}

	// Stream receipts to the client as _intercom/event notifications.
	go func() {
		for {
			select {
			case <-ctx.Done():
				s.n.Unsubscribe(p.Session, ch)
				return
			case receipt, ok := <-ch:
				if !ok {
					return
				}
				s.sendEvent(ctx, p.Session, receipt)
			}
		}
	}()

	return SessionSubscribeResult{SessionID: p.Session, Subscribed: true}, nil
}

// sendEvent emits one event notification to the connected client.
func (s *Server) sendEvent(ctx context.Context, sessionID string, receipt protocol.Receipt) {
	if s.emit == nil {
		return
	}
	payload := map[string]any{
		"sessionId": sessionID,
		"receipt":   receipt,
	}
	s.emit(ctx, protocol.MethodEvent, payload)
}

// sessionRegisterParams are the parameters of _intercom/session/register.
type sessionRegisterParams struct {
	SessionID     string                  `json:"sessionId"`
	InstanceID    string                  `json:"instanceId"`
	Alias         string                  `json:"alias,omitempty"`
	Harness       string                  `json:"harness"`
	Version       string                  `json:"harnessVersion,omitempty"`
	HostID        string                  `json:"hostId,omitempty"`
	PrincipalID   string                  `json:"principalId,omitempty"`
	Capabilities  []protocol.Capability   `json:"capabilities"`
	DeliveryModes []protocol.DeliveryMode `json:"deliveryModes"`
}

// sessionRegisterResult is the response to _intercom/session/register.
type sessionRegisterResult struct {
	SessionID  string `json:"sessionId"`
	Registered bool   `json:"registered"`
}

// sessionRegister records an out-of-process adapter's session.
//
// The adapter's capability claims are stored as given. The core does not
// upgrade them, and an adapter that claims a capability it cannot implement
// is caught by the conformance tests rather than by the core guessing.
func (s *Server) sessionRegister(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p sessionRegisterParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if p.SessionID == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "sessionId is required")
	}
	if p.InstanceID == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "instanceId is required")
	}

	peer := protocol.PeerInfo{
		SessionID:      p.SessionID,
		InstanceID:     p.InstanceID,
		Alias:          p.Alias,
		Harness:        p.Harness,
		HarnessVersion: p.Version,
		HostID:         p.HostID,
		PrincipalID:    p.PrincipalID,
		Capabilities:   p.Capabilities,
		Delivery:       p.DeliveryModes,
		Presence:       protocol.PresenceOnline,
	}
	if err := s.n.RegisterAdapterSession(ctx, peer); err != nil {
		return nil, toRPCError(err)
	}
	return sessionRegisterResult{SessionID: p.SessionID, Registered: true}, nil
}

// messageListPendingParams are the parameters of _intercom/message/listPending.
type messageListPendingParams struct {
	Session string `json:"sessionId"`
	Limit   int    `json:"limit,omitempty"`
}

// messageListPendingResult is the response to _intercom/message/listPending.
type messageListPendingResult struct {
	Messages []protocol.Message `json:"messages"`
}

// messageListPending returns the messages waiting in a session's inbox.
func (s *Server) messageListPending(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p messageListPendingParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if p.Session == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "sessionId is required")
	}
	msgs, err := s.n.PendingInbox(ctx, p.Session, p.Limit)
	if err != nil {
		return nil, toRPCError(err)
	}
	return messageListPendingResult{Messages: msgs}, nil
}

// messageReportParams are the parameters of _intercom/message/report.
type messageReportParams struct {
	MessageID string                 `json:"messageId"`
	Status    protocol.ReceiptStatus `json:"status"`
	Detail    string                 `json:"detail,omitempty"`
}

// messageReportResult is the response to _intercom/message/report.
type messageReportResult struct {
	MessageID string                 `json:"messageId"`
	Status    protocol.ReceiptStatus `json:"status"`
}

// messageReport records an adapter's observed delivery outcome.
func (s *Server) messageReport(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p messageReportParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if err := s.n.ReportDelivery(ctx, p.MessageID, p.Status, p.Detail); err != nil {
		return nil, toRPCError(err)
	}
	return messageReportResult{MessageID: p.MessageID, Status: p.Status}, nil
}

// HeartbeatParams are the parameters of _intercom/heartbeat.
type HeartbeatParams struct {
	Session string `json:"sessionId,omitempty"`
}

// HeartbeatResult is the response to _intercom/heartbeat.
type HeartbeatResult struct {
	SessionID string                 `json:"sessionId"`
	Presence  protocol.PresenceState `json:"presence"`
}

// heartbeat records that a peer is alive.
//
// Presence distinguishes a lost transport from a terminated agent, so a
// peer that stops reporting becomes disconnected rather than ended
// (ICSP-001 section 4).
func (s *Server) heartbeat(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p HeartbeatParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	session := p.Session
	if session == "" {
		session = s.peerSession
	}
	if err := s.n.Heartbeat(ctx, session); err != nil {
		return nil, toRPCError(err)
	}
	presence, err := s.n.Presence(ctx, session)
	if err != nil {
		return nil, toRPCError(err)
	}
	return HeartbeatResult{SessionID: session, Presence: presence}, nil
}

// PermissionGrantParams are the parameters of _intercom/permission/grant.
type PermissionGrantParams struct {
	OwnerSession string `json:"ownerSession"`
	PeerSession  string `json:"peerSession"`
	Role         string `json:"role"`
}

// PermissionResult is the response to a permission mutation.
type PermissionResult struct {
	OwnerSession string `json:"ownerSession"`
	PeerSession  string `json:"peerSession"`
	Role         string `json:"role,omitempty"`
	Granted      bool   `json:"granted"`
}

// permissionGrant grants a peer access to an owner's session.
//
// The caller's identity comes from the transport. A peer without owner
// authority is refused, which is what stops a peer from modifying another
// session's permission policy.
func (s *Server) permissionGrant(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PermissionGrantParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	role := store.Role(p.Role)
	if role == "" {
		role = store.RolePeer
	}
	if err := s.n.GrantPermission(ctx, s.sender(), p.OwnerSession, p.PeerSession, role); err != nil {
		return nil, toRPCError(err)
	}
	return PermissionResult{
		OwnerSession: p.OwnerSession,
		PeerSession:  p.PeerSession,
		Role:         string(role),
		Granted:      true,
	}, nil
}

// PermissionRevokeParams are the parameters of _intercom/permission/revoke.
type PermissionRevokeParams struct {
	OwnerSession string `json:"ownerSession"`
	PeerSession  string `json:"peerSession"`
}

// permissionRevoke revokes a peer's access to an owner's session.
func (s *Server) permissionRevoke(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PermissionRevokeParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if err := s.n.RevokePermission(ctx, s.sender(), p.OwnerSession, p.PeerSession); err != nil {
		return nil, toRPCError(err)
	}
	return PermissionResult{
		OwnerSession: p.OwnerSession,
		PeerSession:  p.PeerSession,
		Granted:      false,
	}, nil
}

// PermissionListParams are the parameters of _intercom/permission/list.
type PermissionListParams struct {
	PeerSession string `json:"peerSession"`
}

// PermissionListResult is the response to _intercom/permission/list.
type PermissionListResult struct {
	Permissions []node.PermissionRecord `json:"permissions"`
}

// permissionList returns the active grants held by a peer.
func (s *Server) permissionList(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PermissionListParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	perms, err := s.n.ListPermissions(ctx, p.PeerSession)
	if err != nil {
		return nil, toRPCError(err)
	}
	return PermissionListResult{Permissions: perms}, nil
}

// sender returns the transport-authenticated sender identity.
func (s *Server) sender() node.Sender {
	return node.Sender{
		PrincipalID: s.peerPrincipal,
		SessionID:   s.peerSession,
	}
}

// resolveTarget extracts the canonical recipient from the flexible "to"
// field, which may be a bare session id or a session reference object.
func resolveTarget(raw json.RawMessage) (string, *protocol.RPCError) {
	if len(raw) == 0 {
		return "", protocol.Errorf(protocol.CodeInvalidParams, "to is required")
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		if asString == "" {
			return "", protocol.Errorf(protocol.CodeInvalidParams, "to is required")
		}
		return asString, nil
	}

	var asObject struct {
		SessionID string `json:"sessionId"`
		Alias     string `json:"alias"`
	}
	if err := json.Unmarshal(raw, &asObject); err != nil {
		return "", protocol.Errorf(protocol.CodeInvalidParams, "to must be a session id or session reference")
	}
	if asObject.SessionID != "" {
		return asObject.SessionID, nil
	}
	if asObject.Alias != "" {
		return asObject.Alias, nil
	}
	return "", protocol.Errorf(protocol.CodeInvalidParams, "to is required")
}

// decodeParams decodes request parameters.
func decodeParams(raw json.RawMessage, out any) *protocol.RPCError {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return protocol.Errorf(protocol.CodeInvalidParams, "invalid params: %v", err)
	}
	return nil
}

// toRPCError converts an internal error into a JSON-RPC error.
func toRPCError(err error) *protocol.RPCError {
	if rpcErr, ok := protocol.AsRPCError(err); ok {
		return rpcErr
	}
	switch {
	case errors.Is(err, node.ErrNotFound):
		return protocol.Errorf(protocol.CodeInvalidParams, "%v", err)
	case errors.Is(err, node.ErrForbidden):
		return protocol.Errorf(protocol.CodeForbidden, "%v", err)
	case errors.Is(err, node.ErrLimitExceeded):
		return protocol.Errorf(protocol.CodeLimitExceeded, "%v", err)
	case errors.Is(err, context.Canceled):
		return protocol.Errorf(protocol.CodeInternalError, "cancelled")
	default:
		return protocol.Errorf(protocol.CodeInternalError, "%v", err)
	}
}

// Namespace returns the Intercom extension namespace.
func Namespace() string { return strings.TrimSuffix(protocol.IntercomMethodPrefix, "/") }
