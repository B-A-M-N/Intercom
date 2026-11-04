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
	"fmt"
	"strings"

	"github.com/bamn/intercom/internal/adapter"
	"github.com/bamn/intercom/internal/node"
	"github.com/bamn/intercom/internal/protocol"
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
}

// NewServer creates an ACP extension server bound to a node.
//
// The peer identity comes from the authenticated transport, never from the
// request payload.
func NewServer(n *node.Node, peerPrincipal, peerSession string) *Server {
	return &Server{n: n, peerPrincipal: peerPrincipal, peerSession: peerSession}
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

// peerListParams are the parameters of _intercom/peer/list.
type peerListParams struct {
	Limit int `json:"limit,omitempty"`
}

// peerListResult is the response to _intercom/peer/list.
type peerListResult struct {
	Peers []protocol.PeerInfo `json:"peers"`
}

// peerList discovers visible peers.
func (s *Server) peerList(ctx context.Context) (any, *protocol.RPCError) {
	peers, err := s.n.Discover(ctx)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "discovery failed: %v", err)
	}
	return peerListResult{Peers: peers}, nil
}

// peerResolveParams are the parameters of _intercom/peer/resolve.
type peerResolveParams struct {
	Alias   string `json:"alias,omitempty"`
	Session string `json:"sessionId,omitempty"`
}

// peerResolveResult is the response to _intercom/peer/resolve.
type peerResolveResult struct {
	Peer protocol.PeerInfo `json:"peer"`
}

// peerResolve resolves an alias or session identifier.
func (s *Server) peerResolve(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p peerResolveParams
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
	return peerResolveResult{Peer: peer}, nil
}

// peerCapabilitiesParams are the parameters of _intercom/peer/capabilities.
type peerCapabilitiesParams struct {
	Session string `json:"sessionId"`
}

// peerCapabilitiesResult is the response to _intercom/peer/capabilities.
type peerCapabilitiesResult struct {
	SessionID    string                `json:"sessionId"`
	Capabilities []protocol.Capability `json:"capabilities"`
}

// peerCapabilities retrieves a peer's effective capabilities.
func (s *Server) peerCapabilities(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p peerCapabilitiesParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	caps, err := s.n.Capabilities(ctx, p.Session)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", p.Session)
	}
	return peerCapabilitiesResult{SessionID: p.Session, Capabilities: caps}, nil
}

// peerPresenceParams are the parameters of _intercom/peer/presence.
type peerPresenceParams struct {
	Session string `json:"sessionId"`
}

// peerPresenceResult is the response to _intercom/peer/presence.
type peerPresenceResult struct {
	SessionID string                 `json:"sessionId"`
	Presence  protocol.PresenceState `json:"presence"`
}

// peerPresence queries a peer's last observed presence.
func (s *Server) peerPresence(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p peerPresenceParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	presence, err := s.n.Presence(ctx, p.Session)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", p.Session)
	}
	return peerPresenceResult{SessionID: p.Session, Presence: presence}, nil
}

// messageSendParams are the parameters of _intercom/message/send.
type messageSendParams struct {
	CommandID     string               `json:"commandId"`
	To            json.RawMessage      `json:"to"`
	Body          protocol.MessageBody `json:"body"`
	Delivery      protocol.Delivery    `json:"delivery"`
	ReplyTo       string               `json:"replyTo,omitempty"`
	CorrelationID string               `json:"correlationId,omitempty"`
	TTLSeconds    int                  `json:"ttlSeconds,omitempty"`
	Metadata      map[string]any       `json:"metadata,omitempty"`
}

// messageSendResult is the response to _intercom/message/send.
//
// It reports durable admission, not recipient processing.
type messageSendResult struct {
	CommandID string                 `json:"commandId"`
	MessageID string                 `json:"messageId"`
	Status    protocol.ReceiptStatus `json:"status"`
}

// messageSend submits a peer message.
func (s *Server) messageSend(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p messageSendParams
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
	return messageSendResult{CommandID: res.CommandID, MessageID: res.MessageID, Status: res.Status}, nil
}

// messageReplyParams are the parameters of _intercom/message/reply.
type messageReplyParams struct {
	CommandID     string `json:"commandId"`
	MessageID     string `json:"messageId"`
	Text          string `json:"text"`
	CorrelationID string `json:"correlationId,omitempty"`
}

// messageReplyResult is the response to _intercom/message/reply.
type messageReplyResult struct {
	CommandID string                 `json:"commandId"`
	MessageID string                 `json:"messageId"`
	Status    protocol.ReceiptStatus `json:"status"`
}

// messageReply sends a correlated reply.
func (s *Server) messageReply(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p messageReplyParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	res, err := s.n.Reply(ctx, s.sender(), p.MessageID, p.Text, p.CorrelationID)
	if err != nil {
		return nil, toRPCError(err)
	}
	return messageReplyResult{CommandID: res.CommandID, MessageID: res.MessageID, Status: res.Status}, nil
}

// messageStatusParams are the parameters of _intercom/message/status.
type messageStatusParams struct {
	MessageID string `json:"messageId"`
}

// messageStatusResult is the response to _intercom/message/status.
type messageStatusResult struct {
	MessageID string                 `json:"messageId"`
	Status    protocol.ReceiptStatus `json:"status"`
	Receipts  []protocol.Receipt     `json:"receipts"`
}

// messageStatus reads delivery receipts.
func (s *Server) messageStatus(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p messageStatusParams
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
	return messageStatusResult{
		MessageID: p.MessageID,
		Status:    receipts[len(receipts)-1].Status,
		Receipts:  receipts,
	}, nil
}

// sessionStateParams are the parameters of _intercom/session/state.
type sessionStateParams struct {
	Session string `json:"sessionId"`
}

// sessionStateResult is the response to _intercom/session/state.
type sessionStateResult struct {
	SessionID string                `json:"sessionId"`
	State     protocol.SessionState `json:"state"`
	Recovery  string                `json:"recovery"`
	Sequence  uint64                `json:"sequence"`
}

// sessionState queries a session's state and recovery classification.
func (s *Server) sessionState(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p sessionStateParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	snap, err := s.n.SessionState(ctx, p.Session)
	if err != nil {
		return nil, toRPCError(err)
	}
	return sessionStateResult{
		SessionID: snap.SessionID,
		State:     snap.State,
		Recovery:  snap.Recovery,
		Sequence:  snap.Sequence,
	}, nil
}

// sessionSnapshotResult is the response to _intercom/session/snapshot.
type sessionSnapshotResult struct {
	Snapshot protocol.Snapshot `json:"snapshot"`
}

// sessionSnapshot obtains a coherent state projection.
func (s *Server) sessionSnapshot(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p sessionStateParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	snap, err := s.n.Snapshot(ctx, p.Session)
	if err != nil {
		return nil, toRPCError(err)
	}
	return sessionSnapshotResult{Snapshot: snap}, nil
}

// sessionReplayParams are the parameters of _intercom/session/replay.
type sessionReplayParams struct {
	Session string `json:"sessionId"`
	Cursor  uint64 `json:"cursor,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

// sessionReplayResult is the response to _intercom/session/replay.
//
// CursorGap is set when the requested history was compacted away, so the
// client knows to recover through a snapshot rather than assume continuity.
type sessionReplayResult struct {
	SessionID         string                   `json:"sessionId"`
	Events            []protocol.EventEnvelope `json:"events"`
	Cursor            uint64                   `json:"cursor"`
	CursorGap         bool                     `json:"cursorGap,omitempty"`
	EarliestAvailable uint64                   `json:"earliestAvailable,omitempty"`
	SnapshotPath      string                   `json:"snapshotPath,omitempty"`
}

// sessionReplay replays the event stream.
func (s *Server) sessionReplay(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p sessionReplayParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	res, err := s.n.Replay(ctx, p.Session, p.Cursor, p.Limit)
	if err != nil {
		return nil, toRPCError(err)
	}
	return sessionReplayResult{
		SessionID:         p.Session,
		Events:            res.Events,
		Cursor:            res.Cursor,
		CursorGap:         res.Gap,
		EarliestAvailable: res.EarliestAvailable,
		SnapshotPath:      res.SnapshotPath,
	}, nil
}

// sessionSubscribeParams are the parameters of _intercom/session/subscribe.
type sessionSubscribeParams struct {
	Session string `json:"sessionId"`
}

// sessionSubscribeResult is the response to _intercom/session/subscribe.
type sessionSubscribeResult struct {
	SessionID  string `json:"sessionId"`
	Subscribed bool   `json:"subscribed"`
}

// sessionSubscribe subscribes to subsequent changes.
func (s *Server) sessionSubscribe(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p sessionSubscribeParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if p.Session == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "sessionId is required")
	}
	s.n.Subscribe(p.Session)
	return sessionSubscribeResult{SessionID: p.Session, Subscribed: true}, nil
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

// ensure the adapter package is referenced so the capability vocabulary stays
// aligned with the adapter contract.
var _ = adapter.ModeNative
var _ = fmt.Sprintf
