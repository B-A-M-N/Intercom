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
	"time"

	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/remote"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/routing"
	"github.com/B-A-M-N/intercom/internal/store"
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
	// delegationRole is "owner" for registered connections and "attached"
	// for scoped MCP clients. Empty means owner (legacy direct callers).
	delegationRole string
	// delegationToken is the scoped token for attached connections. It is
	// re-verified on every request so expiry/revocation apply to live
	// connections, not just at attach time.
	delegationToken string
	// emit sends a server-initiated notification to the connected client.
	// It is nil when the server is used without a transport.
	emit func(ctx context.Context, method string, params any)
}

// SetDelegation marks this server's connection role. Attached connections
// get a strict method allowlist and per-request delegation re-verification.
func (s *Server) SetDelegation(role, token string) {
	s.delegationRole = role
	s.delegationToken = token
}

// attachedAllowlist is the strict method set for scope=mcp clients.
// Notably absent: session/register, session/delegate, session/attach,
// permission/*, conversation/create|close, remote/*.
var attachedAllowlist = map[string]bool{
	"initialize": true,
	protocol.MethodHello: true,
	protocol.MethodPeerList: true,
	protocol.MethodPeerResolve: true,
	protocol.MethodPeerCapabilities: true,
	protocol.MethodPeerPresence: true,
	protocol.MethodPeerRoute: true,
	protocol.MethodConversationHistory: true,
	protocol.MethodMessageSend: true,
	protocol.MethodMessageStatus: true,
	protocol.MethodMessageReply: true,
	protocol.MethodSessionState: true,
	protocol.MethodSessionSnapshot: true,
	protocol.MethodSessionReplay: true,
	protocol.MethodSessionSubscribe: true,
	protocol.MethodMessageListPending: true,
	protocol.MethodMessageReport: true,
	protocol.MethodMessageRenew: true,
	protocol.MethodHeartbeat: true,
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

// HelloResult is the response to the extension's hello method.
type HelloResult struct {
	SpecVersion  string `json:"specVersion"`
	SpecRevision string `json:"specRevision"`
	// Namespace is the reverse-DNS namespace this extension is published
	// under, so a client knows which implementer owns it.
	Namespace string `json:"namespace"`
	// Prefix is the underscore-prefixed method prefix.
	Prefix       string                `json:"prefix"`
	Capabilities []protocol.Capability `json:"capabilities"`
	Profiles     []string              `json:"profiles"`
	Qualifications []protocol.CapabilityQualification `json:"qualifications,omitempty"`
	Meta         map[string]any        `json:"_meta,omitempty"`
}

// Capability advertisement for the ACP agent capabilities block.
//
// This endpoint is an Intercom extension/control-plane participant, not a
// complete ACP agent: it does not implement session/new, session/load, or
// session/prompt. It must not advertise capabilities it does not have.
func (s *Server) initializeResult() InitializeResult {
	return InitializeResult{
		ProtocolVersion: ProtocolVersion,
		AgentCapabilities: map[string]any{
			"loadSession": false,
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
				"namespace":    protocol.ExtensionNamespace,
				"prefix":       protocol.ExtensionPrefix,
				"role":         "extension/control-plane participant; standard ACP session lifecycle is not implemented",
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
	if s.delegationRole == "attached" {
		if s.peerSession == "" || s.delegationToken == "" {
			return nil, protocol.Errorf(protocol.CodeForbidden, "attached client has no delegation")
		}
		if _, err := s.n.VerifyDelegationDetailed(ctx, s.peerSession, s.delegationToken); err != nil {
			return nil, protocol.Errorf(protocol.CodeForbidden, "delegation expired, revoked, or superseded")
		}
		if !attachedAllowlist[method] {
			return nil, protocol.Errorf(protocol.CodeForbidden, "attached MCP client (scope=mcp) may not invoke %q", method)
		}
	}
	switch method {
	case "initialize":
		return s.initializeResult(), nil
	case protocol.MethodHello:
		return s.hello(ctx, params)
	case protocol.MethodPeerList:
		return s.peerList(ctx, params)
	case protocol.MethodPeerResolve:
		return s.peerResolve(ctx, params)
	case protocol.MethodPeerCapabilities:
		return s.peerCapabilities(ctx, params)
	case protocol.MethodPeerPresence:
		return s.peerPresence(ctx, params)
	case protocol.MethodPeerRoute:
		return s.peerRoute(ctx, params)
	case protocol.MethodConversationCreate:
		return s.conversationCreate(ctx, params)
	case protocol.MethodConversationHistory:
		return s.conversationHistory(ctx, params)
	case protocol.MethodConversationClose:
		return s.conversationClose(ctx, params)
	case protocol.MethodRemoteEnroll:
		return s.remoteEnroll(ctx, params)
	case protocol.MethodRemoteList:
		return s.remoteList(ctx, params)
	case protocol.MethodRemoteSend:
		return s.remoteSend(ctx, params)
	case protocol.MethodRemoteStatus:
		return s.remoteStatus(ctx, params)
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
	case protocol.MethodSessionDelegation:
		return s.sessionDelegate(ctx, params)
	case protocol.MethodSessionAttach:
		return s.sessionAttach(ctx, params)
	case protocol.MethodMessageListPending:
		return s.messageListPending(ctx, params)
	case protocol.MethodMessageReport:
		return s.messageReport(ctx, params)
	case protocol.MethodMessageRenew:
		return s.messageRenew(ctx, params)
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

// HelloParams are the parameters of the extension's hello method.
//
// A client states the features it requires, so the host can fail the
// negotiation safely rather than accepting a connection that cannot work
// (ICSP-001 section 8.3).
type HelloParams struct {
	// Require lists the capability identifiers the client cannot operate
	// without.
	Require []protocol.CapabilityID `json:"require,omitempty"`
	// Profiles lists the conformance profiles the client needs.
	Profiles []string `json:"profiles,omitempty"`
	// ClientInfo names the client.
	ClientInfo map[string]any `json:"clientInfo,omitempty"`
}

// hello negotiates protocol features.
//
// Negotiation fails safely when a required feature is unsupported: the
// request is refused with the unsupported code and the list of what was
// missing, rather than the connection being accepted and failing later.
func (s *Server) hello(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p HelloParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}

	advertised := s.n.CapabilityAdvertisement()

	// Build the supported set, so a requirement can be checked against
	// what is actually available rather than against what is named.
	supported := make(map[protocol.CapabilityID]protocol.SupportLevel, len(advertised))
	for _, c := range advertised {
		supported[c.ID] = c.Support
	}

	var missing []protocol.CapabilityID
	for _, want := range p.Require {
		level, ok := supported[want]
		if !ok || level == protocol.SupportUnsupported {
			missing = append(missing, want)
		}
	}

	// The profiles a client requires must also be available.
	available := map[string]bool{}
	for _, profile := range s.n.SupportedProfiles() {
		available[profile] = true
	}
	var missingProfiles []string
	for _, want := range p.Profiles {
		if !available[want] {
			missingProfiles = append(missingProfiles, want)
		}
	}

	if len(missing) > 0 || len(missingProfiles) > 0 {
		return nil, protocol.Errorf(protocol.CodeUnsupported,
			"the requested features are not available: capabilities %v, profiles %v",
			missing, missingProfiles).
			WithData(map[string]any{
				"missingCapabilities": missing,
				"missingProfiles":     missingProfiles,
				"supported":           advertised,
			})
	}

	return s.helloResult(advertised), nil
}

// helloResult builds the successful negotiation response.
func (s *Server) helloResult(advertised []protocol.Capability) HelloResult {
	return HelloResult{
		SpecVersion:  protocol.SpecVersion,
		SpecRevision: protocol.SpecRevision,
		Namespace:    protocol.ExtensionNamespace,
		Prefix:       protocol.ExtensionPrefix,
		Capabilities: advertised,
		Profiles:     s.n.SupportedProfiles(),
		Qualifications: protocol.HarnessQualificationMatrix(),
	}
}

// PeerListParams are the parameters of _intercom/peer/list.
type PeerListParams struct {
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// PeerListResult is the response to _intercom/peer/list.
type PeerListResult struct {
	Peers      []protocol.PeerInfo `json:"peers"`
	NextCursor string              `json:"nextCursor,omitempty"`
	HasMore    bool                `json:"hasMore,omitempty"`
}

// peerList discovers visible peers with cursor pagination.
func (s *Server) peerList(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PeerListParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	peers, next, err := s.n.DiscoverForPage(ctx, s.sender(), p.Limit, p.Cursor)
	if err != nil {
		return nil, toRPCError(err)
	}
	// Probe hasMore without leaking: ask for one more visible peer.
	more := false
	if next != "" {
		if extra, _, err := s.n.DiscoverForPage(ctx, s.sender(), 1, next); err == nil && len(extra) > 0 {
			more = true
		}
	}
	return PeerListResult{Peers: peers, NextCursor: next, HasMore: more}, nil
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
	peer, err := s.n.ResolveFor(ctx, s.sender(), target)
	if err != nil {
		if errors.Is(err, node.ErrNotFound) {
			return nil, protocol.Errorf(protocol.CodeInvalidParams, "no session resolves from %q", target)
		}
		return nil, toRPCError(err)
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
	if err := s.n.AuthorizeObserve(ctx, s.sender(), p.Session); err != nil {
		return nil, toRPCError(err)
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
	if err := s.n.AuthorizeObserve(ctx, s.sender(), p.Session); err != nil {
		return nil, toRPCError(err)
	}
	presence, err := s.n.Presence(ctx, p.Session)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown session %q", p.Session)
	}
	return PeerPresenceResult{SessionID: p.Session, Presence: presence}, nil
}

// PeerRouteParams are the parameters of _intercom/peer/route.
type PeerRouteParams struct {
	Target       string `json:"target,omitempty"`
	Harness      string `json:"harness,omitempty"`
	Capability   string `json:"capability,omitempty"`
	Availability string `json:"availability,omitempty"`
}

// PeerRouteResult is the response to _intercom/peer/route.
type PeerRouteResult struct {
	TargetSession string `json:"targetSession"`
	Via           string `json:"via"`
	Explain       string `json:"explain"`
}

// peerRoute returns an explainable deterministic routing decision.
func (s *Server) peerRoute(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PeerRouteParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	var sel *routing.Selector
	if p.Harness != "" || p.Capability != "" || p.Availability != "" {
		sel = &routing.Selector{Harness: p.Harness, Capability: protocol.CapabilityID(p.Capability), Availability: protocol.PresenceState(p.Availability)}
	}
	route, err := s.n.Route(ctx, s.sender(), p.Target, sel)
	if err != nil {
		return nil, toRPCError(err)
	}
	return PeerRouteResult{TargetSession: route.TargetSession, Via: route.Via, Explain: route.Explain}, nil
}

// ConversationCreateParams are the parameters of _intercom/conversation/create.
type ConversationCreateParams struct {
	ID           string   `json:"conversationId,omitempty"`
	Participants []string `json:"participants"`
	Title        string   `json:"title,omitempty"`
}

// ConversationCreateResult is the response to conversation/create.
type ConversationCreateResult struct {
	ConversationID string `json:"conversationId"`
	Status         string `json:"status"`
}

func (s *Server) conversationCreate(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p ConversationCreateParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if len(p.Participants) == 0 {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "participants are required")
	}
	conv, err := s.n.CreateConversation(ctx, s.sender(), p.ID, p.Participants, p.Title)
	if err != nil {
		return nil, toRPCError(err)
	}
	return ConversationCreateResult{ConversationID: conv.ID, Status: conv.Status}, nil
}

// ConversationHistoryParams are the parameters of _intercom/conversation/history.
type ConversationHistoryParams struct {
	ConversationID string `json:"conversationId"`
	Limit          int    `json:"limit,omitempty"`
	Cursor         string `json:"cursor,omitempty"`
}

// ConversationHistoryResult is the response to conversation/history.
type ConversationHistoryResult struct {
	ConversationID string             `json:"conversationId"`
	Messages       []protocol.Message `json:"messages"`
	NextCursor     string             `json:"nextCursor,omitempty"`
	HasMore        bool               `json:"hasMore,omitempty"`
}

func (s *Server) conversationHistory(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p ConversationHistoryParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	msgs, err := s.n.ConversationHistoryPage(ctx, s.sender(), p.ConversationID, p.Limit, p.Cursor)
	if err != nil {
		return nil, toRPCError(err)
	}
	next := ""
	if len(msgs) > 0 {
		next = msgs[len(msgs)-1].MessageID
	}
	more := false
	if next != "" {
		if extra, err := s.n.ConversationHistoryPage(ctx, s.sender(), p.ConversationID, 1, next); err == nil && len(extra) > 0 {
			more = true
		}
	}
	return ConversationHistoryResult{ConversationID: p.ConversationID, Messages: msgs, NextCursor: next, HasMore: more}, nil
}

// ConversationCloseParams are the parameters of _intercom/conversation/close.
type ConversationCloseParams struct {
	ConversationID string `json:"conversationId"`
}

func (s *Server) conversationClose(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p ConversationCloseParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if err := s.n.CloseConversation(ctx, s.sender(), p.ConversationID); err != nil {
		return nil, toRPCError(err)
	}
	return map[string]any{"conversationId": p.ConversationID, "status": "closed"}, nil
}

// RemoteEnrollParams enrolls a remote host. Fingerprint is the pinned
// TLS identity; no bearer secrets are accepted or logged.
type RemoteEnrollParams struct {
	HostID      string `json:"hostId"`
	Endpoint    string `json:"endpoint"`
	Fingerprint string `json:"fingerprint"`
}

func (s *Server) remoteEnroll(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p RemoteEnrollParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if p.HostID == "" || p.Endpoint == "" || p.Fingerprint == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "hostId, endpoint and fingerprint are required")
	}
	// Owner/operator check: only the local operator may enroll hosts.
	if err := s.n.AuthorizeRemoteAdmin(ctx, s.sender()); err != nil {
		return nil, toRPCError(err)
	}
	// Strict enrollment: TLS handshake + pinned fingerprint + hostId
	// must all verify; any other response fails.
	if _, err := remote.Enroll(ctx, p.Endpoint, s.n.RemoteIdentity(), p.HostID, p.Fingerprint); err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "remote enroll failed: %v", err)
	}
	if err := s.n.Store().EnrollHost(ctx, p.HostID, p.Endpoint, p.Fingerprint); err != nil {
		return nil, toRPCError(err)
	}
	_ = s.n.Store().Audit(ctx, s.sender().PrincipalID, p.HostID, "remote.enrolled", p.Endpoint)
	return map[string]any{"hostId": p.HostID, "endpoint": p.Endpoint, "trust": "trusted"}, nil
}

func (s *Server) remoteList(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	if err := s.n.AuthorizeRemoteAdmin(ctx, s.sender()); err != nil {
		return nil, toRPCError(err)
	}
	hosts, err := s.n.Store().ListRemoteHosts(ctx)
	if err != nil {
		return nil, toRPCError(err)
	}
	return hosts, nil
}

// RemoteSendParams sends to a remote host session.
type RemoteSendParams struct {
	RemoteHost    string `json:"remoteHost"`
	TargetSession string `json:"targetSession"`
	CommandID     string `json:"commandId"`
	Text          string `json:"text"`
	Mode          string `json:"mode,omitempty"`
	CorrelationID string `json:"correlationId,omitempty"`
	TTLSeconds    int    `json:"ttlSeconds,omitempty"`
}

func (s *Server) remoteSend(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p RemoteSendParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	mode := protocol.DeliveryMode(p.Mode)
	if mode == "" {
		mode = protocol.DeliveryNotify
	}
	result, err := s.n.SendRemote(ctx, s.sender(), node.RemoteSendRequest{
		RemoteHost: p.RemoteHost, TargetSession: p.TargetSession, CommandID: p.CommandID,
		Body: protocol.MessageBody{Type: "text", Text: p.Text}, Mode: mode,
		CorrelationID: p.CorrelationID, TTLSeconds: p.TTLSeconds,
	})
	if err != nil {
		return nil, toRPCError(err)
	}
	return result, nil
}

// RemoteStatusParams checks outbound transfer status.
type RemoteStatusParams struct {
	MessageID string `json:"messageId"`
}

func (s *Server) remoteStatus(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p RemoteStatusParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	// Per-transfer ownership: only the original sender session (or
	// same-principal operator) may read its status.
	if err := s.n.AuthorizeRemoteAdmin(ctx, s.sender()); err != nil {
		// Non-operator fallback: check exact sender session.
		transfer, err := s.n.Store().GetRemoteTransfer(ctx, p.MessageID)
		if err != nil {
			return nil, toRPCError(err)
		}
		if transfer.SenderSession != s.sender().SessionID {
			return nil, toRPCError(protocol.Errorf(protocol.CodeForbidden, "transfer belongs to another sender"))
		}
		return transfer, nil
	}
	transfer, err := s.n.Store().GetRemoteTransfer(ctx, p.MessageID)
	if err != nil {
		return nil, toRPCError(err)
	}
	return transfer, nil
}

// MessageSendParams are the parameters of _intercom/message/send.
type MessageSendParams struct {
	CommandID     string               `json:"commandId"`
	To            json.RawMessage      `json:"to"`
	Body          protocol.MessageBody `json:"body"`
	Delivery      protocol.Delivery    `json:"delivery"`
	ReplyTo       string               `json:"replyTo,omitempty"`
	CorrelationID string               `json:"correlationId,omitempty"`
	ConversationID string              `json:"conversationId,omitempty"`
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
		ConversationID: p.ConversationID,
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
	res, err := s.n.ReplyWithCommandID(ctx, s.sender(), p.MessageID, p.Text, p.CorrelationID, p.CommandID)
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
	receipts, err := s.n.MessageStatusFor(ctx, s.sender(), p.MessageID)
	if err != nil {
		return nil, toRPCError(err)
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
	// StateSource records whether the state came from the harness itself or
	// was inferred by the adapter (ICSP-001 section 7.3).
	StateSource string `json:"stateSource,omitempty"`
	Recovery    string `json:"recovery"`
	Sequence    uint64 `json:"sequence"`
}

// sessionState queries a session's state and recovery classification.
func (s *Server) sessionState(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p SessionStateParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if err := s.n.AuthorizeObserve(ctx, s.sender(), p.Session); err != nil {
		return nil, toRPCError(err)
	}
	snap, err := s.n.SessionState(ctx, p.Session)
	if err != nil {
		return nil, toRPCError(err)
	}
	return SessionStateResult{
		SessionID:   snap.SessionID,
		State:       snap.State,
		StateSource: snap.StateSource,
		Recovery:    snap.Recovery,
		Sequence:    snap.Sequence,
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
	if err := s.n.AuthorizeObserve(ctx, s.sender(), p.Session); err != nil {
		return nil, toRPCError(err)
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
	if err := s.n.AuthorizeObserve(ctx, s.sender(), p.Session); err != nil {
		return nil, toRPCError(err)
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
	if err := s.n.AuthorizeObserve(ctx, s.sender(), p.Session); err != nil {
		return nil, toRPCError(err)
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
	TrustDomain   string                  `json:"trustDomain,omitempty"`
	Tags          []string                `json:"tags,omitempty"`
	Capabilities  []protocol.Capability   `json:"capabilities"`
	DeliveryModes []protocol.DeliveryMode `json:"deliveryModes"`
	// SessionSecret is the client-held resume credential. Required to
	// resume an existing session; issued (once) on first creation.
	SessionSecret string `json:"sessionSecret,omitempty"`
}

// sessionRegisterResult is the response to _intercom/session/register.
type sessionRegisterResult struct {
	SessionID string `json:"sessionId"`
	Registered bool   `json:"registered"`
	// SessionSecret is present only on first creation when the client
	// did not supply one. The client must persist it; resume without
	// it is forbidden.
	SessionSecret string `json:"sessionSecret,omitempty"`
}

// sessionRegister records an out-of-process adapter's session.
//
// The adapter's capability claims are stored as given. The core does not
// upgrade them, and an adapter that claims a capability it cannot implement
// is caught by the conformance tests rather than by the core guessing.
// sessionDelegate mints a scoped attached-client token for the bound session.
func (s *Server) sessionDelegate(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p struct {
		Session string `json:"sessionId"`
	}
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	token, err := s.n.IssueDelegationFor(ctx, s.sender(), p.Session)
	if err != nil {
		return nil, toRPCError(err)
	}
	return map[string]any{"delegationToken": token, "sessionId": p.Session, "scope": "mcp"}, nil
}

// sessionAttach verifies a delegation token and binds this connection as an
// attached client. It MUST NOT update instance identity, capabilities, or
// delivery modes - those belong to the owning registration.
func (s *Server) sessionAttach(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p struct {
		Session string `json:"sessionId"`
		Token   string `json:"delegationToken"`
	}
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if p.Session == "" || p.Token == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "sessionId and delegationToken are required")
	}
	// Ensure the session exists (no registration side effect).
	if _, err := s.n.Store().GetSession(ctx, p.Session); err != nil {
		return nil, toRPCError(err)
	}
	if err := s.n.VerifyDelegationFor(ctx, p.Session, p.Token); err != nil {
		return nil, toRPCError(err)
	}
	return map[string]any{"attached": true, "sessionId": p.Session}, nil
}

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

	// Client-supplied principalId/hostId are never authentication evidence.
	// Identity comes from the authenticated transport/server binding; the
	// store rejects resume under a different principal and requires the
	// client-held session secret for resume (same UID is not enough).
	peer := protocol.PeerInfo{
		SessionID:      p.SessionID,
		InstanceID:     p.InstanceID,
		Alias:          p.Alias,
		Harness:        p.Harness,
		HarnessVersion: p.Version,
		HostID:         s.n.HostID(),
		PrincipalID:    s.peerPrincipal,
		Capabilities:   p.Capabilities,
		Delivery:       p.DeliveryModes,
		Presence:       protocol.PresenceOnline,
		TrustDomain:    "local:" + s.n.HostID(),
		Tags:           p.Tags,
		SessionSecret:  p.SessionSecret,
	}
	if s.peerPrincipal == "" {
		peer.PrincipalID = s.n.PrincipalID()
	}
	issued, err := s.n.RegisterAdapterSessionWithSecret(ctx, peer)
	if err != nil {
		return nil, toRPCError(err)
	}
	return sessionRegisterResult{SessionID: p.SessionID, Registered: true, SessionSecret: issued}, nil
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
	msgs, err := s.n.PendingInboxFor(ctx, s.sender(), p.Session, p.Limit)
	if err != nil {
		return nil, toRPCError(err)
	}
	return messageListPendingResult{Messages: msgs}, nil
}

// messageReportParams are the parameters of _intercom/message/report.
type messageReportParams struct {
	MessageID       string                 `json:"messageId"`
	Status          protocol.ReceiptStatus `json:"status"`
	Detail          string                 `json:"detail,omitempty"`
	ClaimToken      string                 `json:"claimToken,omitempty"`
	ClaimGeneration int64                  `json:"claimGeneration,omitempty"`
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
	if err := s.n.ReportDeliveryFor(ctx, s.sender(), p.MessageID, p.Status, p.Detail, p.ClaimToken, p.ClaimGeneration); err != nil {
		return nil, toRPCError(err)
	}
	return messageReportResult{MessageID: p.MessageID, Status: p.Status}, nil
}

// messageRenewParams are the parameters of _intercom/message/renew.
type messageRenewParams struct {
	MessageID       string `json:"messageId"`
	ClaimToken      string `json:"claimToken,omitempty"`
	ClaimGeneration int64  `json:"claimGeneration,omitempty"`
}

// messageRenew extends the caller's claim lease for a long harness operation.
func (s *Server) messageRenew(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p messageRenewParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if err := s.n.RenewClaimFor(ctx, s.sender(), p.MessageID, p.ClaimToken, p.ClaimGeneration); err != nil {
		return nil, toRPCError(err)
	}
	return map[string]any{"messageId": p.MessageID, "renewed": true}, nil
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
	// A heartbeat may only report the caller's own session. A supplied
	// sessionId is ignored: it is not authentication evidence.
	session := s.peerSession
	if session == "" {
		return nil, protocol.Errorf(protocol.CodeUnauthorized, "the caller has no bound session")
	}
	if err := s.n.AuthorizeOwn(ctx, s.sender(), session); err != nil {
		return nil, toRPCError(err)
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
	// ExpiresAt is an optional RFC3339 expiry; empty means no expiry.
	ExpiresAt string `json:"expiresAt,omitempty"`
	// TTLSeconds is an alternative expiry expressed as seconds from now.
	TTLSeconds int `json:"ttlSeconds,omitempty"`
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
	var expires time.Time
	if p.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339, p.ExpiresAt)
		if err != nil {
			return nil, protocol.Errorf(protocol.CodeInvalidParams, "expiresAt must be RFC3339: %v", err)
		}
		expires = t
	} else if p.TTLSeconds > 0 {
		expires = time.Now().UTC().Add(time.Duration(p.TTLSeconds) * time.Second)
	}
	if err := s.n.GrantPermissionWithExpiry(ctx, s.sender(), p.OwnerSession, p.PeerSession, role, expires); err != nil {
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
// Authorization is against the grant subject (peer) and owner, not mere
// local session existence: a peer may list its own grants, and a visible
// local session may be listed by same-principal or granted callers.
// Remote-qualified grant subjects have no local session record, so the
// absence of a record must not skip the check (self-only there).
func (s *Server) permissionList(ctx context.Context, params json.RawMessage) (any, *protocol.RPCError) {
	var p PermissionListParams
	if err := decodeParams(params, &p); err != nil {
		return nil, err
	}
	if p.PeerSession == "" {
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "peerSession is required")
	}
	// Self-listing is always allowed (the caller is the grant subject).
	if s.sender().SessionID == p.PeerSession {
		perms, err := s.n.ListPermissions(ctx, p.PeerSession)
		if err != nil {
			return nil, toRPCError(err)
		}
		return PermissionListResult{Permissions: perms}, nil
	}
	if _, err := s.n.Store().GetSession(ctx, p.PeerSession); err == nil {
		if err := s.n.AuthorizeObserve(ctx, s.sender(), p.PeerSession); err != nil {
			return nil, toRPCError(err)
		}
		perms, err := s.n.ListPermissions(ctx, p.PeerSession)
		if err != nil {
			return nil, toRPCError(err)
		}
		return PermissionListResult{Permissions: perms}, nil
	}
	return nil, protocol.Errorf(protocol.CodeForbidden, "only the grant subject or a visible session may list permissions")
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
	case errors.Is(err, node.ErrForbidden), errors.Is(err, store.ErrForbidden):
		return protocol.Errorf(protocol.CodeForbidden, "%v", err)
	case errors.Is(err, store.ErrConflict):
		return protocol.Errorf(protocol.CodeConflict, "%v", err)
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
