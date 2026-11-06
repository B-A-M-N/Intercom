package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

// JSON-RPC 2.0 error codes used by Intercom.
//
// Standard codes are used where they apply; the -32000..-32099 range is
// reserved by JSON-RPC for implementation-defined server errors.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603

	// CodeIntercomError is the base for Intercom-specific failures.
	CodeIntercomError = -32000
	// CodeUnauthorized means the caller lacks authority for the operation.
	CodeUnauthorized = -32001
	// CodeForbidden means policy refuses the operation for the target.
	CodeForbidden = -32002
	// CodeConflict means a commandId was reused with a conflicting request.
	CodeConflict = -32003
	// CodeUnsupported means the requested capability is not available.
	CodeUnsupported = -32004
	// CodeLimitExceeded means a resource bound was hit; backpressure applied.
	CodeLimitExceeded = -32005
	// CodeExpired means the message expired before delivery completed.
	CodeExpired = -32006
	// CodeCursorGap means requested event history was compacted away.
	CodeCursorGap = -32007
)

// RPCError is a JSON-RPC 2.0 error object.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("intercom rpc error %d: %s", e.Code, e.Message)
}

// Errorf builds an RPCError with a formatted message.
func Errorf(code int, format string, args ...any) *RPCError {
	return &RPCError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// WithData attaches structured data to an RPCError.
func (e *RPCError) WithData(v any) *RPCError {
	b, err := json.Marshal(v)
	if err != nil {
		return e
	}
	e.Data = b
	return e
}

// AsRPCError extracts an *RPCError from err, if present.
func AsRPCError(err error) (*RPCError, bool) {
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr, true
	}
	return nil, false
}

// RPCRequest is a JSON-RPC 2.0 request object.
type RPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// RPCResponse is a JSON-RPC 2.0 response object.
type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// RPCNotification is a JSON-RPC 2.0 notification (a request without an id).
type RPCNotification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether a raw request frame carries no id and is
// therefore a notification rather than a call expecting a response.
func IsNotification(raw json.RawMessage) bool {
	var probe struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	return len(probe.ID) == 0 || string(probe.ID) == "null"
}

// ExtensionNamespace is the ACP extension namespace for this implementation.
//
// Section 8.1 requires a collision-resistant reverse-DNS namespace
// controlled by the implementer to be selected before a stable ACP
// extension is published. The specification's own examples use the
// provisional "_intercom/" prefix; this implementation uses a reverse-DNS
// namespace instead, so the published extension cannot collide with
// another implementer's.
const ExtensionNamespace = "dev.bamn.intercom"

// ExtensionPrefix is the underscore-prefixed method prefix that ACP uses
// for extension methods.
//
// ACP routes any method beginning with an underscore to the extension
// handler, and the reference client prefixes extension methods with an
// underscore on the wire.
const ExtensionPrefix = "_" + ExtensionNamespace + "/"

// Method names for the Intercom ACP extension namespace (ICSP-001 section 8.2).
const (
	MethodHello            = ExtensionPrefix + "hello"
	MethodPeerList         = ExtensionPrefix + "peer/list"
	MethodPeerResolve      = ExtensionPrefix + "peer/resolve"
	MethodPeerCapabilities = ExtensionPrefix + "peer/capabilities"
	MethodPeerPresence     = ExtensionPrefix + "peer/presence"
	MethodPeerRoute        = ExtensionPrefix + "peer/route"
	MethodConversationCreate = ExtensionPrefix + "conversation/create"
	MethodConversationHistory = ExtensionPrefix + "conversation/history"
	MethodConversationClose  = ExtensionPrefix + "conversation/close"
	MethodRemoteEnroll       = ExtensionPrefix + "remote/enroll"
	MethodRemoteList         = ExtensionPrefix + "remote/list"
	MethodRemoteSend         = ExtensionPrefix + "remote/send"
	MethodRemoteStatus       = ExtensionPrefix + "remote/status"
	MethodMessageSend      = ExtensionPrefix + "message/send"
	MethodMessageStatus    = ExtensionPrefix + "message/status"
	MethodMessageReply     = ExtensionPrefix + "message/reply"
	MethodSessionState     = ExtensionPrefix + "session/state"
	MethodSessionSnapshot  = ExtensionPrefix + "session/snapshot"
	MethodSessionReplay    = ExtensionPrefix + "session/replay"
	MethodSessionSubscribe = ExtensionPrefix + "session/subscribe"
	MethodEvent            = ExtensionPrefix + "event"

	// Methods below support the adapter contract in section 12. They let an
	// out-of-process adapter register its honest capability set, claim the
	// messages waiting in its own inbox, and report what it actually did.
	MethodSessionRegister    = ExtensionPrefix + "session/register"
	MethodSessionDelegation   = ExtensionPrefix + "session/delegate"
	MethodSessionAttach       = ExtensionPrefix + "session/attach"
	MethodMessageListPending = ExtensionPrefix + "message/listPending"
	MethodMessageReport      = ExtensionPrefix + "message/report"
	MethodMessageRenew       = ExtensionPrefix + "message/renew"
	MethodHeartbeat          = ExtensionPrefix + "heartbeat"
	MethodPermissionGrant    = ExtensionPrefix + "permission/grant"
	MethodPermissionRevoke   = ExtensionPrefix + "permission/revoke"
	MethodPermissionList     = ExtensionPrefix + "permission/list"
)

// IntercomMethodPrefix identifies methods belonging to the Intercom
// extension.
const IntercomMethodPrefix = ExtensionPrefix

// IsIntercomMethod reports whether method belongs to the Intercom extension.
func IsIntercomMethod(method string) bool {
	return len(method) > len(IntercomMethodPrefix) &&
		method[:len(IntercomMethodPrefix)] == IntercomMethodPrefix
}
