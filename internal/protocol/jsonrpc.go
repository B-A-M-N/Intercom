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

// Method names for the Intercom ACP extension namespace (ICSP-001 section 8.2).
//
// The namespace is provisional and reverse-DNS style; it must be replaced with
// a collision-resistant namespace controlled by the implementer before the
// extension is published as stable.
const (
	MethodHello            = "_intercom/hello"
	MethodPeerList         = "_intercom/peer/list"
	MethodPeerResolve      = "_intercom/peer/resolve"
	MethodPeerCapabilities = "_intercom/peer/capabilities"
	MethodPeerPresence     = "_intercom/peer/presence"
	MethodMessageSend      = "_intercom/message/send"
	MethodMessageStatus    = "_intercom/message/status"
	MethodMessageReply     = "_intercom/message/reply"
	MethodSessionState     = "_intercom/session/state"
	MethodSessionSnapshot  = "_intercom/session/snapshot"
	MethodSessionReplay    = "_intercom/session/replay"
	MethodSessionSubscribe = "_intercom/session/subscribe"
	MethodEvent            = "_intercom/event"

	// Methods below support the adapter contract in section 12. They let an
	// out-of-process adapter register its honest capability set, claim the
	// messages waiting in its own inbox, and report what it actually did.
	MethodSessionRegister    = "_intercom/session/register"
	MethodMessageListPending = "_intercom/message/listPending"
	MethodMessageReport      = "_intercom/message/report"
)

// IntercomMethodPrefix identifies methods belonging to the Intercom extension.
const IntercomMethodPrefix = "_intercom/"

// IsIntercomMethod reports whether method belongs to the Intercom extension.
func IsIntercomMethod(method string) bool {
	return len(method) > len(IntercomMethodPrefix) &&
		method[:len(IntercomMethodPrefix)] == IntercomMethodPrefix
}
