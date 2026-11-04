package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/bamn/intercom/internal/protocol"
)

// Handler answers a JSON-RPC request.
//
// The handler receives the authenticated peer identity so that a sender
// identity is never taken from the request payload.
type Handler func(ctx context.Context, peer PeerIdentity, method string, params json.RawMessage) (any, *protocol.RPCError)

// Server serves JSON-RPC over a transport.
type Server struct {
	handler Handler
	mu      sync.Mutex
	conns   map[Conn]struct{}
	closed  bool
}

// NewServer creates a JSON-RPC server.
func NewServer(h Handler) *Server {
	return &Server{handler: h, conns: make(map[Conn]struct{})}
}

// Serve handles one connection until it closes.
//
// Notifications receive no response. Requests receive exactly one response.
func (s *Server) Serve(ctx context.Context, conn Conn) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.conns[conn] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	peer := conn.PeerIdentity()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame, err := readFrame(ctx, conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(frame) == 0 {
			continue
		}
		s.dispatch(ctx, conn, peer, frame)
	}
}

// dispatch handles one frame.
func (s *Server) dispatch(ctx context.Context, conn Conn, peer PeerIdentity, frame []byte) {
	var req protocol.RPCRequest
	if err := json.Unmarshal(frame, &req); err != nil {
		writeError(conn, nil, protocol.Errorf(protocol.CodeParseError, "invalid JSON: %v", err))
		return
	}
	if req.JSONRPC != "" && req.JSONRPC != "2.0" {
		writeError(conn, req.ID, protocol.Errorf(protocol.CodeInvalidRequest, "unsupported jsonrpc version %q", req.JSONRPC))
		return
	}
	if req.Method == "" {
		writeError(conn, req.ID, protocol.Errorf(protocol.CodeInvalidRequest, "method is required"))
		return
	}

	isNotification := len(req.ID) == 0 || string(req.ID) == "null"

	result, rpcErr := s.handler(ctx, peer, req.Method, req.Params)
	if isNotification {
		return
	}
	if rpcErr != nil {
		writeError(conn, req.ID, rpcErr)
		return
	}
	writeResult(conn, req.ID, result)
}

// readFrame reads one frame from the connection.
func readFrame(ctx context.Context, conn Conn) ([]byte, error) {
	type framer interface {
		ReadFrame(context.Context) ([]byte, error)
	}
	if f, ok := conn.(framer); ok {
		return f.ReadFrame(ctx)
	}
	// Fall back to newline-delimited reading for connections that do not
	// implement framing.
	buf := make([]byte, 0, 4096)
	one := make([]byte, 1)
	for {
		n, err := conn.Read(one)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue
		}
		if one[0] == '\n' {
			return buf, nil
		}
		buf = append(buf, one[0])
		if len(buf) > DefaultMaxFrameBytes {
			return nil, fmt.Errorf("transport: frame exceeds %d bytes", DefaultMaxFrameBytes)
		}
	}
}

// writeResult sends a successful response.
func writeResult(conn Conn, id json.RawMessage, result any) {
	if result == nil {
		result = map[string]any{}
	}
	payload, err := json.Marshal(result)
	if err != nil {
		writeError(conn, id, protocol.Errorf(protocol.CodeInternalError, "marshal result: %v", err))
		return
	}
	resp := protocol.RPCResponse{JSONRPC: "2.0", ID: id, Result: payload}
	b, err := json.Marshal(resp)
	if err != nil {
		return
	}
	_, _ = conn.Write(append(b, '\n'))
}

// writeError sends an error response.
func writeError(conn Conn, id json.RawMessage, e *protocol.RPCError) {
	resp := protocol.RPCResponse{JSONRPC: "2.0", ID: id, Error: e}
	b, err := json.Marshal(resp)
	if err != nil {
		return
	}
	_, _ = conn.Write(append(b, '\n'))
}

// Close shuts the server and every connection it is serving.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	conns := make([]Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
	return nil
}
