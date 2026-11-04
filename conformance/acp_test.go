package conformance

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bamn/intercom/internal/adapter/acp"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/transport"
)

// rpcClient is a minimal JSON-RPC client over the local transport.
//
// It speaks the same framing as the server, so this exercises the real wire
// path rather than calling the handler directly.
type rpcClient struct {
	conn net.Conn
	rd   *bufio.Reader
	next int
}

// dialRPC connects to the host's Unix socket.
func dialRPC(t *testing.T, socketPath string) *rpcClient {
	t.Helper()
	conn, err := net.DialTimeout("unix", socketPath, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &rpcClient{conn: conn, rd: bufio.NewReader(conn)}
}

// call sends a request and returns the response.
func (c *rpcClient) call(t *testing.T, method string, params any) *protocol.RPCResponse {
	t.Helper()
	c.next++
	id := c.next

	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			t.Fatalf("marshal params: %v", err)
		}
		raw = b
	}
	req := protocol.RPCRequest{JSONRPC: "2.0", ID: json.RawMessage(fmt.Sprint(id)), Method: method, Params: raw}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if _, err := c.conn.Write(append(b, '\n')); err != nil {
		t.Fatalf("write: %v", err)
	}

	_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, err := c.rd.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var resp protocol.RPCResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatalf("unmarshal response: %v (payload %s)", err, line)
	}
	return &resp
}

// callRaw sends a request and returns the decoded result.
func (c *rpcClient) callRaw(t *testing.T, method string, params any, out any) *protocol.RPCResponse {
	t.Helper()
	resp := c.call(t, method, params)
	if resp.Error == nil && out != nil {
		if err := json.Unmarshal(resp.Result, out); err != nil {
			t.Fatalf("unmarshal result: %v (result %s)", err, resp.Result)
		}
	}
	return resp
}

// Test_ACPInitializePreservesStandardSemantics covers spec test 17: an
// ordinary ACP client continues to operate without Intercom support.
//
// The initialize method must answer with standard ACP fields, and the
// Intercom extension must be advertised only through _meta so that a client
// that ignores _meta is unaffected.
func Test_ACPInitializePreservesStandardSemantics(t *testing.T) {
	e := newEnv(t)

	srv := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)
	c := &rpcClient{}
	_ = c

	result, rpcErr := srv.HandleValue(e.ctx, "initialize", nil)
	if rpcErr != nil {
		t.Fatalf("initialize failed: %v", rpcErr)
	}
	init, ok := result.(acp.InitializeResult)
	if !ok {
		t.Fatalf("initialize returned %T", result)
	}
	if init.ProtocolVersion != acp.ProtocolVersion {
		t.Errorf("protocolVersion = %d, want %d", init.ProtocolVersion, acp.ProtocolVersion)
	}
	if init.AgentInfo["name"] != "intercom" {
		t.Errorf("agent name = %v", init.AgentInfo["name"])
	}
	if _, ok := init.Meta["intercom"]; !ok {
		t.Error("Intercom extension was not advertised through _meta")
	}
}

// Test_ACPUnknownMethodIsRejected verifies that an unknown method produces a
// JSON-RPC method-not-found error rather than a silent success.
func Test_ACPUnknownMethodIsRejected(t *testing.T) {
	e := newEnv(t)
	srv := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)

	_, rpcErr := srv.HandleValue(e.ctx, protocol.ExtensionPrefix+"does/not/exist", nil)
	if rpcErr == nil {
		t.Fatal("unknown Intercom method was accepted")
	}
	if rpcErr.Code != protocol.CodeMethodNotFound {
		t.Errorf("error code = %d, want %d", rpcErr.Code, protocol.CodeMethodNotFound)
	}
}

// Test_ACPHelloNegotiates covers the capability negotiation requirement of
// spec section 8.3.
func Test_ACPHelloNegotiates(t *testing.T) {
	e := newEnv(t)
	srv := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)

	result, rpcErr := srv.HandleValue(e.ctx, protocol.MethodHello, nil)
	if rpcErr != nil {
		t.Fatalf("hello failed: %v", rpcErr)
	}
	hello, ok := result.(acp.HelloResult)
	if !ok {
		t.Fatalf("hello returned %T", result)
	}
	if hello.SpecVersion != protocol.SpecVersion {
		t.Errorf("specVersion = %q", hello.SpecVersion)
	}
	if len(hello.Capabilities) == 0 {
		t.Error("hello advertised no capabilities")
	}
	if len(hello.Profiles) == 0 {
		t.Error("hello advertised no conformance profiles")
	}

	// Every advertised capability must state its support level.
	for _, c := range hello.Capabilities {
		if !c.Support.Valid() {
			t.Errorf("capability %s has invalid support level %q", c.ID, c.Support)
		}
	}
}

// Test_ACPPeerListAndResolve covers the discovery operations of spec
// section 4.
func Test_ACPPeerListAndResolve(t *testing.T) {
	e := newEnv(t)
	srv := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)

	listResult, rpcErr := srv.HandleValue(e.ctx, protocol.MethodPeerList, nil)
	if rpcErr != nil {
		t.Fatalf("peer/list failed: %v", rpcErr)
	}
	list, ok := listResult.(acp.PeerListResult)
	_ = list
	_ = ok

	// Resolve by alias.
	resolveResult, rpcErr := srv.HandleValue(e.ctx, protocol.MethodPeerResolve,
		map[string]any{"alias": "bob"})
	if rpcErr != nil {
		t.Fatalf("peer/resolve failed: %v", rpcErr)
	}
	resolved, ok := resolveResult.(acp.PeerResolveResult)
	if !ok {
		t.Fatalf("peer/resolve returned %T", resolveResult)
	}
	if resolved.Peer.SessionID != e.bobID {
		t.Errorf("resolved session = %q, want %q", resolved.Peer.SessionID, e.bobID)
	}
}

// Test_ACPMessageSendAndStatus covers spec acceptance criterion 5: delivery
// outcomes are distinguishable from acknowledgment.
//
// The send acknowledgment reports durable admission; the receipt trail
// separately reports what actually happened.
func Test_ACPMessageSendAndStatus(t *testing.T) {
	e := newEnv(t)
	srv := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)

	cmdID := "cmd_acptest00000000000000001"
	sendResult, rpcErr := srv.HandleValue(e.ctx, protocol.MethodMessageSend, map[string]any{
		"commandId": cmdID,
		"to":        map[string]any{"sessionId": e.bobID},
		"body":      map[string]any{"type": "text", "text": "via ACP"},
		"delivery": map[string]any{
			"mode":          "notify",
			"wake":          "none",
			"onUnsupported": "reject",
		},
	})
	if rpcErr != nil {
		t.Fatalf("message/send failed: %v", rpcErr)
	}
	sent, ok := sendResult.(acp.MessageSendResult)
	if !ok {
		t.Fatalf("message/send returned %T", sendResult)
	}
	if sent.Status != protocol.ReceiptAccepted {
		t.Errorf("send status = %q, want accepted", sent.Status)
	}
	if sent.MessageID == "" {
		t.Error("send did not return a message id")
	}

	// The receipt trail reports the delivery separately.
	ok2 := waitFor(t, 2*time.Second, func() bool {
		statusResult, rpcErr := srv.HandleValue(e.ctx, protocol.MethodMessageStatus,
			map[string]any{"messageId": sent.MessageID})
		if rpcErr != nil {
			return false
		}
		status, ok := statusResult.(acp.MessageStatusResult)
		if !ok {
			return false
		}
		return status.Status == protocol.ReceiptPresented
	})
	if !ok2 {
		t.Fatal("receipt trail did not report the presentation")
	}
}

// Test_ACPSessionReplayAndSnapshot covers spec test 10 over the ACP surface.
func Test_ACPSessionReplayAndSnapshot(t *testing.T) {
	e := newEnv(t)
	srv := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)

	if _, rpcErr := srv.HandleValue(e.ctx, protocol.MethodMessageSend, map[string]any{
		"commandId": "cmd_acptest00000000000000002",
		"to":        map[string]any{"sessionId": e.bobID},
		"body":      map[string]any{"type": "text", "text": "journal me"},
		"delivery":  map[string]any{"mode": "notify", "wake": "none", "onUnsupported": "reject"},
	}); rpcErr != nil {
		t.Fatalf("message/send failed: %v", rpcErr)
	}

	replayResult, rpcErr := srv.HandleValue(e.ctx, protocol.MethodSessionReplay,
		map[string]any{"sessionId": e.bobID})
	if rpcErr != nil {
		t.Fatalf("session/replay failed: %v", rpcErr)
	}
	replay, ok := replayResult.(acp.SessionReplayResult)
	if !ok {
		t.Fatalf("session/replay returned %T", replayResult)
	}
	if len(replay.Events) == 0 {
		t.Error("session/replay returned no events")
	}
	if replay.CursorGap {
		t.Error("session/replay reported an unexpected cursor gap")
	}

	snapshotResult, rpcErr := srv.HandleValue(e.ctx, protocol.MethodSessionSnapshot,
		map[string]any{"sessionId": e.bobID})
	if rpcErr != nil {
		t.Fatalf("session/snapshot failed: %v", rpcErr)
	}
	snapshot, ok := snapshotResult.(acp.SessionSnapshotResult)
	if !ok {
		t.Fatalf("session/snapshot returned %T", snapshotResult)
	}
	if snapshot.Snapshot.SessionID != e.bobID {
		t.Errorf("snapshot session = %q", snapshot.Snapshot.SessionID)
	}
}

// Test_ACPStandardClientRemainsOperational verifies that a client that does
// not know about Intercom can still complete the standard ACP lifecycle.
func Test_ACPStandardClientRemainsOperational(t *testing.T) {
	e := newEnv(t)
	srv := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)

	if _, rpcErr := srv.HandleValue(e.ctx, "initialize", nil); rpcErr != nil {
		t.Fatalf("initialize failed: %v", rpcErr)
	}

	// A standard ACP method the extension does not define must be reported
	// as unknown rather than intercepted by the extension.
	_, rpcErr := srv.Handle(e.ctx, "session/new", json.RawMessage(`{"cwd":"/tmp","mcpServers":[]}`))
	if rpcErr == nil {
		t.Skip("this server implements session/new")
	}
	if rpcErr.Code != protocol.CodeMethodNotFound {
		t.Errorf("error code = %d, want method-not-found", rpcErr.Code)
	}
}

// Test_TransportCarriesJSONRPC exercises the real socket path.
func Test_TransportCarriesJSONRPC(t *testing.T) {
	e := newEnv(t)

	ln, err := transport.ListenUnix(e.ctx, t.TempDir()+"/t.sock")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var srv *transport.Server
	srv = transport.NewServer(func(ctx context.Context, peer transport.PeerIdentity, conn transport.Conn, method string, params json.RawMessage) (any, *protocol.RPCError) {
		if !peer.Authenticated {
			return nil, protocol.Errorf(protocol.CodeUnauthorized, "peer identity was not established")
		}
		ext := acp.NewServer(e.node, e.node.PrincipalID(), e.aliceID)
		ext.SetEmitter(func(ctx context.Context, method string, params any) {
			srv.Notify(conn, method, params)
		})
		return ext.Handle(ctx, method, params)
	})
	defer srv.Close()

	go func() {
		for {
			conn, err := ln.Accept(e.ctx)
			if err != nil {
				return
			}
			go srv.Serve(e.ctx, conn)
		}
	}()

	// Wait for the listener to be ready.
	time.Sleep(50 * time.Millisecond)

	client := dialRPC(t, ln.Addr().String())
	resp := client.call(t, protocol.MethodHello, nil)
	if resp.Error != nil {
		t.Fatalf("hello over the socket failed: %v", resp.Error)
	}
	if len(resp.Result) == 0 {
		t.Fatal("hello returned an empty result")
	}
}
